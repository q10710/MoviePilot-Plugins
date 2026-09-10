"""订阅助手（增强版）——完整订阅生命周期管理入口。

插件入口负责配置解析、事件注册、定时任务和各业务域模块组装；具体业务规则由独立领域模块承载。
ResourceSelection 链式事件在这里接入候选准入、洗版串行和删除指纹过滤，保持入口只做编排。
"""
import datetime
import json
import random
import re
import threading
import time
from types import SimpleNamespace
from typing import Any, Dict, List, Tuple, Optional

from apscheduler.triggers.cron import CronTrigger
from pydantic import BaseModel

from app.plugins import _PluginBase
from app import schemas
from app.sdk.config import settings
from app.sdk.events import eventmanager
from app.sdk.logging import logger
from app.sdk.media import MediaInfo, MetaInfo
from app.sdk.services import DownloaderHelper
from app.schemas.types import EventType, ChainEventType, MediaType
from app.chain.storage import StorageChain
from app.chain.subscribe import SubscribeChain
from app.chain.tmdb import TmdbChain
from app.chain.torrents import TorrentsChain
from app.db.oper.downloadhistory import DownloadHistoryOper
from app.db.oper.subscribe import SubscribeOper
from app.db.oper.subscribehistory import SubscribeHistoryOper
from app.db.oper.transferhistory import TransferHistoryOper

from .engine.types import CompletionSignal, SeasonScope
from .engine.site import SiteEpisodesRefreshHandler, SiteEvidenceScanner, SiteEvidenceStore
from .engine.volatility import VolatilityTracker
from .engine.pipeline import CompletionEvidencePipeline
from .guard import CompletionGuard
from .lifecycle import SubscribeLifecycleCoordinator
from .pending.judge import PendingJudge
from .pending.refresh import PendingRefresh
from .pending.state import PendingStateCoordinator
from .pause.airing import AiringPauseChecker
from .pause.manager import PauseManager
from .pause.nodownload import NoDownloadPolicy
from .pause.probe import PausedProbeCoordinator
from .best_version.priority import PriorityManager
from .best_version.converter import BestVersionConverter
from .best_version.orchestrator import BestVersionOrchestrator
from .cleanup import SubscriptionCleanup
from .download.monitor import DownloadMonitor
from .download.cleanup import RELOCATE_RELOCATED, RELOCATE_RETAINED, RELOCATE_SKIP, TorrentCleanup
from .recognition import RecognitionGuard, RecognitionRuntime, RecognitionSettings
from .recognition.audit import redact_sensitive_text
from .shared.deletes import DeletesStore
from .shared.subscribe import (
    build_subscribe_meta,
    format_subscribe_label,
    is_full_best_version_subscribe,
    is_tv_episode_best_version_subscribe,
    resolve_subscribe_media_type,
    subscribe_media_identity,
)
from .postcheck.verifier import CompletionVerifier
from .postcheck.rebuilder import CompletionSubscribeRebuilder
from .postcheck.timeout import PendingTimeoutManager
from .events import EventProxy
from .shared.media import parse_date
from .shared.task import TaskDataManager
from .shared.config import (
    DEFAULT_DELETE_EXCLUDE_TAGS,
    DEFAULT_RECOGNITION_GUARD_CUSTOM_CONFIG,
    DEFAULT_TRACKER_RESPONSE,
    PluginConfig,
)
from .shared.log import detail, truncate_log_value
from .shared.subscribe import format_subscribe


class SummaryPayload(BaseModel):
    """订阅助手概览接口的业务数据模型。"""

    domains: Dict[str, Any]
    pending_count: int
    monitored_torrents: int


class SubscribeAssistantEnhancedQ(_PluginBase):
    """订阅助手 Q 改版——插件入口。

    生命周期：init_plugin → 事件注册 → 定时任务 → stop_service。
    配置界面由 vuetify JSON 表单渲染（不依赖前端构建产物）；
    运行概况由日志和只读 summary API 提供。
    继承 _PluginBase 以获得真实数据层（get_data/save_data）、事件管理器与消息能力。
    在原版基础上新增「订阅超时收容」：下载超时仍未完成的订阅种子不再直接删除，
    而是移入独立收容目录保留做种，达到站点 H&R 时长后再自动清理。
    """

    # 插件名称
    plugin_name = "订阅助手Q改版"
    # 插件描述
    plugin_desc = ("多场景管理订阅，实现订阅全生命周期管理；订阅下载超过收容门槛仍未完成的 H&R 种子"
                   "按绝对时长移入收容目录保留做种，按站点 H&R 时长到期后再清理，避免直接删种造成 H&R 违约。")
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/q10710/MoviePilot-Plugins/main/icons/subscribeassistantenhancedq.png"
    # 插件版本
    plugin_version = "0.9.7"
    _site_cache_candidate_helper_warned = False
    # 插件作者
    plugin_author = "Q"
    # 作者主页
    author_url = "https://github.com/q10710"
    # 插件配置项ID前缀
    plugin_config_prefix = "subscribeassistantenhancedq_"
    # 加载顺序
    plugin_order = 5
    # 可使用的用户级别
    auth_level = 1

    @property
    def name(self) -> str:
        """错误处理读取的插件展示名。"""
        return self.plugin_name

    def __init__(self):
        """初始化插件运行期依赖与一次性任务状态。"""
        super().__init__()
        self._config: Optional[PluginConfig] = None
        self._task_manager: Optional[TaskDataManager] = None
        self._event_proxy: Optional[EventProxy] = None
        self._paused_probe_coordinator: Optional[PausedProbeCoordinator] = None
        self._modules: dict = {}
        self._onlyonce = False
        # DB oper / chain 在 init_plugin 实例化后注入各业务域模块。
        self._subscribe_oper: Optional[SubscribeOper] = None
        self._subscribe_history_oper: Optional[SubscribeHistoryOper] = None
        self._subscribe_chain: Optional[SubscribeChain] = None
        self._subscribe_writer = None
        self._tmdb_chain: Optional[TmdbChain] = None
        self._storage_chain: Optional[StorageChain] = None
        self._transferhistory_oper: Optional[TransferHistoryOper] = None
        self._downloadhistory_oper: Optional[DownloadHistoryOper] = None
        self._downloader_helper: Optional[DownloaderHelper] = None
        # 收容记录（relocate_records）会被「下载任务检查」和「通用巡检」两条服务链路写入，
        # 用非重入锁包住读-改-写临界区，避免并发覆盖导致收容记录丢失。
        self._relocate_lock = threading.Lock()

    def init_plugin(self, config: dict = None):
        """解析配置 → 注入 DB/chain 依赖 → 初始化各业务域模块。"""
        self.stop_service()

        raw_config, should_persist = self._normalize_persisted_config(config or {})
        self._config = PluginConfig(raw_config)

        # 依赖注入：构造即可用且不触发外部网络，供洗版、下载、补搜等业务域写库与查询。
        self._subscribe_oper = SubscribeOper()
        self._subscribe_history_oper = SubscribeHistoryOper()
        self._subscribe_chain = SubscribeChain()
        # SubscribeChain 持有宿主组合根装配的短事务 repository，自动新增洗版订阅复用该事务 owner。
        self._subscribe_writer = self._subscribe_chain.subscription_repository
        self._tmdb_chain = TmdbChain()
        self._storage_chain = StorageChain()
        self._transferhistory_oper = TransferHistoryOper()
        self._downloadhistory_oper = DownloadHistoryOper()
        self._downloader_helper = DownloaderHelper()

        # 任务数据统一走 _PluginBase 的 get_data/save_data 持久化接口。
        self._task_manager = TaskDataManager(
            get_data_fn=self.get_data,
            save_data_fn=self.save_data,
        )

        self._init_modules()

        self._onlyonce = self._config.onlyonce
        if self._config.reset_task:
            self._reset_task_data()
        if self._config.backfill_best_version_now:
            self._run_backfill_now()
        if self._config.onlyonce or self._config.reset_task or self._config.backfill_best_version_now:
            raw_config["onlyonce"] = False
            raw_config["reset_task"] = False
            raw_config["backfill_best_version_now"] = False
            should_persist = True
        if should_persist:
            self.update_config(raw_config)
            self._config = PluginConfig(raw_config)

        # 启动摘要：一眼看清各业务域开关，排查"某能力为何不生效"先看这条
        cfg = self._config
        recognition_mode = cfg.recognition_guard_mode
        recognition_notify = cfg.recognition_guard_notify
        recognition_interval = cfg.recognition_guard_notify_interval
        recognition_recheck = cfg.recognition_guard_tmdb_recheck_mode
        recognition_cache_size = cfg.recognition_guard_cache_maxsize
        recognition_warnings = ",".join(sorted(cfg.recognition_guard_config_warnings)) or "none"
        logger.info(
            "初始化完成："
            f"总开关={cfg.enabled} 完成守卫模式={cfg.completion_guard_mode} "
            f"待定增强={cfg.pending_enhanced_enabled} 暂停优化={cfg.pause_enhanced_enabled} "
            f"洗版类型={cfg.best_version_type} 下载管理={cfg.download_monitor_enabled} "
            f"完成验证={cfg.verify_enabled} 识别增强={recognition_mode} "
            f"站点集数探测={cfg.site_total_probe_enabled} "
            f"站点完结信号={cfg.site_completion_evidence_enabled} "
            f"识别增强通知={recognition_notify} 二次识别={recognition_recheck} "
            f"识别增强通知限频={recognition_interval} 识别增强缓存={recognition_cache_size} "
            f"识别增强告警={recognition_warnings} 通知={cfg.notify}"
        )

    @staticmethod
    def _normalize_persisted_config(config: dict) -> Tuple[dict, bool]:
        """规范化需要持久安全默认值的配置，避免旧空值覆盖表单默认 model。"""
        raw = dict(config or {})
        changed = False
        retired_config_keys = {
            "recognition_guard_enabled",
            "recognition_guard_active",
            "recognition_guard_keyword_config",
            "recognition_guard_target_mode",
            "recognition_guard_missing_year_policy",
            "open_tracker_dialog",
            "progress_diagnostic_mode",
            "progress_diagnostic_stalled_rounds",
            "progress_diagnostic_cooldown_hours",
        }
        for key in retired_config_keys:
            if key in raw:
                raw.pop(key, None)
                changed = True
        default_text_fields = {
            "delete_exclude_tags": DEFAULT_DELETE_EXCLUDE_TAGS,
            "default_tracker_response": DEFAULT_TRACKER_RESPONSE,
        }
        for key, default in default_text_fields.items():
            if key in raw and not str(raw.get(key) or "").strip():
                raw[key] = default
                changed = True
        recognition_defaults = {
            "recognition_guard_mode": "off",
            "recognition_guard_notify": "off",
            "recognition_guard_notify_interval": 3600,
            "recognition_guard_tmdb_recheck_mode": "balanced_strict",
            "recognition_guard_cache_maxsize": 100000,
            "recognition_guard_custom_config": DEFAULT_RECOGNITION_GUARD_CUSTOM_CONFIG,
        }
        for key, default in recognition_defaults.items():
            if key not in raw:
                raw[key] = default
                changed = True
        return raw, changed

    def _init_modules(self):
        """初始化各域模块并注入运行期依赖。"""
        cfg = self._config
        tm = self._task_manager

        volatility = VolatilityTracker(tm, window_days=cfg.volatility_window_days)
        timeout_manager = PendingTimeoutManager(
            tm.read, tm.update,
            timeout_days=cfg.timeout_release_days,
            cadence_acceleration=cfg.timeout_cadence_acceleration,
            subscribe_get_fn=self._subscribe_oper.get,
        )
        completion_rebuilder = CompletionSubscribeRebuilder(
            subscribe_chain=self._subscribe_chain,
            subscribe_oper=self._subscribe_oper,
            default_config_getter=self.systemconfig.get,
            plugin_name=self.plugin_name,
        )
        verifier = CompletionVerifier(
            tm.read, tm.update,
            tmdb_episodes_fn=self._tmdb_episodes,
            subscribe_oper=self._subscribe_oper,
            retention_days=cfg.verify_retention_days,
            notify_fn=self._notify_subscribe,
            rebuild_subscribe_fn=completion_rebuilder.rebuild,
            validate_rebuild_subscribe_fn=completion_rebuilder.validate,
            get_subscribe_image_fn=self._get_subscribe_image,
        )
        priority_manager = PriorityManager(
            tm.read,
            tm.update,
            subscribe_oper=self._subscribe_oper,
            plugin_name=self.plugin_name,
        )
        converter = BestVersionConverter(
            subscribe_oper=self._subscribe_oper,
            subscribe_history_oper=self._subscribe_history_oper,
            clear_tasks_fn=self._task_manager.clear_tasks,
            notify_fn=self._notify_subscribe,
            snapshot_fn=verifier.snapshot,
            format_desc_fn=lambda subscribe, mediainfo: self._format_subscribe_desc(subscribe, mediainfo),
            notification_image_fn=self._resolve_notification_image,
            plugin_name=self.plugin_name,
            subscription_mutation_scope=self._subscribe_chain.sync_subscription_mutation_scope,
        )
        pending_refresh = PendingRefresh()
        pending_state = PendingStateCoordinator(
            tm.read,
            tm.update,
            subscribe_oper=self._subscribe_oper,
        )
        # 用户名自动暂停名单：逗号分隔字符串解析为列表，剔除空白与空项；空名单即不启用该能力
        auto_pause_users = [u.strip() for u in (cfg.auto_pause_users or "").split(",") if u.strip()]
        # 注入 subscribe_oper：pause()/resume() 据此真实写订阅 DB state（S/R），否则只写插件任务数据
        pause_manager = PauseManager(
            tm.read,
            tm.update,
            subscribe_oper=self._subscribe_oper,
            auto_pause_users=auto_pause_users,
            notify_fn=self._send_subscribe_status_notification,
            pending_state=pending_state,
            pause_enhanced_enabled=cfg.pause_enhanced_enabled,
        )
        no_download_policy = NoDownloadPolicy(
            movie_days=cfg.movie_no_download_days,
            tv_days=cfg.tv_no_download_days,
            actions=cfg.no_download_actions,
        )
        tracker_keywords = [k.strip() for k in (cfg.default_tracker_response or "").splitlines() if k.strip()]
        if not cfg.tracker_response_listen:
            tracker_keywords = []
        # 超时处理模式二选一：收容模式下排除标签不生效（否则 H&R 种子会被整段跳过，无法收容）
        exclude_tags = []
        if cfg.hr_mode != "relocate":
            exclude_tags = [t.strip() for t in (cfg.delete_exclude_tags or "").replace("&", ",").split(",")
                            if t.strip()]
        download_monitor = DownloadMonitor(
            tm.read, tm.update,
            timeout_minutes=cfg.download_timeout_minutes,
            progress_threshold=cfg.download_progress_threshold,
            queue_grace_multiplier=cfg.download_queue_grace_multiplier,
            retry_limit=cfg.download_retry_limit,
            tracker_keywords=tracker_keywords,
            exclude_tags=exclude_tags,
            subscribe_oper=self._subscribe_oper,
            state_coordinator=None,
            fetch_fn=self._fetch_downloader_torrent,
            present_fn=self._downloader_torrent_present,
            manual_delete_enabled=cfg.download_monitor_enabled and cfg.manual_delete_listen,
            pending_download_enabled=cfg.pending_download_enabled,
        )

        deletes_store = DeletesStore(tm.read, tm.update)
        torrent_cleanup = TorrentCleanup(
            priority_manager=priority_manager,
            clear_download_pending_fn=download_monitor.clear_download_pending,
            task_data_update=tm.update,
            task_data_read=tm.read,
            deletes_store=deletes_store,
            delete_torrent_fn=self._delete_downloader_torrent,
            relocate_torrent_fn=self._relocate_downloader_torrent,
            search_fn=self._search_subscribe if cfg.auto_search_when_delete else None,
            notify_fn=self._notify_subscribe,
            get_subscribe_image_fn=self._get_subscribe_image,
            subscribe_oper=self._subscribe_oper,
        )

        site_store = SiteEvidenceStore(tm)
        if not cfg.site_total_probe_enabled:
            site_store.clear_all_leases()
        completion_pipeline = CompletionEvidencePipeline(
            tmdb_episodes_fn=self._tmdb_episodes,
            volatility_tracker=volatility,
            config=cfg,
            site_evidence_provider=site_store.read_snapshot,
        )
        site_evidence = SiteEvidenceScanner(
            config=cfg,
            store=site_store,
            candidate_provider=self._site_cache_candidates,
        )
        site_refresh = SiteEpisodesRefreshHandler(
            config=cfg,
            store=site_store,
            subscribe_oper=self._subscribe_oper,
            resolve_missing_fn=self._resolve_subscribe_missing,
            mediainfo_from_dict=self._mediainfo_from_dict,
        )

        airing_checker = AiringPauseChecker(
            pause_days=cfg.airing_pause_days,
            evidence_pipeline=completion_pipeline,
            movie_air_days=cfg.movie_air_pause_days,
            tv_air_days=cfg.tv_air_pause_days,
        )

        pending_judge = PendingJudge(
            config=cfg,
            evidence_pipeline=completion_pipeline,
            subscribe_oper=self._subscribe_oper,
            timeout_manager=timeout_manager,
            task_data_read=tm.read,
            task_data_update=tm.update,
            resolve_missing_fn=self._resolve_subscribe_missing,
            notify_fn=self._send_subscribe_status_notification,
            state_coordinator=pending_state,
        )

        lifecycle = SubscribeLifecycleCoordinator(
            config=cfg,
            subscribe_oper=self._subscribe_oper,
            pause_manager=pause_manager,
            pending_judge=pending_judge,
            pending_state=pending_state,
            airing_checker=airing_checker if cfg.pause_enhanced_enabled else None,
            tmdb_episodes_fn=lambda *args, **kwargs: self._tmdb_episodes(*args, **kwargs),
            recognize_mediainfo_fn=lambda subscribe: self._recognize_mediainfo(subscribe),
            is_tv_fn=lambda mediainfo: self._is_tv_media(mediainfo),
            schedule_initial_pending_search_fn=lambda subscribe: self._schedule_initial_pending_search(subscribe),
            has_active_downloads_fn=lambda sid: bool(
                download_monitor and download_monitor.has_active_downloads(sid)
            ),
            clear_orphan_completion_observation_fn=self._clear_orphan_completion_observation,
            clear_tasks_for_pause_fn=lambda subscribe_id: self._task_manager.clear_tasks_for_pause(
                subscribe_id,
                preserve_subscribe_keys=[
                    "pause_reason",
                    "pause_since",
                    "pause_detail",
                    "paused_probe_resume_guard_reason",
                    "paused_probe_resume_guard_until",
                ],
            ),
        )
        download_monitor.set_state_coordinator(lifecycle.download_pending_adapter())

        guard = CompletionGuard(
            evidence_pipeline=completion_pipeline,
            has_active_downloads_fn=lambda sub: download_monitor.has_active_downloads(
                sub.id),
            mark_pending_fn=lambda subscribe, source="guard_veto", reason="": lifecycle.enter_guard_pending(
                subscribe,
                reason=reason,
            ),
            timeout_manager=timeout_manager,
            mode=cfg.completion_guard_mode,
            pending_download_enabled=cfg.pending_download_enabled,
            resolve_missing_fn=self._resolve_subscribe_missing,
        )
        recognition_guard = RecognitionGuard(
            settings=RecognitionSettings(
                mode=cfg.recognition_guard_mode,
                notify_mode=cfg.recognition_guard_notify,
                notify_interval=cfg.recognition_guard_notify_interval,
                tmdb_recheck_mode=cfg.recognition_guard_tmdb_recheck_mode,
                cache_maxsize=cfg.recognition_guard_cache_maxsize,
                custom_config=cfg.recognition_guard_custom_config,
            ),
            runtime=RecognitionRuntime(
                target_mediainfo_resolver=self._recognize_mediainfo,
                tmdb_episodes_fn=self._tmdb_episodes,
                secondary_recognizer=self._recognize_by_meta_for_recognition,
                logger_fn=detail,
            ),
        )

        orchestrator = BestVersionOrchestrator(
            priority_manager=priority_manager,
            notify_fn=self._notify_subscribe,
            related_downloads_fn=self._related_download_histories,
            best_version_type=cfg.best_version_type,
            notification_image_fn=self._resolve_notification_image,
            plugin_name=self.plugin_name,
            subscribe_writer=self._subscribe_writer,
        )
        subscription_cleanup = SubscriptionCleanup(
            task_data_read=tm.read,
            task_data_update=tm.update,
            get_histories_fn=self._get_transfer_histories,
            delete_media_file_fn=self._delete_media_file,
            delete_history_fn=self._transferhistory_oper.delete,
            send_download_file_deleted_fn=self._send_download_file_deleted,
            notify_fn=self._notify_subscribe,
            get_subscribe_image_fn=self._get_subscribe_image,
            torrent_exists_fn=self._torrent_exists,
            cleanup_history_type=cfg.subscription_cleanup_history_type,
            cleanup_history_scenes=cfg.subscription_cleanup_history_scenes,
        )
        migrated_cleanup_snapshots = subscription_cleanup.migrate_snapshot_identities()
        if migrated_cleanup_snapshots:
            logger.info(f"订阅清理：已迁移 {migrated_cleanup_snapshots} 条 V2 清理快照的媒体身份")
        paused_probe = PausedProbeCoordinator(
            cfg,
            tm.read,
            tm.update,
            subscribe_oper=self._subscribe_oper,
            subscribe_chain=self._subscribe_chain,
            pause_manager=pause_manager,
            download_monitor=download_monitor,
        )
        self._paused_probe_coordinator = paused_probe

        self._event_proxy = EventProxy(
            task_manager=tm,
            subscribe_oper=self._subscribe_oper,
            post_message=self.post_message,
            notify_fn=self._notify_subscribe,
            notification_image_fn=self._resolve_notification_image,
            plugin_name=self.plugin_name,
            deletes_store=deletes_store if cfg.download_monitor_enabled else None,
            skip_deletion=cfg.skip_deletion,
            backfill_enabled=cfg.best_version_backfill_enabled,
            pending_download_enabled=cfg.pending_download_enabled,
            download_monitor_enabled=cfg.download_monitor_enabled,
            guard=guard if cfg.completion_guard_mode != "off" else None,
            recognition_guard=recognition_guard if cfg.recognition_guard_mode != "off" else None,
            volatility=volatility if cfg.volatility_enabled else None,
            site_refresh=site_refresh,
            pending_refresh=pending_refresh if cfg.pending_enhanced_enabled else None,
            pause_manager=pause_manager if cfg.pause_enhanced_enabled else None,
            airing_checker=airing_checker if cfg.pause_enhanced_enabled else None,
            pending_judge=pending_judge if cfg.pending_enhanced_enabled else None,
            pending_state=pending_state,
            lifecycle=lifecycle,
            tmdb_episodes_fn=self._tmdb_episodes,
            mediainfo_from_dict=self._mediainfo_from_dict,
            is_tv_fn=self._is_tv_media,
            detect_existing_episodes_fn=self._detect_existing_episodes,
            detect_backfill_episodes_fn=self._detect_backfill_episodes,
            detect_missing_episodes_fn=self._detect_missing_episodes,
            schedule_initial_pending_search_fn=self._schedule_initial_pending_search,
            resolve_missing_fn=self._resolve_subscribe_missing,
            recognize_mediainfo_fn=self._recognize_mediainfo,
            priority_manager=priority_manager,
            download_monitor=download_monitor,
            verifier=verifier,
            orchestrator=orchestrator,
            subscription_cleanup=subscription_cleanup,
            converter=converter,
            best_version_episode_to_full=cfg.best_version_episode_to_full,
            convert_episode_best_version_to_full_fn=self._convert_episode_best_version_to_full_if_ready,
        )

        self._modules = {
            "volatility": volatility,
            "timeout_manager": timeout_manager,
            "completion_rebuilder": completion_rebuilder,
            "verifier": verifier,
            "priority_manager": priority_manager,
            "converter": converter,
            "pending_judge": pending_judge,
            "pending_state": pending_state,
            "lifecycle": lifecycle,
            "pending_refresh": pending_refresh,
            "pause_manager": pause_manager,
            # airing_checker 同时放入 _modules，供 run_meta_check 周期巡检按 enabled 门控读取
            "airing_checker": airing_checker if cfg.pause_enhanced_enabled else None,
            "no_download_policy": no_download_policy,
            "download_monitor": download_monitor,
            "paused_probe": paused_probe,
            "torrent_cleanup": torrent_cleanup,
            "deletes_store": deletes_store,
            "guard": guard,
            "recognition_guard": recognition_guard,
            "orchestrator": orchestrator,
            "subscription_cleanup": subscription_cleanup,
            "completion_pipeline": completion_pipeline,
            "site_evidence_store": site_store,
            "site_evidence": site_evidence,
            "site_refresh": site_refresh,
        }

    def stop_service(self):
        """清理定时任务和事件监听。"""
        if self._paused_probe_coordinator:
            self._paused_probe_coordinator.stop()
            self._paused_probe_coordinator = None
        self._event_proxy = None
        self._modules = {}

    @staticmethod
    def _format_service_registration(service: Dict[str, Any], schedules: Dict[str, str]) -> str:
        """生成定时任务注册摘要；周期信息由注册入口按配置显式传入，避免从触发器反推。"""
        schedule = schedules.get(service["id"])
        if schedule:
            return f"{service['name']}={schedule}"
        return service["name"]

    def get_service(self) -> List[Dict[str, Any]]:
        """按域开关注册定时任务，并按元数据周期复查待定订阅。

        插件总开关关闭时不注册任何任务。
        每个 job 的 func 指向插件类薄方法，委托对应域模块执行；模块周期方法未就绪时安全跳过。
        周期 job 多用 interval 触发器；洗版订阅检查用 cron 触发器（CronTrigger）；一次性全量巡检用 date 触发器延迟执行。
        """
        if not self._config:
            return []
        if not self._config.enabled:
            return []
        cfg = self._config
        name = self.__class__.__name__
        services: List[Dict[str, Any]] = []
        service_schedules: Dict[str, str] = {}
        if self._onlyonce:
            service_id = f"{name}_onlyonce"
            services.append({
                "id": service_id,
                "name": "立即运行一次",
                "trigger": "date",
                "run_date": datetime.datetime.now() + datetime.timedelta(seconds=3),
                "func": self.run_all_checks,
                "kwargs": {},
            })
            service_schedules[service_id] = "约3s后"
        service_id = f"{name}_meta_check"
        services.append({
            "id": service_id,
            "name": "元数据检查",
            "trigger": "interval",
            "func": self.run_meta_check,
            "kwargs": {"hours": cfg.meta_check_interval_hours},
        })
        service_schedules[service_id] = f"{cfg.meta_check_interval_hours}h"
        if cfg.hr_auto_scan:
            service_id = f"{name}_hr_scan"
            services.append({
                "id": service_id,
                "name": "H&R时长自动刷新",
                "trigger": CronTrigger.from_crontab("30 5 * * *"),
                "func": self.run_hr_hours_scan,
            })
            service_schedules[service_id] = "cron(30 5 * * *)"
        if cfg.pending_download_enabled or cfg.download_monitor_enabled:
            service_id = f"{name}_download"
            services.append({
                "id": service_id,
                "name": "下载任务检查",
                "trigger": "interval",
                "func": self.run_download_timeout_check,
                "kwargs": {"minutes": cfg.download_check_interval_minutes},
            })
            service_schedules[service_id] = f"{cfg.download_check_interval_minutes}m"
        if cfg.best_version_type != "no" and cfg.best_version_cron:
            # 洗版按 cron 调度，区别于其余域的 interval 周期；cron 为空则不注册该任务
            service_id = f"{name}_best_version"
            services.append({
                "id": service_id,
                "name": "洗版订阅检查",
                "trigger": CronTrigger.from_crontab(cfg.best_version_cron),
                "func": self.run_best_version_check,
            })
            service_schedules[service_id] = f"cron({cfg.best_version_cron})"
        if cfg.verify_enabled:
            service_id = f"{name}_verify"
            services.append({
                "id": service_id,
                "name": "自动纠错",
                "trigger": "interval",
                "func": self.run_completion_verify,
                "kwargs": {"hours": cfg.verify_interval_hours},
            })
            service_schedules[service_id] = f"{cfg.verify_interval_hours}h"
        service_id = f"{name}_common_check"
        services.append({
            "id": service_id,
            "name": "通用巡检",
            "trigger": "interval",
            "func": self.run_common_check,
            "kwargs": {"minutes": cfg.auto_check_interval_minutes},
        })
        service_schedules[service_id] = f"{cfg.auto_check_interval_minutes}m"
        detail("注册定时任务：" + "、".join(
            self._format_service_registration(service, service_schedules) for service in services
        ))
        return services

    def run_all_checks(self):
        """一次性执行所有周期检查；各检查会按功能开关自行跳过。"""
        logger.info("立即运行一次：开始全量巡检")
        self.run_meta_check()
        self.run_download_timeout_check()
        self.run_best_version_check()
        if self._config.verify_enabled:
            self.run_completion_verify()
        self.run_common_check()

    def _reset_task_data(self):
        """先恢复增强版持有的订阅状态，再清空全部插件任务数据。"""
        if self._paused_probe_coordinator:
            self._paused_probe_coordinator.stop()
        lifecycle = self._modules.get("lifecycle")
        result = lifecycle.restore_owned_states_before_reset() if lifecycle else None
        if result and result.changed:
            summary = result.message or result.reason
            logger.info(f"重置任务：数据清空前已恢复订阅状态；{summary}")
            self._notify_subscribe("订阅助手数据重置前已恢复订阅状态", text=summary)
        else:
            logger.info("重置任务：数据清空前未发现需要恢复的订阅状态")
        relocate_count = len(self.get_data("relocate_records") or {})
        for key in [
            "subscribes",
            "torrents",
            "blocks",
            "releases",
            "snapshots",
            "deletes",
            "volatility",
            "site_evidence",
            "subscription_cleanup_histories",
            "relocate_records",
            "relocate_retain_notify",
        ]:
            self.save_data(key, {})
        logger.info("重置任务：已清空全部插件任务数据（订阅、下载任务、完成前观察记录、放行令牌、完成快照、删除指纹、集数变化记录、站点证据、订阅清理记录、收容记录）")
        if relocate_count:
            logger.warning(f"重置任务：同时清空了 {relocate_count} 条收容记录，"
                           f"收容目录中的种子不再自动到期删除，需要时请手动处理")

    def _run_backfill_now(self):
        """对现有分集洗版订阅执行一次下载事实回填，并推送扫描结果汇总。"""
        results = {"scanned": 0, "updated": 0, "skipped": 0, "filled_episodes": 0}
        priority = self._modules["priority_manager"]
        for subscribe in (self._subscribe_oper.list(state="N,R,P") or []):
            if (
                not subscribe
                or resolve_subscribe_media_type(subscribe) not in (MediaType.MOVIE, MediaType.TV)
                or not subscribe.best_version
            ):
                continue
            results["scanned"] += 1
            if not priority.can_backfill(subscribe):
                results["skipped"] += 1
                continue
            existing = self._detect_backfill_episodes(subscribe)
            filled_episodes = [
                episode for episode in existing
                if str(episode) not in (subscribe.episode_priority or {})
            ]
            scene = f"plugin_backfill<{self.plugin_name}>"
            if existing and priority.backfill_existing(subscribe, existing, scene=scene):
                results["updated"] += 1
                results["filled_episodes"] += len(filled_episodes)
                detail(f"洗版回填：{format_subscribe(subscribe)} 回填已下载集 {filled_episodes}")
            else:
                results["skipped"] += 1
        logger.info(
            f"洗版回填：完成，扫描 {results['scanned']} 个，写入 {results['updated']} 个，"
            f"跳过 {results['skipped']} 个，累计补写 {results['filled_episodes']} 集"
        )
        self._notify_subscribe(
            "洗版下载事实回填完成",
            action=(
                f"扫描 {results['scanned']} 个订阅，成功回填 {results['updated']} 个，"
                f"跳过 {results['skipped']} 个，累计补写 {results['filled_episodes']} 集"
            ),
        )

    def run_download_timeout_check(self):
        """下载任务检查：读取下载器状态，处理超时无进度、Tracker 删除关键字和手动删种。"""
        monitor = self._modules.get("download_monitor")
        cleanup = self._modules.get("torrent_cleanup") if (
            self._config and self._config.download_monitor_enabled
        ) else None
        if monitor:
            detail("下载任务检查：开始")
            monitor.run_timeout_check(cleanup)
        # Q 版改造：H&R 种子的绝对时长收容（下载满「收容门槛」仍未完成即收容，不受进度阈值影响）
        try:
            self._relocate_overdue(cleanup)
        except Exception as err:
            logger.error(f"订阅收容：超时收容检查异常 {err}", exc_info=True)
        # Q 版改造：同时检查收容种子的到期情况，到期后删除任务
        try:
            self._relocate_expired()
        except Exception as err:
            logger.error(f"订阅收容：收容到期检查异常 {err}")

    def _ensure_best_version_anchor(self, sid, now) -> float:
        """读取洗版首次观察锚点；缺失时以当前时间写入订阅任务数据。"""
        subscribes = self._task_manager.read("subscribes") or {}
        anchor = (subscribes.get(str(sid)) or {}).get("best_version_anchor")
        if anchor:
            return anchor

        def set_anchor(data):
            """在保留订阅既有任务字段的前提下写入首次观察锚点。"""
            data = dict(data or {})
            record = dict(data.get(str(sid)) or {})
            record["best_version_anchor"] = now
            data[str(sid)] = record
            return data

        self._task_manager.update("subscribes", set_anchor)
        return now

    def _best_version_timeout_days(self, subscribe) -> int:
        """按媒体类型读取洗版时限。"""
        if resolve_subscribe_media_type(subscribe) == MediaType.MOVIE:
            return self._config.best_version_movie_remaining_days
        return self._config.best_version_tv_remaining_days

    def _best_version_overdue(self, subscribe, now=None) -> bool:
        """洗版是否超时限：从最近活动时间起算超过对应媒体类型洗版时限。

        活动时间取该订阅在 torrents 任务数据中的最新记录时间；
        无下载记录则按首次观察锚点（缺失则置当前时间）。
        remaining_days=0 表示不限，永不超时。
        """
        days = self._best_version_timeout_days(subscribe)
        if not days:
            return False
        now = now or time.time()
        sid = subscribe.id
        torrents = self._task_manager.read("torrents") or {}
        times = [
            torrent.get("time", 0)
            for torrent in torrents.values()
            if torrent.get("subscribe_id") == sid
        ]
        anchor = self._ensure_best_version_anchor(sid, now)
        last = max(times + [anchor]) if times else anchor
        return (now - last) > days * 86400

    def run_best_version_check(self):
        """洗版巡检：处理洗版超时终止，并兜底推进分集洗版转全集。"""
        if self._config and self._config.best_version_type == "no":
            return
        priority = self._modules.get("priority_manager")
        converter = self._modules.get("converter")
        if not priority or not self._subscribe_oper:
            return
        detail("洗版巡检：开始")
        for subscribe in (self._subscribe_oper.list(state="N,R,P") or []):
            if (
                resolve_subscribe_media_type(subscribe) not in (MediaType.MOVIE, MediaType.TV)
                or not subscribe.best_version
            ):
                continue
            mode_label = self._best_version_mode_label(subscribe)
            mediainfo = self._recognize_mediainfo(subscribe)
            if mediainfo:
                if is_full_best_version_subscribe(subscribe) and self._best_version_overdue(subscribe):
                    logger.info(f"洗版巡检：{format_subscribe(subscribe)} {mode_label}超过洗版时限，标记洗版完成并停止洗版")
                    priority.mark_full_best_version_complete(subscribe)
                    self._notify_subscribe(
                        f"{format_subscribe(subscribe)} {mode_label}超过时限"
                        f"（{self._best_version_timeout_days(subscribe)}天），已标记洗版优先级为完成",
                        image=self._resolve_notification_image(subscribe, mediainfo),
                    )
                    continue
                if (
                    self._config.best_version_episode_to_full
                    and converter
                    and is_tv_episode_best_version_subscribe(subscribe)
                ):
                    self._convert_episode_best_version_to_full_if_ready(
                        subscribe.id,
                        subscribe,
                        mediainfo,
                        trigger="洗版巡检",
                    )
                    continue
            else:
                detail(
                    f"洗版巡检：{format_subscribe(subscribe)} {mode_label}媒体识别失败，本轮跳过；"
                    f"订阅ID：{subscribe.id}，媒体身份："
                    f"{subscribe.media_source or '未设置'}:{subscribe.media_id or '未设置'}，"
                    f"媒体类型：{subscribe.type or '未设置'}，季号：{subscribe.season if subscribe.season is not None else '未设置'}；"
                    f"建议检查订阅名称、年份、媒体来源和媒体 ID、媒体类型和季号"
                )

    @staticmethod
    def _best_version_mode_label(subscribe) -> str:
        """按订阅实际洗版形态返回日志和通知标签。"""
        if is_full_best_version_subscribe(subscribe):
            return "洗版"
        if is_tv_episode_best_version_subscribe(subscribe):
            return "分集洗版"
        return ""

    def run_meta_check(self):
        """元数据检查巡检：枚举订阅并委托生命周期协调器处理单订阅状态流转。"""
        if not self._subscribe_oper:
            return
        lifecycle = self._modules.get("lifecycle")
        detail("元数据巡检：开始")
        for subscribe in (self._subscribe_oper.list(state="N,R,P,S") or []):
            if resolve_subscribe_media_type(subscribe) not in (MediaType.MOVIE, MediaType.TV):
                continue
            if lifecycle:
                lifecycle.handle_meta_check_subscription(subscribe)

    @staticmethod
    def _pending_release_sources(task: dict) -> list[str]:
        """返回需要通过待定判定器复核的业务待定来源。"""
        sources = task.get("pending_sources") if isinstance(task, dict) else None
        if isinstance(sources, dict) and sources:
            ordered_sources = [
                source for source in ("pending_judge", "guard_veto")
                if source in sources
            ]
            return ordered_sources or ["pending_judge"]
        source = task.get("source") if isinstance(task, dict) else None
        if source in ("pending_judge", "guard_veto"):
            return [source]
        return ["pending_judge"]

    def run_pending_release(self):
        """待定释放巡检：活跃来源走待定判定器，残留观察记录只做清理。

        PendingStateCoordinator 对 download_pending、pending_judge、guard_veto 做多来源仲裁；
        guard_veto 退出必须经完成证据流水线复核，孤儿观察记录不参与状态释放。
        """
        detail("待定释放巡检：开始")
        lifecycle = self._modules.get("lifecycle")
        if lifecycle and self._subscribe_oper:
            task_data = self.get_data("subscribes") or {}
            for subscribe in (self._subscribe_oper.list(state="P") or []):
                if resolve_subscribe_media_type(subscribe) not in (MediaType.MOVIE, MediaType.TV):
                    continue
                task = task_data.get(str(subscribe.id), {})
                for source in self._pending_release_sources(task):
                    lifecycle.release_pending_source(
                        subscribe,
                        source=source,
                        reason="待定释放巡检",
                    )

        timeout_manager = self._modules.get("timeout_manager")
        if not timeout_manager or not self._subscribe_oper:
            return
        for sid in list((self.get_data("blocks") or {}).keys()):
            subscribe = self._subscribe_oper.get(int(sid))
            if not subscribe:
                detail(f"待定释放：{format_subscribe_label(subscribe_id=sid)} 已不存在，清理残留完成前观察记录")
                timeout_manager.clear_observation(int(sid))
                timeout_manager.clear_release_token(int(sid))
                continue
            task_data = self.get_data("subscribes") or {}
            task = task_data.get(str(sid), {})
            has_guard_source = (
                task.get("source") == "guard_veto"
                or "guard_veto" in (task.get("pending_sources") or {})
            )
            if subscribe.state == "P" and has_guard_source:
                continue
            detail(f"待定释放：{format_subscribe(subscribe)} 无活跃完成前观察来源，清理残留记录")
            timeout_manager.clear_observation(int(sid))
            timeout_manager.clear_release_token(int(sid))

    def run_pending_state_reconcile(self):
        """修复增强版任务仍声明 P、但所有待定来源均已丢失的状态残留。"""
        lifecycle = self._modules.get("lifecycle")
        if not lifecycle or not self._subscribe_oper:
            return
        for subscribe in (self._subscribe_oper.list(state="P") or []):
            if resolve_subscribe_media_type(subscribe) not in (MediaType.MOVIE, MediaType.TV):
                continue
            lifecycle.reconcile_pending(
                subscribe,
                reason="待定状态一致性检查",
            )

    def _clear_orphan_completion_observation(self, subscribe):
        """恢复无活跃 guard_veto 的 P 残留后，清理同订阅完成前观察状态。"""
        timeout_manager = self._modules.get("timeout_manager")
        if not timeout_manager or not subscribe:
            return
        timeout_manager.clear_observation(subscribe.id)
        timeout_manager.clear_release_token(subscribe.id)

    def run_common_check(self):
        """统一执行待定、无下载及各类本地过期数据清理。

        每个子任务独立捕获异常，避免单个检查失败阻断同轮其他检查。
        """
        tasks = [("待定释放", self.run_pending_release)]
        tasks.append(("待定状态一致性检查", self.run_pending_state_reconcile))
        tasks.append(("无下载处理", self.run_no_download_check))
        tasks.append(("暂停订阅低频补搜", self.run_paused_probe_check))
        tasks.append(("站点证据采样", self.run_site_evidence_scan))
        if self._config.download_monitor_enabled:
            tasks.append(("删除记录清理", self.run_deletes_cleanup))
        tasks.append(("收容到期清理", self._relocate_expired))
        tasks.append(("完成快照清理", self.run_completion_snapshot_cleanup))
        tasks.append(("订阅清理事务清理", self.run_subscription_cleanup_expired))

        detail("通用巡检：开始")
        for task_name, task in tasks:
            try:
                task()
            except Exception as err:
                logger.error(f"通用巡检：{task_name}执行失败：{err}", exc_info=True)

    def run_paused_probe_check(self):
        """暂停订阅低频补搜巡检：登记外部暂停，并按配置安排单订阅搜索。"""
        coordinator = self._modules.get("paused_probe")
        if coordinator:
            coordinator.run()

    def run_site_evidence_scan(self):
        """站点证据采样：只读主程序 RSS/spider 缓存并固化短窗口资源证据。"""
        site_evidence = self._modules.get("site_evidence")
        if not site_evidence or not self._subscribe_oper:
            return
        detail("站点证据采样：开始")
        for subscribe in (self._subscribe_oper.list(state="P,R") or []):
            if (
                subscribe.state in ("P", "R")
                and
                resolve_subscribe_media_type(subscribe) == MediaType.TV
                and not is_full_best_version_subscribe(subscribe)
                and not bool(subscribe.manual_total_episode)
            ):
                site_evidence.refresh_subscribe(subscribe)

    def run_completion_verify(self):
        """完成后自验证巡检：复查完成快照，发现 TMDB 增集后重建订阅并通知。"""
        verifier = self._modules.get("verifier")
        if verifier:
            detail("完成后验证：开始")
            verifier.verify_all()

    def run_completion_snapshot_cleanup(self):
        """按 verify_retention_days 清理 H 快照，不触发自动纠错或 TMDB 请求。"""
        verifier = self._modules.get("verifier")
        if verifier:
            removed = verifier.cleanup_expired()
            if removed:
                logger.info(f"完成快照清理：已清理 {removed} 条过期快照")

    def run_subscription_cleanup_expired(self):
        """清理超过 36 小时的订阅清理事务。"""
        subscription_cleanup = self._modules.get("subscription_cleanup")
        if subscription_cleanup:
            removed = subscription_cleanup.cleanup_expired_clear_histories()
            if removed:
                logger.info(f"订阅清理事务：已清理 {removed} 条超过 36 小时的记录")

    def _last_download_date(self, subscribe) -> Optional[datetime.date]:
        """订阅最近一次真实下载日期（取自主程序下载历史），无则 None。"""
        try:
            mtype = subscribe.type
            title = subscribe.name
            year = subscribe.year
            media_source, media_id = subscribe_media_identity(subscribe)
            if mtype == "电影":
                histories = self._downloadhistory_oper.get_last_by(
                    mtype=mtype,
                    title=title,
                    year=year,
                    media_source=media_source,
                    media_id=media_id,
                )
            else:
                season = subscribe.season
                histories = self._downloadhistory_oper.get_last_by(
                    mtype=mtype,
                    title=title,
                    year=year,
                    season=f"S{int(season):02d}" if season is not None else None,
                    media_source=media_source,
                    media_id=media_id,
                )
            history_dates = [history.date for history in histories or [] if history.date]
            if not history_dates:
                return None
            last_download = max(history_dates)
            if isinstance(last_download, datetime.datetime):
                return last_download.date()
            if isinstance(last_download, datetime.date):
                return last_download
            return (
                parse_date(last_download, fmt="%Y-%m-%d %H:%M:%S")
                or parse_date(last_download)
            )
        except Exception:
            return None

    def _related_download_histories(self, subscribe, raise_on_error: bool = False) -> list:
        """获取同一订阅完成后的分集下载历史，用于判断是否应自动洗版。"""
        try:
            if subscribe.type == "电影":
                histories = self._downloadhistory_oper.get_last_by(
                    mtype=subscribe.type,
                    title=subscribe.name,
                    year=subscribe.year,
                    media_source=subscribe.media_source,
                    media_id=subscribe.media_id,
                )
            else:
                histories = self._downloadhistory_oper.get_last_by(
                    mtype=subscribe.type,
                    title=subscribe.name,
                    year=subscribe.year,
                    season=f"S{int(subscribe.season):02d}" if subscribe.season is not None else None,
                    media_source=subscribe.media_source,
                    media_id=subscribe.media_id,
                )
        except Exception as err:
            logger.warning(f"洗版编排：查询关联下载历史失败，跳过分集洗版判定：{err}")
            if raise_on_error:
                raise
            return []

        related = []
        subscribe_date = self._parse_datetime(subscribe.date)
        for history in histories or []:
            source = history.note.get("source") if isinstance(history.note, dict) else ""
            source_info = self._subscribe_info_from_source(source)
            if not source_info:
                continue
            if source_info.get("id") != subscribe.id:
                continue
            if (
                source_info.get("media_source") != subscribe.media_source
                or str(source_info.get("media_id") or "") != str(subscribe.media_id or "")
            ):
                continue
            if source_info.get("year") != subscribe.year:
                continue
            history_date = self._parse_datetime(history.date)
            if subscribe_date and history_date and history_date <= subscribe_date:
                continue
            if subscribe.type != "电影":
                if source_info.get("season") != subscribe.season:
                    continue
                source_episode_group = source_info.get("episode_group")
                if source_episode_group and source_episode_group != subscribe.episode_group:
                    continue
                if history.episode_group and history.episode_group != subscribe.episode_group:
                    continue
                if self._is_full_pack_download(history, subscribe.total_episode):
                    continue
            related.append(history)
        return related

    def _convert_episode_best_version_to_full_if_ready(
            self,
            subscribe_id,
            subscribe=None,
            mediainfo=None,
            trigger: str = "洗版巡检",
    ) -> bool:
        """在下载待定已释放且媒体库完整覆盖目标范围时，将分集洗版转为全集洗版。"""
        if not self._config or not self._config.best_version_episode_to_full:
            return False
        if not subscribe_id or not self._subscribe_oper:
            return False
        subscribe = subscribe or self._subscribe_oper.get(subscribe_id)
        if not subscribe or not is_tv_episode_best_version_subscribe(subscribe):
            return False

        try:
            start_episode = int(subscribe.start_episode or 1)
            total_episode = int(subscribe.total_episode or 0)
        except (TypeError, ValueError):
            return False
        start_episode = max(start_episode, 1)
        if total_episode < start_episode:
            return False
        target_episodes = set(range(start_episode, total_episode + 1))

        download_monitor = self._modules.get("download_monitor")
        if download_monitor and download_monitor.has_active_downloads(subscribe.id):
            detail(f"{trigger}：{format_subscribe(subscribe)} 仍有下载待定，跳过分集转全集")
            return False

        existing_episodes, missing_episodes = self._detect_episode_coverage(subscribe)
        if missing_episodes or not target_episodes.issubset(set(existing_episodes)):
            return False

        try:
            episode_histories = self._related_download_histories(subscribe, raise_on_error=True)
        except Exception:
            return False
        try:
            current_priority = int(subscribe.current_priority or 0)
        except (TypeError, ValueError):
            current_priority = 0
        full_priority = 0 if len(episode_histories) > 1 else current_priority

        mediainfo = mediainfo or self._recognize_mediainfo(subscribe)
        converter = self._modules.get("converter")
        if not mediainfo or not converter:
            return False
        logger.info(
            f"{trigger}：{format_subscribe(subscribe)} 媒体库已完整覆盖目标范围，"
            f"分集下载历史={len(episode_histories)}，转为全集洗版"
        )
        return converter.convert_to_full(
            subscribe,
            mediainfo,
            current_priority=full_priority,
        )

    @staticmethod
    def _is_full_pack_download(history, total_episode: Optional[int]) -> bool:
        """判断下载历史是否为合集/全集包；全集包不参与分集洗版触发计数。"""
        if not total_episode:
            return False
        meta_info = MetaInfo(title=history.torrent_name, subtitle=history.torrent_description)
        if meta_info.total_episode == total_episode:
            return True
        text = f"{history.torrent_name or ''} {history.torrent_description or ''}"
        patterns = (
            rf"全\s*{int(total_episode)}\s*集",
            rf"complete\s*{int(total_episode)}\s*(?:episodes?|eps?)",
            rf"{int(total_episode)}\s*(?:episodes?|eps?)\s*complete",
        )
        return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns)

    @staticmethod
    def _subscribe_info_from_source(source: str) -> dict:
        """从下载历史 source 中解析订阅信息；解析失败按无关联处理。"""
        if not source or "|" not in source:
            return {}
        _prefix, raw = source.split("|", 1)
        try:
            data = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _parse_datetime(value):
        """解析下载历史/订阅时间，无法解析时返回 None。"""
        if not value:
            return None
        if isinstance(value, datetime.datetime):
            return value
        if isinstance(value, datetime.date):
            return datetime.datetime.combine(value, datetime.time.min)
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.datetime.strptime(str(value), fmt)
            except ValueError:
                continue
        return None

    def run_no_download_check(self):
        """无下载处理巡检：上映后超期且无下载的订阅按策略暂停、完成或删除。"""
        policy = self._modules.get("no_download_policy")
        lifecycle = self._modules.get("lifecycle")
        if not policy or not lifecycle or not self._subscribe_oper or not self._subscribe_history_oper:
            return

        detail("无下载处理巡检：开始")
        for subscribe in (self._subscribe_oper.list(state="N,R,P") or []):
            if resolve_subscribe_media_type(subscribe) not in (MediaType.MOVIE, MediaType.TV):
                continue
            mediainfo = self._recognize_mediainfo(subscribe)
            if not mediainfo:
                continue
            decision = policy.evaluate_detail(
                subscribe,
                mediainfo,
                self._last_download_date(subscribe),
            )
            action = decision.action if decision else None
            subscribe_id = subscribe.id
            if action == "pause":
                logger.info(
                    f"无下载处理：{format_subscribe(subscribe)}(id={subscribe_id}) "
                    f"原因={decision.reason}，处理=暂停订阅"
                )
                result = lifecycle.pause_for_no_download(subscribe, decision.reason)
                if not result.changed:
                    continue
            elif action == "complete":
                logger.info(
                    f"无下载处理：{format_subscribe(subscribe)}(id={subscribe_id}) "
                    f"原因={decision.reason}，处理=写入完成历史并删除订阅"
                )
                payload = subscribe.to_dict()
                self._subscribe_history_oper.add(payload)
                self._subscribe_oper.delete(subscribe_id)
            elif action == "delete":
                logger.info(
                    f"无下载处理：{format_subscribe(subscribe)}(id={subscribe_id}) "
                    f"原因={decision.reason}，处理=删除订阅"
                )
                self._subscribe_oper.delete(subscribe_id)
            else:
                continue
            if action != "pause":
                self._task_manager.clear_tasks(subscribe_id)
            self._send_no_download_notification(subscribe, mediainfo, action, reason=decision.reason)

    def run_deletes_cleanup(self):
        """删除指纹老化清理：移除超过保留期的近期删除资源，避免长期误挡同源资源。"""
        deletes_store = self._modules.get("deletes_store")
        if deletes_store:
            removed = deletes_store.cleanup_expired(self._config.delete_record_retention_hours)
            if removed:
                logger.info(f"删除指纹清理：已清理 {removed} 条过期记录（近期删除资源）")

    def get_state(self) -> bool:
        """返回插件总开关状态。"""
        return self._config is not None and self._config.enabled

    # ---- 事件处理器：注册在插件类上。主程序按 handler.__qualname__ 的首段（类名=plugin_id）
    #      解析运行实例分发（app/core/event.py），故 handler 必须是插件类方法，不能注册 EventProxy
    #      的绑定方法（否则按 "EventProxy" 找不到运行插件、事件永不触发）。实际逻辑委托 EventProxy，
    #      未启用的域在 EventProxy 内部按 get() 短路。----

    @eventmanager.register(ChainEventType.SubscribeCompletionCheck)
    def on_completion_check(self, event):
        """订阅完成检查 → 完成守卫（链式事件，可否决完成）。"""
        if self._event_proxy:
            self._event_proxy.on_completion_check(event)

    @eventmanager.register(ChainEventType.SubscribeEpisodesRefresh)
    def on_episodes_refresh(self, event):
        """订阅集数刷新 → 变更速率记录 + 站点证据消费 + 待定状态观察。"""
        if self._event_proxy:
            self._event_proxy.on_episodes_refresh(event)

    @eventmanager.register(EventType.SubscribeAdded)
    def on_subscribe_added(self, event):
        """订阅新增 → 优先级回填 + 播出暂停 + 待定判定。"""
        if self._event_proxy:
            self._event_proxy.on_subscribe_added(event)

    @eventmanager.register(EventType.SubscribeDeleted)
    def on_subscribe_deleted(self, event):
        """订阅删除 → 清理关联任务数据。"""
        if self._event_proxy:
            self._event_proxy.on_subscribe_deleted(event)

    @eventmanager.register(EventType.SubscribeModified)
    def on_subscribe_modified(self, event):
        """订阅修改 → 任务状态重置 + 普通转洗版回填。"""
        if self._event_proxy:
            self._event_proxy.on_subscribe_modified(event)

    @eventmanager.register(EventType.SubscribeComplete)
    def on_subscribe_complete(self, event):
        """订阅完成 → 任务清理 + H 完成快照 + 自动洗版编排。"""
        if self._event_proxy:
            self._event_proxy.on_subscribe_complete(event)

    @eventmanager.register(EventType.DownloadAdded)
    def on_download_added(self, event):
        """DownloadAdded → 种子监控登记 + 下载待定 hash 确认。"""
        if self._event_proxy:
            self._event_proxy.on_download_added(event)

    @eventmanager.register(EventType.TransferComplete)
    def on_transfer_complete(self, event):
        """整理完成 → 移动模式任务同步清理 + 下载待定清除。"""
        if self._event_proxy:
            self._event_proxy.on_transfer_complete(event)

    @eventmanager.register(ChainEventType.ResourceSelection)
    def on_resource_selection(self, event):
        """ResourceSelection → 洗版待定按集串行 + 识别增强候选准入 + 删除指纹防重过滤。"""
        if self._event_proxy:
            self._event_proxy.on_resource_selection(event)

    @eventmanager.register(ChainEventType.ResourceDownload, priority=9999)
    def on_resource_download(self, event):
        """ResourceDownload → 订阅清理 + 无 hash 下载待定 + 洗版优先级基线。"""
        if self._event_proxy:
            self._event_proxy.on_resource_download(event)

    @eventmanager.register(ChainEventType.TransferIntercept, priority=9999)
    def on_transfer_intercept(self, event):
        """整理拦截 → 订阅清理目标媒体文件。"""
        if self._event_proxy:
            self._event_proxy.on_transfer_intercept(event)

    @eventmanager.register(EventType.PluginAction)
    def on_plugin_action(self, event):
        """插件命令 → /subscribe_toggle 切换订阅状态、/sub_hr_scan 立即刷新站点 H&R 时长。"""
        action = (getattr(event, "event_data", None) or {}).get("action") if event else None
        if action == "hr_scan":
            self.post_message(title="订阅助手Q改版", text="开始刷新站点 H&R 时长 ...")
            self.run_hr_hours_scan()
            return
        if self._event_proxy:
            self._event_proxy.on_plugin_action(event)

    @eventmanager.register(ChainEventType.PluginDataReset)
    def on_plugin_data_reset(self, event):
        """插件数据重置前 → 恢复增强版持有的订阅状态。"""
        event_data = event.event_data
        if not event_data or event_data.plugin_id != self.__class__.__name__ or not event_data.reset_data:
            return
        self._reset_task_data()

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """注册远程命令：切换订阅状态、立即刷新站点 H&R 时长。"""
        return [
            {
                "cmd": "/subscribe_toggle",
                "event": EventType.PluginAction,
                "desc": "切换订阅状态",
                "category": "订阅",
                "data": {"action": "subscribe_toggle"},
            },
            {
                "cmd": "/sub_hr_scan",
                "event": EventType.PluginAction,
                "desc": "立即刷新站点H&R时长",
                "category": "订阅",
                "data": {"action": "hr_scan"},
            },
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        """暴露只读概览接口：返回各业务域启用状态与待定/监控计数。"""
        return [{
            "path": "/summary",
            "endpoint": self._api_summary_response,
            "methods": ["GET"],
            "auth": "bear",
            "summary": "订阅助手（增强版）概览",
            "description": "返回各业务域启用状态与待定/监控计数",
            "response_model": schemas.Response[SummaryPayload],
        }]

    def _api_summary_response(self) -> schemas.Response[SummaryPayload]:
        """返回 V3 插件 API 明确声明的统一响应信封。"""
        return schemas.Response(success=True, data=SummaryPayload(**self._api_summary()))

    def _api_summary(self) -> Dict[str, Any]:
        """概览数据：各业务域启用状态 + 待定订阅与监控种子计数。"""
        cfg = self._config or PluginConfig({})
        subscribes = self.get_data("subscribes") or {}
        torrents = self.get_data("torrents") or {}
        pending = sum(1 for task in subscribes.values()
                      if isinstance(task, dict) and task.get("state") == "P")
        return {
            "domains": {
                "完结守卫模式": cfg.completion_guard_mode,
                "待定增强": cfg.pending_enhanced_enabled,
                "暂停优化": cfg.pause_enhanced_enabled,
                "自动洗版": cfg.best_version_type != "no",
                "下载管理": cfg.download_monitor_enabled,
                "完成后验证": cfg.verify_enabled,
                "站点集数探测": cfg.site_total_probe_enabled,
                "站点完结信号": cfg.site_completion_evidence_enabled,
                "识别增强": cfg.recognition_guard_mode,
            },
            "pending_count": pending,
            "monitored_torrents": len(torrents),
        }

    @staticmethod
    def get_render_mode() -> Tuple[str, str]:
        """使用 vuetify JSON 表单渲染配置页（Q 版不依赖 Vue 构建产物，便于直接发布使用）。"""
        return "vuetify", None

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """返回宿主配置接口需要的表单结构和默认模型，Vue Config 使用默认模型初始化。"""
        from .form import build_form
        return build_form()

    def get_page(self) -> Optional[List[dict]]:
        """返回收容清单：展示当前收容中的 H&R 种子及其到期时间。"""
        records = self.get_data("relocate_records") or {}
        if not records:
            return [{
                "component": "VAlert",
                "props": {"type": "info", "variant": "tonal",
                          "text": "当前没有收容中的 H&R 种子"},
            }]
        rows: List[dict] = [{
            "component": "VAlert",
            "props": {"type": "success", "variant": "tonal",
                      "text": (f"当前收容中 {len(records)} 个 H&R 种子"
                               f"（保存在收容目录继续做种，到期后自动删除任务）")},
        }]
        for torrent_hash, record in list(records.items()):
            rows.append({
                "component": "VAlert",
                "props": {
                    "type": "warning",
                    "variant": "tonal",
                    "text": (f"{record.get('title') or torrent_hash}"
                             f"｜站点 {record.get('site_name') or '-'}"
                             f"｜下载器 {record.get('downloader') or '-'}"
                             f"｜收容 {record.get('relocated_at') or '-'}"
                             f"｜完成 {record.get('completed_at') or '未完成（完成后按 H&R 时长起算）'}"
                             f"｜需做种 {record.get('hr_hours') or '-'} 小时"
                             f"｜到期 {record.get('deadline') or '-'}"),
                },
            })
        return rows

    def _tmdb_episodes(self, tmdbid: int, season: int, episode_group: str = None):
        """查询 TMDB 季内集信息供完成证据流水线构建 SeasonScope；不可用时返回空列表。"""
        if not self._tmdb_chain or not tmdbid or season is None:
            return []
        return self._tmdb_chain.tmdb_episodes(
            tmdbid=tmdbid, season=season, episode_group=episode_group
        ) or []

    @staticmethod
    def _site_cache_candidates(subscribe, **kwargs):
        """读取主程序已有 RSS/spider 缓存候选；不触发站点刷新或缓存写入。"""
        chain = TorrentsChain()
        helper = getattr(chain, "get_subscribe_cache_candidates", None)
        if not callable(helper):
            if not SubscribeAssistantEnhancedQ._site_cache_candidate_helper_warned:
                logger.warning(
                    "信号引擎(S)：当前 MoviePilot 主程序缺少站点缓存候选读取能力，"
                    "跳过站点证据扫描；请升级主程序后再启用站点证据"
                )
                SubscribeAssistantEnhancedQ._site_cache_candidate_helper_warned = True
            return []
        return helper(subscribe, **kwargs)

    @staticmethod
    def _mediainfo_from_dict(data):
        """从事件 mediainfo dict 重建 MediaInfo 对象；空数据返回 None。"""
        if not data:
            return None
        mediainfo = MediaInfo()
        mediainfo.from_dict(data)
        return mediainfo

    @staticmethod
    def _is_tv_media(mediainfo) -> bool:
        """媒体是否为剧集（电影无季/集，不做播出暂停与待定）。"""
        from app.schemas.types import MediaType
        return mediainfo.type == MediaType.TV

    def _recognize_mediainfo(self, subscribe):
        """从订阅识别 MediaInfo，供定时巡检评估完结/释放；识别失败返回 None。"""
        if resolve_subscribe_media_type(subscribe) not in (MediaType.MOVIE, MediaType.TV):
            return None
        meta = build_subscribe_meta(subscribe, failure_context="媒体识别失败")
        if meta is None:
            return None
        try:
            return self.chain.recognize_media(
                meta=meta, mtype=meta.type,
                media_source=subscribe.media_source,
                media_id=subscribe.media_id,
                episode_group=subscribe.episode_group,
                cache=False)
        except Exception as err:
            logger.warning(f"媒体识别失败：{format_subscribe(subscribe)}，错误：{redact_sensitive_text(err)}")
            return None

    def _recognize_by_meta_for_recognition(self, meta_info):
        """识别增强二次识别入口；外部识别失败按无补充证据处理。"""
        if not meta_info:
            return None
        try:
            return self.chain.recognize_media(meta=meta_info, mtype=getattr(meta_info, "type", None), cache=False)
        except Exception as err:
            logger.warning(f"识别增强二次识别失败：{redact_sensitive_text(err)}")
            return None

    def _detect_existing_episodes(self, subscribe) -> list:
        """返回订阅目标范围内媒体库已经存在的集。"""
        existing, _ = self._detect_episode_coverage(subscribe)
        return existing

    def _detect_backfill_episodes(self, subscribe) -> list:
        """返回洗版回填候选：媒体库已有集与订阅 note 中的已下载集并集。"""
        total_episode = subscribe.total_episode or 0
        try:
            total_episode = int(total_episode)
        except (TypeError, ValueError):
            total_episode = 0
        candidates = {
            episode
            for episode in self._detect_existing_episodes(subscribe)
            if isinstance(episode, int) and 1 <= episode <= total_episode
        }
        for episode in subscribe.note or []:
            try:
                episode_number = int(episode)
            except (TypeError, ValueError):
                continue
            if 1 <= episode_number <= total_episode:
                candidates.add(episode_number)
        return sorted(candidates)

    def _detect_missing_episodes(self, subscribe) -> list:
        """返回订阅目标范围内媒体库仍缺失的集。"""
        _, missing = self._detect_episode_coverage(subscribe)
        return missing

    def _resolve_subscribe_missing(self, subscribe, mediainfo, meta=None,
                                   best_version_accept_downloaded: bool = False):
        """按主程序订阅目标口径查询剩余缺集，不触发订阅完成写库。"""
        if meta is None:
            meta = build_subscribe_meta(subscribe, failure_context="目标缺集查询失败")
            if meta is None:
                return False, {}
        if self._subscribe_chain is None:
            logger.warning(f"目标缺集查询失败：{format_subscribe(subscribe)}，主程序订阅链未初始化")
            return False, {}
        return self._subscribe_chain.resolve_subscribe_missing(
            subscribe=subscribe,
            meta=meta,
            mediainfo=mediainfo,
            best_version_accept_downloaded=best_version_accept_downloaded,
        )

    def _detect_episode_coverage(self, subscribe) -> Tuple[list, list]:
        """复用主程序缺集探测并返回 (已存在集, 缺失集)；探测失败按目标集全部缺失处理。"""
        total = subscribe.total_episode or 0
        start_episode = subscribe.start_episode or 1
        target = set(range(start_episode, total + 1))
        if not target:
            return [], []
        try:
            from app.chain.download import DownloadChain
            mediainfo = self._recognize_mediainfo(subscribe)
            if not mediainfo:
                return [], sorted(target)
            season = subscribe.season if subscribe.season is not None else 0
            meta = build_subscribe_meta(subscribe, failure_context="媒体库缺集探测失败")
            if meta is None:
                return [], sorted(target)
            totals = {season: total} if subscribe.season is not None and total else {}
            exist_flag, no_exists = DownloadChain().get_no_exists_info(meta=meta, mediainfo=mediainfo, totals=totals)
            if exist_flag:
                return sorted(target), []
            missing = set()
            matched_scope = False
            for seasons in (no_exists or {}).values():
                info = seasons.get(season) if isinstance(seasons, dict) else None
                if info is None:
                    continue
                matched_scope = True
                eps = info.episodes
                if eps:
                    missing.update(eps)
                else:
                    # 主程序以空 episodes 表示该季目标范围整季缺失。
                    missing.update(target)
            if not matched_scope:
                missing.update(target)
            if missing and missing.isdisjoint(target):
                detail(
                    f"媒体库缺集探测：{format_subscribe(subscribe)} 返回集号 {sorted(missing)[:5]} "
                    f"不在订阅目标集 {start_episode}-{total} 内，按目标集仍缺失处理"
                )
                missing = set(target)
            missing &= target
            return sorted(target - missing), sorted(missing)
        except Exception:
            return [], sorted(target)

    def _delete_downloader_torrent(self, downloader, torrent_hash):
        """从下载器删除种子（delete_file=True，连源文件一并删）；缺下载器服务或参数时跳过。

        删除不可逆，仅由超时/Tracker 巡检判定后经 TorrentCleanup 调用。
        """
        if not self._downloader_helper or not downloader or not torrent_hash:
            return
        service = self._downloader_helper.get_service(name=downloader)
        if service and service.instance:
            logger.info(f"删除种子：从下载器 {downloader} 删除种子 {torrent_hash}（含源文件，不可逆）")
            service.instance.delete_torrents(delete_file=True, ids=torrent_hash)

    def _config_payload(self) -> dict:
        """构造当前完整配置字典（用于写回插件配置）。"""
        if not self._config:
            return {}
        return {key: getattr(self._config, key) for key in self._config.declared_keys()}

    @staticmethod
    def _parse_site_hours_text(text) -> Dict[str, float]:
        """解析「站点名:小时」文本为字典。"""
        result: Dict[str, float] = {}
        for item in str(text or "").replace("；", ",").replace(";", ",").replace("\n", ",").split(","):
            item = item.strip()
            if not item or ":" not in item:
                continue
            name, _, value = item.rpartition(":")
            try:
                result[name.strip()] = float(value.strip())
            except Exception:
                continue
        return result

    def run_hr_hours_scan(self):
        """每天自动抓取站点 H&R 时长并合并进「站点H&R时长」兜底配置。

        合并规则：抓到的值大于等于旧值时更新；比旧值更小时保留旧值并记日志，
        避免抓取误差导致到期提前，造成 H&R 违约。
        """
        if not self._config or not getattr(self._config, "hr_auto_scan", True):
            return
        try:
            from .shared.hr_hours import SiteHrHoursScanner

            found = SiteHrHoursScanner().scan()
        except Exception as err:
            logger.error(f"站点 H&R 时长自动刷新失败：{err}")
            return
        if not found:
            logger.info("站点 H&R 时长自动刷新：本轮未抓到任何站点时长")
            return
        current = self._parse_site_hours_text(getattr(self._config, "site_hr_hours", ""))
        updated = dict(current)
        kept = []
        for name, hours in found.items():
            old = current.get(name)
            if old and hours < old:
                kept.append(f"{name}(保留 {old:g}，抓到 {hours:g})")
                continue
            updated[name] = hours
        merged = ",".join(f"{name}:{hours:g}" for name, hours in updated.items())
        payload = self._config_payload()
        payload["site_hr_hours"] = merged
        self.update_config(payload)
        logger.info(f"站点 H&R 时长自动刷新：抓到 {len(found)} 个站点，写入兜底配置 {len(updated)} 个"
                    f"{('；保留旧值 ' + '、'.join(kept)) if kept else ''}")

    def _hr_site_names(self) -> set:
        """从配置的站点 H&R 时长文本中解析出 H&R 站点名单。"""
        names = set()
        text = str(getattr(self._config, "site_hr_hours", "") or "")
        for item in text.replace("；", ",").replace(";", ",").replace("\n", ",").split(","):
            item = item.strip()
            if not item or ":" not in item:
                continue
            name, _, _ = item.rpartition(":")
            if name.strip():
                names.add(name.strip())
        return names

    def _is_hr_release(self, downloader, torrent_hash, torrent_task=None, subscribe=None) -> bool:
        """判断种子是否属于 H&R：下载器标签为主判据，下载历史站点名兜底。

        标签在运行中可能丢失或被其它插件改写，所以标签未命中时用下载历史里的站点名
        比对「站点H&R时长」名单，避免把普通种子也收容。
        """
        # 主判据：下载器上的 H&R 标签（按用户要求以标签为主）
        try:
            service = self._downloader_helper.get_service(name=downloader) if self._downloader_helper else None
            instance = getattr(service, "instance", None) if service else None
            if instance:
                torrents, error = instance.get_torrents(ids=torrent_hash)
                if torrents and not error:
                    tags = self._torrent_tags(torrents[0])
                    if any(str(tag).strip().upper() in ("H&R", "HR") for tag in tags):
                        return True
        except Exception as err:
            logger.debug(f"订阅收容：读取种子标签失败 {torrent_hash}：{err}")
        # 兜底：站点命中配置的 H&R 站点名单（标签可能在运行中丢失）
        site_name = str((torrent_task or {}).get("site_name") or "").strip()
        if not site_name:
            site_name = self._site_name_from_history(torrent_hash)
        if site_name and site_name in self._hr_site_names():
            return True
        return False

    def _site_name_from_history(self, torrent_hash: str) -> str:
        """从下载历史中取种子所属站点名（下载监控任务记录本身不含站点信息）。"""
        try:
            record = self._downloadhistory_oper.get_by_hash(torrent_hash) if self._downloadhistory_oper else None
            if record:
                return str(getattr(record, "torrent_site", "") or "").strip()
        except Exception as err:
            logger.debug(f"订阅收容：读取下载历史站点失败 {torrent_hash}：{err}")
        return ""

    @staticmethod
    def _site_id_from_name(site_name: str) -> Optional[int]:
        """按站点名反查站点 ID，供直连抓取站点 H&R 页面使用；查不到返回 None。"""
        name = str(site_name or "").strip()
        if not name:
            return None
        try:
            from app.db.oper.site import SiteOper

            for row in SiteOper().list() or []:
                if str(getattr(row, "name", "") or "").strip() == name:
                    site_id = getattr(row, "id", None)
                    return int(site_id) if site_id is not None else None
        except Exception as err:
            logger.debug(f"订阅收容：按站点名查询站点 ID 失败（{name}）：{err}")
        return None

    @staticmethod
    def _torrent_tags(torrent) -> list:
        """读取 qBittorrent 标签或 Transmission labels。"""
        if isinstance(torrent, dict):
            raw = torrent.get("tags") or ""
            return [tag.strip() for tag in str(raw).split(",") if tag.strip()]
        labels = getattr(torrent, "labels", None) or []
        return [str(tag).strip() for tag in labels if str(tag).strip()]

    @staticmethod
    def _parse_hr_hours_from_text(text) -> Optional[float]:
        """从文本中解析 H&R 时长（小时），兼容中文与英文常见写法。"""
        if not text:
            return None
        content = str(text)
        patterns = (
            r"做种(?:时间|时长)[^\d]{0,12}(\d+(?:\.\d+)?)\s*(?:小时|hours?|h)",
            r"H&?R[^\d]{0,24}(\d+(?:\.\d+)?)\s*(?:小时|hours?|h)",
            r"(\d+(?:\.\d+)?)\s*(?:小时|hours?|h)[^\n]{0,24}(?:做种|H&?R)",
        )
        for pattern in patterns:
            match = re.search(pattern, content, re.IGNORECASE)
            if not match:
                continue
            try:
                value = float(match.group(1))
            except Exception:
                continue
            if 1 <= value <= 24 * 30:
                return value
        return None

    def _fetch_site_hr_hours(self, site_id) -> Optional[float]:
        """按站点 ID 或站点名直连抓取 H&R 做种时长（小时）；失败返回 None。

        复用 shared.hr_hours 的直连策略（不走 MoviePilot 代理，避免代理侧 403），
        站点结构不匹配或网络异常时返回 None，由调用方回退到配置或默认值。
        """
        if not site_id:
            return None
        try:
            from .shared.hr_hours import SiteHrHoursScanner

            scanner = SiteHrHoursScanner()
            site = scanner.find_site(site_id)
            if not site:
                return None
            hours = scanner.fetch(site)
            if hours:
                logger.info(f"订阅收容：站点 {site.get('name') or site_id} 直连读取 H&R 时长 {hours:g} 小时")
            return hours
        except Exception as err:
            logger.debug(f"订阅收容：站点 H&R 时长直连读取失败（site={site_id}）：{err}")
        return None

    def _relocate_hours(self, site_name, site_id=None, torrent_task=None) -> float:
        """计算 H&R 做种时长（小时），四级回退：站点配置 → 种子文本 → 站点页面直连 → 默认值。

        优先使用「站点H&R时长」配置（每日自动刷新、人工可覆盖），命中即不发请求；
        其次从种子标题/描述提取，再直连站点页面读取，最后使用默认值。
        取值宁可偏长也不提前删除，避免 H&R 违约。
        """
        for name, configured in self._parse_site_hours_text(
                getattr(self._config, "site_hr_hours", "")).items():
            if site_name and name.strip() == str(site_name).strip():
                logger.info(f"订阅收容：站点 {site_name} 使用配置的 H&R 时长 {configured:g} 小时")
                return configured
        record = torrent_task or {}
        hours = self._parse_hr_hours_from_text(
            f"{record.get('title') or ''} {record.get('description') or ''}"
        )
        if hours:
            logger.info(f"订阅收容：从种子文本读取 H&R 时长 {hours} 小时（{site_name or '-'}）")
            return hours
        hours = self._fetch_site_hr_hours(site_id or site_name)
        if hours:
            return hours
        fallback = float(getattr(self._config, "default_hr_hours", 168) or 168)
        logger.info(f"订阅收容：站点 {site_name or '-'} 未取到 H&R 时长，使用默认 {fallback} 小时")
        return fallback

    def _relocate_overdue(self, cleanup=None):
        """H&R 种子绝对时长收容：下载满「收容门槛」仍未完成的，移入收容目录保种。

        与低进度超时不同，这里按绝对时长判定，慢速但持续下载的 H&R 种子同样会被收容；
        普通种子不在此处理，仍由低进度超时逻辑决定删除。收容失败只保留种子，下一轮继续尝试。
        已进入人工保护期（连续低进度保留）的种子本轮跳过，避免与低进度巡检的通知承诺冲突。
        收容成功后的善后（清任务、清待定、防重指纹、延迟补搜、通知）复用 TorrentCleanup。
        """
        cfg = self._config
        if not cfg or not getattr(cfg, "relocate_enabled", False):
            return
        if getattr(cfg, "hr_mode", "relocate") != "relocate":
            return
        if not self._downloader_helper or not self._task_manager or not cfg.download_monitor_enabled:
            return
        threshold_hours = float(getattr(cfg, "relocate_after_hours", 0) or 0)
        if threshold_hours <= 0:
            return
        cleanup = cleanup or self._modules.get("torrent_cleanup")
        if not cleanup:
            return
        torrents = self._task_manager.read("torrents") or {}
        if not torrents:
            return
        monitor = self._modules.get("download_monitor")
        with self._relocate_lock:
            records = dict(self.get_data("relocate_records") or {})
        threshold_seconds = threshold_hours * 3600
        now_ts = time.time()
        pending = 0
        overdue = 0
        hr_overdue = 0
        skipped = 0
        triggered_subscribe_ids: set = set()
        for torrent_hash, task in list(torrents.items()):
            try:
                if torrent_hash in records:
                    continue
                downloader = task.get("downloader")
                if not torrent_hash or not downloader:
                    skipped += 1
                    continue
                service = self._downloader_helper.get_service(name=downloader)
                instance = getattr(service, "instance", None) if service else None
                if not instance:
                    skipped += 1
                    continue
                torrents_now, error = instance.get_torrents(ids=torrent_hash)
                if error or not torrents_now:
                    # 下载器瞬断或种子已不在，交给低进度/缺失判定处理，避免误判
                    skipped += 1
                    continue
                from .download.torrent import TorrentAdapter
                info = TorrentAdapter.get_info(torrents_now[0], service.type)
                # 严格完成判定：部分下载但仍在上传的种子不能算完成，否则会漏掉本该收容的 H&R 种子
                if info.finished:
                    continue
                pending += 1
                # 优先使用下载器中的真实添加时间，避免插件记录时间偏晚导致门槛变松
                started = float(getattr(info, "add_on", 0) or 0) or float(task.get("time") or 0)
                if not started or now_ts - started < threshold_seconds:
                    continue
                overdue += 1
                subscribe = self._subscribe_oper.get(task.get("subscribe_id")) if (
                    self._subscribe_oper and task.get("subscribe_id")) else None
                if subscribe is None:
                    skipped += 1
                    continue
                if monitor and monitor.is_timeout_protected(subscribe.id, torrent_hash, task):
                    detail(f"订阅收容：{task.get('title') or torrent_hash} 处于连续低进度保护期，本轮跳过收容")
                    continue
                if not self._is_hr_release(downloader, torrent_hash, task, subscribe):
                    continue
                hr_overdue += 1
                elapsed_hours = (now_ts - started) / 3600
                logger.info(f"订阅收容：{task.get('title') or torrent_hash} 已下载 {elapsed_hours:.1f} 小时仍未完成"
                            f"（门槛 {threshold_hours:g} 小时），执行 H&R 收容")
                # 同一订阅同一轮只让首个种子触发补搜，避免同一订阅重复搜索
                search_enabled = subscribe.id not in triggered_subscribe_ids
                triggered_subscribe_ids.add(subscribe.id)
                cleanup.handle_torrent_deleted(
                    subscribe, torrent_hash, reason="timeout",
                    reason_detail=f"下载已超过 {threshold_hours:g} 小时仍未完成",
                    downloader=downloader, delete_from_downloader=True,
                    search_enabled=search_enabled)
            except Exception as err:
                logger.error(f"订阅收容：处理 {torrent_hash} 时异常：{err}", exc_info=True)
        if pending:
            detail(f"订阅收容：本轮检查 {pending} 个未完成下载任务，超过门槛 {overdue} 个，"
                   f"其中 H&R 种子 {hr_overdue} 个（门槛 {threshold_hours:g} 小时）")
        if skipped:
            detail(f"订阅收容：本轮跳过 {skipped} 个无法判定的下载任务（缺下载器或订阅、"
                   f"下载器不可用、种子已不在）")

    def _relocate_downloader_torrent(self, downloader, torrent_hash, subscribe=None,
                                     torrent_task=None) -> str:
        """超时种子收容：不删除任务，改为把保存目录移到收容目录并登记到期时间。

        返回三态字符串：relocated=已成功收容；retained=H&R 种子收容失败（调用方保留种子不删除）；
        skip=未启用收容或非 H&R 种子（调用方按原逻辑删除）。
        收容目录不参与媒体库整理，种子留在收容目录继续做种，到期后由 _relocate_expired 清理。
        """
        cfg = self._config
        if not cfg or not getattr(cfg, "relocate_enabled", False):
            return RELOCATE_SKIP
        if getattr(cfg, "hr_mode", "relocate") != "relocate":
            logger.info("订阅收容：当前为排除标签模式，不执行收容")
            return RELOCATE_SKIP
        # 仅 H&R 种子走收容：普通种子维持原有超时删除逻辑，不占用收容目录
        if not self._is_hr_release(downloader, torrent_hash, torrent_task, subscribe):
            logger.info(f"订阅收容：{torrent_hash} 非 H&R 种子，交回原超时删除逻辑")
            return RELOCATE_SKIP
        # 以下任一环节失败都不能删除 H&R 种子，返回 retained 让调用方保留种子等下一轮重试
        relocate_dir = str(getattr(cfg, "relocate_dir", "") or "").strip()
        if not relocate_dir or not self._downloader_helper or not downloader or not torrent_hash:
            logger.warning(f"订阅收容：{torrent_hash} 缺少收容目录或下载器信息，本轮保留种子")
            return RELOCATE_RETAINED
        # 确保收容目录存在，否则下载器侧移动会失败
        try:
            import os
            os.makedirs(relocate_dir, exist_ok=True)
        except Exception as err:
            logger.warning(f"订阅收容：创建收容目录 {relocate_dir} 失败：{err}")
        service = self._downloader_helper.get_service(name=downloader)
        instance = getattr(service, "instance", None) if service else None
        if not instance:
            logger.warning(f"订阅收容：下载器 {downloader} 不可用，本轮保留种子")
            return RELOCATE_RETAINED
        try:
            moved = instance.set_torrent_location(hash_string=torrent_hash, location=relocate_dir)
        except Exception as err:
            logger.error(f"订阅收容：移动 {torrent_hash} 到 {relocate_dir} 失败：{err}")
            return RELOCATE_RETAINED
        if not moved:
            logger.warning(f"订阅收容：移动 {torrent_hash} 到 {relocate_dir} 未成功，本轮保留种子")
            return RELOCATE_RETAINED
        now = datetime.datetime.now()
        record = torrent_task or {}
        # 下载监控记录不含站点信息，缺失时用下载历史的站点名补齐，否则站点 H&R 时长无法参与到期计算
        site_name = str(record.get("site_name") or "").strip() or self._site_name_from_history(torrent_hash)
        site_id = record.get("site") or self._site_id_from_name(site_name)
        # 收容时先固化站点 H&R 时长：未完成时按进入收容时刻给出预估到期，
        # 下载完成后到期清理会改用「完成时间 + 该时长」重新计算
        hr_hours = self._relocate_hours(site_name, site_id=site_id, torrent_task=record)
        deadline = (now + datetime.timedelta(hours=hr_hours)).strftime("%Y-%m-%d %H:%M:%S")
        entry = {
            "hash": torrent_hash,
            "downloader": downloader,
            "title": record.get("title") or "",
            "site_name": site_name,
            "site_id": site_id,
            "subscribe_id": getattr(subscribe, "id", None),
            "relocated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
            "hr_hours": hr_hours,
            "deadline": deadline,
            "delete_files": bool(getattr(cfg, "relocate_delete_files", True)),
        }
        with self._relocate_lock:
            records = self.get_data("relocate_records") or {}
            records[torrent_hash] = entry
            self.save_data("relocate_records", records)
        logger.info(f"订阅收容：{entry['title'] or torrent_hash} 超时未完成，"
                    f"已转入收容目录 {relocate_dir}，站点 {site_name or '-'}，到期 {deadline}")
        return RELOCATE_RELOCATED

    def _locate_torrent_downloader(self, torrent_hash: str, preferred: Optional[str] = None) -> Tuple[
            Optional[str], bool, Optional[Any]]:
        """定位种子所在的下载器，返回 (下载器名, 结论是否确定, 命中的原始种子对象)。

        结论确定=True：已遍历全部已配置下载器且没有任何一个报错，可据此认定种子确实不存在；
        有下载器报错或不可用时为 False（不可判定），调用方应保留记录等下一轮重试，
        避免把「下载器瞬断」当成「种子已被删除」。
        命中的原始种子对象供调用方复用，避免同一轮对同一 hash 重复查询下载器。
        """
        if not self._downloader_helper or not torrent_hash:
            return None, False, None
        candidates: List[str] = []
        if preferred:
            candidates.append(str(preferred))
        try:
            services = self._downloader_helper.get_services() or {}
        except Exception as err:
            logger.debug(f"订阅收容：获取下载器列表失败：{err}")
            services = {}
        for name in services.keys():
            if name not in candidates:
                candidates.append(name)
        conclusive = bool(services)
        for name in candidates:
            try:
                service = self._downloader_helper.get_service(name=name)
                instance = getattr(service, "instance", None) if service else None
                if not instance:
                    conclusive = False
                    continue
                torrents, error = instance.get_torrents(ids=torrent_hash)
                if error:
                    conclusive = False
                    continue
                if torrents:
                    return name, True, torrents[0]
            except Exception as err:
                logger.debug(f"订阅收容：查询下载器 {name} 中的种子失败 {torrent_hash}：{err}")
                conclusive = False
                continue
        return None, conclusive, None

    def _find_torrent_downloader(self, torrent_hash: str, preferred: Optional[str] = None) -> Optional[str]:
        """定位种子的当前所在下载器：优先给定下载器，其次遍历全部已配置下载器。

        用于应对 H&R 种子下载完成后被「自动转移做种」搬到别的下载器（原任务已被删除）的情况：
        到期删除时必须在实际所在的下载器上操作，否则会出现「到期删不掉」。
        """
        downloader, _, _ = self._locate_torrent_downloader(torrent_hash, preferred=preferred)
        return downloader

    @staticmethod
    def _parse_datetime_text(value) -> Optional[datetime.datetime]:
        """解析「YYYY-MM-DD HH:MM:SS」时间文本；无法解析返回 None。"""
        try:
            return datetime.datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    @staticmethod
    def _torrent_completion_time(raw) -> Optional[datetime.datetime]:
        """从下载器原始种子对象解析完成时间：qb 取 completion_on，tr 取 done_date；取不到返回 None。

        Transmission 的属性在字段未请求时会抛 KeyError，因此逐字段防御式读取。
        """
        def attr(name, default=None):
            if isinstance(raw, dict):
                return raw.get(name, default)
            try:
                return getattr(raw, name, default)
            except Exception:
                return default

        try:
            ts = float(attr("completion_on") or 0)
        except Exception:
            ts = 0.0
        if ts <= 0:
            done = attr("done_date")
            if done is None:
                done = attr("doneDate")
            if isinstance(done, datetime.datetime):
                ts = done.timestamp()
            elif isinstance(done, (int, float)):
                ts = float(done)
        if ts <= 0:
            return None
        try:
            return datetime.datetime.fromtimestamp(ts)
        except Exception:
            return None

    def _relocate_completion_state(self, downloader: str, torrent_hash: str,
                                   raw=None) -> Tuple[bool, bool, Optional[datetime.datetime]]:
        """读取收容种子状态，返回 (是否取到种子, 是否已完成, 完成时间)。

        站点 H&R 从「下载完成」才开始考察：未完成时不能删除收容种子，否则会白等甚至违约；
        完成时间用于把到期点改成「下载完成时间 + 站点 H&R 时长」。
        完成判定用 TorrentInfo.finished（严格：进度到 100% 或下载器明确完成态），
        避免把「部分下载但已在上传」的种子按已完成处理。raw 由调用方传入可省一次下载器查询。
        """
        if not self._downloader_helper or not downloader or not torrent_hash:
            return False, False, None
        try:
            service = self._downloader_helper.get_service(name=downloader)
            instance = getattr(service, "instance", None) if service else None
            if raw is None:
                if not instance:
                    return False, False, None
                torrents, error = instance.get_torrents(ids=torrent_hash)
                if error or not torrents:
                    return False, False, None
                raw = torrents[0]
            from .download.torrent import TorrentAdapter
            info = TorrentAdapter.get_info(raw, service.type)
            return True, bool(info.finished), self._torrent_completion_time(raw)
        except Exception as err:
            logger.debug(f"订阅收容：读取种子完成状态失败 {torrent_hash}（{downloader}）：{err}")
            return False, False, None

    def _relocate_deadline(self, record: dict,
                           completed_at: Optional[datetime.datetime]) -> Optional[datetime.datetime]:
        """计算到期时间：优先「下载完成时间 + 站点 H&R 时长」，缺少完成时间时回退记录里的到期值。"""
        try:
            hours = float(record.get("hr_hours") or 0)
        except Exception:
            hours = 0.0
        # 完成时间优先取下载器当前值，取不到再用之前观察到并写入记录的值
        base = completed_at or self._parse_datetime_text(record.get("completed_at"))
        if base and hours > 0:
            return base + datetime.timedelta(hours=hours)
        return self._parse_datetime_text(record.get("deadline"))

    def _relocate_expired(self):
        """收容到期清理：种子下载完成后做满站点 H&R 时长，才在实际所在下载器删除任务。

        站点 H&R 以「下载完成」为起算点，所以未完成的收容种子不按时间删；
        超过「未完成清理天数」仍未下载完成时，按「站点不计 H&R」删除任务与文件（0 表示不清理）。
        完成后按「完成时间 + 站点 H&R 时长」计算到期点，缺完成时间时回退收容时的预估到期值。
        种子可能因「自动转移做种」被搬到其它下载器，所以每轮先跨下载器定位当前位置并更新记录，
        所有下载器都找不到时视为已不存在，移出收容记录。
        整轮读-改-写用收容锁串行化，避免与超时收容入口并发覆盖收容记录。
        """
        if not self._downloader_helper:
            return
        with self._relocate_lock:
            records = self.get_data("relocate_records") or {}
            if not records:
                return
            now = datetime.datetime.now()
            changed = False
            for torrent_hash, record in list(records.items()):
                downloader, conclusive, raw = self._locate_torrent_downloader(
                    torrent_hash, preferred=record.get("downloader"))
                if not downloader:
                    # 未到期时可能是下载器临时不可用，先保留；到期后仍找不到才移出记录，
                    # 且只有「遍历全部下载器都无报错」才认定种子已不存在，否则保留等下一轮
                    deadline_missing = self._parse_datetime_text(record.get("deadline"))
                    if deadline_missing and now >= deadline_missing:
                        if conclusive:
                            logger.info(f"订阅收容：{record.get('title') or torrent_hash} "
                                        f"已不在任何下载器，移出收容记录")
                            records.pop(torrent_hash, None)
                            changed = True
                        else:
                            logger.warning(f"订阅收容：{record.get('title') or torrent_hash} 已到期但"
                                           f"下载器状态不可判定，保留收容记录等下一轮重试")
                    continue
                if downloader != record.get("downloader"):
                    logger.info(f"订阅收容：{record.get('title') or torrent_hash} 已转移到下载器 "
                                f"{downloader}，更新收容记录")
                    record["downloader"] = downloader
                    records[torrent_hash] = record
                    changed = True
                present, completed, completed_at = self._relocate_completion_state(
                    downloader, torrent_hash, raw=raw)
                if not present:
                    # 本轮取不到种子状态（下载器瞬断等），保留记录等下一轮
                    continue
                if not completed:
                    # 站点 H&R 从下载完成才开始考察，一直下不完的种子不会进入考察，
                    # 因此超过「未完成清理天数」后按「站点不计 H&R」删除任务与文件；未到阈值只跳过
                    limit_days = int(getattr(self._config, "relocate_incomplete_days", 0) or 0)
                    relocated_at = self._parse_datetime_text(record.get("relocated_at"))
                    waited_days = (now - relocated_at).days if relocated_at else 0
                    if limit_days <= 0 or waited_days < limit_days:
                        continue
                    service = self._downloader_helper.get_service(name=downloader)
                    instance = getattr(service, "instance", None) if service else None
                    if not instance:
                        continue
                    try:
                        # 是否保留文件沿用「到期删除源文件」配置，避免同一插件内两处口径不一致
                        instance.delete_torrents(
                            ids=torrent_hash,
                            delete_file=bool(record.get("delete_files", True)))
                    except Exception as err:
                        logger.error(f"订阅收容：未完成清理删除 {torrent_hash} 失败（{downloader}）：{err}")
                        continue
                    logger.info(f"订阅收容未完成清理：已在 {downloader} 删除 "
                                f"{record.get('title') or torrent_hash}"
                                f"（收容于 {record.get('relocated_at')}，已 {waited_days} 天未完成）")
                    if getattr(self._config, "notify", True):
                        try:
                            self.post_message(
                                title="【订阅收容未完成清理】",
                                text=(f"收容后 {waited_days} 天仍未下载完成，按「站点不计 H&R」清理："
                                      f"{record.get('title') or torrent_hash}"
                                      f"（站点 {record.get('site_name') or '-'}，任务与文件已删除）"),
                            )
                        except Exception as err:
                            logger.debug(f"订阅收容：未完成清理通知发送失败：{err}")
                    records.pop(torrent_hash, None)
                    changed = True
                    continue
                if completed_at:
                    completed_text = completed_at.strftime("%Y-%m-%d %H:%M:%S")
                    if record.get("completed_at") != completed_text:
                        record["completed_at"] = completed_text
                        records[torrent_hash] = record
                        changed = True
                deadline = self._relocate_deadline(record, completed_at)
                if not deadline:
                    continue
                deadline_text = deadline.strftime("%Y-%m-%d %H:%M:%S")
                if record.get("deadline") != deadline_text:
                    # 以完成时间为基准刷新到期点，收容清单页展示同一口径
                    record["deadline"] = deadline_text
                    records[torrent_hash] = record
                    changed = True
                if now < deadline:
                    continue
                service = self._downloader_helper.get_service(name=downloader)
                instance = getattr(service, "instance", None) if service else None
                if not instance:
                    continue
                try:
                    instance.delete_torrents(ids=torrent_hash,
                                             delete_file=bool(record.get("delete_files", True)))
                except Exception as err:
                    logger.error(f"订阅收容：到期删除 {torrent_hash} 失败（{downloader}）：{err}")
                    continue
                logger.info(f"订阅收容到期：已在 {downloader} 删除 {record.get('title') or torrent_hash}"
                            f"（收容于 {record.get('relocated_at')}，"
                            f"完成于 {record.get('completed_at') or '-'}，到期 {deadline_text}）")
                if getattr(self._config, "notify", True):
                    try:
                        self.post_message(
                            title="【订阅收容到期】",
                            text=(f"已删除收容种子：{record.get('title') or torrent_hash}"
                                  f"（站点 {record.get('site_name') or '-'}，"
                                  f"完成于 {record.get('completed_at') or '-'}，"
                                  f"已做满 {record.get('hr_hours') or '-'} 小时）"),
                        )
                    except Exception as err:
                        logger.debug(f"订阅收容：到期通知发送失败：{err}")
                records.pop(torrent_hash, None)
                changed = True
            if changed:
                self.save_data("relocate_records", records)

    def _fetch_downloader_torrent(self, downloader, torrent_hash):
        """连下载器取单个种子并映射为 TorrentInfo；取不到或下载器出错返回 None。

        巡检据此判定超时——返回 None 时该种子本轮跳过，避免下载器瞬断被误判为无进度而删种。
        """
        if not self._downloader_helper or not downloader or not torrent_hash:
            return None
        service = self._downloader_helper.get_service(name=downloader)
        if not service or not service.instance:
            return None
        torrents, error = service.instance.get_torrents(ids=torrent_hash)
        if error or not torrents:
            detail(f"下载器查询：{downloader} 取种子 {torrent_hash} 无结果或瞬断（error={bool(error)}），本轮跳过该种子")
            return None
        from .download.torrent import TorrentAdapter
        return TorrentAdapter.get_info(torrents[0], service.type)

    def _downloader_torrent_present(self, downloader, torrent_hash):
        """探测种子是否仍在下载器：True=在；False=下载器可达但已不存在；None=不可判定（无服务/报错）。

        与 _fetch_downloader_torrent 的区别：后者把"报错"与"不存在"都压成 None；本方法据 get_torrents
        的 error 标志区分，让手动删除监听把"用户删种"与"下载器瞬断"分开，避免瞬断误触发删除处理。
        顺序固定为先判下载器可达、再判种子缺失，避免把瞬断当成确删。
        """
        if not self._downloader_helper or not downloader or not torrent_hash:
            return None
        service = self._downloader_helper.get_service(name=downloader)
        if not service or not service.instance:
            return None
        torrents, error = service.instance.get_torrents(ids=torrent_hash)
        if error:
            return None
        return bool(torrents)

    def _schedule_delayed_subscribe_search(self, subscribe, scene: str):
        """随机延迟执行单订阅搜索，并返回实际延迟秒数供调用方展示。"""
        if not self._subscribe_chain or not subscribe:
            return None
        sid = subscribe.id
        if sid:
            delay_minutes = random.uniform(3, 5)
            delay_seconds = delay_minutes * 60
            logger.info(
                f"{scene}：{format_subscribe(subscribe)} 将在 {delay_minutes:.2f} 分钟后触发补全搜索"
            )
            threading.Timer(delay_seconds, lambda: self._subscribe_chain.search(sid=sid)).start()
            return delay_seconds
        return None

    def _search_subscribe(self, subscribe):
        """删种后随机延迟补搜，并返回实际延迟秒数供通知展示。"""
        return self._schedule_delayed_subscribe_search(subscribe, scene="种子删除处理")

    def _schedule_initial_pending_search(self, subscribe):
        """新增订阅进入待定前安排一次单订阅搜索。"""
        return self._schedule_delayed_subscribe_search(subscribe, scene="新增待定处理")

    def _get_transfer_histories(self, media_source, media_id, mtype, season=None, episode=None):
        """按规范媒体身份、类型和季集获取整理历史，避免同名跨来源记录串联。"""
        if not self._transferhistory_oper:
            return []
        if season is not None and episode is not None:
            return self._transferhistory_oper.get_by(
                media_source=media_source, media_id=media_id,
                mtype=mtype, season=season, episode=episode,
            ) or []
        if season is not None:
            return self._transferhistory_oper.get_by(
                media_source=media_source, media_id=media_id,
                mtype=mtype, season=season,
            ) or []
        return self._transferhistory_oper.get_by(
            media_source=media_source, media_id=media_id, mtype=mtype,
        ) or []

    def _delete_media_file(self, fileitem_dict):
        """删除媒体文件（旧源文件或旧媒体库文件）；fileitem_dict 为整理记录的 src/dest_fileitem 序列化形态。

        删除不可逆，仅由订阅清理调用；清理范围由订阅清理配置控制。
        """
        if not self._storage_chain or not fileitem_dict:
            return False
        from app import schemas
        path = fileitem_dict.get("path") if isinstance(fileitem_dict, dict) else None
        logger.info(f"订阅清理：删除媒体文件 {truncate_log_value(path or fileitem_dict)}（不可逆）")
        return self._storage_chain.delete_media_file(schemas.FileItem(**fileitem_dict))

    def _send_download_file_deleted(self, src, download_hash):
        """发 DownloadFileDeleted 事件：主程序据此移除历史下载旧种子。"""
        detail(f"订阅清理：发送 DownloadFileDeleted 事件，hash={download_hash}，通知主程序移除旧下载")
        eventmanager.send_event(EventType.DownloadFileDeleted, {"src": src, "hash": download_hash})

    def _torrent_exists(self, download_hash: str) -> Optional[bool]:
        """跨全部下载器查询旧 hash；任一查询失败且均未命中时返回 None。"""
        if not self._downloader_helper or not download_hash:
            return None
        services = self._downloader_helper.get_services()
        if not services:
            return None
        query_failed = False
        for name, service in services.items():
            if not service or not service.instance:
                query_failed = True
                continue
            try:
                torrents, error = service.instance.get_torrents(ids=download_hash)
            except Exception as err:
                logger.warning(f"订阅清理：查询下载器 {name} 的旧任务失败 hash={download_hash}，错误信息：{err}")
                query_failed = True
                continue
            if error:
                logger.warning(f"订阅清理：下载器 {name} 查询旧任务失败 hash={download_hash}")
                query_failed = True
                continue
            if torrents:
                return True
        if query_failed:
            return None
        return False

    def _format_subscribe_desc(self, subscribe, mediainfo=None) -> str:
        """生成通知标题中的订阅描述，优先使用媒体标题和季号。"""
        title = mediainfo.title_year if mediainfo else subscribe.name
        season = f" S{subscribe.season}" if subscribe.season is not None else ""
        return f"{title}{season}"

    def _send_no_download_notification(self, subscribe, mediainfo, action: str,
                                       reason: Optional[str] = None):
        """发送无下载处理状态通知。"""
        action_name = {"pause": "暂停", "complete": "完成", "delete": "删除"}.get(action, action)
        days = self._config.tv_no_download_days if subscribe.type == "电视剧" else self._config.movie_no_download_days
        title = f"{self._format_subscribe_desc(subscribe, mediainfo)} 近 {days} 天未有下载记录，已标记{action_name}"
        self._notify_subscribe(
            title,
            score=mediainfo.vote_average,
            user=subscribe.username,
            reason=reason or "上映后超期且无下载",
            image=self._resolve_notification_image(subscribe, mediainfo),
            link="#/subscribe/tv?tab=mysub" if subscribe.type == "电视剧" else "#/subscribe/movie?tab=mysub",
        )

    def _send_subscribe_status_notification(self, subscribe, title_suffix: str,
                                            mediainfo=None, detail: Optional[str] = None):
        """发送订阅状态变更通知，沿用状态类消息的标题和正文结构。"""
        mediainfo = mediainfo or self._recognize_mediainfo(subscribe)
        title = f"{self._format_subscribe_desc(subscribe, mediainfo)} {title_suffix}"
        media_type = mediainfo.type.value if mediainfo else subscribe.type
        self._notify_subscribe(
            title,
            score=mediainfo.vote_average if mediainfo else None,
            user=subscribe.username,
            reason=detail,
            image=self._resolve_notification_image(subscribe, mediainfo),
            link="#/subscribe/tv?tab=mysub" if media_type == "电视剧" else "#/subscribe/movie?tab=mysub",
        )

    def _notify_subscribe(self, title, text=None, image=None, link=None,
                          score=None, user=None, reason=None, action=None,
                          follow_up=None, next_step=None, diagnostic: bool = False):
        """按通知开关发送订阅卡片，并统一正文字段顺序。

        状态结果使用单行字段，诊断明细使用多行字段；没有值的字段不输出。
        """
        if not self._config or not self._config.notify:
            return
        from app.schemas import NotificationType
        if link and link.startswith("#"):
            link = settings.MP_DOMAIN(link)
        text = self._format_notification_text(
            text=text,
            score=score,
            user=user,
            reason=reason,
            action=action,
            follow_up=follow_up if follow_up is not None else next_step,
            diagnostic=diagnostic,
        )
        message_options = {}
        if not image:
            text = self._append_notification_source(text)
            message_options["disable_web_page_preview"] = True
        self.post_message(
            mtype=NotificationType.Subscribe,
            title=title,
            text=text,
            image=image or None,
            link=link,
            **message_options,
        )

    def _append_notification_source(self, text: Optional[str]) -> str:
        """无图消息追加插件来源，便于在通知转发和多插件场景中识别发送方。"""
        source = f"来源：{self.plugin_name}"
        return f"{text}\n\n{source}" if text else source

    @staticmethod
    def _format_notification_text(text=None, score=None, user=None, reason=None,
                                  action=None, follow_up=None, diagnostic: bool = False):
        """按评分、用户、原因、处理、后续顺序生成通知正文。"""
        fields = [
            ("评分", score),
            ("用户", user),
            ("原因", reason),
            ("处理", action),
            ("后续", follow_up),
        ]
        parts = []
        if text not in (None, ""):
            parts.append(str(text))
        parts.extend([
            f"{label}：{str(value).replace(chr(10), '；')}"
            for label, value in fields
            if value not in (None, "")
        ])
        if not parts:
            return text
        separator = "\n" if diagnostic else "，"
        return separator.join(parts)

    @staticmethod
    def _get_subscribe_image(subscribe):
        """优先返回订阅背景图，其次返回海报的 w500 地址。"""
        if subscribe.backdrop:
            return subscribe.backdrop.replace("original", "w500")
        if subscribe.poster:
            return subscribe.poster.replace("original", "w500")
        return ""

    def _resolve_notification_image(self, subscribe=None, mediainfo=None):
        """解析媒体通知图片，优先保持订阅卡片当前展示图片。"""
        subscribe_image = self._get_subscribe_image(subscribe) if subscribe else ""
        if subscribe_image:
            return subscribe_image
        return mediainfo.get_message_image() if mediainfo else None
