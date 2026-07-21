"""过期订阅清理插件。

检查电视剧订阅，超过指定天数没有下载过新剧集的自动取消订阅。
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.core.event import eventmanager, Event
from app.db.subscribe_oper import SubscribeOper
from app.db.downloadhistory_oper import DownloadHistoryOper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, MediaType


class StaleSubCleaner(_PluginBase):
    """过期订阅清理插件。"""

    plugin_name = "过期订阅清理"
    plugin_desc = "检查电视剧订阅，超过指定天数未下载新剧集则自动取消订阅。"
    plugin_icon = "stalesubcleaner.png"
    plugin_version = "1.0.0"
    plugin_label = "订阅"
    plugin_author = "local"
    plugin_config_prefix = "stalesubcleaner_"
    plugin_order = 61
    auth_level = 1

    _enabled = False
    _cron: str = "0 6 * * *"
    _stale_days: int = 15
    _notify: bool = True
    _scheduler = None
    _subscribe_oper = None
    _download_oper = None
    _last_run: Optional[str] = None
    _last_cleaned: List[Dict[str, Any]] = []
    _total_cleaned: int = 0

    def init_plugin(self, config: dict = None) -> None:
        """根据插件配置初始化运行状态。"""
        self.stop_service()
        self._subscribe_oper = SubscribeOper()
        self._download_oper = DownloadHistoryOper()
        saved = self.get_data("state") or {}
        self._total_cleaned = saved.get("total_cleaned", 0)
        if not config:
            self._enabled = False
            return
        self._enabled = bool(config.get("enabled"))
        self._cron = config.get("cron") or "0 6 * * *"
        self._stale_days = int(config.get("stale_days") or 15)
        self._notify = bool(config.get("notify"))
        logger.info(
            f"初始化完成, enabled={self._enabled}, cron={self._cron}, "
            f"stale_days={self._stale_days}"
        )
        if self._enabled:
            self._schedule_service()

    def get_state(self) -> bool:
        """获取插件启用状态。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """返回插件远程命令列表。"""
        return [
            {
                "cmd": "/stale_sub_clean",
                "event": EventType.PluginAction,
                "desc": "手动触发过期订阅检查",
                "category": "订阅",
                "data": {"action": "stale_sub_clean"},
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        """返回插件 API 列表。"""
        return [
            {
                "path": "/check",
                "endpoint": self._api_check,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "手动触发过期订阅检查",
            },
        ]

    def get_form(self) -> Tuple[Optional[List[dict]], Dict[str, Any]]:
        """返回插件配置表单与默认配置。"""
        return [
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VSwitch",
                                "props": {"model": "enabled", "label": "启用插件"},
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VTextField",
                                "props": {
                                    "model": "cron",
                                    "label": "检查时间（Cron 表达式）",
                                    "placeholder": "0 6 * * *",
                                },
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VTextField",
                                "props": {
                                    "model": "stale_days",
                                    "label": "过期天数（超过此天数未下载新集则取消）",
                                    "placeholder": "15",
                                    "type": "number",
                                },
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VSwitch",
                                "props": {"model": "notify", "label": "操作时发送通知"},
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "cron": "0 6 * * *",
            "stale_days": 15,
            "notify": True,
        }

    def get_page(self) -> Optional[List[dict]]:
        """返回插件详情页面。"""
        if not self._enabled:
            return [{"component": "VAlert", "props": {"type": "warning", "text": "插件未启用"}}]

        last_run = self._last_run or "尚未运行"
        page = [
            {
                "component": "VCard",
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {"title": "过期订阅清理"},
                    },
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "text": (
                                        f"上次检查: {last_run}\n"
                                        f"过期天数阈值: {self._stale_days} 天\n"
                                        f"累计取消: {self._total_cleaned} 个"
                                    ),
                                    "variant": "tonal",
                                },
                            }
                        ],
                    },
                    {
                        "component": "VCardActions",
                        "content": [
                            {
                                "component": "VBtn",
                                "props": {"color": "primary"},
                                "text": "立即检查",
                                "events": {
                                    "click": {
                                        "api": "plugin/StaleSubCleaner/check",
                                        "method": "get",
                                        "params": {"apikey": settings.API_TOKEN},
                                    }
                                },
                            }
                        ],
                    },
                ],
            }
        ]

        if self._last_cleaned:
            page.append({
                "component": "VCard",
                "content": [
                    {"component": "VCardTitle", "props": {"title": "最近取消的订阅"}},
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VDataTable",
                                "props": {
                                    "headers": [
                                        {"title": "订阅", "key": "name"},
                                        {"title": "季", "key": "season"},
                                        {"title": "最后下载", "key": "last_download"},
                                        {"title": "闲置天数", "key": "stale_days"},
                                        {"title": "时间", "key": "cleaned_time"},
                                    ],
                                    "items": self._last_cleaned[:50],
                                    "itemsPerPage": 20,
                                },
                            }
                        ],
                    },
                ],
            })

        return page

    def stop_service(self) -> None:
        """停止插件后台服务并释放资源。"""
        if self._scheduler:
            self._scheduler.shutdown(wait=False)
            self._scheduler = None

    # ── 定时服务 ──────────────────────────────────────────────

    def _schedule_service(self) -> None:
        """注册定时检查服务。"""
        if not self._scheduler:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.start()
        self._scheduler.remove_all_jobs()
        self._scheduler.add_job(
            func=self._do_check,
            trigger=CronTrigger.from_crontab(self._cron),
            name="StaleSubCleaner_check",
            id="StaleSubCleaner_check",
        )
        logger.info(f"定时检查已注册: {self._cron}")

    # ── 核心逻辑 ──────────────────────────────────────────────

    def _do_check(self) -> None:
        """执行过期订阅检查。"""
        logger.info("开始检查过期订阅")
        subscribes = self._subscribe_oper.list(state="R")
        if not subscribes:
            logger.info("无订阅，跳过")
            return

        now = datetime.now()
        cutoff = now - timedelta(days=self._stale_days)
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        cleaned: List[Dict[str, Any]] = []

        for sub in subscribes:
            # 只看电视剧
            if sub.type != MediaType.TV.value:
                continue
            # 跳过洗版订阅
            if sub.best_version:
                continue

            # 获取该订阅的最后下载时间
            last_download = self._get_last_download_time(sub.tmdbid, sub.season)

            if last_download is None:
                # 从未下载过，用订阅创建时间
                last_time = self._parse_time(sub.date)
                if last_time is None:
                    continue
                last_download_str = "从未下载"
            else:
                last_time = last_download
                last_download_str = last_download.strftime("%Y-%m-%d")

            # 计算闲置天数
            stale_days = (now - last_time).days

            if last_time > cutoff:
                continue

            # 超过阈值，取消订阅
            try:
                self._subscribe_oper.update(sid=sub.id, payload={"state": "S"})
                cleaned.append({
                    "name": sub.name,
                    "year": sub.year,
                    "season": sub.season,
                    "last_download": last_download_str,
                    "stale_days": str(stale_days),
                    "cleaned_time": now_str,
                })
                logger.info(
                    f"取消过期订阅: {sub.name} S{sub.season} "
                    f"(闲置 {stale_days} 天, 最后下载: {last_download_str})"
                )
            except Exception as e:
                logger.info(f"取消订阅失败: {sub.name} - {e}")

        self._last_run = now_str
        self._total_cleaned += len(cleaned)
        self._last_cleaned = cleaned
        self.save_data("state", {"total_cleaned": self._total_cleaned})

        logger.info(f"检查完成: 取消 {len(cleaned)} 个过期订阅")

        if self._notify and cleaned:
            names = "、".join(
                f"{c['name']} S{c['season']}({c['stale_days']}天)"
                for c in cleaned[:10]
            )
            suffix = f"等 {len(cleaned)} 个" if len(cleaned) > 10 else ""
            self.post_message(
                title="过期订阅清理",
                text=f"已取消过期订阅: {names}{suffix}",
            )

    def _get_last_download_time(self, tmdbid: int, season: int) -> Optional[datetime]:
        """获取指定订阅的最后一次下载时间。"""
        try:
            records = self._download_oper.get_by_type_tmdbid(
                tmdbid=tmdbid, mtype=MediaType.TV.value
            )
            if not records:
                return None
            # 可能是单个对象或列表
            if not hasattr(records, '__iter__'):
                records = [records]
            latest = None
            for r in records:
                # 按季过滤
                r_season = getattr(r, 'season', None) or getattr(r, 'seasons', None)
                if r_season is not None and r_season != season:
                    continue
                r_time = self._parse_time(getattr(r, 'date', None))
                if r_time and (latest is None or r_time > latest):
                    latest = r_time
            return latest
        except Exception as e:
            logger.debug(f"查询下载记录失败: tmdbid={tmdbid} S{season} - {e}")
        return None

    @staticmethod
    def _parse_time(value: Any) -> Optional[datetime]:
        """解析时间字符串。"""
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        try:
            return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                return datetime.strptime(str(value), "%Y-%m-%d")
            except ValueError:
                return None

    # ── API ───────────────────────────────────────────────────

    async def _api_check(self, apikey: str = "") -> Dict[str, Any]:
        """API: 手动触发检查。"""
        self._do_check()
        return {
            "success": True,
            "cleaned": len(self._last_cleaned),
            "last_run": self._last_run,
        }

    # ── 事件处理 ──────────────────────────────────────────────

    @eventmanager.register(EventType.PluginAction)
    def _on_plugin_action(self, event: Event = None) -> None:
        """处理插件命令事件。"""
        if not event or not event.event_data:
            return
        if event.event_data.get("action") != "stale_sub_clean":
            return
        logger.info("收到手动检查命令")
        self._do_check()
        self.post_message(
            title="过期订阅清理",
            text=f"检查完成\n取消过期订阅: {len(self._last_cleaned)} 个",
        )
