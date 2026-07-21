"""洗版订阅守护插件。

定时检查所有电视剧订阅的洗版状态：
- 未完结但被误标为洗版的 → 取消洗版，恢复普通订阅
- 已完结的洗版订阅保留不动（由订阅助手魔改版负责创建洗版订阅）

判断逻辑：
1. 媒体库中该季已全集入库 → 视为已完结，保留洗版
2. TMDB status=Ended 且 lack=0 → 视为已完结，保留洗版
3. 其他情况（Returning Series / in_production / lack>0）→ 取消洗版
"""

from datetime import datetime
from app.log import logger
from typing import Any, Dict, List, Optional, Set, Tuple

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.chain.tmdb import TmdbChain
from app.chain.mediaserver import MediaServerChain
from app.core.config import settings
from app.core.event import eventmanager, Event
from app.db.subscribe_oper import SubscribeOper
from app.plugins import _PluginBase
from app.schemas import MediaInfo
from app.schemas.types import EventType, MediaType



class BestVersionGuard(_PluginBase):
    """洗版订阅守护插件。"""

    plugin_name = "洗版守护"
    plugin_desc = "定时检查电视剧订阅：未完结的误标洗版自动取消，恢复普通订阅继续追更。"
    plugin_icon = "bestversionguard.png"
    plugin_version = "2.4.1"
    plugin_label = "订阅"
    plugin_author = "local"
    plugin_config_prefix = "bestversionguard_"
    plugin_order = 60
    auth_level = 1

    _enabled = False
    _cron: str = "0 3 * * *"
    _notify: bool = True
    _scheduler = None
    _subscribe_oper = None
    _fixed_count: int = 0
    _last_run: Optional[str] = None
    _last_fixed: List[Dict[str, Any]] = []
    # 持久化：已取消洗版的订阅 ID，避免重复操作
    _fixed_ids: Set[str] = set()

    def init_plugin(self, config: dict = None) -> None:
        self.stop_service()
        self._subscribe_oper = SubscribeOper()
        saved = self.get_data("state") or {}
        self._fixed_ids = set(saved.get("fixed_ids", []))
        self._fixed_count = saved.get("fixed_count", 0)
        if not config:
            self._enabled = False
            return
        self._enabled = bool(config.get("enabled"))
        self._cron = config.get("cron") or "0 3 * * *"
        self._notify = bool(config.get("notify"))
        logger.info(f"初始化完成, enabled={self._enabled}, cron={self._cron}, "
                    f"已取消 {len(self._fixed_ids)} 条")
        if self._enabled:
            self._schedule_service()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return [
            {
                "cmd": "/bestversion_guard",
                "event": EventType.PluginAction,
                "desc": "手动触发洗版守护检查",
                "category": "订阅",
                "data": {"action": "bestversion_guard_check"},
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/check",
                "endpoint": self._api_check,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "手动触发洗版检查",
            },
        ]

    def get_form(self) -> Tuple[Optional[List[dict]], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VSwitch",
                        "props": {"model": "enabled", "label": "启用插件"},
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "cron",
                            "label": "检查时间（Cron 表达式）",
                            "placeholder": "0 3 * * *",
                        },
                    },
                    {
                        "component": "VSwitch",
                        "props": {"model": "notify", "label": "操作时发送通知"},
                    },
                ],
            }
        ], {
            "enabled": False,
            "cron": "0 3 * * *",
            "notify": True,
        }

    def get_page(self) -> Optional[List[dict]]:
        if not self._enabled:
            return [{"component": "VAlert", "props": {"type": "warning", "text": "插件未启用"}}]

        last_run = self._last_run or "尚未运行"
        page = [
            {
                "component": "VCard",
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {"title": "洗版守护"},
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
                                        f"累计取消洗版: {self._fixed_count} 个"
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
                                        "api": "plugin/BestVersionGuard/check",
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

        if self._last_fixed:
            page.append({
                "component": "VCard",
                "content": [
                    {"component": "VCardTitle", "props": {"title": "最近取消洗版"}},
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VDataTable",
                                "props": {
                                    "headers": [
                                        {"title": "订阅", "key": "name"},
                                        {"title": "季", "key": "season"},
                                        {"title": "原因", "key": "reason"},
                                        {"title": "时间", "key": "fixed_time"},
                                    ],
                                    "items": self._last_fixed[:50],
                                    "itemsPerPage": 20,
                                },
                            }
                        ],
                    },
                ],
            })

        return page

    def stop_service(self) -> None:
        if self._scheduler:
            self._scheduler.shutdown(wait=False)
            self._scheduler = None

    # ── 定时服务 ──────────────────────────────────────────────

    def _schedule_service(self) -> None:
        if not self._scheduler:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.start()
        self._scheduler.remove_all_jobs()
        self._scheduler.add_job(
            func=self._guard_check,
            trigger=CronTrigger.from_crontab(self._cron),
            name="BestVersionGuard_check",
            id="BestVersionGuard_check",
        )
        logger.info(f"定时检查已注册: {self._cron}")

    # ── 辅助方法 ──────────────────────────────────────────────

    def _save_state(self) -> None:
        self.save_data("state", {
            "fixed_ids": list(self._fixed_ids),
            "fixed_count": self._fixed_count,
        })

    def _get_library_episodes(self, tmdbid: int, season: int, title: str, year: str) -> Set[int]:
        try:
            mi = MediaInfo()
            mi.tmdb_id = tmdbid
            mi.type = MediaType.TV
            mi.season = season
            mi.title = title
            mi.year = year
            chain = MediaServerChain()
            exists = chain.media_exists(mediainfo=mi)
            if exists and exists.seasons:
                eps = exists.seasons.get(season) or exists.seasons.get(str(season))
                if eps:
                    return set(eps)
        except Exception as e:
            logger.debug(f"查询媒体库失败: {title} S{season} - {e}")
        return set()

    def _get_tmdb_season_total(self, tmdbid: int, season: int) -> int:
        try:
            tmdb_chain = TmdbChain()
            seasons_info = tmdb_chain.tmdb_seasons(tmdbid=tmdbid)
            if seasons_info:
                for s in seasons_info:
                    sn = getattr(s, "season_number", None)
                    if sn == season:
                        return getattr(s, "episode_count", 0) or 0
        except Exception:
            pass
        return 0

    def _is_season_complete(self, tmdbid: int, season: int, total: int, title: str, year: str) -> bool:
        """判断指定季是否已完结（媒体库全集入库）。"""
        if total <= 0:
            return False
        lib_eps = self._get_library_episodes(tmdbid, season, title, year)
        if not lib_eps:
            return False
        for ep in range(1, total + 1):
            if ep not in lib_eps:
                return False
        return True

    # ── 核心逻辑 ──────────────────────────────────────────────

    # 订阅助手魔改版创建洗版订阅时使用的 username
    ASSISTANT_USERNAME = "订阅助手魔改版"

    def _guard_check(self) -> None:
        logger.info("开始检查")
        subscribes = self._subscribe_oper.list(state="R")
        if not subscribes:
            logger.info("无订阅，跳过")
            return

        tmdb_chain = TmdbChain()
        fixed_list: List[Dict[str, Any]] = []
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        tmdb_cache: Dict[int, dict] = {}

        def _get_tmdb(tmdbid: int) -> Optional[dict]:
            if tmdbid not in tmdb_cache:
                try:
                    tmdb_cache[tmdbid] = tmdb_chain.tmdb_info(mtype=MediaType.TV, tmdbid=tmdbid) or {}
                except Exception:
                    tmdb_cache[tmdbid] = {}
            return tmdb_cache[tmdbid] or None

        for sub in subscribes:
            if sub.type != MediaType.TV.value:
                continue
            if not sub.tmdbid:
                continue

            # ── 处理订阅助手魔改版创建的洗版订阅 ──
            is_assistant_best = (
                sub.best_version
                and getattr(sub, "username", "") == self.ASSISTANT_USERNAME
            )
            # ── 处理普通订阅的 best_version 标记 ──
            is_normal_best = sub.best_version and not is_assistant_best

            if not is_assistant_best and not is_normal_best:
                continue

            tmdb_info = _get_tmdb(sub.tmdbid)
            if not tmdb_info:
                continue

            tmdb_status = tmdb_info.get("status", "")
            in_production = tmdb_info.get("in_production", True)

            # 获取该季 TMDB 总集数，判断媒体库是否全集入库
            season_total = self._get_tmdb_season_total(sub.tmdbid, sub.season)
            lib_complete = self._is_season_complete(
                sub.tmdbid, sub.season, season_total, sub.name, sub.year
            ) if season_total > 0 else False

            # 判断该季是否已完结
            season_ended = lib_complete or (
                tmdb_status == "Ended" and (not sub.lack_episode or sub.lack_episode <= 0)
            )

            if season_ended:
                fix_key = str(sub.id)
                if fix_key in self._fixed_ids:
                    self._fixed_ids.discard(fix_key)
                    self._save_state()
                continue

            # ── 未完结，需要处理 ──
            fix_key = str(sub.id)
            if fix_key in self._fixed_ids:
                continue

            if in_production:
                reason = "TMDB 制作中"
            elif tmdb_status == "Ended":
                reason = f"TMDB 已完结但缺 {sub.lack_episode} 集"
            else:
                reason = f"TMDB {tmdb_status}"

            if is_assistant_best:
                # 订阅助手魔改版创建的洗版订阅，取消洗版标记（不删除，保留订阅继续追更）
                try:
                    self._subscribe_oper.update(sid=sub.id, payload={"best_version": 0})
                    self._fixed_ids.add(fix_key)
                    self._save_state()
                    fixed_list.append({
                        "name": sub.name, "year": sub.year, "season": sub.season,
                        "reason": reason, "fixed_time": now_str,
                    })
                    logger.info(f"取消洗版(助手): {sub.name} S{sub.season} ({reason})")
                except Exception as e:
                    logger.info(f"取消洗版失败: {sub.name} - {e}")
            else:
                # 普通订阅的 best_version 标记，取消洗版
                try:
                    self._subscribe_oper.update(sid=sub.id, payload={"best_version": 0})
                    self._fixed_ids.add(fix_key)
                    self._save_state()
                    fixed_list.append({
                        "name": sub.name, "year": sub.year, "season": sub.season,
                        "reason": reason, "fixed_time": now_str,
                    })
                    logger.info(f"取消洗版: {sub.name} S{sub.season} ({reason})")
                except Exception as e:
                    logger.info(f"取消洗版失败: {sub.name} - {e}")

        self._last_run = now_str
        self._fixed_count += len(fixed_list)
        self._last_fixed = fixed_list

        logger.info(f"检查完成: 取消 {len(fixed_list)} 个")

        if self._notify and fixed_list:
            names = "、".join(f"{f['name']} S{f['season']}" for f in fixed_list[:10])
            suffix = f"等 {len(fixed_list)} 个" if len(fixed_list) > 10 else ""
            self.post_message(title="洗版守护", text=f"取消洗版: {names}{suffix}")

    # ── API ───────────────────────────────────────────────────

    async def _api_check(self, apikey: str = "") -> Dict[str, Any]:
        self._guard_check()
        return {
            "success": True,
            "fixed_count": len(self._last_fixed),
            "last_run": self._last_run,
        }

    # ── 事件处理 ──────────────────────────────────────────────

    @eventmanager.register(EventType.PluginAction)
    def _on_plugin_action(self, event: Event = None) -> None:
        if not event or not event.event_data:
            return
        if event.event_data.get("action") != "bestversion_guard_check":
            return
        logger.info("收到手动检查命令")
        self._guard_check()
        self.post_message(
            title="洗版守护",
            text=f"检查完成\n取消洗版: {len(self._last_fixed)} 个",
        )
