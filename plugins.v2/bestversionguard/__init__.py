"""洗版订阅守护插件。

定时检查所有电视剧订阅的洗版状态：
- 未完结但被误标为洗版的 → 取消洗版，恢复普通订阅
- 已完结的洗版订阅保留不动（由订阅助手魔改版负责创建洗版订阅）

判断逻辑：
1. 媒体库中该季已全集入库 → 视为已完结，保留洗版
   （媒体库检查用「媒体身份 + 本地索引条目 ID」定位，条目名与 TMDB 中文名不一致时也能命中）
2. TMDB status=Ended 且 lack=0 → 视为已完结，保留洗版
3. 其他情况（Returning Series / in_production / lack>0）→ 取消洗版
"""

import time
from datetime import datetime
from app.log import logger
from typing import Any, Dict, List, Optional, Set, Tuple

from apscheduler.triggers.cron import CronTrigger

from app.chain.tmdb import TmdbChain
from app.chain.mediaserver import MediaServerChain
from app.core.config import settings
from app.core.event import eventmanager, Event
from app.db.subscribe_oper import SubscribeOper
from app.plugins import _PluginBase
from app.schemas.types import EventType, MediaSource, MediaType
# 统一用插件 SDK 暴露的领域 MediaInfo：app.schemas 里还有一个同名 pydantic 模型，
# 缺 get_poster_image 等方法，传给主程序媒体库接口会抛 AttributeError。
from app.sdk.media import MediaInfo

# 本地媒体库索引（数据库）查询入口：用于按媒体身份取媒体服务器的条目 ID。
# 主程序版本不同路径可能变化，取不到时降级为「不带条目 ID」查询，插件仍可正常加载运行。
try:
    from app.db.oper.mediaserver import MediaServerOper
except Exception:  # pragma: no cover - 兼容旧版主程序路径
    MediaServerOper = None


# TMDB 身份来源标识：新版 MoviePilot 的订阅/媒体条目用 media_source + media_id 描述媒体身份
TMDB_MEDIA_SOURCE = "themoviedb"



class BestVersionGuard(_PluginBase):
    """洗版订阅守护插件。"""

    plugin_name = "洗版守护Q自用版"
    plugin_desc = "定时检查电视剧订阅：未完结的误标洗版自动取消，恢复普通订阅继续追更。"
    plugin_icon = "https://raw.githubusercontent.com/q10710/MoviePilot-Plugins/main/icons/bestversionguard.png"
    plugin_version = "2.5.6"
    plugin_label = "订阅"
    plugin_author = "Q"
    author_url = "https://github.com/q10710"
    plugin_config_prefix = "bestversionguard_"
    plugin_order = 60
    auth_level = 1

    _enabled = False
    _cron: str = "0 3 * * *"
    _notify: bool = True
    # 媒体库文件丢失（库缺集但订阅认为已完成）时，自动重置订阅触发重新下载
    _reset_missing_enabled: bool = True
    # 同一订阅重置限频天数（0 = 不限频），避免媒体库查询异常导致反复重下
    _reset_cooldown_days: int = 1

    _subscribe_oper = None
    _fixed_count: int = 0
    _last_run: Optional[str] = None
    _last_fixed: List[Dict[str, Any]] = []
    _last_reset: List[Dict[str, Any]] = []
    # 持久化：已取消洗版的订阅 ID，避免重复操作
    _fixed_ids: Set[str] = set()
    # 持久化：已重置订阅 ID -> 上次重置时间戳（用于限频）
    _reset_records: Dict[str, float] = {}

    def init_plugin(self, config: dict = None) -> None:
        self.stop_service()
        self._subscribe_oper = SubscribeOper()
        saved = self.get_data("state") or {}
        self._fixed_ids = set(saved.get("fixed_ids", []))
        self._fixed_count = saved.get("fixed_count", 0)
        self._reset_records = {str(k): float(v) for k, v in (saved.get("reset_records") or {}).items()}
        # 最近一次明细需持久化：否则重启后数据页只显示累计数量、看不到具体影视名
        self._last_run = saved.get("last_run") or self._last_run
        self._last_fixed = saved.get("last_fixed") or []
        self._last_reset = saved.get("last_reset") or []
        if not config:
            self._enabled = False
            return
        self._enabled = bool(config.get("enabled"))
        self._cron = config.get("cron") or "0 3 * * *"
        self._notify = bool(config.get("notify"))
        self._reset_missing_enabled = bool(config.get("reset_missing_enabled", True))
        # 配置缺省时必须回落到 1 天限频：不能用 `or 0`，否则未提交该键时会变成"不限频"
        raw_cooldown = config.get("reset_cooldown_days")
        try:
            self._reset_cooldown_days = int(raw_cooldown) if raw_cooldown not in (None, "") else 1
        except (TypeError, ValueError):
            self._reset_cooldown_days = 1
        logger.info(f"初始化完成, enabled={self._enabled}, cron={self._cron}, "
                    f"库缺集重置={self._reset_missing_enabled}（限频 {self._reset_cooldown_days} 天），"
                    f"已取消 {len(self._fixed_ids)} 条")
        if self._enabled:
            # 定时任务交由 MoviePilot 主调度器统一注册（见 get_service）
            logger.info(f"定时检查将由主调度器注册: {self._cron}")

    def get_state(self) -> bool:
        return self._enabled

    def get_service(self) -> List[Dict[str, Any]]:
        """向 MoviePilot 主调度器注册定时检查服务（标准做法）。

        此前用插件内自建的 BackgroundScheduler：插件重载后该线程会失效且不再恢复，
        定时任务会静默停摆，因此改为标准 get_service()，由主调度器统一管理并自动恢复。
        """
        if not self._enabled or not self._cron:
            return []
        try:
            trigger = CronTrigger.from_crontab(self._cron, timezone=settings.TZ)
        except Exception as err:
            logger.error(f"cron 表达式无效，未注册定时任务：{self._cron} - {err}")
            return []
        return [{
            "id": f"{self.__class__.__name__}Check",
            "name": "洗版守护检查",
            "trigger": trigger,
            "func": self._guard_check,
        }]

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
                    {
                        "component": "VSwitch",
                        "props": {
                            "model": "reset_missing_enabled",
                            "label": "媒体库文件丢失时重置订阅（重新下载）",
                            "hint": ("媒体库中该季确实缺集、但订阅认为已下完（说明文件被删了）时，"
                                     "清空下载事实并恢复缺集数，让订阅重新搜索下载。"),
                            "persistent-hint": True,
                        },
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "reset_cooldown_days",
                            "label": "重置限频（天，0=不限频）",
                            "type": "number",
                            "hint": "同一订阅在该天数内最多重置一次，避免媒体库查询异常导致反复重下",
                            "persistent-hint": True,
                        },
                    },
                ],
            }
        ], {
            "enabled": False,
            "cron": "0 3 * * *",
            "notify": True,
            "reset_missing_enabled": True,
            "reset_cooldown_days": 1,
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
        # 定时任务已交由 MoviePilot 主调度器统一管理，无需在此手动清理
        pass

    # ── 辅助方法 ──────────────────────────────────────────────

    def _save_state(self) -> None:
        self.save_data("state", {
            "fixed_ids": list(self._fixed_ids),
            "fixed_count": self._fixed_count,
            "reset_records": self._reset_records,
            "last_run": self._last_run,
            "last_fixed": (self._last_fixed or [])[:50],
            "last_reset": (self._last_reset or [])[:50],
        })

    def _can_reset(self, subscribe_id: Any) -> bool:
        """判断该订阅当前是否允许重置（按限频天数去抖，0 表示不限频）。"""
        if self._reset_cooldown_days <= 0:
            return True
        last = self._reset_records.get(str(subscribe_id))
        if not last:
            return True
        return (time.time() - float(last)) >= self._reset_cooldown_days * 86400

    def _reset_subscribe(self, sub: Any) -> bool:
        """重置订阅，让它重新搜索下载；字段与主程序「订阅重置」保持一致。

        媒体库那份被删后，主程序仍以为该季已下完（lack_episode=0、note 记录全部集数），
        不会自动补下；清空下载事实并恢复缺集数后，下一轮订阅搜索即会重新获取。
        """
        if not self._subscribe_oper:
            return False
        payload = {
            "note": [],
            "lack_episode": sub.total_episode,
            "current_priority": None,
            "current_audio_format": None,
            "current_bitrate": None,
            "current_bit_depth": None,
            "current_sample_rate": None,
            "episode_priority": {},
            "manual_total_episode": 0,
            "state": "R",
        }
        try:
            self._subscribe_oper.update(sid=sub.id, payload=payload)
        except Exception as e:
            logger.info(f"重置订阅失败: {sub.name} - {e}")
            return False
        self._reset_records[str(sub.id)] = time.time()
        self._save_state()
        return True

    @staticmethod
    def _resolve_tmdbid(sub: Any) -> Optional[int]:
        """从订阅对象解析 TMDB ID。

        新版 MoviePilot 的 Subscribe 已移除 tmdbid 字段，改用 media_source + media_id；
        为兼容两种版本，这里优先读旧字段，其次从 media_source/media_id 推导，
        并兼容 media_source 为枚举/字符串、media_id 为数字字符串两种形态。
        """
        legacy = getattr(sub, "tmdbid", None)
        if legacy:
            try:
                return int(legacy)
            except (TypeError, ValueError):
                pass
        media_source = getattr(sub, "media_source", None)
        if media_source is not None:
            media_source = str(
                getattr(media_source, "value", media_source) or ""
            ).strip().lower()
        if media_source and media_source != TMDB_MEDIA_SOURCE:
            return None
        media_id = getattr(sub, "media_id", None)
        if not media_id:
            return None
        try:
            return int(str(media_id).strip())
        except (TypeError, ValueError):
            return None

    def _resolve_library_item_id(self, season: int, title: str, year: str,
                                 media_id: Optional[str]) -> Optional[str]:
        """按媒体身份从本地媒体库索引取媒体服务器条目 ID（与主程序查库口径一致）。

        只按标题查询时，媒体服务器模块在拿不到条目 ID 的前提下会退回「标题严格相等」匹配；
        库内条目名与 TMDB 中文名不一致时（例如库内为英文名 The Agency、TMDB 中文名「传奇办公室」）
        该匹配必然失败，导致「库内已全集」被误判为「库内没有」。带条目 ID 后再查可精确命中。
        """
        if MediaServerOper is None or not media_id:
            return None
        try:
            return MediaServerOper().get_item_id(
                title=title,
                year=year,
                mtype=MediaType.TV.value,
                media_source=TMDB_MEDIA_SOURCE,
                media_id=str(media_id),
                season=season,
            )
        except Exception as e:
            logger.debug(f"查询媒体库索引失败: {title} S{season} - {e}")
            return None

    def _query_media_exists(self, season: int, title: str, year: str,
                            media_id: Optional[str] = None,
                            itemid: Optional[str] = None) -> Set[int]:
        """查询媒体服务器中该季已入库的集号；media_id 为空表示不按媒体身份查询。"""
        try:
            mi = MediaInfo()
            mi.type = MediaType.TV
            mi.season = season
            mi.title = title
            mi.year = year
            if media_id:
                mi.media_source = MediaSource.TMDB
                mi.media_id = str(media_id)
                if str(media_id).isdigit():
                    mi.tmdb_id = int(media_id)
            chain = MediaServerChain()
            # 不传 server：主程序会遍历全部已配置的媒体服务器（本机 Emby、飞牛影视等）
            exists = chain.media_exists(mediainfo=mi, itemid=itemid)
            if exists and exists.seasons:
                eps = exists.seasons.get(season) or exists.seasons.get(str(season))
                if eps:
                    return set(eps)
        except Exception as e:
            logger.debug(f"查询媒体库失败: {title} S{season} - {e}")
        return set()

    def _get_library_episodes(self, tmdbid: int, season: int, title: str, year: str,
                              media_id: Optional[str] = None) -> Set[int]:
        """取媒体库中该季已入库的集号。

        主口径：完整媒体身份 + 本地索引条目 ID（条目名不一致也能命中）；
        兜底：不带条目 ID 按原方式再查一次，覆盖本地索引中没有身份记录的媒体服务器。
        """
        resolved_id = str(media_id or tmdbid)
        itemid = self._resolve_library_item_id(season, title, year, resolved_id)
        episodes = self._query_media_exists(season, title, year,
                                            media_id=resolved_id, itemid=itemid)
        if not episodes:
            episodes = self._query_media_exists(season, title, year)
        return episodes

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

    def _missing_library_episodes(self, tmdbid: int, season: int, total: int,
                                  title: str, year: str,
                                  media_id: Optional[str] = None) -> List[int]:
        """返回媒体库中该季缺失的集号；库内全集时返回空列表。"""
        if total <= 0:
            return []
        lib_eps = self._get_library_episodes(tmdbid, season, title, year, media_id)
        return [ep for ep in range(1, total + 1) if ep not in lib_eps]

    @staticmethod
    def _format_missing_episodes(missing_eps: List[int], max_show: int = 12) -> str:
        """把缺失集号格式化为可读文本；缺失较少时列出具体集号。"""
        if not missing_eps:
            return "0 集"
        if len(missing_eps) <= max_show:
            detail = "、".join(f"E{ep:02d}" for ep in missing_eps)
            return f"{len(missing_eps)} 集（{detail}）"
        return f"{len(missing_eps)} 集"

    # ── 核心逻辑 ──────────────────────────────────────────────

    # 订阅助手魔改版创建洗版订阅时使用的 username
    ASSISTANT_USERNAME = "订阅助手魔改版"

    def _guard_check(self) -> None:
        logger.info("开始检查")
        subscribes = self._subscribe_oper.list(state=None)
        if not subscribes:
            logger.info("无订阅，跳过")
            return

        tmdb_chain = TmdbChain()
        fixed_list: List[Dict[str, Any]] = []
        reset_list: List[Dict[str, Any]] = []
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
            # 新版 MoviePilot 的 Subscribe 已移除 tmdbid 字段，改用 media_source + media_id
            tmdbid = self._resolve_tmdbid(sub)
            if not tmdbid:
                continue
            # 跳过用户手动暂停(S)的订阅，只处理运行中(R)、待定(P)、新建(N)等
            if sub.state == "S":
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

            tmdb_info = _get_tmdb(tmdbid)
            if not tmdb_info:
                continue

            tmdb_status = tmdb_info.get("status", "")
            in_production = tmdb_info.get("in_production", True)

            # 获取该季 TMDB 总集数，判断媒体库是否全集入库
            season_total = self._get_tmdb_season_total(tmdbid, sub.season)
            missing_eps: List[int] = []
            if season_total > 0:
                missing_eps = self._missing_library_episodes(
                    tmdbid, sub.season, season_total, sub.name, sub.year,
                    getattr(sub, "media_id", None),
                )
            lib_complete = season_total > 0 and not missing_eps

            # 判断该季是否已完结
            season_ended = lib_complete or (
                tmdb_status == "Ended" and (not sub.lack_episode or sub.lack_episode <= 0)
            )

            # ── 媒体库文件丢失：库确实缺集，但订阅认为已下完（lack_episode<=0） ──
            # 说明媒体库那份被删了（如硬链接断开后被清理），主程序不会自动补下，
            # 需重置订阅把下载事实清空、恢复缺集数，让后续订阅搜索重新获取并洗版整理。
            if (
                self._reset_missing_enabled
                and season_total > 0
                and not lib_complete
                and (sub.total_episode or 0) > 0
                and (not sub.lack_episode or sub.lack_episode <= 0)
            ):
                if sub.state == "P":
                    # 已有下载在进行，等它跑完再判断，避免重复下载
                    logger.info(f"库缺集但当前有下载进行中，暂不重置: {sub.name} S{sub.season}")
                    continue
                if not self._can_reset(sub.id):
                    logger.info(f"库缺集但处于重置限频期（{self._reset_cooldown_days} 天），跳过: "
                                f"{sub.name} S{sub.season}")
                    continue
                missing_text = self._format_missing_episodes(missing_eps)
                if self._reset_subscribe(sub):
                    logger.info(f"库缺集重置订阅: {sub.name} S{sub.season} "
                                f"(媒体库缺 {missing_text}，等待重新下载)")
                    reset_list.append({
                        "name": sub.name, "year": sub.year, "season": sub.season,
                        "reason": f"媒体库缺 {missing_text}但订阅认为已下完",
                        "reset_time": now_str,
                    })
                    continue

            if season_ended:
                fix_key = str(sub.id)
                if fix_key in self._fixed_ids:
                    self._fixed_ids.discard(fix_key)
                    self._save_state()
                # 已完结的洗版订阅，确保只下载整季合集而非散装
                if not sub.best_version_full:
                    try:
                        self._subscribe_oper.update(sid=sub.id, payload={"best_version_full": 1})
                        logger.info(f"设置整季洗版: {sub.name} S{sub.season}")
                    except Exception as e:
                        logger.info(f"设置整季洗版失败: {sub.name} - {e}")
                continue

            # ── 未完结，需要处理 ──
            fix_key = str(sub.id)
            if fix_key in self._fixed_ids:
                # 之前取消过洗版但被重新开启了，清除记录重新处理
                self._fixed_ids.discard(fix_key)

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
        # 仅在本轮确有结果时更新明细，保留最近一次非空清单（重启后数据页仍能看到影视名）
        if fixed_list:
            self._last_fixed = fixed_list
        if reset_list:
            self._last_reset = reset_list
        self._save_state()

        logger.info(f"检查完成: 取消 {len(fixed_list)} 个，库缺集重置 {len(reset_list)} 个")

        if self._notify and (fixed_list or reset_list):
            lines = self._format_lists(fixed_list, reset_list)
            self.post_message(title="洗版守护", text="\n".join(lines))

    @staticmethod
    def _format_lists(fixed_list: List[Dict[str, Any]],
                      reset_list: List[Dict[str, Any]],
                      limit: int = 10) -> List[str]:
        """格式化取消洗版/重置订阅清单（前 N 个明细 + 总数后缀），供通知复用。"""
        lines: List[str] = []
        if fixed_list:
            names = "、".join(
                f"{f.get('name', '')} S{f.get('season')}" for f in fixed_list[:limit]
            )
            suffix = f"等 {len(fixed_list)} 个" if len(fixed_list) > limit else ""
            lines.append(f"取消洗版: {names}{suffix}")
        if reset_list:
            names = "、".join(
                f"{f.get('name', '')} S{f.get('season')}" for f in reset_list[:limit]
            )
            suffix = f"等 {len(reset_list)} 个" if len(reset_list) > limit else ""
            lines.append(f"媒体库文件丢失，已重置订阅重新下载: {names}{suffix}")
        return lines

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
        lines = self._format_lists(self._last_fixed, self._last_reset)
        if lines:
            text = f"检查完成（最近一次 {self._last_run or '未知'}）\n" + "\n".join(lines)
        else:
            text = "检查完成\n未发现需要处置的订阅"
        self.post_message(title="洗版守护", text=text)
