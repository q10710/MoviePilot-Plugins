"""过期订阅清理插件。

检查电视剧订阅，超过指定天数没有下载过新剧集的自动取消订阅。
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

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

    plugin_name = "过期订阅清理Q自用版"
    plugin_desc = "检查电视剧订阅，超过指定天数未下载新剧集则自动取消订阅。"
    plugin_icon = "https://raw.githubusercontent.com/q10710/MoviePilot-Plugins/main/icons/stalesubcleaner.png"
    plugin_version = "1.1.9"
    plugin_label = "订阅"
    plugin_author = "Q"
    author_url = "https://github.com/q10710"
    plugin_config_prefix = "stalesubcleaner_"
    plugin_order = 61
    auth_level = 1

    _enabled = False
    _cron: str = "0 6 * * *"
    _stale_days: int = 15
    _notify: bool = True

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
        # 最近一次取消明细需持久化：否则重启后数据页只显示累计数量、看不到具体影视名
        self._last_run = saved.get("last_run") or self._last_run
        self._last_cleaned = saved.get("cleaned") or []
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
            # 定时任务交由 MoviePilot 主调度器统一注册（见 get_service）
            logger.info(f"定时检查将由主调度器注册: {self._cron}")

    def get_state(self) -> bool:
        """获取插件启用状态。"""
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
            "name": "过期订阅清理检查",
            "trigger": trigger,
            "func": self._do_check,
        }]

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
        """返回插件详情页面（2026-09-17 按统一界面标准改版，仅调整展示层）。"""
        if not self._enabled:
            return [{
                "component": "VAlert",
                "props": {
                    "type": "warning",
                    "variant": "tonal",
                    "density": "compact",
                    "prepend-icon": "mdi-alert-outline",
                    "text": "插件未启用。启用后按周期检查电视剧订阅，闲置超过阈值的自动取消（置为停止，可恢复）。",
                },
            }]

        accent, ok, warn, info_c = "#6366f1", "#10b981", "#f59e0b", "#3b82f6"

        def _rgba(hex_color: str, alpha: float) -> str:
            h = hex_color.lstrip("#")
            return f"rgba({int(h[0:2], 16)},{int(h[2:4], 16)},{int(h[4:6], 16)},{alpha})"

        def _tile(icon: str, color: str, size: int = 44) -> dict:
            return {
                "component": "div",
                "props": {
                    "class": "d-flex align-center justify-center flex-shrink-0",
                    "style": (f"width: {size}px; height: {size}px; border-radius: 12px; "
                              f"background: {_rgba(color, 0.14)};"),
                },
                "content": [{
                    "component": "VIcon",
                    "props": {"size": int(size * 0.55), "style": f"color: {color};"},
                    "text": icon,
                }],
            }

        def _chip(text: str, icon: str, color: str) -> dict:
            return {
                "component": "VChip",
                "props": {"size": "small", "variant": "tonal", "color": color},
                "content": [
                    {"component": "VIcon", "props": {"size": 14, "class": "mr-1"}, "text": icon},
                    {"component": "span", "text": text},
                ],
            }

        def _stat(value, label: str, hint: str, icon: str, color: str) -> dict:
            return {
                "component": "div",
                "props": {
                    "class": "d-flex align-center ga-3 h-100 pa-3",
                    "style": (f"background: {_rgba(color, 0.08)}; border: 1px solid {_rgba(color, 0.22)}; "
                              f"border-radius: 12px;"),
                },
                "content": [
                    _tile(icon, color, 40),
                    {
                        "component": "div",
                        "content": [
                            {"component": "div",
                             "props": {"class": "text-h5 font-weight-black", "style": "line-height: 1.1;"},
                             "text": str(value)},
                            {"component": "div", "props": {"class": "text-body-2 font-weight-medium"},
                             "text": label},
                            {"component": "div",
                             "props": {"class": "text-caption text-medium-emphasis",
                                       "style": "white-space: normal;"},
                             "text": hint},
                        ],
                    },
                ],
            }

        last_run = self._last_run or "尚未运行"
        cleaned = [c for c in (self._last_cleaned or []) if isinstance(c, dict)]
        cron_text = getattr(self, "_cron", "") or "未设置"
        notify_on = bool(getattr(self, "_notify", True))

        page: List[dict] = []

        # ① 概览头部
        page.append({
            "component": "VCard",
            "props": {"variant": "flat", "rounded": "xl", "class": "mb-4 overflow-hidden",
                      "style": "border: 1px solid rgba(128,128,128,0.18); position: relative;"},
            "content": [
                {"component": "div",
                 "props": {"class": "d-none d-sm-flex",
                           "style": (f"position: absolute; width: 180px; height: 180px; border-radius: 50%; "
                                     f"top: -70px; left: -50px; background: {_rgba(accent, 0.08)};")}},
                {"component": "div",
                 "props": {"class": "d-none d-sm-flex",
                           "style": (f"position: absolute; width: 220px; height: 220px; border-radius: 50%; "
                                     f"bottom: -120px; right: -60px; background: {_rgba(ok, 0.07)};")}},
                {
                    "component": "div",
                    "props": {"class": "pa-4", "style": "position: relative;"},
                    "content": [
                        {
                            "component": "div",
                            "props": {"class": "d-flex align-center ga-3"},
                            "content": [
                                _tile("mdi-timer-sand-empty", accent, 48),
                                {
                                    "component": "div",
                                    "content": [
                                        {"component": "div", "props": {"class": "text-h6 font-weight-bold"},
                                         "text": "过期订阅清理"},
                                        {"component": "div",
                                         "props": {"class": "text-caption text-medium-emphasis"},
                                         "text": "闲置超过阈值的电视剧订阅自动取消（置为停止，可恢复）"},
                                    ],
                                },
                                {"component": "VSpacer"},
                                {
                                    "component": "VBtn",
                                    "props": {"color": "primary", "variant": "flat", "size": "small",
                                              "prepend-icon": "mdi-magnify-scan"},
                                    "text": "立即检查",
                                    "events": {"click": {
                                        "api": "plugin/StaleSubCleaner/check",
                                        "method": "get",
                                        "params": {"apikey": settings.API_TOKEN},
                                    }},
                                },
                            ],
                        },
                        {"component": "VDivider", "props": {"class": "my-3"}},
                        {
                            "component": "div",
                            "props": {"class": "d-flex flex-wrap ga-2"},
                            "content": [
                                _chip(f"上次检查 {last_run}", "mdi-clock-outline", "primary"),
                                _chip(f"周期 {cron_text}", "mdi-calendar-clock", "info"),
                                _chip(f"阈值 {self._stale_days} 天", "mdi-timer-sand", "warning"),
                                _chip("通知 开启" if notify_on else "通知 关闭",
                                      "mdi-bell-outline", "success" if notify_on else "secondary"),
                            ],
                        },
                    ],
                },
            ],
        })

        # ② 统计卡片
        page.append({
            "component": "VRow",
            "props": {"dense": True, "class": "mb-4"},
            "content": [
                {"component": "VCol", "props": {"cols": 12, "sm": 6, "md": 3},
                 "content": [_stat(self._total_cleaned or 0, "累计取消订阅",
                                   "历史累计置为停止的订阅数", "mdi-archive-cancel-outline", accent)]},
                {"component": "VCol", "props": {"cols": 12, "sm": 6, "md": 3},
                 "content": [_stat(len(cleaned), "最近一次取消",
                                   "最近一轮检查取消的订阅数", "mdi-format-list-bulleted", ok)]},
                {"component": "VCol", "props": {"cols": 12, "sm": 6, "md": 3},
                 "content": [_stat(f"{self._stale_days} 天", "闲置阈值",
                                   "最后一次成功下载距今超过该天数即取消", "mdi-timer-sand", warn)]},
                {"component": "VCol", "props": {"cols": 12, "sm": 6, "md": 3},
                 "content": [_stat("启用" if self._enabled else "停用", "运行状态",
                                   f"上次运行 {last_run}", "mdi-shield-check-outline", info_c)]},
            ],
        })

        # ③ 明细卡片
        if cleaned:
            rows: List[dict] = []
            for item in cleaned[:50]:
                season = item.get("season")
                rows.append({
                    "component": "tr",
                    "content": [
                        {"component": "td", "props": {"class": "text-body-2"},
                         "text": item.get("name") or "-"},
                        {"component": "td", "props": {"class": "text-body-2"},
                         "text": f"S{season}" if season else "-"},
                        {"component": "td", "props": {"class": "text-caption"},
                         "text": item.get("last_download") or "-"},
                        {"component": "td", "content": [{
                            "component": "VChip",
                            "props": {"size": "x-small", "variant": "tonal", "color": "warning"},
                            "text": f"{item.get('stale_days') or '-'} 天",
                        }]},
                        {"component": "td", "props": {"class": "text-caption"},
                         "text": item.get("cleaned_time") or "-"},
                    ],
                })
            detail_content = [{
                "component": "div",
                "props": {"style": "max-height: 420px; overflow: auto; scrollbar-width: thin;"},
                "content": [{
                    "component": "VTable",
                    "props": {"density": "comfortable", "hover": True, "style": "min-width: 560px;"},
                    "content": [
                        {"component": "thead", "content": [{"component": "tr", "content": [
                            {"component": "th", "text": "订阅"},
                            {"component": "th", "text": "季"},
                            {"component": "th", "text": "最后下载"},
                            {"component": "th", "text": "闲置天数"},
                            {"component": "th", "text": "取消时间"},
                        ]}]},
                        {"component": "tbody", "content": rows},
                    ],
                }],
            }]
        else:
            detail_content = [{
                "component": "VAlert",
                "props": {"type": "success", "variant": "tonal", "density": "compact",
                          "prepend-icon": "mdi-check-circle-outline",
                          "text": "最近一轮检查没有取消任何订阅。"},
            }]

        page.append({
            "component": "VCard",
            "props": {"variant": "flat", "rounded": "xl", "class": "mb-3 overflow-hidden",
                      "style": "border: 1px solid rgba(128,128,128,0.18);"},
            "content": [
                {"component": "div",
                 "props": {"class": "d-flex align-center ga-3 px-4 pt-4 pb-3"},
                 "content": [
                     _tile("mdi-format-list-bulleted", warn, 34),
                     {"component": "div", "props": {"class": "text-subtitle-1 font-weight-bold"},
                      "text": "最近取消的订阅"},
                     {"component": "VSpacer"},
                     _chip(f"{len(cleaned)} 条", "mdi-counter", "warning"),
                 ]},
                {"component": "VDivider"},
                {"component": "VCardText", "props": {"class": "px-4 pt-3 pb-4"},
                 "content": detail_content},
            ],
        })

        # ④ 口径说明
        page.append({
            "component": "div",
            "props": {"class": "d-flex ga-3 pa-3 mt-1",
                      "style": "background: rgba(139,92,246,0.08); border-radius: 12px;"},
            "content": [
                {"component": "VIcon",
                 "props": {"size": "small", "class": "mt-1", "style": f"color: {'#8b5cf6'};"},
                 "text": "mdi-information-outline"},
                {"component": "div", "props": {"class": "text-caption", "style": "line-height: 1.7;"},
                 "content": [
                     {"component": "div", "props": {"class": "font-weight-bold"}, "text": "判定口径"},
                     {"component": "div", "text": "· 只处理电视剧订阅，洗版订阅跳过。"},
                     {"component": "div",
                      "text": f"· 闲置天数按「该订阅范围内最后一次成功下载」距今天数计算，超过 {self._stale_days} 天即取消。"},
                     {"component": "div", "text": "· 取消 = 置为停止（S），记录保留，可手动恢复。"},
                 ]},
            ],
        })

        return page

    def stop_service(self) -> None:
        """停止插件后台服务并释放资源。"""
        # 定时任务已交由 MoviePilot 主调度器统一管理，无需在此手动清理
        pass

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
            identity = self._resolve_media_identity(sub)
            if not identity:
                logger.info(f"跳过订阅（无法解析媒体身份）: {sub.name} S{sub.season}")
                continue
            last_download = self._get_last_download_time(identity[0], identity[1], sub.season)

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
        saved_state = {
            "total_cleaned": self._total_cleaned,
            "last_run": now_str,
        }
        # 仅在本轮确有取消时更新明细，保留最近一次非空清单（重启后数据页仍能看到影视名）
        if cleaned:
            self._last_cleaned = cleaned
            saved_state["cleaned"] = cleaned[:50]
        self.save_data("state", saved_state)

        logger.info(f"检查完成: 取消 {len(cleaned)} 个过期订阅")

        if self._notify and cleaned:
            self.post_message(
                title="过期订阅清理",
                text=f"已取消过期订阅: {self._format_cleaned_names(cleaned)}",
            )

    @staticmethod
    def _format_cleaned_names(cleaned: List[Dict[str, Any]], limit: int = 10) -> str:
        """格式化取消清单（前 N 个明细 + 总数后缀），供通知复用。"""
        if not cleaned:
            return "无"
        names = "、".join(
            f"{c.get('name', '')} S{c.get('season')}({c.get('stale_days')}天)"
            for c in cleaned[:limit]
        )
        suffix = f"等 {len(cleaned)} 个" if len(cleaned) > limit else ""
        return f"{names}{suffix}"

    @staticmethod
    def _resolve_media_identity(sub: Any) -> Optional[Tuple[str, str]]:
        """解析订阅的媒体身份（media_source + media_id），兼容旧版 tmdbid 字段。

        新版 MoviePilot 的 Subscribe 已移除 tmdbid，改用 media_source + media_id；
        旧字段仅作兜底，两者都取不到时返回 None，由调用方跳过该订阅。
        """
        media_source = getattr(sub, "media_source", None)
        if media_source is not None:
            media_source = str(getattr(media_source, "value", media_source) or "").strip().lower()
        media_id = getattr(sub, "media_id", None)
        if media_source and media_id:
            return media_source, str(media_id).strip()
        legacy = getattr(sub, "tmdbid", None)
        if legacy:
            return "themoviedb", str(legacy)
        return None

    def _get_last_download_time(self, media_source: str, media_id: str,
                               season: int) -> Optional[datetime]:
        """按媒体身份获取指定订阅的最后一次下载时间（按季过滤）。"""
        try:
            records = self._download_oper.get_by_media_identity(
                media_source=media_source, media_id=str(media_id)
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
                if r_season:
                    season_text = str(r_season).upper()
                    if season is not None and (
                        f"S{int(season):02d}" not in season_text
                        and f"S{season}" not in season_text
                    ):
                        continue
                r_time = self._parse_time(getattr(r, 'date', None))
                if r_time and (latest is None or r_time > latest):
                    latest = r_time
            return latest
        except Exception as e:
            logger.debug(f"查询下载记录失败: {media_source}:{media_id} S{season} - {e}")
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
        if self._last_cleaned:
            detail = f"取消过期订阅 {len(self._last_cleaned)} 个: {self._format_cleaned_names(self._last_cleaned)}"
        else:
            detail = "取消过期订阅: 0 个"
        self.post_message(
            title="过期订阅清理",
            text=f"检查完成\n{detail}",
        )
