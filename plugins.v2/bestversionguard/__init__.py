"""洗版订阅守护插件。

职责分两块，**只看订阅指向的那一季**，不看整剧状态：

1) **订阅新增的那一刻**（监听 SubscribeAdded，本插件唯一的「普通订阅 → 洗版」入口）：
   判断这一季是否已播完——已播完就直接开整季洗版（best_version / best_version_full = 1），
   避免同一批集反复「下不动 → 超时删种 → 补搜 → 再下」的空转；
   未播完就让它当普通订阅正常追更（若新订阅本身带着洗版标记，则取消该标记恢复普通订阅）。
   这台订阅从哪来（手动 / 缺失插件补建 / 榜单命中）不影响判定。
2) **定时巡检**（每小时）：只维护**已经是洗版**的订阅——
   该季未播完 → 取消洗版、恢复普通订阅继续追更；该季已播完 → 维持整季洗版并在库缺集时重置洗版进度。
   **已存在订阅的洗版不由本插件开启**：它们逐集正常下载，等这一季下完，由订阅助手Q 的洗版编排
   在「订阅完成」时自动新建整季洗版订阅（用户口径 2026-09-16）。**普通订阅在巡检里一律不处理**。

判断逻辑：
1. 该季是否播完：取该季全部分集的 air_date，全部已过才算播完（只考虑单季）；
   若媒体库该季已齐全，同样按「已播完」处理，并兜住 TMDB 分集日期缺失/异常的情况
2. 该季已播完但媒体库缺集（整季缺或个别集缺）→ 重置洗版进度（清 current_priority），
   让主程序重新搜索补集 —— 顶档（current_priority=100）会被主程序视为「洗版完成」而不再搜索，
   库缺集也补不回来
3. 该季尚未播完 → 取消洗版（仅对洗版订阅；普通订阅不做动作）
4. 库缺集且订阅却认为已下完（lack_episode<=0）→ 重置订阅触发重新下载（原有能力）
5. 取不到该季分集信息 → 保守跳过本轮，不做任何改动

媒体库检查用「媒体身份 + 本地索引条目 ID」定位，条目名与 TMDB 中文名不一致时也能命中。
"""

import threading
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
    plugin_desc = ("只看订阅那一季：订阅新增时判定——该季已播完就直接开整季洗版，未播完保持普通订阅；"
                   "定时巡检只维护已是洗版的订阅（未播完取消洗版、已播完维持整季洗版并在库缺集时重置进度）。")
    plugin_icon = "https://raw.githubusercontent.com/q10710/MoviePilot-Plugins/main/icons/bestversionguard.png"
    plugin_version = "2.8.3"
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
    _enabled_count: int = 0
    _last_run: Optional[str] = None
    _last_fixed: List[Dict[str, Any]] = []
    _last_reset: List[Dict[str, Any]] = []
    # 最近一次「原本是普通订阅、被自动开启洗版」的明细（持久化）
    _last_enabled: List[Dict[str, Any]] = []
    # 持久化：已取消洗版的订阅 ID，避免重复操作
    _fixed_ids: Set[str] = set()
    # 持久化：已重置订阅 ID -> 上次重置时间戳（用于限频）
    _reset_records: Dict[str, float] = {}
    # 即时判定（订阅新增事件）串行化：事件可能在短时间内大量触发（如榜单插件批量建订阅），
    # 若并发执行会同时改插件数据、并按条数放大 TMDB 与媒体库查询压力。
    _single_check_lock = threading.Lock()
    # 插件状态落盘互斥：避免并发写入时后写覆盖先写（丢失限频记录/取消记录）
    _state_lock = threading.Lock()

    def init_plugin(self, config: dict = None) -> None:
        self.stop_service()
        self._subscribe_oper = SubscribeOper()
        saved = self.get_data("state") or {}
        self._fixed_ids = set(saved.get("fixed_ids", []))
        self._fixed_count = saved.get("fixed_count", 0)
        self._enabled_count = saved.get("enabled_count", 0)
        self._reset_records = {str(k): float(v) for k, v in (saved.get("reset_records") or {}).items()}
        # 最近一次明细需持久化：否则重启后数据页只显示累计数量、看不到具体影视名
        self._last_run = saved.get("last_run") or self._last_run
        self._last_fixed = saved.get("last_fixed") or []
        self._last_reset = saved.get("last_reset") or []
        self._last_enabled = saved.get("last_enabled") or []
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
        """返回插件数据页：概览头部、统计卡片、最近一次变更明细与判定口径说明。

        排版参考社区成熟插件：卡片用半透明色块 + 图标方块做视觉锚点，状态用彩色标签，
        明细表用 VTable + thead/tbody（VDataTable 在当前渲染器下表头不显示、只剩分页脚）。
        本方法只负责展示，不改变任何判定逻辑与数据。
        """
        if not self._enabled:
            return [{
                "component": "VAlert",
                "props": {
                    "type": "warning",
                    "variant": "tonal",
                    "density": "compact",
                    "prepend-icon": "mdi-alert-outline",
                    "text": "插件未启用：启用后才会在订阅新增时判定该季是否已播完，并对已是洗版的订阅做巡检维护",
                },
            }]

        def rgba(hex_color: str, alpha: float) -> str:
            """把 #rrggbb 颜色转成指定透明度的 rgba 值（用于半透明底与描边）。"""
            value = hex_color.lstrip("#")
            red, green, blue = int(value[0:2], 16), int(value[2:4], 16), int(value[4:6], 16)
            return f"rgba({red}, {green}, {blue}, {alpha})"

        def icon_tile(icon: str, color: str, size: int = 40) -> dict:
            """图标方块：圆角半透明底色 + 同色图标，用于卡片与标题的视觉锚点。"""
            return {
                "component": "div",
                "props": {
                    "class": "d-flex align-center justify-center flex-shrink-0",
                    "style": (f"width: {size}px; height: {size}px; border-radius: 12px; "
                              f"background: {rgba(color, 0.14)};"),
                },
                "content": [{
                    "component": "VIcon",
                    "props": {"size": int(size * 0.56), "style": f"color: {color};"},
                    "text": icon,
                }],
            }

        def chip(text: str, color: str = "primary", icon: Optional[str] = None,
                 size: str = "small", variant: str = "tonal") -> dict:
            """状态标签：可选前置图标，颜色使用主题色名。"""
            content: List[dict] = []
            if icon:
                content.append({
                    "component": "VIcon",
                    "props": {"size": 14, "class": "mr-1"},
                    "text": icon,
                })
            content.append({"component": "span", "text": text})
            return {
                "component": "VChip",
                "props": {"size": size, "variant": variant, "color": color},
                "content": content,
            }

        def stat_card(icon: str, label: str, value: int, color: str, hint: str) -> dict:
            """统计卡片：图标方块 + 大号数值 + 名称 + 说明。"""
            return {
                "component": "VCol",
                "props": {"cols": 12, "sm": 6, "md": 3},
                "content": [{
                    "component": "div",
                    "props": {
                        "class": "d-flex align-center ga-3 h-100 pa-3",
                        "style": (f"background: {rgba(color, 0.08)}; "
                                  f"border: 1px solid {rgba(color, 0.22)}; border-radius: 12px;"),
                    },
                    "content": [
                        icon_tile(icon, color),
                        {
                            "component": "div",
                            "props": {"class": "flex-grow-1", "style": "min-width: 0;"},
                            "content": [
                                {"component": "div",
                                 "props": {"class": "text-h5 font-weight-black", "style": "line-height: 1.1;"},
                                 "text": str(value)},
                                {"component": "div",
                                 "props": {"class": "text-body-2 font-weight-medium"},
                                 "text": label},
                                {"component": "div",
                                 "props": {"class": "text-caption text-medium-emphasis",
                                           "style": "white-space: normal;"},
                                 "text": hint},
                            ],
                        },
                    ],
                }],
            }

        def detail_section(title: str, icon: str, color: str, items: List[dict],
                           time_key: str, empty_text: str, badge: dict) -> dict:
            """明细卡片：图标标题 + 数量标签 + 可滚动表格（无数据时显示提示）。"""
            if items:
                head_cells = [
                    {"component": "th", "text": text}
                    for text in ("订阅", "季", "原因", "时间")
                ]
                rows = []
                for item in items[:50]:
                    season = item.get("season")
                    rows.append({
                        "component": "tr",
                        "content": [
                            {"component": "td", "props": {"style": "white-space: nowrap;"},
                             "text": f"{item.get('name') or '-'}（{item.get('year') or '-'}）"},
                            {"component": "td", "props": {"style": "white-space: nowrap;"},
                             "text": f"S{season}" if season is not None else "-"},
                            {"component": "td", "props": {"class": "text-body-2"},
                             "text": item.get("reason") or "-"},
                            {"component": "td", "props": {"class": "text-caption", "style": "white-space: nowrap;"},
                             "text": item.get(time_key) or "-"},
                        ],
                    })
                body = [{
                    "component": "div",
                    "props": {"style": "max-height: 420px; overflow: auto; scrollbar-width: thin;"},
                    "content": [{
                        "component": "VTable",
                        "props": {"hover": True, "density": "comfortable", "style": "min-width: 560px;"},
                        "content": [
                            {"component": "thead", "content": [{"component": "tr", "content": head_cells}]},
                            {"component": "tbody", "content": rows},
                        ],
                    }],
                }]
            else:
                body = [{
                    "component": "VAlert",
                    "props": {"type": "success", "variant": "tonal", "density": "compact",
                              "prepend-icon": "mdi-check-circle-outline", "text": empty_text},
                }]
            return {
                "component": "VCard",
                "props": {"variant": "flat", "rounded": "xl", "class": "mb-3 overflow-hidden",
                          "style": "border: 1px solid rgba(128, 128, 128, 0.18);"},
                "content": [
                    {"component": "div", "props": {"class": "d-flex align-center ga-3 px-4 pt-4 pb-3"},
                     "content": [
                         icon_tile(icon, color, 34),
                         {"component": "div", "props": {"class": "text-subtitle-1 font-weight-bold"},
                          "text": title},
                         {"component": "VSpacer"},
                         badge,
                     ]},
                    {"component": "VDivider"},
                    {"component": "VCardText", "props": {"class": "px-4 pt-3 pb-4"}, "content": body},
                ],
            }

        last_run = self._last_run or "尚未运行"
        accent, ok_color, warn_color, info_color, alt_color = (
            "#6366f1", "#10b981", "#f59e0b", "#3b82f6", "#8b5cf6",
        )

        # ── 概览头部 ──
        hero = {
            "component": "VCard",
            "props": {"variant": "flat", "rounded": "xl", "class": "mb-4 overflow-hidden",
                      "style": "position: relative; border: 1px solid rgba(128, 128, 128, 0.18);"},
            "content": [
                {"component": "div", "props": {
                    "class": "d-none d-sm-flex",
                    "style": ("position: absolute; width: 150px; height: 150px; border-radius: 50%; "
                              f"background: {rgba(accent, 0.08)}; top: -60px; left: -40px;")},
                 "content": []},
                {"component": "div", "props": {
                    "class": "d-none d-sm-flex",
                    "style": ("position: absolute; width: 190px; height: 190px; border-radius: 50%; "
                              f"background: {rgba(ok_color, 0.07)}; bottom: -75px; right: -55px;")},
                 "content": []},
                {"component": "div", "props": {"class": "pa-4", "style": "position: relative;"},
                 "content": [
                     {"component": "div", "props": {"class": "d-flex align-center ga-3 flex-wrap"},
                      "content": [
                          icon_tile("mdi-shield-star-outline", accent, 48),
                          {"component": "div", "props": {"class": "flex-grow-1", "style": "min-width: 200px;"},
                           "content": [
                               {"component": "div", "props": {"class": "text-h6 font-weight-bold"},
                                "text": "洗版守护"},
                               {"component": "div",
                                "props": {"class": "text-caption text-medium-emphasis"},
                                "text": "订阅新增时判定该季是否已播完；定时巡检只维护已是洗版的订阅"},
                           ]},
                          {"component": "VBtn",
                           "props": {"color": "primary", "variant": "flat", "size": "small",
                                     "prepend-icon": "mdi-refresh"},
                           "text": "立即检查",
                           "events": {"click": {
                               "api": "plugin/BestVersionGuard/check",
                               "method": "get",
                               "params": {"apikey": settings.API_TOKEN},
                           }}},
                      ]},
                     {"component": "VDivider", "props": {"class": "my-3"}},
                     {"component": "div", "props": {"class": "d-flex flex-wrap ga-2"},
                      "content": [
                          chip(f"上次检查 {last_run}", "primary", icon="mdi-clock-outline"),
                          chip(f"检查周期 {self._cron or '未设置'}", "secondary",
                               icon="mdi-calendar-clock"),
                          chip("库缺集重置 开启" if self._reset_missing_enabled else "库缺集重置 关闭",
                               "success" if self._reset_missing_enabled else "warning",
                               icon="mdi-restore"),
                          chip(f"限频 {self._reset_cooldown_days} 天", "info",
                               icon="mdi-timer-sand"),
                      ]},
                 ]},
            ],
        }

        # ── 统计卡片 ──
        stats_row = {
            "component": "VRow",
            "props": {"dense": True, "class": "mb-4"},
            "content": [
                stat_card("mdi-star-check-outline", "累计开启洗版", self._enabled_count,
                          ok_color, "该季已播完，改走整季洗版"),
                stat_card("mdi-star-off-outline", "累计取消洗版", self._fixed_count,
                          warn_color, "该季未播完，恢复普通订阅"),
                stat_card("mdi-restore", "已重置进度", len(self._reset_records),
                          info_color, "库缺集，重置洗版重新补集"),
                stat_card("mdi-format-list-bulleted", "在管取消记录", len(self._fixed_ids),
                          alt_color, "已取消洗版的订阅条数"),
            ],
        }

        # ── 最近一次变更明细 ──
        page: List[dict] = [
            hero,
            stats_row,
            detail_section(
                "最近一次开启洗版（原为普通订阅）", "mdi-star-plus-outline", ok_color,
                self._last_enabled, "enable_time", "最近一次检查没有新开洗版的订阅",
                chip(f"{len(self._last_enabled)} 条", "success"),
            ),
            detail_section(
                "最近一次取消洗版", "mdi-star-off-outline", warn_color,
                self._last_fixed, "fixed_time", "最近一次检查没有取消洗版的订阅",
                chip(f"{len(self._last_fixed)} 条", "warning"),
            ),
            detail_section(
                "最近一次重置洗版进度", "mdi-restore", info_color,
                self._last_reset, "reset_time", "最近一次检查没有重置进度的订阅",
                chip(f"{len(self._last_reset)} 条", "info"),
            ),
        ]

        # ── 判定口径说明 ──
        page.append({
            "component": "div",
            "props": {
                "class": "d-flex ga-3 pa-3 mt-1",
                "style": (f"background: {rgba(accent, 0.06)}; "
                          f"border: 1px solid {rgba(accent, 0.20)}; border-radius: 12px;"),
            },
            "content": [
                {"component": "VIcon",
                 "props": {"size": "small", "class": "flex-shrink-0", "style": f"color: {accent};"},
                 "text": "mdi-information-outline"},
                {"component": "div", "props": {"class": "text-caption", "style": "line-height: 1.7;"},
                 "content": [
                     {"component": "div", "props": {"class": "font-weight-medium mb-1"},
                      "text": "判定口径"},
                     {"component": "div",
                      "text": "① 订阅新增那一刻：该季已播完就直接开整季洗版，未播完保持普通订阅；"},
                     {"component": "div",
                      "text": "② 定时巡检只维护已是洗版的订阅：未播完取消洗版，已播完且库缺集则重置洗版进度；"},
                     {"component": "div",
                      "text": "③ 单季判据＝该季分集已全部播出，或库内该季已齐全；取不到分集信息时保守跳过。"},
                 ]},
            ],
        })

        return page

    def stop_service(self) -> None:
        # 定时任务已交由 MoviePilot 主调度器统一管理，无需在此手动清理
        pass

    # ── 辅助方法 ──────────────────────────────────────────────

    def _save_state(self) -> None:
        # 加锁后再取快照：保证写入的是当前内存状态的完整视图，避免并发写互相覆盖
        with self._state_lock:
            self.save_data("state", {
                "fixed_ids": list(self._fixed_ids),
                "fixed_count": self._fixed_count,
                "enabled_count": self._enabled_count,
                "reset_records": dict(self._reset_records),
                "last_run": self._last_run,
                "last_fixed": (self._last_fixed or [])[:50],
                "last_reset": (self._last_reset or [])[:50],
                "last_enabled": (self._last_enabled or [])[:50],
            })

    def _can_reset(self, subscribe_id: Any) -> bool:
        """判断该订阅当前是否允许重置（按限频天数去抖，0 表示不限频）。"""
        if self._reset_cooldown_days <= 0:
            return True
        last = self._reset_records.get(str(subscribe_id))
        if not last:
            return True
        return (time.time() - float(last)) >= self._reset_cooldown_days * 86400

    def _reset_subscribe(self, sub: Any, extra: Optional[Dict[str, Any]] = None) -> bool:
        """重置订阅，让它重新搜索下载；字段与主程序「订阅重置」保持一致。

        媒体库那份被删后，主程序仍以为该季已下完（lack_episode=0、note 记录全部集数），
        不会自动补下；清空下载事实并恢复缺集数后，下一轮订阅搜索即会重新获取。

        extra：可选的附加字段（例如该季已播完时一并开启整季洗版），
        与重置字段在同一次写入里提交，避免要等下一轮才补上。
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
        if extra:
            payload.update({k: v for k, v in extra.items() if v is not None})
        try:
            self._subscribe_oper.update(sid=sub.id, payload=payload)
        except Exception as e:
            logger.info(f"重置订阅失败: {sub.name} - {e}")
            return False
        # 字典项写入是原子操作；随后 _save_state() 会持锁取快照并落盘。
        # 注意不要在此处再加 _state_lock：_save_state 内部已持同一把不可重入锁，会死锁。
        self._reset_records[str(sub.id)] = time.time()
        self._save_state()
        return True

    def _reset_best_version_progress(self, sub: Any) -> bool:
        """只重置「洗版进度标记」，让主程序重新搜索补集。

        与 _reset_subscribe 的区别：不改 lack_episode / note / state，
        因此不会干扰正在进行的普通补集下载，只清掉「已洗版到的档位」这类完成标记。
        场景：该季已播完、媒体库却缺集，而 current_priority 已达顶档被主程序当成「洗版完成」。
        """
        if not self._subscribe_oper:
            return False
        payload = {
            "current_priority": None,
            "current_audio_format": None,
            "current_bitrate": None,
            "current_bit_depth": None,
            "current_sample_rate": None,
            "episode_priority": {},
        }
        try:
            self._subscribe_oper.update(sid=sub.id, payload=payload)
        except Exception as e:
            logger.info(f"重置洗版进度失败: {sub.name} - {e}")
            return False
        # 同上：不要在此处取 _state_lock，避免与 _save_state 内的锁重入死锁。
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

    def _get_season_air_status(self, tmdbid: int, season: int
                               ) -> Tuple[Optional[bool], int, int, List[int]]:
        """判断目标单季是否已播完（只考虑这一季，不看整剧状态）。

        返回 (该季是否已播完, 该季总集数, 已播出集数, 尚未播出的集号)。
        首项为 None 表示取不到分集数据，调用方应保守跳过本轮、不做任何改动。

        判定口径：
        - 以该季分集的 air_date 为准，全部已过（含今天）才算播完；
        - air_date 缺失或格式非法的集视为「尚未播出」，宁可判未播完，也不提前判播完；
        - 分集条数少于 TMDB 报告的该季集数时视为数据不全，同样不判播完。
        """
        try:
            episodes = TmdbChain().tmdb_episodes(tmdbid=tmdbid, season=season)
        except Exception as e:
            logger.debug(f"查询 TMDB 季分集失败: {tmdbid} S{season} - {e}")
            episodes = []
        if not episodes:
            return None, 0, 0, []

        today = datetime.now().date()
        pending: List[int] = []
        aired = 0
        for ep in episodes:
            ep_no = getattr(ep, "episode_number", None) or 0
            air_date = (getattr(ep, "air_date", None) or "").strip()
            if not air_date:
                pending.append(ep_no)
                continue
            try:
                aired_date = datetime.strptime(air_date, "%Y-%m-%d").date()
            except ValueError:
                pending.append(ep_no)
                continue
            if aired_date > today:
                pending.append(ep_no)
            else:
                aired += 1

        season_total = self._get_tmdb_season_total(tmdbid, season)
        if season_total > len(episodes):
            # TMDB 报告的该季集数多于已录入的分集，数据不全，不判播完
            return False, season_total, aired, pending
        return (len(pending) == 0), (season_total or len(episodes)), aired, pending

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

    def _guard_check(self, subscribe_id: Optional[int] = None) -> None:
        """检查订阅。

        subscribe_id 不为空：订阅新增的那一刻判定这一条（订阅新增事件触发，见 _on_subscribe_added）——
        该季已播完就直接开整季洗版；未播完就让它当普通订阅（若本身带洗版标记则取消）。**这是本
        插件唯一的「普通订阅 → 洗版」入口**：已经在跑的订阅不需要在这里转，它们由订阅助手Q 在
        订阅完成时自动新建整季洗版订阅（用户口径 2026-09-16）。

        subscribe_id 为空：定时巡检（主调度器每小时触发）——只维护**已经是洗版**的订阅：
        未播完的取消洗版、已播完的维持整季洗版并在库缺集时重置洗版进度。不给普通订阅开洗版。
        """
        single = subscribe_id is not None
        # 只有「刚订阅的那一刻」允许把普通订阅转成洗版；定时巡检不开这个口子。
        allow_enable = single
        if single:
            sub = self._subscribe_oper.get(subscribe_id)
            if not sub:
                return
            # 处理全部电视剧订阅：该季已播完就开洗版，不管订阅是什么原因加进来的
            # （用户口径 2026-09-16）。非电视剧/无身份/S 状态仍在主循环里跳过。
            subscribes = [sub]
            logger.info(f"订阅新增检查：{sub.name} S{sub.season} (id={subscribe_id})")
        else:
            logger.info("开始检查")
            subscribes = self._subscribe_oper.list(state=None)
        if not subscribes:
            logger.info("无订阅，跳过")
            return

        tmdb_chain = TmdbChain()
        fixed_list: List[Dict[str, Any]] = []
        reset_list: List[Dict[str, Any]] = []
        enable_list: List[Dict[str, Any]] = []
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

            # 定时巡检只处理「已是洗版」的订阅（未播完的取消洗版、已播完的维持整季洗版）。
            # 已经存在的普通订阅不在本插件的管辖范围：它们正常逐集下载，等这一季下完，
            # 由订阅助手Q 的洗版编排自动新建整季洗版订阅（用户口径 2026-09-16）。
            # 「普通订阅 → 洗版」的转换只发生在订阅新增那一刻（allow_enable）。
            # 历史上这里还按 username 区分来源，各分支处置完全相同，已合并。
            if not allow_enable and not sub.best_version:
                continue

            tmdb_info = _get_tmdb(tmdbid)
            tmdb_status = (tmdb_info or {}).get("status", "")

            # 获取该季 TMDB 总集数，判断媒体库是否全集入库
            season_total = self._get_tmdb_season_total(tmdbid, sub.season)
            missing_eps: List[int] = []
            if season_total > 0:
                missing_eps = self._missing_library_episodes(
                    tmdbid, sub.season, season_total, sub.name, sub.year,
                    getattr(sub, "media_id", None),
                )
            lib_complete = season_total > 0 and not missing_eps

            # 判断「这一季」是否已播完：只考虑单季，不看整剧状态。
            # 整剧 Returning Series 只代表还会有下一季，本季是否播完要看本季分集的播出日期。
            # 取不到分集信息时保守跳过本轮（不取消、不重置），避免 TMDB 数据缺失导致误判。
            season_finished, season_tmdb_total, aired_count, _pending_eps = \
                self._get_season_air_status(tmdbid, sub.season)
            if season_finished is None:
                logger.warning(f"取不到 TMDB 季分集信息，本轮跳过: {sub.name} S{sub.season}")
                continue
            # 库内该季已齐全时同样按「已播完」处理：内容都在库里了，本季没有可追的；
            # 同时兜住 TMDB 分集播出日期缺失/异常（实测有剧库内 190 集齐全，
            # 但 TMDB 分集日期异常被判「已播 50/190」，仅凭日期会误判成未播完）。
            season_ended = season_finished or lib_complete

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
                # 该季已播完时，同一次写入里一并开启整季洗版：
                # 用户口径（2026-09-16）——只要该季播完就开洗版，不管订阅是什么原因加进来的。
                extra: Dict[str, Any] = {}
                enable_now = allow_enable and season_ended and not sub.best_version
                if enable_now:
                    extra["best_version"] = 1
                    extra["best_version_full"] = 1
                if self._reset_subscribe(sub, extra=extra):
                    logger.info(f"库缺集重置订阅: {sub.name} S{sub.season} "
                                f"(媒体库缺 {missing_text}，等待重新下载)")
                    reset_list.append({
                        "name": sub.name, "year": sub.year, "season": sub.season,
                        "reason": f"媒体库缺 {missing_text}但订阅认为已下完",
                        "reset_time": now_str,
                    })
                    if enable_now:
                        logger.info(f"该季已播完，已开启整季洗版（原为普通订阅）: "
                                    f"{sub.name} S{sub.season}")
                        enable_list.append({
                            "name": sub.name, "year": sub.year, "season": sub.season,
                            "reason": "该季已播完", "enable_time": now_str,
                        })
                    continue

            if season_ended:
                fix_key = str(sub.id)
                if fix_key in self._fixed_ids:
                    self._fixed_ids.discard(fix_key)
                    self._save_state()
                # 该季已播完：确保处于整季洗版状态（单季播完就洗版）。
                # 刚订阅的那一刻若还是普通订阅，这里一并开启洗版——同一批集反复
                # 「下不动→超时删种→补搜→再下」纯属浪费带宽与磁盘 IO，
                # 整季洗版让主程序只认整季包、一次到位。已在跑的订阅不做这种转换。
                payload: Dict[str, Any] = {}
                newly_enabled = allow_enable and not sub.best_version
                if newly_enabled:
                    payload["best_version"] = 1
                if not sub.best_version_full:
                    payload["best_version_full"] = 1
                if payload:
                    try:
                        self._subscribe_oper.update(sid=sub.id, payload=payload)
                        if newly_enabled:
                            logger.info(f"该季已播完，已开启整季洗版（原为普通订阅）: "
                                        f"{sub.name} S{sub.season}")
                            enable_list.append({
                                "name": sub.name, "year": sub.year, "season": sub.season,
                                "reason": "该季已播完", "enable_time": now_str,
                            })
                        else:
                            logger.info(f"该季已播完，开启整季洗版: {sub.name} S{sub.season}")
                    except Exception as e:
                        logger.info(f"开启整季洗版失败: {sub.name} - {e}")
                # 该季已播完但媒体库缺集 → 重置洗版进度，让主程序重新搜索补集。
                # 主程序把 current_priority 当作「已洗版到的档位」，顶档（100）视为洗版完成，
                # 库缺集也不会再搜索 → 内容缺了却补不回来，必须清掉进度标记。
                if self._reset_missing_enabled and season_total > 0 and not lib_complete \
                        and sub.current_priority:
                    if sub.state == "P":
                        # 已有下载在进行，不打扰：等它下完再由后续轮次判断，避免重复搜索与重复通知。
                        # 与「整订阅重置」保持同一道安全边界（下载中一律跳过）。
                        logger.info(f"库缺集但当前有下载进行中，暂不重置洗版进度: {sub.name} S{sub.season}")
                    elif not self._can_reset(sub.id):
                        logger.info(f"库缺集但处于重置限频期（{self._reset_cooldown_days} 天），跳过: "
                                    f"{sub.name} S{sub.season}")
                    elif self._reset_best_version_progress(sub):
                        missing_text = self._format_missing_episodes(missing_eps)
                        logger.info(f"该季已播完但媒体库缺 {missing_text}，已重置洗版进度等待重新下载: "
                                    f"{sub.name} S{sub.season}")
                        reset_list.append({
                            "name": sub.name, "year": sub.year, "season": sub.season,
                            "reason": f"该季已播完但媒体库缺 {missing_text}，已重置洗版进度",
                            "reset_time": now_str,
                        })
                continue

            # ── 该季尚未播完 ──
            # 只有洗版订阅需要「取消洗版」；普通订阅本就没开洗版，直接跳过（不做无意义动作）。
            if not sub.best_version:
                continue
            fix_key = str(sub.id)
            if fix_key in self._fixed_ids:
                # 之前取消过洗版但被重新开启了，清除记录重新处理
                self._fixed_ids.discard(fix_key)

            reason = f"单季未播完（已播 {aired_count}/{season_tmdb_total} 集）"
            if tmdb_status:
                reason = f"{reason}，TMDB {tmdb_status}"

            # 取消洗版标记（不删除订阅，保留订阅继续追更）
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

        # 「开启洗版」的明细与累计计数两种模式都要记：新增那一刻是本插件唯一的开启入口，
        # 若只在巡检里累计，数据页的「最近开启洗版」与累计数会永远是空的。
        self._enabled_count += len(enable_list)
        if enable_list:
            self._last_enabled = enable_list
        if not single:
            # 单条（订阅新增触发）不覆盖定时巡检的「最近一次检查/取消洗版/重置」明细与累计数，
            # 避免互相干扰；仅在本轮确有结果时更新明细，保留最近一次非空清单。
            self._last_run = now_str
            self._fixed_count += len(fixed_list)
            if fixed_list:
                self._last_fixed = fixed_list
            if reset_list:
                self._last_reset = reset_list
        self._save_state()

        logger.info(f"检查完成: 开启洗版 {len(enable_list)} 个，取消洗版 {len(fixed_list)} 个，"
                    f"重置 {len(reset_list)} 个")

        if self._notify and (fixed_list or reset_list or enable_list):
            lines = self._format_lists(fixed_list, reset_list, enable_list)
            self.post_message(title="洗版守护", text="\n".join(lines))

    @staticmethod
    def _format_lists(fixed_list: List[Dict[str, Any]],
                      reset_list: List[Dict[str, Any]],
                      enable_list: Optional[List[Dict[str, Any]]] = None,
                      limit: int = 10) -> List[str]:
        """格式化开启洗版/取消洗版/重置订阅清单（前 N 个明细 + 总数后缀），供通知复用。"""
        lines: List[str] = []
        if enable_list:
            names = "、".join(
                f"{e.get('name', '')} S{e.get('season')}" for e in enable_list[:limit]
            )
            suffix = f"等 {len(enable_list)} 个" if len(enable_list) > limit else ""
            lines.append(f"已开启洗版: {names}{suffix}")
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
            lines.append(f"已重置订阅/洗版进度，等待重新下载: {names}{suffix}")
        return lines

    # ── API ───────────────────────────────────────────────────

    async def _api_check(self, apikey: str = "") -> Dict[str, Any]:
        self._guard_check()
        return {
            "success": True,
            "enabled_count": len(self._last_enabled),
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
        lines = self._format_lists(self._last_fixed, self._last_reset, self._last_enabled)
        if lines:
            text = f"检查完成（最近一次 {self._last_run or '未知'}）\n" + "\n".join(lines)
        else:
            text = "检查完成\n未发现需要处置的订阅"
        self.post_message(title="洗版守护", text=text)

    @eventmanager.register(EventType.SubscribeAdded)
    def _on_subscribe_added(self, event: Event = None) -> None:
        """订阅新增事件：立即判定该订阅，不必等每小时巡检（判据与定时巡检完全一致）。"""
        if not self._enabled:
            return
        data = getattr(event, "event_data", None)
        if not isinstance(data, dict):
            return
        try:
            subscribe_id = int(data.get("subscribe_id") or 0)
        except (TypeError, ValueError):
            return
        if not subscribe_id:
            return
        # 该事件在订阅创建流程中同步派发，这里用独立线程执行，
        # 避免 TMDB / 媒体库查询拖慢订阅创建；异常一律只记日志。
        threading.Thread(
            target=self._check_added_subscribe,
            args=(subscribe_id,),
            name=f"BestVersionGuardAdded-{subscribe_id}",
            daemon=True,
        ).start()

    def _check_added_subscribe(self, subscribe_id: int) -> None:
        """订阅新增后的即时判定（异常只记日志，绝不中断主流程）。

        用锁串行执行：批量建订阅时不会同时压测 TMDB/媒体库，也不会并发改写插件数据。
        等锁超时则跳过本轮（整点巡检会覆盖该订阅），不堆积线程。
        """
        acquired = self._single_check_lock.acquire(timeout=120)
        if not acquired:
            logger.warning(f"已有即时检查在运行，本次跳过（整点巡检会覆盖）: id={subscribe_id}")
            return
        try:
            self._guard_check(subscribe_id=subscribe_id)
        except Exception as e:
            logger.error(f"订阅新增即时检查失败: id={subscribe_id} - {e}")
        finally:
            self._single_check_lock.release()
