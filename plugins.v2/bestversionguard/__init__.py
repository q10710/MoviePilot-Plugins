"""洗版订阅守护插件。

职责分三块，**只看订阅指向的那一季**，不看整剧状态：

1) **订阅新增的那一刻**（监听 SubscribeAdded，本插件唯一的「普通订阅 → 洗版」入口）：
   判断这一季是否已播完——已播完就直接开整季洗版（best_version / best_version_full = 1），
   避免同一批集反复「下不动 → 超时删种 → 补搜 → 再下」的空转；
   未播完就让它当普通订阅正常追更（若新订阅本身带着洗版标记，则取消该标记恢复普通订阅）。
   这台订阅从哪来（手动 / 缺失插件补建 / 榜单命中）不影响判定。
2) **定时巡检**（每小时）：只维护**已经是洗版**的订阅——
   该季未播完 → 取消洗版、恢复普通订阅继续追更；该季已播完 → 维持整季洗版并在库缺集时重置洗版进度。
   **已存在订阅的洗版不由本插件开启**：它们逐集正常下载，等这一季下完，由订阅助手Q 的洗版编排
   在「订阅完成」时自动新建整季洗版订阅（用户口径 2026-09-16）。**普通订阅在巡检里一律不处理**。
3) **定时巡检 · 普通订阅库缺集对账回退**（`reconcile_enabled`，可开关）：
   媒体库缺集、但订阅的下载事实 note 仍把这些集标成已完成时（典型链路：红种源被清理 → 硬链接断开 →
   媒体库副本按规则删除 → 主程序按「缺失集数 = 库缺集 − note」把这些集当成已下载而永不重下），
   把这部分集从 note 摘除并同步缺集数，让下一轮订阅搜索重新补下。三态处置：
   - 该季未播完 → 只回退缺集，普通订阅逐集正常追补；
   - 该季已播完 → 回退缺集的同一次写入里转整季洗版（best_version/full=1），只认整季包一次补齐并完成洗版；
     转洗版后限时（reconcile_grace_days，默认 7 天）仍缺集说明站上没有整季包 → 兜底把 best_version 置回 0，
     恢复普通订阅逐集补；
   - 已是洗版的订阅由 2) 的既有逻辑维护，本模块只负责上面的兜底回退，不重复处理。
   护栏：媒体库整季查空需连续 3 轮、部分缺失需连续 2 轮确认同一批缺集才动手；S/P 状态与手动冻结总集数
   （manual_total_episode）的订阅不参与；同一订阅限频；同一集累计回退达止损次数（reconcile_stop_count）
   即拉黑该订阅、停止自动回退并推送高危清单；每轮最多处理 reconcile_max_per_run 部。

判断逻辑（2.10.0 起为「多源完结信号 + 严取消门槛」，判定不对称，防误取消）：
1. **「已播完」证据（任一成立即算播完，用于开洗版/保洗版）**：
   - TMDB 剧级状态 Ended/Canceled，或该季末集有唯一 finale 标记且无后续集（强证据）；
   - TMDB 有更晚的季（中置信：本季必然已播完）；
   - 该季全部分集 air_date 均已过（单季全播完）；
   - 媒体库该季已齐全（内容都在库里，本季没有可追的）；
   - 豆瓣条目「更新至期数 >= 总期数」（国内综艺 TMDB 数据常缺，豆瓣按季独立建条目更准）；
   - 站点缓存候选标题命中「全集/完结/全N集」等强完结表述（仅用于保住洗版，不用于开洗版）。
2. **「未播完」强证据（只有它能触发取消）**：该季存在 air_date 明确在未来、且集号超出
   最后已播集的分集。缺 air_date 的集只算「未知」，不再当未播完（旧口径误取消主因）。
3. **取消洗版门槛（三层同时满足才取消）**：有明确未来集 + 三源（TMDB/豆瓣/站点）无任一
   完结证据 + 连续 cancel_confirm_rounds 轮巡检同判；取消后自动监测——一旦出现完结证据，
   自动重开整季洗版（同一订阅 reopen_cooldown_days 天内最多一次，防来回翻转）。
4. 该季已播完但媒体库缺集 → 重置洗版进度（清 current_priority），让主程序重新搜索补集；
5. 库缺集且订阅却认为已下完（lack_episode<=0）→ 重置订阅触发重新下载（原有能力）
6. 取不到该季分集信息 → 保守跳过本轮，不做任何改动

媒体库检查用「媒体身份 + 本地索引条目 ID」定位，条目名与 TMDB 中文名不一致时也能命中；
已配置多台媒体服务器（如 Emby + 飞牛影视）时按并集合并，任一服务器上存在该集即视为存在（可播放）。
"""

import threading
import time
from datetime import date, datetime
import re
from app.log import logger
from typing import Any, Dict, List, Optional, Set, Tuple

from apscheduler.triggers.cron import CronTrigger

from app.chain.tmdb import TmdbChain
from app.chain.torrents import TorrentsChain
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

# 豆瓣完结信号（2.10.0）：国内综艺在 TMDB 常缺分集日期，豆瓣按季独立建条目且带
# 「更新至第 X 期 / 总期数」，作为 TMDB 之外的第二完结数据源。主程序路径变化时自动降级。
try:
    from app.modules.douban.apiv2 import DoubanApi
except Exception:  # pragma: no cover - 兼容旧版主程序路径
    DoubanApi = None


# TMDB 身份来源标识：新版 MoviePilot 的订阅/媒体条目用 media_source + media_id 描述媒体身份
TMDB_MEDIA_SOURCE = "themoviedb"

# 站点完结证据：种子标题命中完结表述（强证据）；带「更至/更新至」或集数对不上总期数的
# 只算弱证据（仅阻止取消洗版，不作为判完结依据）。
_SITE_COMPLETE_RE = re.compile(r"\b(?:complete|completed)\b|完结|全集|全\s*\d+\s*[集期话]", re.IGNORECASE)
_SITE_ONGOING_RE = re.compile(r"更至|更新至|更新中|未完结|未完成|ongoing|连载|预计|待更", re.IGNORECASE)
_SITE_TOTAL_RE = re.compile(r"全\s*(\d+)\s*[集期话]")



class BestVersionGuard(_PluginBase):
    """洗版订阅守护插件。"""

    plugin_name = "洗版守护Q自用版"
    plugin_desc = ("只看订阅那一季：订阅新增时判定——该季已播完就直接开整季洗版，未播完保持普通订阅；"
                   "定时巡检维护已是洗版的订阅（未播完取消洗版、已播完维持整季洗版并在库缺集时重置进度）；"
                   "普通订阅库缺集自动对账回退补齐（删掉的文件被订阅重新补回）；"
                   "TMDB+豆瓣+站点多源完结信号，严取消门槛（连续确认），误取消自动重开自愈。")
    plugin_icon = "https://raw.githubusercontent.com/q10710/MoviePilot-Plugins/main/icons/bestversionguard.png"
    plugin_version = "2.10.2"
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
    # 普通订阅库缺集对账回退（删完自动补，见模块说明 3）
    _reconcile_enabled: bool = True
    # 同一订阅对账回退限频天数（0 = 不限频）
    _reconcile_cooldown_days: int = 7
    # 每轮巡检最多处理的订阅数，避免一轮补集打满带宽磁盘
    _reconcile_max_per_run: int = 5
    # 同一集累计回退次数上限：达到即拉黑该订阅、停止自动回退并推送高危清单
    _reconcile_stop_count: int = 3
    # 完结季转整季洗版后仍缺集的兜底天数：超过则回退普通订阅逐集补
    _reconcile_grace_days: int = 7
    # 豆瓣完结信号：TMDB 之外的第二完结数据源（国内综艺 TMDB 分集数据常缺）
    _douban_signal_enabled: bool = True
    # 站点完结证据：RSS/spider 缓存标题命中「全集/完结/全N集」（仅用于保住洗版）
    _site_signal_enabled: bool = True
    # 取消洗版前的连续确认轮数（每轮 = 一次巡检，默认每小时）
    _cancel_confirm_rounds: int = 3
    # 取消洗版后出现完结证据时自动重开整季洗版（误取消自愈）
    _reopen_enabled: bool = True
    # 同一订阅自动重开限频天数（0 = 不限频），防取消↔重开来回翻转
    _reopen_cooldown_days: int = 7

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
    # 对账回退：订阅 ID -> 上次回退动作时间戳（限频用）
    _reconcile_records: Dict[str, float] = {}
    # 对账回退：订阅 ID -> 待确认缺集 {"rolled": [集号], "count": 轮次}（连续确认护栏）
    _reconcile_pending: Dict[str, Dict[str, Any]] = {}
    # 对账回退：订阅 ID -> {集号: 已回退次数}（集级止损计数）
    _reconcile_strikes: Dict[str, Dict[str, int]] = {}
    # 对账回退：止损拉黑的订阅 ID（停止自动回退，进高危清单）
    _reconcile_blocked: Set[str] = set()
    # 完结转洗版兜底：订阅 ID -> 转整季洗版时间戳（超过宽限仍缺集则回退普通订阅）
    _convert_records: Dict[str, float] = {}
    # 取消洗版连续确认：订阅 ID -> {"count": 轮次, "signature": 证据签名}（证据变化即清零）
    _cancel_pending: Dict[str, Dict[str, Any]] = {}
    # 自动重开：订阅 ID -> 上次重开时间戳（限频用）
    _reopen_records: Dict[str, float] = {}
    # 紡计自动重开次数与最近一次明细（数据页/通知展示）
    _reopened_count: int = 0
    _last_reopened: List[Dict[str, Any]] = []
    # 豆瓣完结查询缓存：「剧名|季播出年」-> (写入时间戳, 是否完结, 说明)。
    # 带 TTL（_DOUBAN_CACHE_TTL 秒）而非永久缓存：豆瓣数据随播出推进会变
    # （11/13 → 13/13），永久缓存会让完结信号逐渐失灵；但也不能每轮清空——
    # 豆瓣 API 有 60 秒限流，每轮全量重查会打爆。折中 6 小时。
    _douban_cache: Dict[str, Tuple[float, Optional[bool], str]] = {}
    _DOUBAN_CACHE_TTL = 6 * 3600
    # 累计对账回退次数与最近一次明细（数据页/通知展示）
    _reconcile_count: int = 0
    _last_reconcile: List[Dict[str, Any]] = []
    _last_high_risk: List[Dict[str, Any]] = []
    # 即时判定（订阅新增事件）串行化：事件可能在短时间内大量触发（如榜单插件批量建订阅），
    # 若并发执行会同时改插件数据、并按条数放大 TMDB 与媒体库查询压力。
    _single_check_lock = threading.Lock()
    # 巡检互斥（2.10.2）：定时任务/手动命令/API 三个巡检入口共用，占用即跳过——
    # 并发跑同一轮会竞争「读计数→改→写回」并重复通知。布尔标志即可：
    # 巡检本就低频（每小时），跳过的一次会被下一轮覆盖，不需要排队。
    _patrol_running: bool = False
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
        self._reconcile_records = {str(k): float(v) for k, v in (saved.get("reconcile_records") or {}).items()}
        self._reconcile_pending = {str(k): dict(v or {}) for k, v in (saved.get("reconcile_pending") or {}).items()}
        self._reconcile_strikes = {
            str(k): {str(ek): int(ev) for ek, ev in (v or {}).items()}
            for k, v in (saved.get("reconcile_strikes") or {}).items()
        }
        self._reconcile_blocked = set(str(x) for x in (saved.get("reconcile_blocked") or []))
        self._convert_records = {str(k): float(v) for k, v in (saved.get("convert_records") or {}).items()}
        self._cancel_pending = {
            str(k): dict(v or {}) for k, v in (saved.get("cancel_pending") or {}).items()
        }
        self._reopen_records = {str(k): float(v) for k, v in (saved.get("reopen_records") or {}).items()}
        self._reopened_count = saved.get("reopened_count", 0)
        self._last_reopened = saved.get("last_reopened") or []
        self._reconcile_count = saved.get("reconcile_count", 0)
        self._last_reconcile = saved.get("last_reconcile") or []
        self._last_high_risk = saved.get("last_high_risk") or []
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
        # 对账回退配置（同上：显式解析缺省值，避免空串/非法值）
        self._reconcile_enabled = bool(config.get("reconcile_enabled", True))
        self._reconcile_cooldown_days = self._int_cfg(config.get("reconcile_cooldown_days"), 7)
        self._reconcile_max_per_run = max(1, self._int_cfg(config.get("reconcile_max_per_run"), 5))
        self._reconcile_stop_count = max(1, self._int_cfg(config.get("reconcile_stop_count"), 3))
        self._reconcile_grace_days = max(0, self._int_cfg(config.get("reconcile_grace_days"), 7))
        self._douban_signal_enabled = bool(config.get("douban_signal_enabled", True))
        self._site_signal_enabled = bool(config.get("site_signal_enabled", True))
        self._cancel_confirm_rounds = max(1, self._int_cfg(config.get("cancel_confirm_rounds"), 3))
        self._reopen_enabled = bool(config.get("reopen_enabled", True))
        self._reopen_cooldown_days = max(0, self._int_cfg(config.get("reopen_cooldown_days"), 7))
        logger.info(f"初始化完成, enabled={self._enabled}, cron={self._cron}, "
                    f"库缺集重置={self._reset_missing_enabled}（限频 {self._reset_cooldown_days} 天），"
                    f"对账回退={self._reconcile_enabled}（限频 {self._reconcile_cooldown_days} 天，"
                    f"每轮上限 {self._reconcile_max_per_run} 部，止损 {self._reconcile_stop_count} 次，"
                    f"转洗版兜底 {self._reconcile_grace_days} 天，已拉黑 {len(self._reconcile_blocked)} 条），"
                    f"完结信号：豆瓣={self._douban_signal_enabled} 站点={self._site_signal_enabled}，"
                    f"取消确认 {self._cancel_confirm_rounds} 轮，"
                    f"自动重开={self._reopen_enabled}（限频 {self._reopen_cooldown_days} 天），"
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
            {
                "path": "/reconcile_unblock",
                "endpoint": self._api_reconcile_unblock,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "解除对账回退止损拉黑（可带 subscribe_id 只解除单条）",
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
                    {
                        "component": "VSwitch",
                        "props": {
                            "model": "reconcile_enabled",
                            "label": "普通订阅库缺集对账回退（删完自动补）",
                            "hint": ("媒体库缺集但订阅的下载事实仍标记为已完成时（文件被删后主程序不会重下），"
                                     "把这些集从下载事实中摘除并同步缺集数：未播完的季逐集补，"
                                     "已播完的季转整季洗版一次补齐（限时找不到整季包则回退逐集补）。"),
                            "persistent-hint": True,
                        },
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "reconcile_cooldown_days",
                            "label": "对账回退限频（天，0=不限频）",
                            "type": "number",
                            "hint": "同一订阅在该天数内最多回退一次，防止补了又删、删了又补的循环打满带宽",
                            "persistent-hint": True,
                        },
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "reconcile_max_per_run",
                            "label": "每轮最多处理部数",
                            "type": "number",
                            "hint": "每次巡检最多对账回退多少部订阅，剩余下一轮继续，避免一轮补集打满带宽磁盘",
                            "persistent-hint": True,
                        },
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "reconcile_stop_count",
                            "label": "同集止损次数",
                            "type": "number",
                            "hint": "同一集累计回退达到该次数仍被删除，停止该订阅的自动回退并推送高危清单",
                            "persistent-hint": True,
                        },
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "reconcile_grace_days",
                            "label": "转洗版兜底天数",
                            "type": "number",
                            "hint": "已完结季转整季洗版后仍缺集超过该天数（站上没有整季包），回退为普通订阅逐集补",
                            "persistent-hint": True,
                        },
                    },
                    {
                        "component": "VSwitch",
                        "props": {
                            "model": "douban_signal_enabled",
                            "label": "豆瓣完结信号（第二数据源）",
                            "hint": ("TMDB 之外再查豆瓣：条目「更新至期数 >= 总期数」判已播完。"
                                     "国内综艺 TMDB 分集数据常缺失，豆瓣按季独立建条目更准。"
                                     "用于开洗版与保洗版，匹配不上时不影响现有行为。"),
                            "persistent-hint": True,
                        },
                    },
                    {
                        "component": "VSwitch",
                        "props": {
                            "model": "site_signal_enabled",
                            "label": "站点全集证据（仅防误取消）",
                            "hint": ("读取主程序已有搜索缓存标题，命中「全集/完结/全N集」等强表述时"
                                     "保住洗版不取消（零站点请求）。只用于防误取消，不用于开洗版。"),
                            "persistent-hint": True,
                        },
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "cancel_confirm_rounds",
                            "label": "取消洗版确认轮数",
                            "type": "number",
                            "hint": ("有明确未来播出集且三源（TMDB/豆瓣/站点）都无完结证据后，"
                                     "还需连续该轮数巡检同判才执行取消（每轮约 1 小时），防数据瞬态误取消"),
                            "persistent-hint": True,
                        },
                    },
                    {
                        "component": "VSwitch",
                        "props": {
                            "model": "reopen_enabled",
                            "label": "误取消自动重开洗版",
                            "hint": ("被本插件取消洗版后若出现完结证据（TMDB/豆瓣/站点任一），"
                                     "自动重开整季洗版并通知，自愈误取消。"),
                            "persistent-hint": True,
                        },
                    },
                    {
                        "component": "VTextField",
                        "props": {
                            "model": "reopen_cooldown_days",
                            "label": "自动重开限频（天，0=不限频）",
                            "type": "number",
                            "hint": "同一订阅在该天数内最多自动重开一次，防取消↔重开来回翻转",
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
            "reconcile_enabled": True,
            "reconcile_cooldown_days": 7,
            "reconcile_max_per_run": 5,
            "reconcile_stop_count": 3,
            "reconcile_grace_days": 7,
            "douban_signal_enabled": True,
            "site_signal_enabled": True,
            "cancel_confirm_rounds": 3,
            "reopen_enabled": True,
            "reopen_cooldown_days": 7,
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
                                "text": "订阅新增时判定该季是否已播完；巡检维护洗版订阅、对账回退补齐库缺集"},
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
                          chip("对账回退 开启" if self._reconcile_enabled else "对账回退 关闭",
                               "success" if self._reconcile_enabled else "warning",
                               icon="mdi-restore-alert"),
                          chip(f"对账限频 {self._reconcile_cooldown_days} 天 / 每轮 {self._reconcile_max_per_run} 部",
                               "secondary", icon="mdi-timer-outline"),
                          chip(f"豆瓣信号 {'开' if self._douban_signal_enabled else '关'}"
                               f" / 站点证据 {'开' if self._site_signal_enabled else '关'}",
                               "info", icon="mdi-database-search"),
                          chip(f"取消确认 {self._cancel_confirm_rounds} 轮"
                               f" / 自动重开 {'开' if self._reopen_enabled else '关'}",
                               "secondary", icon="mdi-refresh"),
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
                stat_card("mdi-restart", "累计自动重开", self._reopened_count,
                          ok_color, "取消后出现完结证据，自愈重开洗版"),
                stat_card("mdi-restore", "已重置进度", len(self._reset_records),
                          info_color, "库缺集，重置洗版重新补集"),
                stat_card("mdi-format-list-bulleted", "在管取消记录", len(self._fixed_ids),
                          alt_color, "已取消洗版的订阅条数"),
                stat_card("mdi-restore-alert", "累计对账回退", self._reconcile_count,
                          ok_color, "库缺集但订阅标记已完成，摘除后重新补下"),
                stat_card("mdi-alert-octagon-outline", "高危止损拉黑", len(self._reconcile_blocked),
                          warn_color, "同集反复被删已停止自动回退，需人工处理"),
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
                "最近一次自动重开（误取消自愈）", "mdi-restart", ok_color,
                self._last_reopened, "reopen_time", "没有触发过自动重开",
                chip(f"{len(self._last_reopened)} 条", "success"),
            ),
            detail_section(
                "最近一次重置洗版进度", "mdi-restore", info_color,
                self._last_reset, "reset_time", "最近一次检查没有重置进度的订阅",
                chip(f"{len(self._last_reset)} 条", "info"),
            ),
            detail_section(
                "最近一次对账回退（库缺集重新补下）", "mdi-restore-alert", ok_color,
                self._last_reconcile, "time", "最近一次检查没有对账回退的订阅",
                chip(f"{len(self._last_reconcile)} 条", "success"),
            ),
            detail_section(
                "高危止损清单（已停止自动回退）", "mdi-alert-octagon-outline", warn_color,
                self._last_high_risk, "time", "没有被止损拉黑的订阅",
                chip(f"{len(self._last_high_risk)} 条", "warning"),
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
                      "text": "判定口径（2.10.0 多源完结信号 + 严取消门槛）"},
                     {"component": "div",
                      "text": "① 订阅新增那一刻：该季已播完就直接开整季洗版，未播完保持普通订阅；"},
                     {"component": "div",
                      "text": "② 定时巡检只维护已是洗版的订阅：未播完取消洗版，已播完且库缺集则重置洗版进度；"},
                     {"component": "div",
                      "text": "③ 已播完＝任一成立：TMDB Ended/Canceled/末集finale标记/更晚的季/"
                              "该季分集全部已播/库内齐全/豆瓣「更新至≥总期数」/站点全集强证据（仅保洗版）；"},
                     {"component": "div",
                      "text": "④ 取消洗版需同时满足：TMDB 有明确未来播出集 + 三源无完结证据 + "
                              "连续 N 轮确认（缺日期的集按未知处理，不再触发取消）；"},
                     {"component": "div",
                      "text": "⑤ 取消后若出现完结证据自动重开整季洗版（误取消自愈，7 天限频防翻转）。"},
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
                "reconcile_records": dict(self._reconcile_records),
                "reconcile_pending": {k: dict(v or {}) for k, v in self._reconcile_pending.items()},
                "reconcile_strikes": {k: dict(v or {}) for k, v in self._reconcile_strikes.items()},
                "reconcile_blocked": list(self._reconcile_blocked),
                "convert_records": dict(self._convert_records),
                "cancel_pending": {k: dict(v or {}) for k, v in self._cancel_pending.items()},
                "reopen_records": dict(self._reopen_records),
                "reopened_count": self._reopened_count,
                "last_reopened": (self._last_reopened or [])[:50],
                "reconcile_count": self._reconcile_count,
                "last_reconcile": (self._last_reconcile or [])[:50],
                "last_high_risk": (self._last_high_risk or [])[:50],
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

    @staticmethod
    def _int_cfg(value: Any, default: int) -> int:
        """把表单里的数字配置安全转成 int；空串/非法值回落到默认值。"""
        try:
            if value in (None, ""):
                return default
            return int(value)
        except (TypeError, ValueError):
            return default

    def _can_reconcile(self, subscribe_id: Any) -> bool:
        """对账回退限频：同一订阅在限频期内不再处理（0 天 = 不限频）。"""
        if self._reconcile_cooldown_days <= 0:
            return True
        last = self._reconcile_records.get(str(subscribe_id))
        if not last:
            return True
        return (time.time() - float(last)) >= self._reconcile_cooldown_days * 86400

    def _can_reopen(self, subscribe_id: Any) -> bool:
        """误取消自愈重开限频：同一订阅在限频期内不再重开（0 天 = 不限频）。"""
        if self._reopen_cooldown_days <= 0:
            return True
        last = self._reopen_records.get(str(subscribe_id))
        if not last:
            return True
        return (time.time() - float(last)) >= self._reopen_cooldown_days * 86400

    def _reconcile_normal_subs(self, subscribes: List[Any], now_str: str
                               ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """普通订阅库缺集对账回退（见模块说明 3）。

        返回 (回退明细, 本轮新增高危明细)。每轮最多处理 reconcile_max_per_run 部；
        单条异常只记日志，不影响其余订阅与主巡检。
        """
        actions: List[Dict[str, Any]] = []
        risks: List[Dict[str, Any]] = []
        if not self._reconcile_enabled or not self._subscribe_oper:
            return actions, risks
        # 通用性护栏：没有可用媒体服务器（未配置/全部禁用/主程序版本不支持枚举）时
        # 查库永远为空，绝不能据此回退下载事实——整轮跳过。
        if not self._library_server_names():
            logger.info("未配置可用的媒体服务器，本轮跳过对账回退")
            return actions, risks
        # 同一轮内的 TMDB 查询按 (tmdbid, season) 去重，几十部订阅不会打爆 TMDB
        air_cache: Dict[Tuple[int, int], Any] = {}
        total_cache: Dict[Tuple[int, int], int] = {}
        acted = 0
        for sub in subscribes:
            if acted >= self._reconcile_max_per_run:
                logger.info(f"对账回退已达本轮上限 {self._reconcile_max_per_run} 部，其余下一轮继续")
                break
            try:
                item = self._reconcile_one(sub, air_cache, total_cache, now_str)
            except Exception as e:
                logger.error(f"对账回退处理异常: {getattr(sub, 'name', '')} - {e}")
                continue
            if not item:
                continue
            if item.get("risk"):
                risks.append(item)
                continue
            acted += 1
            actions.append(item)
        return actions, risks

    def _reconcile_one(self, sub: Any, air_cache: Dict[Tuple[int, int], Any],
                       total_cache: Dict[Tuple[int, int], int],
                       now_str: str) -> Optional[Dict[str, Any]]:
        """处理单条订阅的对账回退；返回动作明细、高危明细或 None（无动作）。"""
        if sub.type != MediaType.TV.value:
            return None
        key = str(sub.id)
        # S=用户暂停、P=已有下载在进行：都不打扰
        if sub.state in ("S", "P"):
            return None
        if key in self._reconcile_blocked:
            return None
        # 用户手动冻结总集数（微短剧手设 99 等）的订阅不参与自动对账
        if getattr(sub, "manual_total_episode", 0):
            return None
        tmdbid = self._resolve_tmdbid(sub)
        if not tmdbid:
            return None
        if not self._can_reconcile(sub.id):
            return None
        cache_key = (tmdbid, sub.season)
        if cache_key not in total_cache:
            total_cache[cache_key] = self._get_tmdb_season_total(tmdbid, sub.season)
        season_total = total_cache[cache_key]
        if cache_key not in air_cache:
            # 对账回退沿用 TMDB 分级信号；豆瓣完结信号不参与本路径——
            # 它决定「转整季洗版」（开洗版方向），按口径只认 TMDB/库内证据。
            air_cache[cache_key] = self._season_signals(tmdbid, sub.season, None, air_cache)
        air = air_cache[cache_key]
        if not air or not air.get("tmdb_ok"):
            # 取不到该季分集信息：保守跳过，维持原对账口径
            return None
        season_finished = bool(self._tmdb_ended_tags(air))
        lib_eps = self._get_library_episodes(tmdbid, sub.season, sub.name, sub.year,
                                             getattr(sub, "media_id", None))
        missing_eps = [ep for ep in range(1, season_total + 1) if ep not in lib_eps] if season_total > 0 else []
        lib_complete = season_total > 0 and not missing_eps
        season_ended = season_finished or lib_complete
        if sub.best_version:
            # 洗版订阅由主巡检逻辑维护；本函数只负责「完结转洗版后限时兜底回退」
            return self._reconcile_grace_revert(sub, key, lib_eps, lib_complete, season_ended, now_str)
        note_set = {int(x) for x in (sub.note or []) if str(x).isdigit()}
        if not note_set:
            return None
        rolled = sorted(note_set - set(lib_eps))
        if not rolled:
            # 没有「note 标完成但库里没有」的集：只清理待确认记录
            self._reconcile_pending.pop(key, None)
            return None
        # 连续确认护栏：整季查空（媒体服务器可能重启/瞬断）需 3 轮、部分缺失需 2 轮，且每轮缺集一致
        pending = self._reconcile_pending.get(key)
        if pending and pending.get("rolled") == rolled:
            count = int(pending.get("count") or 0) + 1
        else:
            count = 1
        required_rounds = 3 if not lib_eps else 2
        if count < required_rounds:
            self._reconcile_pending[key] = {"rolled": rolled, "count": count}
            logger.info(f"对账回退待确认({count}/{required_rounds}): {sub.name} S{sub.season} "
                        f"缺 {self._format_missing_episodes(rolled)}")
            return None
        self._reconcile_pending.pop(key, None)
        # 集级止损预检：任一集回退次数将达上限 → 拉黑该订阅并进高危清单
        strikes = self._reconcile_strikes.setdefault(key, {})
        for ep in rolled:
            if int(strikes.get(str(ep), 0)) + 1 >= self._reconcile_stop_count:
                self._reconcile_blocked.add(key)
                reason = f"E{ep:02d} 已回退 {strikes.get(str(ep))} 次仍被删除，停止自动回退"
                logger.warning(f"对账回退止损拉黑: {sub.name} S{sub.season} ({reason})")
                self._save_state()
                return {
                    "risk": True,
                    "name": sub.name, "year": sub.year, "season": sub.season,
                    "episodes": rolled, "reason": reason, "time": now_str,
                }
        # 外科回退：只摘掉「note 标完成但库里没有」的集，其余字段（总集数/每集优先级/状态）不动。
        # 缺集数按主程序同一口径递增：缺失集数 = 库缺集 − note，note 摘除后差集恰好增加 len(rolled)。
        new_note = sorted(note_set - set(rolled))
        payload: Dict[str, Any] = {
            "note": new_note,
            "lack_episode": (sub.lack_episode or 0) + len(rolled),
        }
        converting = False
        if season_ended and season_total > 0 and not lib_complete:
            # 已完结的季：同一次写入转整季洗版，只认整季包一次补齐并完成洗版（用户口径）
            payload["best_version"] = 1
            payload["best_version_full"] = 1
            converting = True
        try:
            self._subscribe_oper.update(sid=sub.id, payload=payload)
        except Exception as e:
            logger.info(f"对账回退写入失败: {sub.name} S{sub.season} - {e}")
            return None
        for ep in rolled:
            strikes[str(ep)] = int(strikes.get(str(ep), 0)) + 1
        self._reconcile_records[key] = time.time()
        if converting:
            self._convert_records[key] = time.time()
        self._reconcile_count += 1
        self._save_state()
        action = "回退缺集并转整季洗版" if converting else "回退缺集，等待重新下载"
        logger.info(f"对账回退: {sub.name} S{sub.season} {action} "
                    f"({self._format_missing_episodes(rolled)})")
        return {
            "name": sub.name, "year": sub.year, "season": sub.season,
            "episodes": rolled, "action": action,
            "reason": "媒体库缺集但订阅标记为已完成",
            "time": now_str,
        }

    def _reconcile_grace_revert(self, sub: Any, key: str, lib_eps: Set[int],
                                lib_complete: bool, season_ended: bool,
                                now_str: str) -> Optional[Dict[str, Any]]:
        """完结季转整季洗版后的兜底：限时仍缺集（站上没有整季包）→ 回退普通订阅逐集补。"""
        converted_at = self._convert_records.get(key)
        if not converted_at:
            return None
        if lib_complete:
            # 已补齐：只清理兜底计时
            self._convert_records.pop(key, None)
            self._save_state()
            return None
        if not lib_eps:
            # 媒体库整季查空（可能媒体服务器瞬断）不做兜底回退，防止误关洗版
            return None
        if not season_ended:
            return None
        if (time.time() - float(converted_at)) < self._reconcile_grace_days * 86400:
            return None
        try:
            self._subscribe_oper.update(sid=sub.id, payload={"best_version": 0})
        except Exception as e:
            logger.info(f"转洗版兜底回退失败: {sub.name} S{sub.season} - {e}")
            return None
        self._convert_records.pop(key, None)
        self._reconcile_records[key] = time.time()
        self._save_state()
        logger.info(f"转洗版兜底回退: {sub.name} S{sub.season} 已过 "
                    f"{self._reconcile_grace_days} 天仍缺集，回退普通订阅逐集补")
        return {
            "name": sub.name, "year": sub.year, "season": sub.season,
            "episodes": [], "action": "转洗版兜底回退，逐集补",
            "reason": f"转整季洗版 {self._reconcile_grace_days} 天仍缺集（站上没有整季包）",
            "time": now_str,
        }

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
                            itemid: Optional[str] = None,
                            server: Optional[str] = None) -> Set[int]:
        """查询媒体服务器中该季已入库的集号；media_id 为空表示不按媒体身份查询。

        server 为空时主程序遍历全部媒体服务器并返回「第一台找到该剧的服务器」的结果；
        指定 server 时只查该服务器，供多服务器并集合并使用。
        """
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
            exists = chain.media_exists(mediainfo=mi, itemid=itemid, server=server)
            if exists and exists.seasons:
                eps = exists.seasons.get(season) or exists.seasons.get(str(season))
                if eps:
                    return set(eps)
        except Exception as e:
            logger.debug(f"查询媒体库失败: {title} S{season} - {e}")
        return set()

    def _get_library_episodes(self, tmdbid: int, season: int, title: str, year: str,
                              media_id: Optional[str] = None) -> Set[int]:
        """取媒体库中该季已入库的集号（所有已启用媒体服务器取并集）。

        主口径：完整媒体身份 + 本地索引条目 ID（条目名不一致也能命中）；
        再按已启用的每台媒体服务器逐台查询取并集——任一服务器上有该集就算存在（可播放），
        避免只看第一台服务器而把另一台还有的集误判为缺失；
        都没命中时按原方式（不带条目 ID）再查一次兜底。
        """
        resolved_id = str(media_id or tmdbid)
        itemid = self._resolve_library_item_id(season, title, year, resolved_id)
        episodes = self._query_media_exists(season, title, year,
                                            media_id=resolved_id, itemid=itemid)
        for server_name in self._library_server_names():
            episodes |= self._query_media_exists(season, title, year,
                                                 media_id=resolved_id, server=server_name)
        if not episodes:
            episodes = self._query_media_exists(season, title, year)
        return episodes

    @staticmethod
    def _library_server_names() -> List[str]:
        """返回已启用的影视媒体服务器名称（与主程序 API 同源），取不到时返回空列表。

        通用性：不硬编码任何播放器类型，Emby/Jellyfin/Plex/飞牛/绿联/TrimeMedia/ZSpace
        等凡是主程序能列出的实例都会逐台查询；纯音乐服务器（如 Navidrome）没有剧集
        概念，直接排除，避免无意义查询。
        """
        try:
            from app.application.mediaserver import get_mediaserver_configs
            return [c.name for c in get_mediaserver_configs()
                    if c.name and c.enabled and c.type != "navidrome"]
        except Exception as e:
            logger.debug(f"获取媒体服务器列表失败: {e}")
            return []

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

    def _season_signals(self, tmdbid: int, season: int,
                        tmdb_info: Optional[dict],
                        cache: Dict[Tuple[int, int], Dict[str, Any]]) -> Dict[str, Any]:
        """TMDB 单季分级完结信号（同轮按 (tmdbid, season) 缓存，见模块说明「判断逻辑」）。

        返回字段：
        - tmdb_ok：是否取到该季分集；
        - aired / total / unknown：已播集数、该季总集数、缺 air_date 的集数（未知）；
        - future：air_date 明确在未来、能证明「尚未播完」的集号（唯一可触发取消的强证据）；
        - all_aired：分集全部有日期且全部已过（单季播完的基础信号）；
        - finale：该季最后一集带 finale 标记（强完结信号）；
        - later_season：整剧存在更晚的季（本季必然已播完）；
        - status：TMDB 剧级状态（Ended/Canceled 为强完结信号）。
        """
        key = (tmdbid, season)
        if key in cache:
            return cache[key]
        sig: Dict[str, Any] = {
            "tmdb_ok": False, "aired": 0, "total": 0, "unknown": 0,
            "future": [], "all_aired": False, "finale": False,
            "later_season": False, "status": "",
        }
        cache[key] = sig
        try:
            episodes = TmdbChain().tmdb_episodes(tmdbid=tmdbid, season=season)
        except Exception as e:
            logger.debug(f"查询 TMDB 季分集失败: {tmdbid} S{season} - {e}")
            episodes = []
        sig["tmdb_ok"] = bool(episodes)
        if episodes:
            today = date.today()
            last_aired_no = 0
            for ep in episodes:
                ep_no = getattr(ep, "episode_number", None) or 0
                air_date = (getattr(ep, "air_date", None) or "").strip()
                if not air_date:
                    # 缺日期只算「未知」，不再当未播完（旧口径误取消主因）
                    sig["unknown"] += 1
                    continue
                try:
                    aired_date = datetime.strptime(air_date, "%Y-%m-%d").date()
                except ValueError:
                    sig["unknown"] += 1
                    continue
                if aired_date > today:
                    sig["future"].append(ep_no)
                else:
                    sig["aired"] += 1
                    last_aired_no = max(last_aired_no, ep_no)
            sig["future"] = sorted(x for x in sig["future"] if x > last_aired_no)
            last_ep = episodes[-1]
            sig["finale"] = (
                str(getattr(last_ep, "episode_type", "") or "").strip().lower() == "finale"
            )
            sig["all_aired"] = not sig["future"] and not sig["unknown"]
        sig["total"] = self._get_tmdb_season_total(tmdbid, season) or len(episodes)
        if tmdb_info:
            sig["status"] = str(tmdb_info.get("status") or "")
            for s in (tmdb_info.get("seasons") or []):
                try:
                    if int(s.get("season_number") or 0) > season:
                        sig["later_season"] = True
                        break
                except (TypeError, ValueError):
                    continue
        return sig

    @staticmethod
    def _tmdb_ended_tags(sig: Dict[str, Any]) -> List[str]:
        """把 TMDB 分级信号收敛为「已播完」证据标签（任一成立即算播完）。"""
        tags: List[str] = []
        if sig.get("status") in ("Ended", "Canceled"):
            tags.append(f"TMDB {sig['status']}")
        if sig.get("later_season"):
            tags.append("TMDB 存在更晚的季")
        if sig.get("finale") and not sig.get("future"):
            tags.append("TMDB 末集 finale 标记")
        if sig.get("all_aired") and sig.get("tmdb_ok"):
            tags.append(f"该季分集全部已播（{sig['aired']}/{sig['total']}）")
        return tags

    @staticmethod
    def _season_year(tmdb_info: Optional[dict], season: int,
                     fallback: Optional[str]) -> Optional[str]:
        """取该季播出年份（用于豆瓣按年匹配），取不到时回落订阅年份。"""
        for s in ((tmdb_info or {}).get("seasons") or []):
            try:
                if int(s.get("season_number") or 0) == season:
                    air = str(s.get("air_date") or "")[:4]
                    if air.isdigit():
                        return air
            except (TypeError, ValueError):
                continue
        return fallback

    def _douban_completion(self, sub: Any, season_year: Optional[str]
                           ) -> Tuple[Optional[bool], str]:
        """豆瓣完结信号：条目「更新至期数 >= 总期数」判已播完（国内综艺 TMDB 数据常缺）。

        返回 (是否已播完 True/False/None=无法判断, 说明)。
        匹配收紧：标题完全相等或互含，且年份与该季播出年份 ±1；匹配不上一律返回 None，
        调用方按「无法判断」处理，不改变现有行为。
        """
        if not self._douban_signal_enabled or DoubanApi is None:
            return None, ""
        name = (getattr(sub, "name", "") or "").strip()
        if not name:
            return None, ""
        cache_key = f"{name}|{season_year or ''}"
        cached = self._douban_cache.get(cache_key)
        if cached and (time.time() - cached[0]) < self._DOUBAN_CACHE_TTL:
            return cached[1], cached[2]
        result: Tuple[Optional[bool], str] = (None, "")
        try:
            api = DoubanApi()
            search = api.tv_search(name) or {}
            best = None  # (匹配级, 年份接近度, target)
            for item in (search.get("items") or []):
                target = item.get("target") or {}
                title = str(target.get("title") or "").strip()
                if not title:
                    continue
                if title == name:
                    level = 2
                elif name in title or title in name:
                    level = 1
                else:
                    continue
                year = str(target.get("year") or "").strip()
                year_delta = 99
                if season_year and year.isdigit():
                    year_delta = abs(int(year) - int(season_year))
                    if year_delta > 1:
                        continue
                if best is None or (level, -year_delta) > (best[0], best[1]):
                    best = (level, -year_delta, target)
            if best:
                target = best[2]
                detail = api.tv_detail(str(target.get("id") or "")) or {}
                count = int(detail.get("episodes_count") or 0)
                last = detail.get("last_episode_number")
                title = str(target.get("title") or name)
                if count and last:
                    ok = int(last) >= count
                    result = (ok, f"豆瓣《{title}》 {last}/{count} 期已完结"
                                  if ok else f"豆瓣《{title}》 {last}/{count} 期未完结")
        except Exception as e:
            logger.debug(f"豆瓣完结查询失败: {name} - {e}")
        self._douban_cache[cache_key] = (time.time(), result[0], result[1])
        return result

    def _site_completion_evidence(self, sub: Any) -> Tuple[bool, str]:
        """站点完结证据：只读主程序 RSS/spider 缓存候选标题，零站点请求。

        强证据 = 命中「完结/全集/全N集/complete」且不含「更至/更新至」等进行中表述；
        带「全N集」时 N 需不低于订阅总集数（能取到时）才算强证据。
        仅用于「保住洗版」（防误取消），不用于开洗版——站点标题「全集」在更新中也常见。
        """
        if not self._site_signal_enabled:
            return False, ""
        try:
            helper = getattr(TorrentsChain(), "get_subscribe_cache_candidates", None)
            if not callable(helper):
                return False, ""
            contexts = helper(sub, allow_title_match=False) or []
        except Exception as e:
            logger.debug(f"读取站点缓存候选失败: {getattr(sub, 'name', '')} - {e}")
            return False, ""
        sub_total = int(getattr(sub, "total_episode", 0) or 0)
        for context in contexts:
            meta_info = getattr(context, "meta_info", None)
            torrent_info = getattr(context, "torrent_info", None)
            text = " ".join(
                str(value or "")
                for value in (
                    getattr(torrent_info, "title", None),
                    getattr(meta_info, "title", None),
                    getattr(meta_info, "subtitle", None),
                )
            )
            if not text or not _SITE_COMPLETE_RE.search(text):
                continue
            if _SITE_ONGOING_RE.search(text):
                continue
            m = _SITE_TOTAL_RE.search(text)
            if m and sub_total and int(m.group(1)) < sub_total:
                # 「全N集」但 N 低于目标总集数：更像已放出部分的合集，不算完结
                continue
            title = str(getattr(torrent_info, "title", "") or "")[:80]
            return True, f"站点全集证据: {title}"
        return False, ""

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
        """统一入口：巡检三入口（定时任务/手动命令/API）互斥，即时判定沿用单条锁。

        巡检若被并发触发（如整点调度与手动「立即检查」撞上），后到者直接跳过——
        并发跑同一轮会竞争「读计数→改→写回」并重复通知。新增即时判定
        （subscribe_id 非空）不走本互斥，它由 _single_check_lock 单独串行。
        """
        if subscribe_id is None:
            if self._patrol_running:
                logger.info("已有洗版守护巡检正在运行，本次触发跳过（等下一轮）")
                return
            self._patrol_running = True
            try:
                self._guard_check_impl()
            finally:
                self._patrol_running = False
        else:
            self._guard_check_impl(subscribe_id=subscribe_id)

    def _guard_check_impl(self, subscribe_id: Optional[int] = None) -> None:
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
        reopened_this_run = 0
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        tmdb_cache: Dict[int, dict] = {}
        signal_cache: Dict[Tuple[int, int], Dict[str, Any]] = {}

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
            # 2.10.0：被本插件取消过洗版的普通订阅（_fixed_ids 在管）也放进来，
        # 完结证据出现时自动重开整季洗版（误取消自愈，见模块说明「判断逻辑」3）。
            if not allow_enable and not sub.best_version and str(sub.id) not in self._fixed_ids:
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

            # ── 多源完结判定（2.10.0，见模块说明「判断逻辑」）──
            # TMDB 分级信号 + 豆瓣 + 媒体库齐全，任一成立即「已播完」；
            # 唯一能触发取消的是 TMDB「明确未来播出集」这一强未播完证据。
            sig = self._season_signals(tmdbid, sub.season, tmdb_info, signal_cache)
            season_tmdb_total = sig["total"]
            aired_count = sig["aired"]
            ended_tags = self._tmdb_ended_tags(sig)
            douban_ok, douban_note = self._douban_completion(
                sub, self._season_year(tmdb_info, sub.season, sub.year))
            if douban_ok:
                ended_tags.append(douban_note)
            if not sig["tmdb_ok"] and not lib_complete and douban_ok is None:
                # TMDB 取不到分集且豆瓣也无法判断、库内也不齐：无任何依据，
                # 保守跳过本轮（不取消、不重置）。TMDB 缺分集但豆瓣能判时照常处理
                # ——这正是国内综艺 TMDB 数据缺失的主场景。
                logger.warning(f"取不到 TMDB 季分集且豆瓣无法判断，本轮跳过: "
                               f"{sub.name} S{sub.season}")
                continue
            # 库内该季已齐全同样按「已播完」处理：内容都在库里，本季没有可追的；
            # 兜住 TMDB 分集日期缺失/异常（实测库内 190 集齐全却被判「已播 50/190」）。
            if lib_complete:
                ended_tags.append("媒体库该季齐全")
            season_ended = bool(ended_tags)
            future_eps: List[int] = sig["future"]
            # 站点全集证据只用于「保住洗版」（防误取消），不参与开洗版
            site_hit = False
            site_note = ""
            if sub.best_version and not season_ended:
                site_hit, site_note = self._site_completion_evidence(sub)
                if site_hit:
                    season_ended = True
                    ended_tags.append(site_note)

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
                # 该季已播完：确保处于整季洗版状态（单季播完就洗版）。
                # 刚订阅的那一刻若还是普通订阅，这里一并开启洗版——同一批集反复
                # 「下不动→超时删种→补搜→再下」纯属浪费带宽与磁盘 IO，
                # 整季洗版让主程序只认整季包、一次到位。已在跑的订阅不做这种转换。
                payload: Dict[str, Any] = {}
                newly_enabled = allow_enable and not sub.best_version
                if newly_enabled:
                    payload["best_version"] = 1
                # 误取消自愈（2.10.0）：本插件取消过洗版的订阅（在管记录）出现完结证据
                # 时自动重开整季洗版；同一订阅 reopen_cooldown_days 天内最多一次，防翻转。
                auto_reopened = False
                if fix_key in self._fixed_ids and not sub.best_version and not newly_enabled:
                    if not self._reopen_enabled:
                        # 用户关闭了自动重开：清掉在管记录，不再监测自愈
                        self._fixed_ids.discard(fix_key)
                        self._save_state()
                    elif self._can_reopen(sub.id):
                        # 只置 payload 与标志；清在管记录、记限频时间戳都挪到
                        # update() 成功之后——若写库失败，记录保留、限频不消耗，
                        # 下一轮还能重试（2.10.2：旧版先清后写，写库瞬时失败
                        # 会让该订阅永久退出自愈监测且 7 天内不再试）。
                        auto_reopened = True
                        payload["best_version"] = 1
                        payload["best_version_full"] = 1
                    else:
                        # 重开限频期：保留在管记录、不清除——限频期过后的下一轮
                        # 出现完结证据仍会重开；若像旧版那样清掉记录，该订阅会
                        # 永久退出重开监测，限频期过后也无法自愈。
                        logger.info(f"已取消洗版的订阅出现完结证据，但处于重开限频期"
                                    f"（{self._reopen_cooldown_days} 天），保持普通订阅: "
                                    f"{sub.name} S{sub.season}")
                elif fix_key in self._fixed_ids:
                    # 已是洗版订阅（经本插件重开或其它途径恢复）：清掉在管记录即可
                    self._fixed_ids.discard(fix_key)
                    self._save_state()
                if (sub.best_version or payload.get("best_version")) and not sub.best_version_full \
                        and "best_version_full" not in payload:
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
                        elif auto_reopened:
                            # 写库成功才清在管记录并消耗限频（2.10.2 顺序修复）
                            self._reopen_records[fix_key] = time.time()
                            self._fixed_ids.discard(fix_key)
                            logger.info(f"误取消自愈：完结证据出现，已自动重开整季洗版: "
                                        f"{sub.name} S{sub.season}（{'；'.join(ended_tags)}）")
                            self._reopened_count += 1
                            reopened_this_run += 1
                            self._last_reopened.append({
                                "name": sub.name, "year": sub.year, "season": sub.season,
                                "reason": f"完结证据：{'；'.join(ended_tags)}",
                                "reopen_time": now_str,
                            })
                            self._save_state()
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

            # ── 无完结证据：进入取消洗版门槛（三层同时满足才取消，见模块说明 3）──
            # 只有洗版订阅需要「取消洗版」；普通订阅本就没开洗版，直接跳过（不做无意义动作）。
            if not sub.best_version:
                continue
            fix_key = str(sub.id)
            if fix_key in self._fixed_ids:
                # 之前取消过洗版但被重新开启了，清除记录重新处理
                self._fixed_ids.discard(fix_key)

            # 门槛 1：TMDB 有明确未来播出的分集（唯一强未播完证据）。
            # 缺 air_date 的集只算「未知」，不再触发取消（旧口径误取消主因）。
            if not future_eps:
                self._cancel_pending.pop(fix_key, None)
                logger.info(f"无明确未来播出集（未知 {sig['unknown']} 集按未知处理），"
                            f"不取消洗版: {sub.name} S{sub.season}")
                continue
            # 门槛 2 已在上方处理：三源（TMDB/豆瓣/站点）任一完结证据都会转入
            # season_ended 分支；能走到这里即三源均无完结证据。
            # 门槛 3：连续 cancel_confirm_rounds 轮巡检证据一致才执行取消。
            signature = f"{tmdbid}|{sub.season}|{','.join(str(x) for x in future_eps)}"
            pending = self._cancel_pending.get(fix_key)
            if pending and pending.get("signature") == signature:
                count = int(pending.get("count") or 0) + 1
            else:
                count = 1
            if count < self._cancel_confirm_rounds:
                self._cancel_pending[fix_key] = {"count": count, "signature": signature}
                self._save_state()
                logger.info(f"取消洗版待确认({count}/{self._cancel_confirm_rounds}): "
                            f"{sub.name} S{sub.season}（明确未播集 {future_eps}）")
                continue
            self._cancel_pending.pop(fix_key, None)

            reason = (f"明确未播完（未来播出集 {future_eps}，已播 "
                      f"{aired_count}/{season_tmdb_total} 集，连续 {count} 轮确认，三源无完结证据）")
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

        # 清理已删除订阅的在管取消记录（2.10.2）：订阅被删后其 ID 永久留在
        # _fixed_ids 里会虚增「在管取消记录」计数；只在巡检模式做，即时判定
        # 只带一条订阅、会误清其它在管记录。
        if not single:
            live_ids = {str(s.id) for s in subscribes}
            stale_keys = [k for k in self._fixed_ids if k not in live_ids]
            if stale_keys:
                for k in stale_keys:
                    self._fixed_ids.discard(k)
                logger.info(f"清理已删除订阅的在管取消记录 {len(stale_keys)} 条")
                self._save_state()

        # 普通订阅库缺集对账回退（仅定时巡检；订阅新增事件不触发本逻辑）
        reconcile_list: List[Dict[str, Any]] = []
        high_risk_list: List[Dict[str, Any]] = []
        if not single:
            reconcile_list, high_risk_list = self._reconcile_normal_subs(subscribes, now_str)

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
            if reconcile_list:
                self._last_reconcile = reconcile_list
            if high_risk_list:
                self._last_high_risk = high_risk_list
        self._save_state()

        logger.info(f"检查完成: 开启洗版 {len(enable_list)} 个，取消洗版 {len(fixed_list)} 个，"
                    f"重开洗版 {reopened_this_run} 个，重置 {len(reset_list)} 个，"
                    f"对账回退 {len(reconcile_list)} 个"
                    + (f"，新增高危 {len(high_risk_list)} 个" if high_risk_list else ""))

        if self._notify and (fixed_list or reset_list or enable_list or reconcile_list
                             or high_risk_list or reopened_this_run):
            lines = self._format_lists(fixed_list, reset_list, enable_list,
                                       reconcile_list=reconcile_list, risk_list=high_risk_list,
                                       reopen_count=reopened_this_run)
            self.post_message(title="洗版守护", text="\n".join(lines))

    @staticmethod
    def _format_lists(fixed_list: List[Dict[str, Any]],
                      reset_list: List[Dict[str, Any]],
                      enable_list: Optional[List[Dict[str, Any]]] = None,
                      limit: int = 10,
                      reconcile_list: Optional[List[Dict[str, Any]]] = None,
                      risk_list: Optional[List[Dict[str, Any]]] = None,
                      reopen_count: int = 0) -> List[str]:
        """格式化开启/取消/重开洗版、重置订阅、对账回退、高危清单，供通知复用。"""
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
        if reopen_count:
            lines.append(f"误取消自愈：自动重开洗版 {reopen_count} 个")
        if reset_list:
            names = "、".join(
                f"{f.get('name', '')} S{f.get('season')}" for f in reset_list[:limit]
            )
            suffix = f"等 {len(reset_list)} 个" if len(reset_list) > limit else ""
            lines.append(f"已重置订阅/洗版进度，等待重新下载: {names}{suffix}")
        if reconcile_list:
            names = "、".join(
                f"{r.get('name', '')} S{r.get('season')}({r.get('action', '')})" for r in reconcile_list[:limit]
            )
            suffix = f"等 {len(reconcile_list)} 个" if len(reconcile_list) > limit else ""
            lines.append(f"对账回退缺集，已重新补下: {names}{suffix}")
        if risk_list:
            names = "、".join(
                f"{r.get('name', '')} S{r.get('season')}({r.get('reason', '')})" for r in risk_list[:limit]
            )
            suffix = f"等 {len(risk_list)} 个" if len(risk_list) > limit else ""
            lines.append(f"⚠ 高危止损（已停止自动回退，请人工处理）: {names}{suffix}")
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

    async def _api_reconcile_unblock(self, apikey: str = "", subscribe_id: int = 0) -> Dict[str, Any]:
        """解除对账回退的止损拉黑与计数；带 subscribe_id 只解除单条。"""
        if subscribe_id:
            key = str(subscribe_id)
            self._reconcile_blocked.discard(key)
            self._reconcile_strikes.pop(key, None)
            self._reconcile_records.pop(key, None)
            self._reconcile_pending.pop(key, None)
        else:
            self._reconcile_blocked.clear()
            self._reconcile_strikes.clear()
        self._save_state()
        logger.info(f"解除对账回退拉黑: subscribe_id={subscribe_id or '全部'}，"
                    f"剩余拉黑 {len(self._reconcile_blocked)} 条")
        return {"success": True, "blocked": len(self._reconcile_blocked)}

    # ── 事件处理 ──────────────────────────────────────────────

    @eventmanager.register(EventType.PluginAction)
    def _on_plugin_action(self, event: Event = None) -> None:
        if not event or not event.event_data:
            return
        if event.event_data.get("action") != "bestversion_guard_check":
            return
        logger.info("收到手动检查命令")
        self._guard_check()
        lines = self._format_lists(self._last_fixed, self._last_reset, self._last_enabled,
                                   reconcile_list=self._last_reconcile, risk_list=self._last_high_risk)
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
