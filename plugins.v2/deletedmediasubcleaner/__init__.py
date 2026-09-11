"""删档订阅清理插件。

监控媒体服务器（Emby / Jellyfin / Plex 等）中电影与电视剧条目的存在情况：
当某个媒体从媒体库消失（本地文件被删除）后，若连续 N 天仍未回到媒体库，
同时系统中仍存在对应订阅、且该订阅在此期间没有任何新的下载记录，
则按配置处置该订阅（仅通知 / 停止订阅 / 删除订阅）。

安全设计：
1. 媒体服务器读取失败、或本轮没有取到任何条目时整轮中止，不做任何处置；
2. 首次运行只建立基线快照，不参与判定，避免把历史缺失误判为刚被删除；
3. 缺失期间出现新的下载记录视为订阅仍在工作，本轮不处置。
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.triggers.cron import CronTrigger

from app.chain.mediaserver import MediaServerChain
from app.core.event import eventmanager, Event
from app.db.oper.downloadhistory import DownloadHistoryOper
from app.db.oper.subscribe import SubscribeOper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, MediaSource, NotificationType
from app.sdk.services import MediaServerHelper

# 快照与缺失记录的数据键
DATA_SNAPSHOT = "snapshot"
DATA_MISSING = "missing"
DATA_STATE = "state"

# 支持的媒体类型
MEDIA_TYPE_MOVIE = "电影"
MEDIA_TYPE_TV = "电视剧"


class DeletedMediaSubCleaner(_PluginBase):
    """删档订阅清理插件。"""

    plugin_name = "删档订阅清理Q自用版"
    plugin_desc = ("媒体库中的电影/电视剧被删除后，若订阅仍存在且迟迟不下载，"
                   "连续达到宽限天数后按配置清理该订阅。")
    plugin_icon = ("https://raw.githubusercontent.com/q10710/MoviePilot-Plugins/"
                   "main/icons/deletedmediasubcleaner.png")
    plugin_version = "1.0.2"
    plugin_label = "订阅"
    plugin_author = "Q"
    author_url = "https://github.com/q10710"
    plugin_config_prefix = "deletedmediasubcleaner_"
    plugin_order = 62
    auth_level = 1

    # 运行状态
    _enabled: bool = False
    _notify: bool = True
    _scan_times: int = 1
    _missing_days: int = 3
    _action: str = "delete"
    _media_types: List[str] = [MEDIA_TYPE_MOVIE, MEDIA_TYPE_TV]
    _servers: List[str] = []
    _cron: str = "20 4 * * *"

    # 依赖对象
    _subscribe_oper = None
    _download_oper = None
    _media_chain = None
    _downloader_helper = None

    def init_plugin(self, config: dict = None) -> None:
        """根据插件配置初始化运行状态。"""
        self.stop_service()
        self._enabled = False
        self._subscribe_oper = SubscribeOper()
        self._download_oper = DownloadHistoryOper()
        self._media_chain = MediaServerChain()
        self._downloader_helper = MediaServerHelper()
        if not config:
            return
        self._enabled = bool(config.get("enabled"))
        self._notify = bool(config.get("notify", True))
        self._scan_times = max(1, min(int(config.get("scan_times") or 1), 4))
        self._missing_days = max(1, int(config.get("missing_days") or 3))
        self._action = str(config.get("action") or "delete")
        self._media_types = self._split_media_types(config.get("media_types"))
        self._servers = self._split_text(config.get("servers"))
        self._cron = self._build_cron(self._scan_times)
        logger.info(f"{self.plugin_name} 初始化完成：enabled={self._enabled}，"
                    f"类型={self._media_types}，宽限={self._missing_days}天，"
                    f"动作={self._action}，定时={self._cron}，服务器={self._servers or '全部'}")

    @staticmethod
    def _build_cron(times: int) -> str:
        """根据每日扫描次数生成定时表达式（分钟固定 20 分）。"""
        hours = {1: "4", 2: "4,16", 3: "4,12,20", 4: "4,10,16,22"}.get(int(times), "4")
        return f"20 {hours} * * *"

    @staticmethod
    def _split_text(value: Any) -> List[str]:
        """把逗号分隔的配置文本解析成列表。"""
        if not value:
            return []
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        return [item.strip() for item in str(value).split(",") if item.strip()]

    @classmethod
    def _split_media_types(cls, value: Any) -> List[str]:
        """解析需要监控的媒体类型，默认电影与电视剧。"""
        items = cls._split_text(value)
        result = [item for item in items if item in (MEDIA_TYPE_MOVIE, MEDIA_TYPE_TV)]
        return result or [MEDIA_TYPE_MOVIE, MEDIA_TYPE_TV]

    def get_state(self) -> bool:
        """返回插件启用状态。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """返回插件远程命令列表。"""
        return [
            {
                "cmd": "/del_media_sub_check",
                "event": EventType.PluginAction,
                "desc": "删档订阅清理：立即检测（不处置）",
                "category": "订阅",
                "data": {"action": "check"},
            },
            {
                "cmd": "/del_media_sub_run",
                "event": EventType.PluginAction,
                "desc": "删档订阅清理：检测并处置",
                "category": "订阅",
                "data": {"action": "run"},
            },
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        """本插件不对外提供 API。"""
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        """注册定时检测服务。"""
        if self._enabled and self._cron:
            return [
                {
                    "id": "DeletedMediaSubCleaner",
                    "name": "删档订阅检查",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.run_check,
                    "kwargs": {"handle": True},
                }
            ]
        return []

    def stop_service(self) -> None:
        """停止插件后台服务（定时任务由 MoviePilot 框架统一管理）。"""
        return None

    @eventmanager.register(EventType.PluginAction)
    def handle_command(self, event: Event = None) -> None:
        """处理远程命令事件。"""
        if not event or not event.event_data:
            return
        action = event.event_data.get("action")
        if action not in ("check", "run"):
            return
        self.post_message(
            channel=event.event_data.get("channel"),
            title="删档订阅清理",
            text="开始执行媒体库缺失检查 ...",
        )
        self.run_check(handle=action == "run")

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """返回插件配置表单与默认配置。"""
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                            "hint": "开启后按定时任务扫描媒体库缺失情况",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify",
                                            "label": "发送通知",
                                            "hint": "发现缺失或处置订阅时发送通知",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "scan_times",
                                            "label": "每日扫描次数",
                                            "items": [
                                                {"title": "1 次", "value": 1},
                                                {"title": "2 次", "value": 2},
                                                {"title": "3 次", "value": 3},
                                                {"title": "4 次", "value": 4},
                                            ],
                                            "hint": "扫描时间由次数自动生成",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "missing_days",
                                            "label": "缺失宽限天数",
                                            "type": "number",
                                            "min": "1",
                                            "hint": "媒体从媒体库消失后连续缺失多少天才处置",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "action",
                                            "label": "达标后动作",
                                            "items": [
                                                {"title": "仅通知", "value": "notify"},
                                                {"title": "暂停订阅", "value": "stop"},
                                                {"title": "删除订阅", "value": "delete"},
                                            ],
                                            "hint": "默认删除订阅。删除只影响订阅记录，不删除任何文件",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "media_types",
                                            "label": "监控媒体类型",
                                            "items": [
                                                {"title": "电影", "value": MEDIA_TYPE_MOVIE},
                                                {"title": "电视剧", "value": MEDIA_TYPE_TV},
                                            ],
                                            "multiple": True,
                                            "chips": True,
                                            "hint": "默认电影与电视剧都监控",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "servers",
                                            "label": "媒体服务器（留空为全部）",
                                            "hint": "多个服务器用英文逗号分隔，例如 emby,飞牛影视",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "warning",
                                            "variant": "tonal",
                                            "text": ("判定口径：媒体库条目消失 → 连续缺失达到宽限天数 → "
                                                     "订阅仍存在 → 期间没有新的下载记录 → 处置该订阅。"
                                                     "首次运行只建立基线，不做任何处置。"),
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "notify": True,
            "scan_times": 1,
            "missing_days": 3,
            "action": "delete",
            "media_types": [MEDIA_TYPE_MOVIE, MEDIA_TYPE_TV],
            "servers": "",
        }

    def get_page(self) -> List[dict]:
        """返回插件详情页，展示最近检测结果与待处置清单。"""
        state = self.get_data(DATA_STATE) or {}
        missing: Dict[str, dict] = self.get_data(DATA_MISSING) or {}
        handled: List[dict] = state.get("last_handled") or []

        rows: List[dict] = []
        if not state:
            rows.append({
                "component": "VAlert",
                "props": {"type": "info", "variant": "tonal", "text": "尚未运行，请先启用插件或执行一次检测"},
            })
        else:
            rows.append({
                "component": "VAlert",
                "props": {
                    "type": "success" if not missing else "warning",
                    "variant": "tonal",
                    "text": (f"最近检测：{state.get('last_run') or 'N/A'}，"
                             f"媒体库条目 {state.get('library_count') or 0} 个，"
                             f"待处置缺失 {len(missing)} 个，"
                             f"累计处置 {state.get('total_handled') or 0} 个"),
                },
            })

        if missing:
            rows.append({"component": "div", "text": "当前处于缺失观察期的媒体", "props": {"class": "text-subtitle-1 mt-3"}})
            for key, item in list(missing.items())[:20]:
                rows.append({
                    "component": "VAlert",
                    "props": {
                        "type": "info",
                        "variant": "tonal",
                        "text": (f"{item.get('title')}（{item.get('year') or '-'}｜{item.get('type')}）"
                                 f" 首次缺失 {item.get('first_missing')}，"
                                 f"已缺失 {item.get('last_days') or 0} 天"),
                    },
                })

        if handled:
            rows.append({"component": "div", "text": "最近处置记录", "props": {"class": "text-subtitle-1 mt-3"}})
            for item in handled[:20]:
                rows.append({
                    "component": "VAlert",
                    "props": {
                        "type": "success",
                        "variant": "tonal",
                        "text": (f"[{item.get('time')}] {item.get('title')}（{item.get('year') or '-'}）"
                                 f" → {item.get('result')}（缺失 {item.get('days')} 天）"),
                    },
                })

        return rows

    # ── 主流程 ────────────────────────────────────────────────

    def run_check(self, handle: bool = False) -> None:
        """执行一轮检测，handle=True 时按配置处置达标项。"""
        if not self._enabled:
            logger.warning(f"{self.plugin_name} 未启用，跳过本轮检测")
            return

        now = datetime.now()
        current = self.__collect_library_items()
        if current is None:
            logger.error("媒体库扫描失败，本轮中止，不做任何处置")
            return
        if not current:
            logger.warning("媒体库中未取到任何条目，本轮中止以避免误判")
            return

        snapshot: Dict[str, dict] = self.get_data(DATA_SNAPSHOT) or {}
        missing: Dict[str, dict] = self.get_data(DATA_MISSING) or {}
        state: Dict[str, Any] = self.get_data(DATA_STATE) or {}

        # 首次运行只建立基线
        if not snapshot:
            self.save_data(DATA_SNAPSHOT, current)
            state.update({
                "last_run": now.strftime("%Y-%m-%d %H:%M:%S"),
                "library_count": len(current),
                "baseline": True,
            })
            self.save_data(DATA_STATE, state)
            logger.info(f"首次运行，已建立媒体库基线快照（{len(current)} 个条目），本轮不做判定")
            return

        # 新增的缺失项
        disappeared = [key for key in snapshot if key not in current]
        for key in disappeared:
            if key in missing:
                continue
            info = snapshot.get(key) or {}
            missing[key] = {
                "title": info.get("title"),
                "year": info.get("year"),
                "type": info.get("type"),
                "media_source": info.get("media_source"),
                "media_id": info.get("media_id"),
                "first_missing": now.strftime("%Y-%m-%d %H:%M:%S"),
                "last_days": 0,
            }
            logger.info(f"检测到媒体库条目消失：{info.get('title')}（{info.get('year') or '-'}）")

        # 已恢复的缺失项
        recovered = [key for key in list(missing.keys()) if key in current]
        for key in recovered:
            info = missing.pop(key)
            logger.info(f"缺失媒体已回到媒体库，取消观察：{info.get('title')}")

        # 更新缺失天数并判定
        handled: List[dict] = []
        for key, item in list(missing.items()):
            first_missing = self.__parse_time(item.get("first_missing"))
            if not first_missing:
                missing.pop(key, None)
                continue
            lasted_days = (now - first_missing).days
            item["last_days"] = lasted_days
            if lasted_days < self._missing_days:
                continue

            # 达标：检查订阅与下载情况
            subscribes = self.__match_subscribes(item)
            if not subscribes:
                logger.info(f"{item.get('title')} 缺失 {lasted_days} 天但已无对应订阅，移出观察")
                missing.pop(key, None)
                continue

            download_state = self.__check_download_activity(item)
            if download_state is None:
                logger.warning(f"{item.get('title')} 下载记录查询失败，本轮不处置")
                continue
            if download_state:
                logger.info(f"{item.get('title')} 缺失 {lasted_days} 天，但期间有新的下载记录，继续观察")
                continue

            result = self.__handle_subscribes(subscribes=subscribes, item=item,
                                              days=lasted_days, handle=handle)
            if result:
                handled.append(result)
                missing.pop(key, None)

        # 保存快照与观察数据
        self.save_data(DATA_SNAPSHOT, current)
        self.save_data(DATA_MISSING, missing)
        state.update({
            "last_run": now.strftime("%Y-%m-%d %H:%M:%S"),
            "library_count": len(current),
            "baseline": False,
            "last_handled": handled[:50],
            "total_handled": int(state.get("total_handled") or 0) + len(handled),
        })
        self.save_data(DATA_STATE, state)

        logger.info(f"本轮检查完成：媒体库 {len(current)} 个条目，观察中 {len(missing)} 个，处置 {len(handled)} 个")
        if handled and self._notify:
            self.__notify_handled(handled)

    # ── 媒体库扫描 ────────────────────────────────────────────

    def __collect_library_items(self) -> Optional[Dict[str, dict]]:
        """扫描媒体服务器，返回 媒体标识 -> 条目信息 的快照；失败时返回 None。"""
        try:
            services = self._downloader_helper.get_services(
                name_filters=self._servers or None
            )
        except Exception as err:
            logger.error(f"获取媒体服务器实例失败：{err}")
            return None
        if not services:
            logger.error("没有可用的媒体服务器实例")
            return None

        snapshot: Dict[str, dict] = {}
        success_servers = 0
        for name in services.keys():
            libraries = None
            try:
                libraries = self._media_chain.librarys(server=name)
            except Exception as err:
                logger.error(f"获取媒体服务器 {name} 媒体库失败：{err}")
                continue
            if libraries is None:
                logger.warning(f"媒体服务器 {name} 未返回媒体库列表，跳过该服务器")
                continue
            success_servers += 1
            for library in libraries:
                if library.type not in self._media_types:
                    continue
                try:
                    # 显式分页拉取，避免媒体服务器分页细节导致条目缺失
                    offset = 0
                    page_size = 200
                    while True:
                        page_items = list(self._media_chain.items(
                            server=name, library_id=library.id,
                            start_index=offset, limit=page_size,
                        ))
                        if not page_items:
                            break
                        for item in page_items:
                            if not item:
                                continue
                            key = self.__media_key(item)
                            if not key:
                                continue
                            snapshot[key] = {
                                "title": item.title,
                                "year": item.year,
                                "type": library.type,
                                "media_source": item.media_source,
                                "media_id": item.media_id,
                            }
                        if len(page_items) < page_size:
                            break
                        offset += len(page_items)
                except Exception as err:
                    logger.error(f"读取媒体服务器 {name} 媒体库 {library.name} 条目失败：{err}")

        if success_servers == 0:
            return None
        return snapshot

    @staticmethod
    def __media_key(item: Any) -> Optional[str]:
        """生成媒体条目的唯一标识：优先使用数据源 ID。"""
        media_source = getattr(item, "media_source", None)
        media_id = getattr(item, "media_id", None)
        if media_source and media_id:
            return f"{media_source}:{media_id}"
        title = getattr(item, "title", None)
        if title:
            return f"title:{title}|{getattr(item, 'year', '') or ''}"
        return None

    # ── 订阅与下载判定 ────────────────────────────────────────

    def __match_subscribes(self, item: Dict[str, Any]) -> List[Any]:
        """按媒体身份匹配仍存在的订阅。"""
        media_source = item.get("media_source")
        media_id = str(item.get("media_id") or "")
        title = item.get("title")
        year = str(item.get("year") or "")
        media_type = item.get("type")

        try:
            subscribes = self._subscribe_oper.list(state=None) or []
        except Exception as err:
            logger.error(f"读取订阅列表失败：{err}")
            return []

        matched = []
        for sub in subscribes:
            if media_type and sub.type != media_type:
                continue
            sub_media_id = str(getattr(sub, "media_id", "") or "")
            sub_tmdbid = str(getattr(sub, "tmdbid", "") or "")
            if media_id and media_id in (sub_media_id, sub_tmdbid):
                # 数据源一致，或订阅使用 TMDB 编号
                if media_source and sub_media_id and sub_media_id != media_id:
                    continue
                matched.append(sub)
                continue
            if title and sub.name == title and (not year or str(sub.year or "") == year):
                matched.append(sub)
        return matched

    def __check_download_activity(self, item: Dict[str, Any]) -> Optional[bool]:
        """检查缺失期间是否有新的下载记录；查询失败返回 None。"""
        media_source = item.get("media_source")
        media_id = str(item.get("media_id") or "")
        if not media_source or not media_id:
            return False
        first_missing = self.__parse_time(item.get("first_missing"))
        if not first_missing:
            return False
        try:
            source = MediaSource(media_source)
        except Exception:
            source = media_source
        try:
            records = self._download_oper.get_by_media_identity(
                media_source=source, media_id=media_id
            ) or []
        except Exception as err:
            logger.warning(f"查询下载记录失败（{item.get('title')}）：{err}")
            return None
        threshold = first_missing.strftime("%Y-%m-%d %H:%M:%S")
        for record in records:
            record_date = str(getattr(record, "date", "") or "")
            if record_date and record_date >= threshold:
                return True
        return False

    # ── 处置 ──────────────────────────────────────────────────

    def __handle_subscribes(self, subscribes: List[Any], item: Dict[str, Any],
                            days: int, handle: bool) -> Optional[dict]:
        """按配置处置达标订阅，返回处理结果摘要。"""
        title = item.get("title")
        year = item.get("year")
        names = "、".join(str(sub.name) for sub in subscribes[:3])
        results = []

        for sub in subscribes:
            if not handle:
                results.append("检测模式未处置")
                continue
            try:
                if self._action == "delete":
                    self._subscribe_oper.delete(sid=sub.id)
                    results.append("已删除订阅")
                elif self._action == "stop":
                    self._subscribe_oper.update(sid=sub.id, payload={"state": "S"})
                    results.append("已暂停订阅")
                else:
                    results.append("仅通知")
            except Exception as err:
                logger.error(f"处置订阅 {sub.name} 失败：{err}")
                results.append(f"处置失败：{err}")

        result_text = "；".join(dict.fromkeys(results)) or "仅通知"
        logger.info(f"{title}（{year}）缺失 {days} 天且订阅未下载 → {result_text}（订阅：{names}）")

        if not handle and self._notify:
            self.post_message(
                mtype=NotificationType.Subscribe,
                title="【删档订阅检测】",
                text=(f"{title}（{year or '-'}）已从媒体库消失 {days} 天，"
                      f"订阅仍在且期间没有下载记录，当前为检测模式未处置。"),
            )

        return {
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "title": title,
            "year": year,
            "result": result_text,
            "days": days,
        }

    def __notify_handled(self, handled: List[dict]) -> None:
        """发送本轮处置汇总通知。"""
        lines = [
            f"{item.get('title')}（{item.get('year') or '-'}）→ {item.get('result')}"
            for item in handled[:10]
        ]
        suffix = f"\n以及其它 {len(handled) - 10} 个" if len(handled) > 10 else ""
        self.post_message(
            mtype=NotificationType.Subscribe,
            title="【删档订阅清理】",
            text="以下媒体已从媒体库删除且订阅长期未下载，已按配置处理：\n" + "\n".join(lines) + suffix,
        )

    @staticmethod
    def __parse_time(value: Any) -> Optional[datetime]:
        """解析时间字符串。"""
        if not value:
            return None
        try:
            return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None
