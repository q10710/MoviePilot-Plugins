"""硬链接孤立文件检查插件。

扫描下载目录和媒体库目录，找出 links=1 的孤立视频文件。
每次扫描重新检查，不依赖历史记录。
同一文件连续 3 次扫描都出现在孤立列表中 → 自动删除。

另附「空壳目录」清理：媒体库里只剩 nfo/海报/字幕、没有任何视频的季目录或剧目录，
可按配置在出现后直接删除（媒体库维护，与硬链接判定无关）。
"""

import os
import re
import time
import shutil
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType


class LinkChecker(_PluginBase):
    """硬链接孤立文件检查插件。"""

    plugin_name = "硬链接检查Q自用版"
    plugin_desc = "扫描下载目录和媒体库目录中的孤立硬链接文件，连续3天孤立自动删除；并可清理只剩元数据、没有视频的空壳季目录/剧目录。"
    plugin_icon = "https://raw.githubusercontent.com/q10710/MoviePilot-Plugins/main/icons/linkchecker.png"
    plugin_version = "3.3.4"
    plugin_label = "文件管理"
    plugin_author = "Q"
    author_url = "https://github.com/q10710"
    plugin_config_prefix = "linkchecker_"
    plugin_order = 50
    auth_level = 1

    _VIDEO_EXTS = {".mkv", ".mp4", ".ts", ".avi", ".m2ts", ".mov", ".wmv", ".flv", ".webm", ".m4v"}

    # 空壳目录判据：目录下所有文件的扩展名都落在本白名单内（元数据/字幕/字体/校验文件）才算「只剩壳」。
    # 采用白名单而不是黑名单：只要出现任何视频、音频或未知类型文件，就不认为是空壳，避免误删有实体内容的目录。
    _META_EXTS = {
        ".nfo", ".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp",
        ".srt", ".ass", ".ssa", ".sub", ".idx", ".sup", ".vtt",
        ".ttf", ".otf", ".xml", ".sfv", ".txt", ".log", ".url", ".db",
    }
    # 影视条目标识文件名：目录内出现任一即认为该目录是「剧/电影条目目录」，而不是分类目录。
    _MEDIA_MARKER_FILES = {
        "tvshow.nfo", "movie.nfo", "poster.jpg", "folder.jpg", "fanart.jpg", "backdrop.jpg",
    }
    # 无扩展名的刮削图文件名（Kodi/Emby 常见）：这类文件算元数据，不影响空壳判定；
    # 其它来源不明的无扩展名文件按实体文件处理，保护目录不被误删。
    _ARTIFACT_NAMES = {
        "fanart", "backdrop", "logo", "clearlogo", "clearart", "landscape",
        "banner", "thumb", "disc", "discart", "art", "keyart", "cdart",
        "poster", "folder", "season", "tvshow", "default",
    }
    # 季目录名特征（小写、整名匹配），如 Season 1 / S01 / Specials / 第1季 / OVA。
    _SEASON_DIR_PATTERNS = (
        r"^season\s*\d+$",
        r"^s\d{1,2}$",
        r"^specials?$",
        r"^第\s*\d+\s*季$",
        r"^ova$",
    )

    # 常见收容目录名：路径中任意一层目录名命中即跳过扫描。
    # 收容目录里的种子通常 links=1、且媒体库侧没有对应 inode，天然符合「孤立」判定，
    # 但它是有意保留做种的，不能当残留清理；按目录名兜底可让不同环境都自动兼容。
    _RELOCATE_DIR_NAMES = {"hr", "h&r", "relocate", "reloc", "收容"}

    _enabled = False
    _download_dirs: List[str] = []
    _library_dirs: List[str] = []
    _ignore_dirs: List[str] = []
    _exclude_dirs: List[str] = []
    _relocate_dirs: List[str] = []
    _cron: str = ""
    _auto_delete: bool = False
    _delete_threshold: int = 3
    _notify: bool = False
    # 是否允许删除媒体库侧的断开残留（默认关闭：媒体库那份通常是唯一副本）
    _allow_library_delete: bool = False
    # 曾经出现过硬链接（links>=2）的文件指纹，用于判断「曾经被硬链接过、如今已断开」
    _linked_seen: set = None
    _last_scan_time: Optional[str] = None
    _last_download_orphans: List[Dict[str, Any]] = []
    _last_library_orphans: List[Dict[str, Any]] = []
    # 持久化：文件路径 → 连续出现天数
    _orphan_tracker: Dict[str, int] = {}
    # 上次扫描日期，同一天多次扫描不重复计数
    _last_scan_date: str = ""
    # 本次扫描删除的文件数
    _last_deleted: int = 0
    # 是否清理空壳目录（独立开关，默认关闭，公开给他人使用时需显式开启）
    _clean_empty_dirs: bool = False
    # 空壳目录删除阈值（按天计，默认 1 = 出现即清理）
    _empty_dir_days: int = 1
    # 空壳目录静置小时数：目录内最新文件距今不足该时长则跳过，避免误删正在整理写入的目录
    _empty_dir_grace_hours: int = 24
    # 空壳目录跟踪记录：目录路径 → 连续出现天数
    _empty_tracker: Dict[str, int] = {}
    # 本次扫描识别到的空壳目录（未删除的）
    _last_empty_dirs: List[Dict[str, Any]] = []
    # 本次扫描删除的空壳目录数
    _last_deleted_dirs: int = 0

    def init_plugin(self, config: dict = None) -> None:
        """根据插件配置初始化运行状态。"""
        self.stop_service()
        self._download_dirs = []
        self._library_dirs = []
        self._ignore_dirs = []
        self._exclude_dirs = []
        self._relocate_dirs = []
        self._cron = ""
        self._auto_delete = False
        self._delete_threshold = 3
        self._notify = False
        self._allow_library_delete = False
        self._linked_seen = set()
        self._clean_empty_dirs = False
        self._empty_dir_days = 1
        self._empty_dir_grace_hours = 24
        self._empty_tracker = {}
        self._last_empty_dirs = []
        self._last_deleted_dirs = 0
        # 加载持久化的跟踪记录
        saved = self.get_data("tracker") or {}
        self._orphan_tracker = saved.get("orphans", {})
        self._linked_seen = set(saved.get("linked") or [])
        self._empty_tracker = dict(saved.get("empty") or {})
        if not config:
            self._enabled = False
            return
        self._enabled = bool(config.get("enabled"))
        raw_dl = config.get("download_dirs") or ""
        raw_lib = config.get("library_dirs") or ""
        raw_ignore = config.get("ignore_dirs") or ""
        raw_exclude = config.get("exclude_dirs") or ""
        self._download_dirs = [d.strip() for d in raw_dl.split("\n") if d.strip()]
        self._library_dirs = [d.strip() for d in raw_lib.split("\n") if d.strip()]
        self._ignore_dirs = [d.strip() for d in raw_ignore.split("\n") if d.strip()]
        self._exclude_dirs = [d.strip() for d in raw_exclude.split("\n") if d.strip()]
        self._relocate_dirs = self._collect_relocate_dirs()
        self._cron = config.get("cron") or ""
        self._auto_delete = bool(config.get("auto_delete"))
        self._delete_threshold = int(config.get("delete_threshold") or 3)
        self._notify = bool(config.get("notify"))
        self._allow_library_delete = bool(config.get("allow_library_delete"))
        self._clean_empty_dirs = bool(config.get("clean_empty_dirs"))
        # 配置缺省值不能用 `or 默认值`：键未提交时应回落到默认值，而不是被当成 0
        raw_days = config.get("empty_dir_days")
        try:
            self._empty_dir_days = int(raw_days) if raw_days not in (None, "") else 1
        except (TypeError, ValueError):
            self._empty_dir_days = 1
        if self._empty_dir_days < 1:
            self._empty_dir_days = 1
        raw_grace = config.get("empty_dir_grace_hours")
        try:
            self._empty_dir_grace_hours = int(raw_grace) if raw_grace not in (None, "") else 24
        except (TypeError, ValueError):
            self._empty_dir_grace_hours = 24
        if self._empty_dir_grace_hours < 0:
            self._empty_dir_grace_hours = 0
        logger.info(
            f"初始化完成, enabled={self._enabled}, cron={self._cron}, "
            f"auto_delete={self._auto_delete}, threshold={self._delete_threshold}, "
            f"允许删媒体库残留={self._allow_library_delete}, 已记录硬链接指纹={len(self._linked_seen)}, "
            f"清理空壳目录={self._clean_empty_dirs}(阈值{self._empty_dir_days}天/静置{self._empty_dir_grace_hours}小时), "
            f"跟踪空壳目录={len(self._empty_tracker)}, "
            f"额外排除={len(self._exclude_dirs)} 项, 收容目录={self._relocate_dirs or '（按目录名兜底）'}"
        )
        if self._enabled and self._cron:
            # 服务注册交给 get_service()，由 MoviePilot 主调度器统一调度（插件重载后自动恢复）
            logger.info(f"定时扫描将由主调度器注册: {self._cron}")

    def get_state(self) -> bool:
        """获取插件启用状态。"""
        return self._enabled

    def get_service(self) -> List[Dict[str, Any]]:
        """向 MoviePilot 主调度器注册定时扫描服务（标准做法）。

        旧版用插件内自建的 BackgroundScheduler：重载插件会导致该线程失效且不再恢复
        （实测 2026-09-11 改时间并重载后，14:30 的扫描没有触发），因此改为标准 get_service()，
        由主调度器统一管理，插件重载或主程序重启后自动恢复，也能在调度器列表中查到。
        """
        if not self._enabled or not self._cron:
            return []
        try:
            trigger = CronTrigger.from_crontab(self._cron, timezone=settings.TZ)
        except Exception as err:
            logger.error(f"定时扫描 cron 表达式无效，未注册定时任务：{self._cron} - {err}")
            return []
        return [{
            "id": f"{self.__class__.__name__}Scan",
            "name": "硬链接检查定时扫描",
            "trigger": trigger,
            "func": self._scheduled_scan,
        }]

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """返回插件远程命令列表。"""
        return [
            {
                "cmd": "/link_check",
                "event": EventType.PluginAction,
                "desc": "手动触发硬链接孤立文件扫描",
                "category": "文件管理",
                "data": {"action": "link_check_scan"},
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        """返回插件 API 列表。"""
        return [
            {
                "path": "/scan",
                "endpoint": self._api_scan,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "触发孤立文件扫描",
            },
            {
                "path": "/clean",
                "endpoint": self._api_clean,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "手动清理孤立文件",
            },
            {
                "path": "/reset",
                "endpoint": self._api_reset,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "重置跟踪记录",
            },
            {
                "path": "/clean_empty",
                "endpoint": self._api_clean_empty,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "手动清理识别到的空壳目录",
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
                                    "model": "download_dirs",
                                    "label": "下载目录（每行一个）",
                                    "placeholder": "/nastools/data/downloads/dianying/",
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
                                    "model": "library_dirs",
                                    "label": "媒体库目录（每行一个）",
                                    "placeholder": "/nastools/data/media/dianying/",
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
                                    "model": "ignore_dirs",
                                    "label": "忽略目录（每行一个，路径片段匹配）",
                                    "placeholder": "/nastools/data/downloads/dianying/外语电影/",
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
                                    "model": "exclude_dirs",
                                    "label": "额外排除目录（每行一个，路径片段匹配）",
                                    "placeholder": "/downloads/收容/",
                                    "hint": (
                                        "收容目录（保种目录）会自动排除：既按目录名 hr / h&r / relocate / 收容 兜底，"
                                        "也会自动读取订阅助手插件里配置的收容目录；其它需要保护的目录可在此追加"
                                    ),
                                    "persistent-hint": True,
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
                                    "model": "cron",
                                    "label": "定时扫描 Cron（留空不启用）",
                                    "placeholder": "0 4 * * *",
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
                                "props": {
                                    "model": "auto_delete",
                                    "label": "启用自动删除（连续孤立达到阈值后自动删除）",
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
                                    "model": "delete_threshold",
                                    "label": "自动删除阈值（连续天数，默认 3）",
                                    "placeholder": "3",
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
                                "props": {"model": "notify", "label": "发现孤立文件或删除时通知"},
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VSwitch",
                                "props": {
                                    "model": "allow_library_delete",
                                    "label": "允许删除媒体库侧的断开残留（默认关闭）",
                                    "hint": (
                                        "默认只清理下载侧：当硬链接断开且媒体库那份已不存在时，删除下载侧残留。"
                                        "媒体库侧的断开残留默认只报告不删除（那份通常是唯一副本）。"
                                    ),
                                    "persistent-hint": True,
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
                                "props": {
                                    "model": "clean_empty_dirs",
                                    "label": "清理空壳目录（媒体库里只剩 nfo/海报、没有视频的季/剧目录，默认关闭）",
                                    "hint": (
                                        "只扫媒体库目录：目录下所有文件都是元数据/字幕（nfo/jpg/srt 等），"
                                        "且能识别为剧集或电影条目目录时才判为空壳，分类目录（国漫/国产剧等）不受影响；"
                                        "目录内最新文件距今不足「空壳静置小时数」的会跳过，避免整理写入中被误删。"
                                    ),
                                    "persistent-hint": True,
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
                                    "model": "empty_dir_days",
                                    "label": "空壳目录清理阈值（连续天数，默认 1 = 出现即清理）",
                                    "placeholder": "1",
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
                                "component": "VTextField",
                                "props": {
                                    "model": "empty_dir_grace_hours",
                                    "label": "空壳静置小时数（目录内最新文件距今不足该时长则跳过，默认 24）",
                                    "placeholder": "24",
                                    "type": "number",
                                },
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "download_dirs": "",
            "library_dirs": "",
            "ignore_dirs": "",
            "exclude_dirs": "",
            "cron": "",
            "auto_delete": False,
            "delete_threshold": 3,
            "notify": False,
            "allow_library_delete": False,
            "clean_empty_dirs": False,
            "empty_dir_days": 1,
            "empty_dir_grace_hours": 24,
        }

    def get_page(self) -> Optional[List[dict]]:
        """返回插件详情页面。"""
        if not self._enabled:
            return [{"component": "VAlert", "props": {"type": "warning", "text": "插件未启用"}}]

        scan_time = self._last_scan_time or "尚未扫描"
        dl_count = len(self._last_download_orphans)
        lib_count = len(self._last_library_orphans)
        dl_size = self._format_size(sum(f.get("_size", 0) for f in self._last_download_orphans))
        lib_size = self._format_size(sum(f.get("_size", 0) for f in self._last_library_orphans))
        tracking_count = len(self._orphan_tracker)
        empty_count = len(self._last_empty_dirs)

        page = [
            {
                "component": "VCard",
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {"title": f"扫描时间: {scan_time}"},
                    },
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 4},
                                        "content": [
                                            {
                                                "component": "VAlert",
                                                "props": {
                                                    "type": "warning" if dl_count else "success",
                                                    "text": f"下载目录孤立: {dl_count} 个 ({dl_size})",
                                                    "variant": "tonal",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 4},
                                        "content": [
                                            {
                                                "component": "VAlert",
                                                "props": {
                                                    "type": "warning" if lib_count else "success",
                                                    "text": f"媒体库孤立: {lib_count} 个 ({lib_size})",
                                                    "variant": "tonal",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 4},
                                        "content": [
                                            {
                                                "component": "VAlert",
                                                "props": {
                                                    "type": "info",
                                                    "text": f"跟踪中: {tracking_count} 个文件",
                                                    "variant": "tonal",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12},
                                        "content": [
                                            {
                                                "component": "VAlert",
                                                "props": {
                                                    "type": "warning" if empty_count else "success",
                                                    "variant": "tonal",
                                                    "text": (
                                                        f"空壳目录: {empty_count} 个（本次已删除 {self._last_deleted_dirs} 个，"
                                                        f"清理开关: {'开' if self._clean_empty_dirs else '关'}，"
                                                        f"阈值 {self._empty_dir_days} 天，静置 {self._empty_dir_grace_hours} 小时）"
                                                    ),
                                                },
                                            }
                                        ],
                                    },
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VCardActions",
                        "content": [
                            {
                                "component": "VBtn",
                                "props": {"color": "primary"},
                                "text": "立即扫描",
                                "events": {
                                    "click": {
                                        "api": "plugin/LinkChecker/scan",
                                        "method": "get",
                                        "params": {"apikey": settings.API_TOKEN},
                                    }
                                },
                            },
                            {
                                "component": "VBtn",
                                "props": {"color": "warning", "disabled": dl_count == 0},
                                "text": "清理下载目录",
                                "events": {
                                    "click": {
                                        "api": "plugin/LinkChecker/clean",
                                        "method": "get",
                                        "params": {"target": "download", "apikey": settings.API_TOKEN},
                                    }
                                },
                            },
                            {
                                "component": "VBtn",
                                "props": {"color": "warning", "disabled": lib_count == 0},
                                "text": "清理媒体库",
                                "events": {
                                    "click": {
                                        "api": "plugin/LinkChecker/clean",
                                        "method": "get",
                                        "params": {"target": "library", "apikey": settings.API_TOKEN},
                                    }
                                },
                            },
                            {
                                "component": "VBtn",
                                "props": {"color": "error", "variant": "outlined"},
                                "text": "重置跟踪",
                                "events": {
                                    "click": {
                                        "api": "plugin/LinkChecker/reset",
                                        "method": "get",
                                        "params": {"apikey": settings.API_TOKEN},
                                    }
                                },
                            },
                            {
                                "component": "VBtn",
                                "props": {"color": "warning", "variant": "outlined", "disabled": empty_count == 0},
                                "text": "清理空壳目录",
                                "events": {
                                    "click": {
                                        "api": "plugin/LinkChecker/clean_empty",
                                        "method": "get",
                                        "params": {"apikey": settings.API_TOKEN},
                                    }
                                },
                            },
                        ],
                    },
                ],
            }
        ]

        if self._last_download_orphans:
            page.append(self._build_table_card("下载目录孤立文件", self._last_download_orphans))

        if self._last_library_orphans:
            page.append(self._build_table_card("媒体库孤立文件", self._last_library_orphans))

        if self._last_empty_dirs:
            page.append(self._build_empty_dir_card(self._last_empty_dirs))

        return page

    def _build_table_card(self, title: str, items: List[Dict[str, Any]]) -> dict:
        """构建文件列表卡片。"""
        list_items = []
        for item in items[:100]:
            count = item.get("_track_count", 0)
            count_str = f" [连续{count}天]" if count > 1 else ""
            list_items.append({
                "component": "VListItem",
                "content": [
                    {
                        "component": "VListItemTitle",
                        "text": f"{item['file_name']}{count_str}",
                    },
                    {
                        "component": "VListItemSubtitle",
                        "text": f"{item['dir_path']} | {item['size_str']} | {item['mtime']} | 硬链接: {item['nlink']}",
                    },
                    {
                        "component": "VListItemSubtitle",
                        "text": f"媒体: {item['media_info']}",
                    },
                ],
            })
        return {
            "component": "VCard",
            "content": [
                {"component": "VCardTitle", "props": {"title": f"{title} ({len(items)} 个)"}},
                {
                    "component": "VCardText",
                    "content": [
                        {
                            "component": "VList",
                            "props": {"dense": True},
                            "content": list_items,
                        }
                    ],
                },
            ],
        }

    def _build_empty_dir_card(self, items: List[Dict[str, Any]]) -> dict:
        """构建空壳目录列表卡片。"""
        list_items = []
        for item in items[:100]:
            count = item.get("_track_count", 0)
            count_str = f" [连续{count}天]" if count > 1 else ""
            list_items.append({
                "component": "VListItem",
                "content": [
                    {
                        "component": "VListItemTitle",
                        "text": f"{item['dir_name']}{count_str}",
                    },
                    {
                        "component": "VListItemSubtitle",
                        "text": (
                            f"{item['dir_path']} | 剩余元数据文件 {item['file_count']} 个 "
                            f"| 最新更新 {item['mtime']}"
                        ),
                    },
                ],
            })
        return {
            "component": "VCard",
            "content": [
                {"component": "VCardTitle", "props": {"title": f"空壳目录 ({len(items)} 个)"}},
                {
                    "component": "VCardText",
                    "content": [
                        {
                            "component": "VList",
                            "props": {"dense": True},
                            "content": list_items,
                        }
                    ],
                },
            ],
        }

    def stop_service(self) -> None:
        """停止插件后台服务并释放资源。"""
        # 定时任务已交由 MoviePilot 主调度器统一管理，无需在此手动清理
        pass

    def _scheduled_scan(self) -> None:
        """定时扫描入口。"""
        logger.info("定时扫描开始")
        self._do_scan()
        total = len(self._last_download_orphans) + len(self._last_library_orphans)
        if self._notify and (
            total > 0
            or self._last_deleted > 0
            or self._last_empty_dirs
            or self._last_deleted_dirs > 0
        ):
            self._send_notify()

    # ── 核心逻辑 ──────────────────────────────────────────────

    def _save_tracker(self) -> None:
        """持久化跟踪记录。"""
        self.save_data("tracker", {
            "orphans": self._orphan_tracker,
            "linked": sorted(self._linked_seen),
            "empty": self._empty_tracker,
        })

    @staticmethod
    def _path_key(path: str) -> str:
        """路径指纹：用于记录「曾经被硬链接过」的文件，避免持久化超长路径列表。"""
        return hashlib.md5(str(path).encode("utf-8", errors="ignore")).hexdigest()[:20]

    def _do_scan(self) -> None:
        """执行扫描：找出「曾经被硬链接、如今只剩一份」的文件，按天计数并在达阈值后处置。

        判定要点（2026-09-11 重做）：
        - 只有 links 曾经 ≥2（说明下载侧与媒体库侧曾经同时存在）而现在 =1 的文件才算「断开残留」；
          从未被硬链接过的文件（移动入库、独立文件等）一律不处理，避免把媒体库正常文件当残留删除。
        - 媒体库侧默认只报告不删除（那份通常是唯一副本），除非显式开启「允许删除媒体库残留」。
        - 下载侧的删除仍受「自动删除」开关与阈值控制。
        """
        self._last_download_orphans = []
        self._last_library_orphans = []
        self._last_deleted = 0

        # 扫描两侧全部视频文件（含硬链接），用于建立/更新「曾经硬链接」历史
        all_dl = self._scan_files(self._download_dirs)
        all_lib = self._scan_files(self._library_dirs)
        dl_prefixes = tuple(d.rstrip("/") + "/" for d in self._download_dirs)

        current_paths: set = set()
        download_side: set = set()
        for f in all_dl + all_lib:
            path = f["_path"]
            key = self._path_key(path)
            if int(f.get("_nlink") or 1) >= 2:
                # 当前仍是硬链接：记入历史，供以后判断是否断开
                self._linked_seen.add(key)
                continue
            if key in self._linked_seen:
                current_paths.add(path)
                if dl_prefixes and path.startswith(dl_prefixes):
                    download_side.add(path)

        # 展示列表：只列断开残留（下载侧与媒体库侧分开）
        dl_orphans = [f for f in all_dl if f["_path"] in download_side]
        lib_orphans = [f for f in all_lib if f["_path"] in current_paths and f["_path"] not in download_side]

        # 按天计数：同一天多次扫描只算一次
        today = datetime.now().strftime("%Y-%m-%d")
        is_new_day = today != self._last_scan_date

        if is_new_day:
            self._last_scan_date = today
            # 新的一天：本次出现的 +1 天，本次未出现的清零
            for path in list(self._orphan_tracker.keys()):
                if path in current_paths:
                    self._orphan_tracker[path] += 1
                else:
                    del self._orphan_tracker[path]
            for path in current_paths:
                if path not in self._orphan_tracker:
                    self._orphan_tracker[path] = 1
        else:
            # 同一天：只添加新出现的，已有记录不变
            for path in current_paths:
                if path not in self._orphan_tracker:
                    self._orphan_tracker[path] = 1

        # 检查是否达到删除阈值
        to_delete: List[str] = []
        for path, days in self._orphan_tracker.items():
            if days < self._delete_threshold:
                continue
            if path in download_side:
                if self._auto_delete:
                    to_delete.append(path)
            elif self._allow_library_delete:
                # 媒体库侧默认只报告；开启后才会删除（那份通常是唯一副本，谨慎）
                to_delete.append(path)

        # 删除达到阈值的文件
        for path in to_delete:
            try:
                if os.path.exists(path):
                    os.remove(path)
                    self._last_deleted += 1
                    logger.info(f"自动删除孤立文件(连续{self._orphan_tracker[path]}天): {path}")
                del self._orphan_tracker[path]
            except OSError as e:
                logger.info(f"自动删除失败: {path} - {e}")

        # 空壳目录清理（媒体库维护，独立开关与阈值）
        self._scan_empty_dirs(is_new_day)

        self._save_tracker()

        # 过滤掉已删除的文件，构建展示列表
        deleted_paths = set(to_delete)
        self._last_download_orphans = [
            {**f, "_track_count": self._orphan_tracker.get(f["_path"], 0)}
            for f in dl_orphans if f["_path"] not in deleted_paths
        ]
        self._last_library_orphans = [
            {**f, "_track_count": self._orphan_tracker.get(f["_path"], 0)}
            for f in lib_orphans if f["_path"] not in deleted_paths
        ]

        self._last_scan_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        dl = len(self._last_download_orphans)
        lib = len(self._last_library_orphans)
        logger.info(
            f"扫描完成: 下载目录 {dl} 个, 媒体库 {lib} 个, "
            f"自动删除 {self._last_deleted} 个, 跟踪 {len(self._orphan_tracker)} 个, "
            f"空壳目录 {len(self._last_empty_dirs)} 个(本轮删除 {self._last_deleted_dirs} 个)"
        )

    def _do_clean(self, target: str) -> int:
        """手动清理孤立文件。"""
        deleted = 0
        files = []
        if target in ("download", "all"):
            files.extend(self._last_download_orphans)
            self._last_download_orphans = []
        if target in ("library", "all"):
            files.extend(self._last_library_orphans)
            self._last_library_orphans = []
        for f in files:
            path = f["_path"]
            try:
                os.remove(path)
                deleted += 1
                # 从跟踪记录中移除
                self._orphan_tracker.pop(path, None)
                logger.info(f"手动删除: {path}")
            except OSError as e:
                logger.info(f"删除失败: {path} - {e}")
        self._save_tracker()
        logger.info(f"手动清理完成: {deleted} 个文件")
        return deleted

    def _scan_empty_dirs(self, is_new_day: bool) -> None:
        """扫描媒体库空壳目录并按配置清理（独立开关，默认关闭）。

        空壳目录 = 目录下只剩元数据/字幕文件、没有任何视频的季目录或剧目录。
        与硬链接判定无关，属媒体库维护：视频被删除或转移后目录里可能只剩 nfo/海报，
        媒体服务器仍会把它显示成剧集条目，因此需要清掉。
        安全边界：只扫媒体库目录；全部文件必须命中元数据白名单；必须能识别为影视条目目录；
        目录内最新文件需静置足够久；忽略/排除/收容目录一律跳过。
        """
        self._last_empty_dirs = []
        self._last_deleted_dirs = 0
        if not self._clean_empty_dirs or not self._library_dirs:
            self._empty_tracker = {}
            return

        candidates: Dict[str, Dict[str, Any]] = {}
        for base_dir in self._library_dirs:
            if not os.path.isdir(base_dir):
                continue
            base_abs = os.path.abspath(base_dir)
            for root, dirnames, _ in os.walk(base_abs):
                # 排除被忽略/额外排除/收容的目录及其子树
                dirnames[:] = [
                    d for d in dirnames
                    if not self._is_ignored(os.path.join(root, d) + os.sep)
                ]
                if os.path.abspath(root) == base_abs:
                    continue
                info = self._empty_dir_candidate(root)
                if info:
                    candidates[root] = info

        current = set(candidates.keys())
        if is_new_day:
            # 新的一天：本次仍存在的 +1，消失的清零
            for path in list(self._empty_tracker.keys()):
                if path in current:
                    self._empty_tracker[path] += 1
                else:
                    del self._empty_tracker[path]
            for path in current:
                self._empty_tracker.setdefault(path, 1)
        else:
            for path in current:
                self._empty_tracker.setdefault(path, 1)

        to_delete = [
            path for path, days in self._empty_tracker.items()
            if days >= self._empty_dir_days
        ]
        # 按路径深度倒序：先删季目录，再删已变空的剧目录
        for path in sorted(to_delete, key=lambda p: len(Path(p).parts), reverse=True):
            if not os.path.isdir(path):
                self._empty_tracker.pop(path, None)
                continue
            days = self._empty_tracker.get(path, 0)
            try:
                shutil.rmtree(path)
                self._last_deleted_dirs += 1
                logger.info(f"删除空壳目录(连续{days}天): {path}")
            except OSError as e:
                logger.info(f"删除空壳目录失败: {path} - {e}")
            self._empty_tracker.pop(path, None)

        self._last_empty_dirs = sorted(
            [
                {**info, "_track_count": self._empty_tracker.get(path, 0)}
                for path, info in candidates.items()
                if path in self._empty_tracker
            ],
            key=lambda x: x["dir_path"],
        )
        if candidates or self._last_deleted_dirs:
            logger.info(
                f"空壳目录检查: 识别 {len(candidates)} 个, 本轮删除 {self._last_deleted_dirs} 个, "
                f"跟踪 {len(self._empty_tracker)} 个"
            )

    def _empty_dir_candidate(self, root: str) -> Optional[Dict[str, Any]]:
        """判断目录是否为空壳目录，是则返回展示信息，否则返回 None。"""
        newest = 0.0
        file_count = 0
        has_marker = False
        for cur, dirnames, filenames in os.walk(root):
            dirnames[:] = [
                d for d in dirnames
                if not self._is_ignored(os.path.join(cur, d) + os.sep)
            ]
            for fname in filenames:
                fpath = os.path.join(cur, fname)
                if self._is_ignored(fpath):
                    continue
                lower = fname.lower()
                if lower.startswith("."):
                    # 系统隐藏文件（如 .DS_Store）不影响空壳判定
                    continue
                ext = os.path.splitext(lower)[1]
                if not ext:
                    if lower not in self._ARTIFACT_NAMES:
                        # 无扩展名且不是已知刮削图 → 来源不明，按实体文件处理
                        return None
                elif ext not in self._META_EXTS:
                    # 存在视频、音频或未知类型文件 → 不是空壳
                    return None
                file_count += 1
                if lower in self._MEDIA_MARKER_FILES or lower.startswith("season"):
                    has_marker = True
                try:
                    mtime = os.stat(fpath).st_mtime
                except OSError:
                    continue
                if mtime > newest:
                    newest = mtime
        if file_count <= 0:
            # 全空目录不处理：可能是整理过程中的中间态
            return None
        if not has_marker and not self._looks_like_media_dir(root):
            # 分类目录（国漫/国产剧/欧美剧等）没有条目标识文件，也不会命中季/年份命名，直接跳过
            return None
        if self._empty_dir_grace_hours > 0 and newest:
            if (time.time() - newest) < self._empty_dir_grace_hours * 3600:
                # 目录仍在写入/整理中，等静置足够久再处理
                return None
        return {
            "dir_path": root,
            "dir_name": os.path.basename(root.rstrip("/\\")) or root,
            "file_count": file_count,
            "mtime": datetime.fromtimestamp(newest).strftime("%Y-%m-%d %H:%M") if newest else "未知",
        }

    def _looks_like_media_dir(self, path: str) -> bool:
        """按目录名判断是否像影视条目目录（季目录或带年份的剧/电影目录）。"""
        name = os.path.basename(path.rstrip("/\\"))
        lowered = name.lower()
        for pattern in self._SEASON_DIR_PATTERNS:
            if re.match(pattern, lowered):
                return True
        # 剧名/片名通常带年份，如「恶魔法则 (2023)」「Some.Movie.2024」
        return bool(re.search(r"(19|20)\d{2}", name))

    def _do_clean_empty(self) -> int:
        """手动删除本次识别到的空壳目录，返回删除数量。"""
        deleted = 0
        for info in sorted(
            self._last_empty_dirs,
            key=lambda x: len(Path(x["dir_path"]).parts),
            reverse=True,
        ):
            path = info["dir_path"]
            if not os.path.isdir(path):
                self._empty_tracker.pop(path, None)
                continue
            try:
                shutil.rmtree(path)
                deleted += 1
                logger.info(f"手动删除空壳目录: {path}")
            except OSError as e:
                logger.info(f"删除空壳目录失败: {path} - {e}")
            self._empty_tracker.pop(path, None)
        self._last_empty_dirs = []
        self._last_deleted_dirs += deleted
        self._save_tracker()
        return deleted

    def _send_notify(self) -> None:
        """发送通知。"""
        dl = len(self._last_download_orphans)
        lib = len(self._last_library_orphans)
        dl_size = self._format_size(sum(f.get("_size", 0) for f in self._last_download_orphans))
        lib_size = self._format_size(sum(f.get("_size", 0) for f in self._last_library_orphans))
        parts = []
        if dl > 0:
            parts.append(f"下载目录孤立: {dl} 个 ({dl_size})")
        if lib > 0:
            parts.append(f"媒体库孤立: {lib} 个 ({lib_size})")
        if self._last_deleted > 0:
            parts.append(f"自动删除: {self._last_deleted} 个")
        if self._last_deleted_dirs > 0:
            parts.append(f"删除空壳目录: {self._last_deleted_dirs} 个")
        empty = len(self._last_empty_dirs)
        if empty > 0:
            parts.append(f"空壳目录待处理: {empty} 个（开关{'已开启' if self._clean_empty_dirs else '未开启'}）")
        if parts:
            self.post_message(title="硬链接检查", text="\n".join(parts))

    # ── API ───────────────────────────────────────────────────

    async def _api_scan(self, apikey: str = "") -> Dict[str, Any]:
        """API: 触发扫描。"""
        self._do_scan()
        return {
            "success": True,
            "scan_time": self._last_scan_time,
            "download_count": len(self._last_download_orphans),
            "library_count": len(self._last_library_orphans),
            "deleted": self._last_deleted,
            "tracking": len(self._orphan_tracker),
            "empty_dir_count": len(self._last_empty_dirs),
            "empty_dir_deleted": self._last_deleted_dirs,
        }

    async def _api_clean_empty(self, apikey: str = "") -> Dict[str, Any]:
        """API: 手动清理识别到的空壳目录。"""
        deleted = self._do_clean_empty()
        return {"success": True, "deleted": deleted}

    async def _api_clean(self, target: str = "all", apikey: str = "") -> Dict[str, Any]:
        """API: 手动清理。"""
        deleted = self._do_clean(target)
        return {"success": True, "deleted": deleted}

    async def _api_reset(self, apikey: str = "") -> Dict[str, Any]:
        """API: 重置跟踪记录。"""
        self._orphan_tracker = {}
        self._save_tracker()
        logger.info("跟踪记录已重置")
        return {"success": True, "message": "跟踪记录已重置"}

    # ── 事件处理 ──────────────────────────────────────────────

    @eventmanager.register(EventType.PluginAction)
    def _on_plugin_action(self, event: Event = None) -> None:
        """处理插件命令事件。"""
        if not event or not event.event_data:
            return
        if event.event_data.get("action") != "link_check_scan":
            return
        logger.info("收到手动扫描命令")
        self._do_scan()
        self._send_notify()

    # ── 内部方法 ──────────────────────────────────────────────

    def _collect_relocate_dirs(self) -> List[str]:
        """从其它插件的配置里读取收容目录（订阅助手Q / 官方订阅助手），用于自动排除。

        收容目录中的种子在媒体库侧没有对应 inode，天然符合「孤立」判定，但它是有意保种的内容。
        未安装或未配置时返回空列表，不影响原有行为；读取失败只记日志。
        """
        dirs: List[str] = []
        try:
            from app.db.oper.systemconfig import SystemConfigOper

            oper = SystemConfigOper()
            for plugin_id in ("SubscribeAssistantEnhancedQ", "SubscribeAssistantEnhanced"):
                try:
                    cfg = oper.get(f"plugin.{plugin_id}")
                except Exception:
                    continue
                if not isinstance(cfg, dict):
                    continue
                relocate_dir = str(cfg.get("relocate_dir") or "").strip()
                if relocate_dir and relocate_dir not in dirs:
                    dirs.append(relocate_dir)
        except Exception as err:
            logger.debug(f"读取收容目录配置失败（扫描时会按目录名兜底排除）：{err}")
        return dirs

    def _is_ignored(self, filepath: str) -> bool:
        """判断文件路径是否需要跳过。

        依次检查：忽略目录、额外排除目录、收容目录（来自订阅助手配置），
        最后按目录名兜底排除常见收容目录（hr / h&r / relocate / 收容 等）。
        """
        for ignore in list(self._ignore_dirs) + list(self._exclude_dirs) + list(self._relocate_dirs):
            if ignore and ignore in filepath:
                return True
        parts = {part.strip().lower() for part in Path(filepath).parts}
        return bool(parts & self._RELOCATE_DIR_NAMES)

    def _scan_files(self, dirs: List[str]) -> List[Dict[str, Any]]:
        """扫描目录下所有视频文件（含硬链接），返回文件信息与链接数。

        与旧版不同：这里不过滤 links，保留链接数 ≥2 的文件，用于判断「曾经被硬链接过」——
        只有曾经是硬链接、现在链接数为 1（说明另一侧已被删除）的文件才进入清理候选。
        """
        results: List[Dict[str, Any]] = []
        for base_dir in dirs:
            if not os.path.isdir(base_dir):
                continue
            for root, _, files in os.walk(base_dir):
                for fname in files:
                    ext = os.path.splitext(fname)[1].lower()
                    if ext not in self._VIDEO_EXTS:
                        continue
                    fpath = os.path.join(root, fname)
                    if self._is_ignored(fpath):
                        continue
                    try:
                        stat = os.stat(fpath)
                    except OSError:
                        continue
                    results.append({
                        "dir_path": root,
                        "file_name": fname,
                        "size_str": self._format_size(stat.st_size),
                        "mtime": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M"),
                        "nlink": str(stat.st_nlink),
                        "media_info": self._extract_media_info(fpath),
                        "_path": fpath,
                        "_size": stat.st_size,
                        "_nlink": int(stat.st_nlink),
                    })
        results.sort(key=lambda x: (x["dir_path"], x["file_name"]))
        return results

    def _scan_dirs(self, dirs: List[str]) -> List[Dict[str, Any]]:
        """扫描目录，返回链接数为 1（即两侧只剩一份）的视频文件。"""
        return [f for f in self._scan_files(dirs) if f.get("_nlink", 1) == 1]

    def _extract_media_info(self, filepath: str) -> str:
        """从文件路径提取详细媒体信息。"""
        path_parts = Path(filepath).parts
        info_parts: List[str] = []

        category = ""
        for part in path_parts:
            if part in ("电影", "外语电影", "国产电影", "动画电影"):
                category = "电影"
                break
            if part in ("电视剧", "欧美剧", "国产剧", "日韩剧", "动漫"):
                category = "剧集"
                break

        fname = Path(filepath).name
        for part in reversed(path_parts):
            if part == fname:
                continue
            m = re.match(r"^(.+?)\s*\(\d{4}\)", part)
            if m:
                title = m.group(1).strip()
                info_parts.append(f"{category} / {title}" if category else title)
                break
            m = re.match(r"^(.+?)\.(19|20)\d{2}", part)
            if m:
                title = m.group(1).replace(".", " ").strip()
                info_parts.append(f"{category} / {title}" if category else title)
                break

        m = re.search(r"[Ss](\d{1,2})[Ee](\d{1,3})", filepath)
        if m:
            info_parts.append(f"S{m.group(1).zfill(2)}E{m.group(2).zfill(2)}")

        return " | ".join(info_parts) if info_parts else "未知"

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """格式化文件大小。"""
        if size_bytes >= 1024 ** 4:
            return f"{size_bytes / (1024 ** 4):.1f} TB"
        if size_bytes >= 1024 ** 3:
            return f"{size_bytes / (1024 ** 3):.1f} GB"
        if size_bytes >= 1024 ** 2:
            return f"{size_bytes / (1024 ** 2):.1f} MB"
        if size_bytes >= 1024:
            return f"{size_bytes / 1024:.1f} KB"
        return f"{size_bytes} B"
