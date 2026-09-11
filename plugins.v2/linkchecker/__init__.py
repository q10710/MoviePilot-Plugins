"""硬链接孤立文件检查插件。

扫描下载目录和媒体库目录，找出 links=1 的孤立视频文件。
每次扫描重新检查，不依赖历史记录。
同一文件连续 3 次扫描都出现在孤立列表中 → 自动删除。
"""

import os
import re
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType


class LinkChecker(_PluginBase):
    """硬链接孤立文件检查插件。"""

    plugin_name = "硬链接检查"
    plugin_desc = "扫描下载目录和媒体库目录中的孤立硬链接文件，连续3天孤立自动删除。"
    plugin_icon = "https://raw.githubusercontent.com/q10710/MoviePilot-Plugins/main/icons/linkchecker.png"
    plugin_version = "3.2.0"
    plugin_label = "文件管理"
    plugin_author = "local"
    plugin_config_prefix = "linkchecker_"
    plugin_order = 50
    auth_level = 1

    _VIDEO_EXTS = {".mkv", ".mp4", ".ts", ".avi", ".m2ts", ".mov", ".wmv", ".flv", ".webm", ".m4v"}

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
    _scheduler = None
    _last_scan_time: Optional[str] = None
    _last_download_orphans: List[Dict[str, Any]] = []
    _last_library_orphans: List[Dict[str, Any]] = []
    # 持久化：文件路径 → 连续出现天数
    _orphan_tracker: Dict[str, int] = {}
    # 上次扫描日期，同一天多次扫描不重复计数
    _last_scan_date: str = ""
    # 本次扫描删除的文件数
    _last_deleted: int = 0

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
        # 加载持久化的跟踪记录
        saved = self.get_data("tracker") or {}
        self._orphan_tracker = saved.get("orphans", {})
        self._linked_seen = set(saved.get("linked") or [])
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
        logger.info(
            f"初始化完成, enabled={self._enabled}, cron={self._cron}, "
            f"auto_delete={self._auto_delete}, threshold={self._delete_threshold}, "
            f"允许删媒体库残留={self._allow_library_delete}, 已记录硬链接指纹={len(self._linked_seen)}, "
            f"额外排除={len(self._exclude_dirs)} 项, 收容目录={self._relocate_dirs or '（按目录名兜底）'}"
        )
        if self._enabled and self._cron:
            self._schedule_service()

    def get_state(self) -> bool:
        """获取插件启用状态。"""
        return self._enabled

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
                        ],
                    },
                ],
            }
        ]

        if self._last_download_orphans:
            page.append(self._build_table_card("下载目录孤立文件", self._last_download_orphans))

        if self._last_library_orphans:
            page.append(self._build_table_card("媒体库孤立文件", self._last_library_orphans))

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

    def stop_service(self) -> None:
        """停止插件后台服务并释放资源。"""
        if self._scheduler:
            self._scheduler.shutdown(wait=False)
            self._scheduler = None

    # ── 定时服务 ──────────────────────────────────────────────

    def _schedule_service(self) -> None:
        """注册定时扫描服务。"""
        if not self._scheduler:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.start()
        self._scheduler.remove_all_jobs()
        self._scheduler.add_job(
            func=self._scheduled_scan,
            trigger=CronTrigger.from_crontab(self._cron),
            name="LinkChecker_scan",
            id="LinkChecker_scan",
        )
        logger.info(f"定时扫描已注册: {self._cron}")

    def _scheduled_scan(self) -> None:
        """定时扫描入口。"""
        logger.info("定时扫描开始")
        self._do_scan()
        total = len(self._last_download_orphans) + len(self._last_library_orphans)
        if self._notify and (total > 0 or self._last_deleted > 0):
            self._send_notify()

    # ── 核心逻辑 ──────────────────────────────────────────────

    def _save_tracker(self) -> None:
        """持久化跟踪记录。"""
        self.save_data("tracker", {
            "orphans": self._orphan_tracker,
            "linked": sorted(self._linked_seen),
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
            f"自动删除 {self._last_deleted} 个, 跟踪 {len(self._orphan_tracker)} 个"
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
        }

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
