"""做种守卫插件。

检测两类做种异常并持续追踪：
1. 无效做种：下载器中存在已完成/应做种的任务，但本地文件已丢失或任务报错，实际无法做种；
2. 孤儿源文件：本地下载目录中存在文件，但所有启用下载器都没有对应的做种任务。

两类异常均按"连续 N 天"宽限期追踪，达标后按配置的动作处理（仅通知 / 移动 / 删除），
支持 qBittorrent、Transmission、rTorrent 等所有 MoviePilot 已配置的下载器实例。
安全设计：下载器连接失败时本轮直接跳过对应下载器，绝不把任何文件误判为孤儿。
"""

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType, NotificationType


class SeedSourceGuard(_PluginBase):
    """做种守卫插件。"""

    plugin_name = "做种守卫"
    plugin_desc = ("检测本地源文件是否有下载器在做种、下载器是否存在文件丢失的无效做种任务；"
                   "连续N天异常可通知或按策略处置，杜绝无效做种与孤儿文件。")
    plugin_icon = "seedguard.png"
    plugin_version = "1.0.0"
    plugin_label = "下载管理"
    plugin_author = "local"
    plugin_config_prefix = "seedsourceguard_"
    plugin_order = 66
    auth_level = 1

    # 状态字段默认值
    _enabled: bool = False
    _notify: bool = True
    _cron: str = "15 3 * * *"
    _days: int = 3
    _scan_dirs: List[str] = []
    _exclude_keys: List[str] = []
    _downloaders: List[str] = []
    _max_depth: int = 3
    _no_seed_action: str = "notify"
    _invalid_action: str = "notify"
    _move_path: str = ""
    _allow_delete: bool = False

    # 下载中/排队下载等不应视为异常的状态（文件未就绪）
    _IGNORE_STATES = {
        "downloading", "stalleddl", "metadl", "forceddl",
        "checkingdl", "queueddl", "stoppeddl", "pauseddl",
        "moving", "allocating", "checkingresumedata", "queued",
    }
    # 应立即判为无效做种的任务状态（报错或文件缺失）
    _INVALID_STATES = {"error", "missingfiles", "missing", "uploaderror"}
    # 应处于做种/保种的任务状态
    _HOLD_STATES = {
        "uploading", "forcedup", "queuedup", "stoppedup", "stalledup",
        "pausedup", "checkingup", "seeding", "seed_pending",
        "completed", "finished", "paused", "stopped", "checking",
    }

    _downloader_helper = None

    def init_plugin(self, config: dict = None) -> None:
        """根据插件配置初始化运行状态。"""
        self.stop_service()
        self._enabled = False
        if not config:
            return
        self._enabled = bool(config.get("enabled"))
        self._notify = bool(config.get("notify", True))
        self._cron = str(config.get("cron") or "15 3 * * *")
        self._days = int(config.get("days") or 3)
        if self._days < 1:
            self._days = 1
        self._max_depth = int(config.get("max_depth") or 3)
        if self._max_depth < 1:
            self._max_depth = 1
        self._scan_dirs = self._split_lines(config.get("scan_dirs"))
        self._exclude_keys = self._split_lines(config.get("exclude_dirs"))
        self._downloaders = self._split_comma(config.get("downloaders"))
        self._no_seed_action = str(config.get("no_seed_action") or "notify")
        self._invalid_action = str(config.get("invalid_action") or "notify")
        self._move_path = str(config.get("move_path") or "").strip()
        self._allow_delete = bool(config.get("allow_delete"))

    # ── 基础接口 ──────────────────────────────────────────────

    def get_state(self) -> bool:
        """获取插件启用状态。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """返回插件远程命令列表。"""
        return [
            {
                "cmd": "/seed_guard_check",
                "event": EventType.PluginAction,
                "desc": "做种守卫：立即检测（不处置）",
                "category": "下载管理",
                "data": {"action": "check"},
            },
            {
                "cmd": "/seed_guard_run",
                "event": EventType.PluginAction,
                "desc": "做种守卫：立即检测并处置",
                "category": "下载管理",
                "data": {"action": "run"},
            },
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        """返回插件 API 列表。"""
        return []

    def get_form(self) -> Tuple[Optional[List[dict]], Dict[str, Any]]:
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
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify",
                                            "label": "异常通知",
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
                                            "model": "days",
                                            "label": "连续异常天数阈值",
                                            "type": "number",
                                            "hint": "连续 N 天仍异常才触发处置",
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
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VCronField",
                                        "props": {
                                            "model": "cron",
                                            "label": "检测周期",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "downloaders",
                                            "label": "参与检测的下载器",
                                            "hint": "留空检测全部；多个用逗号分隔（显示名称）",
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
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "scan_dirs",
                                            "label": "扫描的下载/整理目录",
                                            "rows": 3,
                                            "hint": "每行一个 MoviePilot 下载保存目录（即会整理入库的目录），自动向下探测资源层；仅检测这些目录下的文件是否无做种任务",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "exclude_dirs",
                                            "label": "排除目录关键词",
                                            "rows": 3,
                                            "hint": "每行一个关键词，目录名或路径包含即跳过，如 音乐、儿童",
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
                                            "model": "no_seed_action",
                                            "label": "孤儿源文件处置",
                                            "items": [
                                                {"title": "仅通知", "value": "notify"},
                                                {"title": "移动到回收目录", "value": "move"},
                                                {"title": "删除文件", "value": "delete"},
                                            ],
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
                                            "model": "invalid_action",
                                            "label": "无效做种处置",
                                            "items": [
                                                {"title": "仅通知", "value": "notify"},
                                                {"title": "删除任务(保留文件)", "value": "delete_torrent"},
                                                {"title": "删除任务及文件", "value": "delete_all"},
                                            ],
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
                                            "model": "move_path",
                                            "label": "回收目录",
                                            "hint": "移动处置的目标目录",
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
                                        "component": "VTextField",
                                        "props": {
                                            "model": "max_depth",
                                            "label": "目录探测深度",
                                            "type": "number",
                                            "hint": "默认 3，一般无需修改",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "allow_delete",
                                            "label": "允许执行删除/移动处置",
                                            "hint": "关闭时即使配置了删除也仅提示，不做任何破坏性操作",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "warning",
                            "variant": "tonal",
                            "text": ("安全规则：下载器连接失败或取不到任务列表时，本轮自动跳过该下载器，"
                                     "绝不会把任何文件判为孤儿。删除/移动类处置要求：连续 N 天异常 + 本页开关已打开。"),
                        },
                    },
                ],
            }
        ], {
            "enabled": False,
            "notify": True,
            "cron": "15 3 * * *",
            "days": 3,
            "scan_dirs": "/nastools/data/downloads/dianying/\n/nastools/data/downloads/tv/",
            "exclude_dirs": "音乐\nmusic\n儿童\n临时下载\ndouyin\n杰伦十代\nflac\nape\nwav",
            "downloaders": "",
            "max_depth": 3,
            "no_seed_action": "notify",
            "invalid_action": "notify",
            "move_path": "",
            "allow_delete": False,
        }

    def get_page(self) -> Optional[List[dict]]:
        """返回插件数据页：运行概览与异常明细（中文卡片布局）。"""
        if not self._enabled:
            return None
        state = self.get_data("state") or {}
        no_seed = state.get("no_seed_active") or []
        invalid = state.get("invalid_active") or []
        unavailable = state.get("unavailable") or []
        last_run = state.get("last_run") or "尚未运行"
        handled = state.get("handled") or []
        total = len(no_seed) + len(invalid) + len(unavailable)
        # ── 顶部状态条 ──
        if total == 0:
            head_type = "success"
            head_text = f"一切正常：未发现孤儿文件与无效做种（最近检测：{last_run}）"
        else:
            head_type = "warning"
            head_text = (f"共发现 {total} 项异常（最近检测：{last_run}）"
                         "，连续 3 天仍存在将按配置处置，删除类处置需先开启允许开关")
        children = [
            {
                "component": "VAlert",
                "props": {
                    "type": head_type,
                    "variant": "tonal",
                    "text": head_text,
                },
            }
        ]
        # ── 统计卡片 ──
        stats = [
            ("孤儿源文件", len(no_seed), "orange-darken-2"),
            ("无效做种", len(invalid), "red-darken-2"),
            ("下载器异常", len(unavailable), "grey-darken-1"),
        ]
        row_content = []
        for label, num, color in stats:
            row_content.append({
                "component": "VCol",
                "props": {"cols": 12, "md": 4},
                "content": [
                    {
                        "component": "VCard",
                        "props": {"variant": "tonal"},
                        "content": [
                            {
                                "component": "VCardText",
                                "props": {"class": "text-center py-3"},
                                "content": [
                                    {
                                        "component": "div",
                                        "props": {
                                            "class": f"text-h3 font-weight-black {color}",
                                        },
                                        "text": str(num),
                                    },
                                    {
                                        "component": "div",
                                        "props": {
                                            "class": "text-body-2 text-medium-emphasis",
                                        },
                                        "text": label,
                                    },
                                ],
                            }
                        ],
                    }
                ],
            })
        children.append({"component": "VRow", "content": row_content})
        # ── 明细分组卡片 ──
        sections = [
            {
                "key": "no_seed",
                "title": "孤儿源文件（本地存在但一直无下载器做种）",
                "color": "warning",
                "empty": "未发现孤儿源文件",
                "items": no_seed,
            },
            {
                "key": "invalid",
                "title": "无效做种（任务仍在但文件丢失或报错）",
                "color": "error",
                "empty": "未发现无效做种任务",
                "items": invalid,
            },
            {
                "key": "unavailable",
                "title": "下载器异常（本轮已跳过，未做任何处置）",
                "color": "secondary",
                "empty": "全部下载器在线",
                "items": [
                    {"name": item.get("name", ""), "err": item.get("err", "")}
                    for item in unavailable
                ],
            },
        ]
        for sec in sections:
            items = sec["items"]
            content = []
            if not items:
                content.append({
                    "component": "VAlert",
                    "props": {
                        "type": "success",
                        "variant": "tonal",
                        "text": sec["empty"],
                    },
                })
            else:
                content.append({
                    "component": "VRow",
                    "content": [
                        {
                            "component": "VCol",
                            "props": {"cols": 12},
                            "content": [
                                {
                                    "component": "VCard",
                                    "props": {
                                        "variant": "outlined",
                                        "color": sec["color"],
                                    },
                                    "content": [
                                        {
                                            "component": "VCardText",
                                            "props": {"class": "py-2"},
                                            "content": [
                                                {
                                                    "component": "div",
                                                    "props": {
                                                        "class": "text-body-2 d-flex align-center",
                                                    },
                                                    "content": [
                                                        {
                                                            "component": "div",
                                                            "props": {
                                                                "class": f"text-{sec['color']} mr-2",
                                                            },
                                                            "text": "●",
                                                        },
                                                        {
                                                            "component": "div",
                                                            "text": (
                                                                f"{sec['title']}，共 {len(items)} 项"
                                                            ),
                                                        },
                                                    ],
                                                },
                                            ],
                                        },
                                    ],
                                }
                            ],
                        }
                    ],
                })
                for item in items:
                    if sec["key"] == "no_seed":
                        line = f"{item.get('path', '')}"
                        extra = f"已持续 {item.get('days', 1)} 天"
                    elif sec["key"] == "invalid":
                        line = f"[{item.get('dl', '')}] {item.get('name', '')}"
                        extra = f"已持续 {item.get('days', 1)} 天"
                    else:
                        line = f"{item.get('name', '')}"
                        extra = str(item.get("err", ""))[:120]
                    content.append({
                        "component": "VRow",
                        "props": {"align": "center", "class": "px-2"},
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 9},
                                "content": [
                                    {
                                        "component": "div",
                                        "props": {"class": "text-body-2"},
                                        "text": line,
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "div",
                                        "props": {
                                            "class": "text-caption text-medium-emphasis text-right",
                                        },
                                        "text": extra,
                                    }
                                ],
                            },
                        ],
                    })
            children.append({
                "component": "VCard",
                "props": {"variant": "flat", "class": "mt-3"},
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {
                            "class": "text-subtitle-1 font-weight-bold",
                        },
                        "text": sec["title"],
                    },
                    {
                        "component": "VCardText",
                        "content": content,
                    },
                ],
            })
        # ── 最近处置记录 ──
        handled_rows = [
            f"{item.get('time', '')}　{item.get('text', '')}" for item in handled[-10:]
        ]
        record_card = [
            {
                "component": "VAlert",
                "props": {
                    "type": "info",
                    "variant": "tonal",
                    "text": "暂无处置记录（当前处于仅通知模式）",
                },
            }
        ] if not handled_rows else [
            {
                "component": "div",
                "props": {"class": "text-body-2"},
                "content": [
                    {"component": "div", "props": {"class": "py-1"}, "text": row}
                    for row in handled_rows
                ],
            }
        ]
        children.append({
            "component": "VCard",
            "props": {"variant": "flat", "class": "mt-3"},
            "content": [
                {
                    "component": "VCardTitle",
                    "props": {"class": "text-subtitle-1 font-weight-bold"},
                    "text": "最近处置记录",
                },
                {"component": "VCardText", "content": record_card},
            ],
        })
        return children


    def get_service(self) -> List[Dict[str, Any]]:
        """注册定时检测服务。"""
        if self._enabled and self._cron:
            return [
                {
                    "id": "SeedSourceGuard",
                    "name": "做种守卫检测",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self._run_check,
                    "kwargs": {"handle": True},
                }
            ]
        return []

    def stop_service(self) -> None:
        """停止插件后台服务并释放资源（定时任务由 MoviePilot 框架统一管理）。"""
        return None

    # ── 调度与命令 ────────────────────────────────────────────

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
            title="做种守卫",
            text="开始执行检测...",
            userid=event.event_data.get("user"),
        )
        self._run_check(handle=(action == "run"))

    # ── 核心检测 ──────────────────────────────────────────────

    def _run_check(self, handle: bool = False) -> None:
        """执行一轮完整检测，handle=True 时按配置处置达标项。"""
        if not self._enabled:
            logger.warning("做种守卫未启用，跳过本轮检测")
            return
        # 状态容器
        report = {
            "no_seed": [],     # [{path}]
            "invalid": [],     # [{dl, name, hash}]
            "unavailable": [],  # [{name, err}]
        }
        task_index = {}   # 下载器名 -> 任务列表 [{hash,name,root,state,complete}]
        try:
            self._downloader_helper = self._get_helper()
        except Exception as err:
            logger.error(f"初始化下载器助手失败：{err}")
            self._notify_unavailable("下载器助手初始化失败", str(err), handle=handle)
            return

        services = self._downloader_helper.get_services(name_filters=self._downloaders or None)
        if not services:
            logger.warning("未获取到任何已配置的下载器，本轮跳过")
            return

        # 1. 逐下载器获取任务
        for name, service in services.items():
            instance = getattr(service, "instance", None)
            if instance is None:
                report["unavailable"].append({"name": name, "err": "无下载器实例"})
                continue
            try:
                if hasattr(instance, "is_inactive") and instance.is_inactive():
                    report["unavailable"].append({"name": name, "err": "下载器未连接"})
                    continue
                torrents, error = instance.get_torrents()
            except Exception as err:
                logger.error(f"下载器 {name} 获取任务失败：{err}")
                report["unavailable"].append({"name": name, "err": str(err)[:200]})
                continue
            if error:
                logger.error(f"下载器 {name} 获取任务返回错误：{error}")
                report["unavailable"].append({"name": name, "err": str(error)[:200]})
                continue
            normalized = self._normalize_torrents(name, torrents or [])
            task_index[name] = normalized
            logger.info(f"下载器 {name}：任务 {len(normalized)} 个")

        # 2. 若所选下载器全部不可用：直接中止，绝不做任何处置
        if not task_index:
            logger.error("所有下载器均不可用，本轮检测中止，不进行任何处置")
            self._save_report(report, last_run="全部下载器不可用，已中止")
            self._notify_summary(report, handled=handle)
            return

        # 3. 无效做种检测
        invalid_candidates = self._detect_invalid(task_index)
        report["invalid"] = invalid_candidates

        # 4. 孤儿源文件检测
        no_seed_candidates = self._detect_no_seed(task_index)
        report["no_seed"] = no_seed_candidates

        # 5. 按天计数并处置
        handled_list = self._settle(report, handle=handle)

        # 6. 落盘与通知
        self._save_report(report, handled_list=handled_list)
        self._notify_summary(report, handled=handle, handled_list=handled_list)

    # ── 数据获取工具 ──────────────────────────────────────────

    @staticmethod
    def _get_helper():
        """构造 MoviePilot 下载器助手实例。"""
        from app.helper.downloader import DownloaderHelper
        return DownloaderHelper()

    @staticmethod
    def _split_lines(value: Optional[str]) -> List[str]:
        """按行拆分配置文本，忽略空行。"""
        if not value:
            return []
        return [line.strip() for line in str(value).splitlines() if line.strip()]

    @staticmethod
    def _split_comma(value: Optional[str]) -> List[str]:
        """按逗号拆分配置文本，忽略空项。"""
        if not value:
            return []
        return [item.strip() for item in str(value).split(",") if item.strip()]

    @staticmethod
    def _tget(torrent: Any, *keys: str, default: Any = None) -> Any:
        """兼容 dict 与对象的属性读取，按别名链逐个尝试。"""
        for key in keys:
            try:
                if isinstance(torrent, dict):
                    if key in torrent and torrent.get(key) not in (None, ""):
                        return torrent.get(key)
                else:
                    value = getattr(torrent, key, None)
                    if value is not None and value != "":
                        return value
            except Exception:
                continue
        return default

    def _normalize_torrents(self, dl_name: str, torrents: list) -> List[Dict[str, Any]]:
        """将下载器原始任务对象规范化为内部结构。"""
        result = []
        for torrent in torrents or []:
            try:
                state = str(self._tget(
                    torrent, "state", "status", "torrent_status", default=""
                )).lower()
                name = str(self._tget(torrent, "name", "torrent_name", default="") or "")
                hash_value = str(self._tget(
                    torrent, "hash", "hashString", "id", default=""
                ) or "")
                progress = self._tget(torrent, "progress", "percent_done", default=0) or 0
                root = self._task_root(torrent, dl_name)
                if not root or not name:
                    continue
                result.append({
                    "dl": dl_name,
                    "hash": hash_value,
                    "name": name,
                    "state": state,
                    "progress": float(progress or 0),
                    "root": root,
                })
            except Exception as err:
                logger.debug(f"解析下载器 {dl_name} 任务失败：{err}")
                continue
        return result

    def _task_root(self, torrent: Any, dl_name: str) -> str:
        """提取任务内容根路径，兼容 qB/TR/rTorrent 等客户端。"""
        try:
            # qBittorrent：content_path 已含文件或目录
            content_path = self._tget(
                torrent, "content_path", "contentPath", default=""
            )
            if content_path:
                return str(content_path).rstrip("/")
            # 保存目录 + 名称
            save_path = self._tget(
                torrent, "save_path", "savePath", "downloadDir",
                "download_dir", "directory", default=""
            )
            name = self._tget(torrent, "name", "torrent_name", default="")
            if save_path:
                return str(save_path).rstrip("/")
            if name:
                logger.debug(f"下载器 {dl_name} 任务 {name} 无保存路径信息")
        except Exception as err:
            logger.debug(f"提取任务路径失败：{err}")
        return ""

    # ── 异常判定 ──────────────────────────────────────────────

    def _detect_invalid(self, task_index: Dict[str, list]) -> List[Dict[str, Any]]:
        """检测无效做种任务：应做种但文件丢失或任务报错。"""
        invalid = []
        for dl_name, tasks in task_index.items():
            for task in tasks:
                state = task["state"]
                if state in self._IGNORE_STATES:
                    continue
                root = task["root"]
                # 根路径为空或直接报错状态：判无效
                broken_state = state in self._INVALID_STATES or not root
                # 路径不存在或目录为空
                file_broken = False
                if root:
                    file_broken = not self._path_ok(root)
                if broken_state or file_broken:
                    invalid.append({
                        "dl": dl_name,
                        "hash": task["hash"],
                        "name": task["name"],
                        "root": root,
                        "state": state,
                    })
        return invalid

    @staticmethod
    def _path_ok(root: str) -> bool:
        """判断任务根路径是否真实存在且非空。"""
        try:
            path = Path(root)
            if not path.exists():
                return False
            if path.is_dir():
                # 目录存在但没有任何内容也视为文件丢失
                try:
                    return any(path.iterdir())
                except OSError:
                    return False
            return path.is_file()
        except Exception:
            return False

    def _detect_no_seed(self, task_index: Dict[str, list]) -> List[Dict[str, Any]]:
        """检测孤儿源文件：扫描目录中存在但没有任何做种任务覆盖的目录。"""
        if not self._scan_dirs:
            logger.warning("未配置源文件扫描根目录，跳过孤儿文件检测")
            return []
        # 所有下载器任务根路径集合
        task_roots = []
        for tasks in task_index.values():
            for task in tasks:
                root = task["root"]
                if root:
                    task_roots.append(root)
        # 建前缀树快速判断：任何任务根路径以 dir 为祖先即覆盖
        no_seed = []
        for scan_root in self._scan_dirs:
            base = Path(scan_root)
            if not base.is_dir():
                logger.warning(f"扫描根目录不存在：{scan_root}")
                continue
            self._walk_scan(base, 1, task_roots, no_seed)
        return no_seed

    def _walk_scan(self, directory: Path, depth: int, task_roots: List[str],
                   no_seed: List[Dict[str, Any]]) -> None:
        """递归探测目录树，找出未被任务覆盖的资源目录。"""
        if depth > self._max_depth:
            return
        try:
            entries = sorted(directory.iterdir())
        except OSError:
            return
        for entry in entries:
            if not entry.is_dir():
                continue
            # 隐藏目录与符号链接跳过
            if entry.name.startswith(".") or entry.is_symlink():
                continue
            if self._is_excluded(entry):
                continue
            path = str(entry)
            if self._covered(path, task_roots):
                # 该目录已被任务覆盖，其下视为有做种，不再下钻
                continue
            if depth < self._max_depth:
                # 未覆盖且还有深度：先看下层是否有任务覆盖（分类层级场景）
                children = [c for c in entry.iterdir() if c.is_dir()]
                if children:
                    self._walk_scan(entry, depth + 1, task_roots, no_seed)
                    continue
            # 叶子目录：仅当含实际文件时才视为孤儿源文件，纯空目录忽略
            if self._has_files(entry, depth):
                no_seed.append({"path": path})

    def _has_files(self, directory: Path, depth: int) -> bool:
        """判断目录内（至多到最大深度）是否包含实际文件。"""
        try:
            for item in directory.iterdir():
                if item.is_file():
                    return True
                if (item.is_dir() and not item.name.startswith(".")
                        and not item.is_symlink()
                        and depth < self._max_depth):
                    if self._has_files(item, depth + 1):
                        return True
        except OSError:
            return False
        return False

    def _is_excluded(self, entry: Path) -> bool:
        """判断目录是否命中排除关键词（大小写不敏感）。"""
        if not self._exclude_keys:
            return False
        full = entry.name.lower()
        for key in self._exclude_keys:
            if key and key.strip().lower() in full:
                return True
        return False

    @staticmethod
    def _covered(path: str, task_roots: List[str]) -> bool:
        """判断目录是否被任一任务根路径覆盖（本身或其下）。"""
        norm = path.rstrip("/")
        for root in task_roots:
            root_norm = root.rstrip("/")
            if root_norm == norm or root_norm.startswith(norm + "/"):
                return True
        return False

    # ── 计数与处置 ────────────────────────────────────────────

    def _settle(self, report: Dict[str, Any], handle: bool) -> List[Dict[str, Any]]:
        """按连续天数阈值推进记录，并执行达标项的处置动作。"""
        days_data = self.get_data("days") or {}
        handled_list = []
        today = datetime.now().strftime("%Y-%m-%d")

        # 孤儿源文件计数
        no_seed_map = {}
        for item in report["no_seed"]:
            no_seed_map[item["path"]] = item
        stale_no_seed = self._bump_days(days_data, "no_seed", no_seed_map, today)
        for key, days in stale_no_seed.items():
            path = key.split(":", 1)[1]
            handled = self._handle_no_seed(path, days, handle)
            if handled:
                handled_list.append(handled)
                days_data.pop(key, None)

        # 无效做种计数
        invalid_map = {}
        for item in report["invalid"]:
            key = f"{item['dl']}:{item['hash'] or item['root']}"
            invalid_map[key] = item
        stale_invalid = self._bump_days(days_data, "invalid", invalid_map, today)
        for key, days in stale_invalid.items():
            item = invalid_map.get(key) or {}
            handled = self._handle_invalid(item, days, handle)
            if handled:
                handled_list.append(handled)
                days_data.pop(key, None)

        # 清理已恢复的旧记录（保留当前仍异常项的天数历史）
        for prefix in ("no_seed:", "invalid:"):
            for old_key in [k for k in days_data if k.startswith(prefix)]:
                if prefix == "no_seed:" and old_key.split(":", 1)[1] not in no_seed_map:
                    days_data.pop(old_key, None)
                elif prefix == "invalid:" and old_key[len(prefix):] not in invalid_map:
                    days_data.pop(old_key, None)

        self.save_data("days", days_data)
        # 供数据页展示的活跃异常（含持续天数）
        active_no_seed = [
            {"path": path, "days": self._calc_days(days_data, f"no_seed:{path}", today)}
            for path in no_seed_map
        ]
        active_invalid = []
        for key, item in invalid_map.items():
            active_invalid.append({
                "dl": item.get("dl", ""),
                "name": item.get("name", ""),
                "days": self._calc_days(days_data, f"invalid:{key}", today),
            })
        self.save_data("active", {
            "no_seed_active": active_no_seed,
            "invalid_active": active_invalid,
        })
        return handled_list

    def _bump_days(self, days_data: dict, prefix: str,
                   current: Dict[str, Any], today: str) -> Dict[str, int]:
        """推进计数并返回达到阈值的关键项。"""
        stale = {}
        for key in current:
            full_key = f"{prefix}:{key}"
            first = days_data.get(full_key) or today
            days = self._calc_days(days_data, full_key, today)
            if days >= self._days:
                stale[key] = days
            else:
                days_data[full_key] = first
        return stale

    @staticmethod
    def _calc_days(days_data: dict, full_key: str, today: str) -> int:
        """计算某项已持续的天数。"""
        first = days_data.get(full_key)
        if not first:
            return 1
        try:
            start = datetime.strptime(str(first), "%Y-%m-%d")
            end = datetime.strptime(today, "%Y-%m-%d")
            return max(1, (end - start).days + 1)
        except Exception:
            return 1

    # ── 处置动作 ──────────────────────────────────────────────

    def _handle_no_seed(self, path: str, days: int, handle: bool) -> Optional[Dict[str, Any]]:
        """处置达到天数的孤儿源文件。"""
        action = self._no_seed_action
        text = f"孤儿源文件 {path}（连续 {days} 天无做种任务）"
        if action == "notify" or not handle:
            logger.warning(f"检测到孤儿源文件：{path}，连续 {days} 天")
            return {"time": datetime.now().strftime("%m-%d %H:%M"), "text": f"检测到 {text}"}
        if not self._allow_delete:
            logger.warning(f"孤儿源文件 {path} 已达标但未开启删除开关，跳过处置")
            return {"time": datetime.now().strftime("%m-%d %H:%M"), "text": f"达标未处置 {text}"}
        try:
            if action == "delete":
                import shutil
                shutil.rmtree(path, ignore_errors=True)
                logger.info(f"已删除孤儿源文件：{path}")
                return {"time": datetime.now().strftime("%m-%d %H:%M"),
                        "text": f"已删除 {text}"}
            if action == "move":
                if not self._move_path:
                    logger.warning("未配置回收目录，跳过移动")
                    return None
                import shutil
                target = Path(self._move_path)
                target.mkdir(parents=True, exist_ok=True)
                dest = target / Path(path).name
                shutil.move(path, str(dest))
                logger.info(f"已移动孤儿源文件：{path} -> {dest}")
                return {"time": datetime.now().strftime("%m-%d %H:%M"),
                        "text": f"已移动 {text} -> {dest}"}
        except Exception as err:
            logger.error(f"处置孤儿源文件失败 {path}：{err}")
            return {"time": datetime.now().strftime("%m-%d %H:%M"),
                    "text": f"处置失败 {text}：{err}"}
        return None

    def _handle_invalid(self, item: Dict[str, Any], days: int,
                        handle: bool) -> Optional[Dict[str, Any]]:
        """处置达到天数的无效做种任务。"""
        action = self._invalid_action
        name = item.get("name", "")
        dl = item.get("dl", "")
        hash_value = item.get("hash", "")
        text = f"无效做种 {dl}：{name}（连续 {days} 天文件丢失/报错）"
        if action == "notify" or not handle:
            logger.warning(f"检测到无效做种：{dl} / {name}，连续 {days} 天")
            return {"time": datetime.now().strftime("%m-%d %H:%M"), "text": f"检测到 {text}"}
        if not self._allow_delete:
            logger.warning(f"无效做种 {dl} / {name} 已达标但未开启删除开关，跳过处置")
            return {"time": datetime.now().strftime("%m-%d %H:%M"), "text": f"达标未处置 {text}"}
        if action in ("delete_torrent", "delete_all") and hash_value:
            delete_file = (action == "delete_all")
            try:
                services = self._downloader_helper.get_services(name_filters=[dl])
                instance = getattr(services.get(dl), "instance", None) if services else None
                if instance is None:
                    logger.warning(f"下载器 {dl} 不可用，无法删除任务")
                    return None
                instance.delete_torrents(delete_file, [hash_value])
                logger.info(f"已删除无效做种任务：{dl} / {name}（delete_file={delete_file}）")
                return {"time": datetime.now().strftime("%m-%d %H:%M"),
                        "text": f"已删除任务 {'及文件 ' if delete_file else ''}{text}"}
            except Exception as err:
                logger.error(f"删除无效做种任务失败 {dl} / {name}：{err}")
                return {"time": datetime.now().strftime("%m-%d %H:%M"),
                        "text": f"删除失败 {text}：{err}"}
        return None

    # ── 落盘与通知 ────────────────────────────────────────────

    def _save_report(self, report: Dict[str, Any], last_run: str = None,
                     handled_list: List[Dict[str, Any]] = None) -> None:
        """保存本轮检测结果到数据页状态。"""
        state = self.get_data("state") or {}
        active = self.get_data("active") or {}
        prev_handled = state.get("handled") or []
        new_handled = (handled_list or []) + prev_handled
        state.update({
            "last_run": last_run or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "unavailable": report.get("unavailable") or [],
            "handled": new_handled[-50:],
        })
        state.update(active)
        self.save_data("state", state)

    def _notify_summary(self, report: Dict[str, Any], handled: bool,
                        handled_list: List[Dict[str, Any]] = None) -> None:
        """汇总通知本轮检测结果。"""
        if not self._notify:
            return
        lines = []
        no_seed = report.get("no_seed") or []
        invalid = report.get("invalid") or []
        unavailable = report.get("unavailable") or []
        if no_seed:
            lines.append(f"孤儿源文件 {len(no_seed)} 个（无做种任务）")
            for item in no_seed[:8]:
                lines.append(f"  - {item.get('path', '')}")
            if len(no_seed) > 8:
                lines.append(f"  ...等 {len(no_seed)} 个")
        if invalid:
            lines.append(f"无效做种任务 {len(invalid)} 个（文件丢失/报错）")
            for item in invalid[:8]:
                lines.append(f"  - [{item.get('dl', '')}] {item.get('name', '')}")
            if len(invalid) > 8:
                lines.append(f"  ...等 {len(invalid)} 个")
        if unavailable:
            lines.append(f"下载器异常 {len(unavailable)} 个（已跳过，未做任何处置）")
            for item in unavailable[:5]:
                lines.append(f"  - {item.get('name', '')}: {item.get('err', '')[:80]}")
        if handled_list:
            done = [h for h in handled_list if str(h.get("text", "")).startswith(("已", "达标未"))]
            if done:
                lines.append(f"处置动作：{len(done)} 项")
                for h in done[-5:]:
                    lines.append(f"  - {h.get('text', '')[:100]}")
        if not lines:
            lines.append("本轮检测正常，无孤儿文件、无无效做种、下载器全部在线。")
        self.post_message(
            title=f"做种守卫（{'处置模式' if handled else '仅检测'}）",
            text="\n".join(lines),
            mtype=NotificationType.SiteMessage,
        )

    def _notify_unavailable(self, name: str, err: str, handle: bool) -> None:
        """下载器初始化失败时通知。"""
        if not self._notify:
            return
        self.post_message(
            title="做种守卫",
            text=f"下载器检测失败：{name}\n{err}\n本轮未执行任何处置。",
            mtype=NotificationType.SiteMessage,
        )
