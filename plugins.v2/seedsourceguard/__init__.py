"""做种守卫插件。

检测三类做种异常并持续追踪：
1. 无效做种：下载器中存在已完成/应做种的任务，但本地文件已丢失或任务报错，实际无法做种；
2. 孤儿源文件：本地下载目录中存在文件，但所有启用下载器都没有对应的做种任务；
3. 红种做种：做种任务的 tracker 全部通告失败（站点删除/风控），可按配置删除种子，
   当源文件无其它正常辅种覆盖时连同源文件一起删除。

三类异常均按"连续 N 天"宽限期追踪，达标后按配置的动作处理（仅通知 / 移动 / 删除），
支持 qBittorrent、Transmission、rTorrent 等所有 MoviePilot 已配置的下载器实例。
安全设计：下载器连接失败时本轮直接跳过对应下载器，绝不把任何文件误判为孤儿；
红种占做种数达 80% 阈值时判定站点/网络级故障，自动保护不处置。
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

    plugin_name = "做种守卫Q自用版"
    plugin_desc = ("检测本地源文件是否有下载器在做种、下载器是否存在文件丢失的无效做种或"
                   "tracker 全部失败的做种任务；连续N天异常可通知或按策略处置，杜绝无效做种与孤儿文件。")
    plugin_icon = "https://raw.githubusercontent.com/q10710/MoviePilot-Plugins/main/icons/seedsourceguard.png"
    plugin_version = "1.1.10"
    plugin_label = "下载管理"
    plugin_author = "Q"
    author_url = "https://github.com/q10710"
    plugin_config_prefix = "seedsourceguard_"
    plugin_order = 66
    auth_level = 1

    # 状态字段默认值
    _enabled: bool = False
    _notify: bool = True
    _scan_times: int = 2
    _clean_days: int = 7
    _cron: str = "15 3,15 * * *"
    _days: int = 14  # 内部计数阈值 = 每日扫描次数 × 清理宽限天数，按实际扫描轮次累计
    _scan_dirs: List[str] = []
    _exclude_keys: List[str] = []
    _downloaders: List[str] = []
    _max_depth: int = 3
    _no_seed_action: str = "notify"
    _invalid_action: str = "notify"
    _red_action: str = "delete"
    _move_path: str = ""
    _allow_delete: bool = False

    # 红种做种保护：单下载器红种占做种候选数比例达标时视为站点/网络故障，本轮只通知不处置
    _RED_PROTECT_RATIO: float = 0.8
    # qBittorrent 需逐任务查询 tracker 状态，任务数超过该上限时跳过红种检测
    _QB_TRACKER_QUERY_LIMIT: int = 500

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
        # 扫描频率与清理宽限分开配置：每日扫描次数决定 cron，宽限天数 × 每日次数 = 扫描轮次阈值
        scan_times = int(config.get("scan_times") or 0)
        clean_days = int(config.get("clean_days") or 0)
        if scan_times >= 1 and clean_days >= 1:
            self._scan_times = min(scan_times, 6)
            self._clean_days = clean_days
        else:
            # 旧配置迁移：从 cron 反推每日次数，天数 = 原轮次阈值 ÷ 每日次数
            old_cron = str(config.get("cron") or "")
            old_days = int(config.get("days") or 0)
            times = self._times_from_cron(old_cron)
            self._scan_times = max(1, min(times, 6))
            self._clean_days = max(1, (old_days or 14) // self._scan_times)
        self._cron = self._build_cron(self._scan_times)
        self._days = self._scan_times * self._clean_days
        self._max_depth = int(config.get("max_depth") or 3)
        if self._max_depth < 1:
            self._max_depth = 1
        self._scan_dirs = self._split_lines(config.get("scan_dirs"))
        self._exclude_keys = self._split_lines(config.get("exclude_dirs"))
        self._downloaders = self._split_comma(config.get("downloaders"))
        self._no_seed_action = str(config.get("no_seed_action") or "notify")
        self._invalid_action = str(config.get("invalid_action") or "notify")
        self._red_action = str(config.get("red_action") or "delete")
        self._move_path = str(config.get("move_path") or "").strip()
        self._allow_delete = bool(config.get("allow_delete"))

    @staticmethod
    def _build_cron(times: int) -> str:
        """根据每日扫描次数生成 cron（分钟固定 15 分）。"""
        hours = {1: "3", 2: "3,15", 3: "3,11,19", 4: "3,9,15,21",
                 5: "3,7,11,15,19", 6: "3,7,11,15,19,23"}.get(int(times), "3,15")
        return f"15 {hours} * * *"

    @staticmethod
    def _times_from_cron(cron: str) -> int:
        """从 cron 的小时字段反推每日扫描次数。"""
        try:
            hour_part = str(cron or "").split()[1]
            count = len([h for h in hour_part.split(",") if h.strip()])
            return max(1, count) if count else 1
        except Exception:
            return 1

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
                                            "model": "clean_days",
                                            "label": "清理宽限天数",
                                            "type": "number",
                                            "hint": ("连续 N 天仍异常才触发处置；内部按实际扫描轮次累计"
                                                     "（N × 每日扫描次数），关机/停用期不计数"),
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
                                            "model": "scan_times",
                                            "label": "每日扫描次数",
                                            "items": [
                                                {"title": "每天 1 次", "value": 1},
                                                {"title": "每天 2 次", "value": 2},
                                                {"title": "每天 3 次", "value": 3},
                                                {"title": "每天 4 次", "value": 4},
                                            ],
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": ("扫描时间由每日次数自动生成并均匀分布"
                                                     "（1 次 03:15；2 次 03:15/15:15；3 次 03/11/19；4 次 03/09/15/21）。"
                                                     "每轮均为独立全量检测，不读取上次结果。"),
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
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
                                        "component": "VSelect",
                                        "props": {
                                            "model": "red_action",
                                            "label": "红种做种处置",
                                            "items": [
                                                {"title": "仅通知", "value": "notify"},
                                                {"title": "删除种子(无其它辅种时连同源文件删除)", "value": "delete"},
                                            ],
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": ("红种判定：做种任务的全部 tracker 连续通告失败。"
                                                     "删除种子时自动判断：该源文件仍被其它正常任务覆盖则只删种子保留文件；"
                                                     "无任何其它正常辅种时删除种子并连同源文件一起删除。"
                                                     "单下载器红种占做种数 80% 以上时触发站点/网络级故障保护，本轮只通知不处置。"),
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
                                     "绝不会把任何文件判为孤儿；红种占做种数达 80% 阈值时自动保护不处置。"
                                     "删除/移动类处置要求：连续 N 次扫描仍异常（按实际扫描轮次累计，关机/停用期不计数） + 本页开关已打开。"),
                        },
                    },
                ],
            }
        ], {
            "enabled": False,
            "notify": True,
            "scan_times": 2,
            "clean_days": 7,
            "scan_dirs": "/nastools/data/downloads/dianying/\n/nastools/data/downloads/tv/",
            "exclude_dirs": "音乐\nmusic\n儿童\n临时下载\ndouyin\n杰伦十代\nflac\nape\nwav",
            "downloaders": "",
            "max_depth": 3,
            "no_seed_action": "notify",
            "invalid_action": "notify",
            "red_action": "delete",
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
        red = state.get("red_active") or []
        unavailable = state.get("unavailable") or []
        last_run = state.get("last_run") or "尚未运行"
        handled = state.get("handled") or []
        total = len(no_seed) + len(invalid) + len(red) + len(unavailable)
        # ── 顶部状态条 ──
        if total == 0:
            head_type = "success"
            head_text = f"一切正常：未发现孤儿文件、无效做种与红种做种（最近检测：{last_run}）"
        else:
            head_type = "warning"
            head_text = (f"共发现 {total} 项异常（最近检测：{last_run}）"
                         f"，连续 {self._days} 次扫描仍存在将按配置处置，删除类处置需先开启允许开关")
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
            ("红种做种", len(red), "deep-purple-darken-2"),
            ("下载器异常", len(unavailable), "grey-darken-1"),
        ]
        row_content = []
        for label, num, color in stats:
            row_content.append({
                "component": "VCol",
                "props": {"cols": 12, "md": 3},
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
                "key": "red",
                "title": "红种做种（tracker 全部通告失败，占做种数 80% 以上时自动保护）",
                "color": "deep-purple-darken-2",
                "empty": "未发现红种做种任务",
                "items": red,
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
                        extra = f"已持续 {item.get('days', 1)} 次扫描"
                    elif sec["key"] in ("invalid", "red"):
                        line = f"[{item.get('dl', '')}] {item.get('name', '')}"
                        extra = f"已持续 {item.get('days', 1)} 次扫描"
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
                    "text": "暂无处置记录",
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

        # 3b. 红种做种检测（tracker 全部通告失败）
        red_candidates, hold_count = self._detect_red(task_index, services)
        report["red"] = red_candidates
        report["hold_count"] = hold_count

        # 4. 孤儿源文件检测
        no_seed_candidates = self._detect_no_seed(task_index)
        report["no_seed"] = no_seed_candidates

        # 5. 按天计数并处置
        handled_list = self._settle(report, handle=handle, task_index=task_index)

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
                    # Transmission 的 get_torrents 已附带 trackerStats，供红种判定；qb/rTorrent 为空
                    "tracker_stats": self._tget(
                        torrent, "trackerStats", "tracker_stats", default=None
                    ),
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

    def _detect_red(self, task_index: Dict[str, list],
                    services: Optional[dict] = None) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
        """检测红种做种任务：处于做种状态但 tracker 全部通告失败。

        返回 (红种明细列表, 各下载器做种候选数)。qBittorrent 需逐任务查询
        tracker 状态，候选超过上限时跳过；Transmission 直接使用任务自带的
        trackerStats，无额外请求开销。
        """
        red: List[Dict[str, Any]] = []
        hold_count: Dict[str, int] = {}
        for dl_name, tasks in task_index.items():
            candidates = [t for t in tasks if self._is_hold_state(t["state"])]
            hold_count[dl_name] = len(candidates)
            if not candidates:
                continue
            instance = None
            if services:
                instance = getattr(services.get(dl_name), "instance", None)
            provider = self._provider_kind(instance)
            if provider == "qb":
                if len(candidates) > self._QB_TRACKER_QUERY_LIMIT:
                    logger.warning(
                        f"下载器 {dl_name} 做种任务 {len(candidates)} 个超过红种检测上限，本轮跳过红种检测"
                    )
                    continue
                qbc = getattr(instance, "qbc", None)
                if qbc is None:
                    continue
                for task in candidates:
                    hash_value = task.get("hash", "")
                    if not hash_value:
                        continue
                    try:
                        trackers = qbc.torrents_trackers(torrent_hash=hash_value) or []
                    except Exception as err:
                        # 单任务查询失败不影响其余任务的红种检测
                        logger.debug(f"下载器 {dl_name} 查询 tracker 状态失败 {hash_value}：{err}")
                        continue
                    if self._torrent_red(task, trackers):
                        red.append(self._red_item(task, dl_name))
                continue
            # Transmission / 其它：优先使用任务自带 trackerStats
            for task in candidates:
                if self._torrent_red(task, task.get("tracker_stats")):
                    red.append(self._red_item(task, dl_name))
        return red, hold_count

    @staticmethod
    def _provider_kind(instance: Any) -> str:
        """判断下载器实例类型（qb/tr/其它）。"""
        if instance is None:
            return "unknown"
        try:
            if getattr(instance, "qbc", None) is not None:
                return "qb"
            if getattr(instance, "trc", None) is not None:
                return "tr"
        except Exception:
            return "unknown"
        return "unknown"

    @staticmethod
    def _red_item(task: Dict[str, Any], dl_name: str) -> Dict[str, Any]:
        """构造红种明细项。"""
        return {
            "dl": dl_name,
            "hash": task.get("hash", ""),
            "name": task.get("name", ""),
            "root": task.get("root", ""),
            "state": task.get("state", ""),
        }

    def _torrent_red(self, task: Dict[str, Any], tracker_stats: Any) -> bool:
        """判定单个任务是否为红种：做种状态、文件完好且 tracker 全部失败。"""
        state = task.get("state", "")
        if state in self._INVALID_STATES or not self._is_hold_state(state):
            return False
        if not self._path_ok(task.get("root", "")):
            # 文件丢失/目录为空归无效做种处理，不在红种路径重复计数
            return False
        return self._tracker_all_failed(tracker_stats)

    @staticmethod
    def _tracker_all_failed(tracker_stats: Any) -> bool:
        """解析 tracker 数据：存在失败记录且无任何成功通告时判红。

        兼容 qBittorrent 的 torrents_trackers（status 字段）与 Transmission 的
        trackerStats（lastAnnounceSucceeded / lastAnnounceTime 字段）。
        """
        stats = tracker_stats or []
        real = []
        for item in stats:
            if not item:
                continue
            if isinstance(item, dict):
                url = str(item.get("url") or item.get("host") or "")
            else:
                url = str(getattr(item, "url", "") or getattr(item, "host", ""))
            if url.startswith("**"):  # DHT / PeX / LSD 等内置 tracker 不计
                continue
            real.append(item)
        if not real:
            return False

        def fget(it: Any, key: str, default: Any = None) -> Any:
            """兼容 dict 与对象的字段读取，并同时兼容 camelCase / snake_case 字段名。

            Transmission 的 trackerStats 经 transmission-rpc 封装后暴露
            last_announce_succeeded 等 snake_case 属性，而原始 RPC 字段为
            lastAnnounceSucceeded，因此按 camelCase 名读取不到真实值时自动
            再尝试其 snake_case 形式。
            """
            candidates = [key]
            snake = "".join("_" + ch.lower() if ch.isupper() else ch for ch in key)
            if snake != key:
                candidates.append(snake)
            for name in candidates:
                try:
                    value = it.get(name) if isinstance(it, dict) else getattr(it, name, None)
                except Exception:
                    continue
                if value is not None:
                    return value
            return default
        announced = False
        succeeded = False
        for it in real:
            if fget(it, "status") is not None:  # qBittorrent tracker 状态
                status = int(fget(it, "status") or 0)
                if status in (2, 3):  # working / updating：至少一个正常则不红
                    return False
                if status == 4:  # notWorking：明确通告失败
                    announced = True
                continue
            # Transmission trackerStats
            succ = fget(it, "lastAnnounceSucceeded")
            atime = int(fget(it, "lastAnnounceTime") or 0)
            result = str(fget(it, "lastAnnounceResult") or "")
            if atime > 0:
                announced = True
            if succ:
                succeeded = True
            if result and "Success" in result:
                succeeded = True
        if succeeded:
            return False
        return announced

    @staticmethod
    def _is_hold_state(state: Any) -> bool:
        """判断任务是否处于应做种/保种的状态。

        兼容 qBittorrent 文本状态与 Transmission 的数字状态（seed=6、seed_wait=5）。
        """
        text = str(state or "").strip().lower()
        if text in SeedSourceGuard._HOLD_STATES:
            return True
        if text.isdigit() and int(text) in (5, 6):  # seed_wait / seed
            return True
        return False

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

    def _settle(self, report: Dict[str, Any], handle: bool,
                task_index: Optional[Dict[str, list]] = None) -> List[Dict[str, Any]]:
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

        # 红种做种计数（带站点/网络级故障保护）
        red_map = {}
        for item in report.get("red") or []:
            key = f"{item['dl']}:{item['hash'] or item['root']}"
            red_map[key] = item
        protected = self._red_protected_keys(red_map, report.get("hold_count") or {})
        for key in sorted(protected):
            item = red_map[key]
            handled_list.append({
                "time": datetime.now().strftime("%m-%d %H:%M"),
                "text": (f"站点级故障保护：{item.get('dl', '')} 红种 {protected[key]}，"
                         "本轮不处置，仅通知"),
            })
        red_map_active = {k: v for k, v in red_map.items() if k not in protected}
        # 正常辅种覆盖判定集合：排除本轮无效/红种任务根路径后，仅“正常任务”可覆盖源文件
        bad_roots = set()
        for item in list(invalid_map.values()) + list(red_map.values()):
            root_v = (item.get("root") or "").rstrip("/")
            if root_v:
                bad_roots.add(root_v)
        healthy_roots = []
        for dl_name, tasks in (task_index or {}).items():
            for task in tasks:
                root_v = (task.get("root") or "").rstrip("/")
                if root_v and root_v not in bad_roots:
                    healthy_roots.append(root_v)
        stale_red = self._bump_days(days_data, "red", red_map_active, today)
        for key, days in stale_red.items():
            item = red_map_active.get(key) or {}
            handled = self._handle_red(item, days, handle, healthy_roots)
            if handled:
                handled_list.append(handled)
                days_data.pop(key, None)

        # 清理已恢复的旧记录（保留当前仍异常项的天数历史）
        for prefix in ("no_seed:", "invalid:", "red:"):
            for old_key in [k for k in days_data if k.startswith(prefix)]:
                body = old_key[len(prefix):]
                if prefix == "no_seed:" and body not in no_seed_map:
                    days_data.pop(old_key, None)
                elif prefix == "invalid:" and body not in invalid_map:
                    days_data.pop(old_key, None)
                elif prefix == "red:" and body not in red_map_active:
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
        active_red = []
        for key, item in red_map_active.items():
            active_red.append({
                "dl": item.get("dl", ""),
                "name": item.get("name", ""),
                "days": self._calc_days(days_data, f"red:{key}", today),
            })
        self.save_data("active", {
            "no_seed_active": active_no_seed,
            "invalid_active": active_invalid,
            "red_active": active_red,
        })
        return handled_list

    def _red_protected_keys(self, red_map: Dict[str, Any],
                            hold_count: Dict[str, int]) -> Dict[str, str]:
        """按下载器判定是否触发站点/网络级故障保护。

        单下载器红种占该下载器做种候选数比例 >=80% 时，该下载器所有红种
        本轮不处置（不计天数），仅返回保护说明。
        """
        protected: Dict[str, str] = {}
        red_by_dl: Dict[str, List[str]] = {}
        for key, item in red_map.items():
            dl = item.get("dl", "")
            red_by_dl.setdefault(dl, []).append(key)
        for dl, keys in red_by_dl.items():
            total = int(hold_count.get(dl) or 0)
            if total <= 0:
                total = len(keys)
            count = len(keys)
            if total > 0 and count / total >= self._RED_PROTECT_RATIO:
                for key in keys:
                    protected[key] = f"{count}/{total}"
                logger.warning(
                    f"下载器 {dl} 红种 {count}/{total} 达 {int(self._RED_PROTECT_RATIO * 100)}% 阈值，"
                    "判定站点/网络故障，本轮保护不处置"
                )
        return protected

    def _bump_days(self, days_data: dict, prefix: str,
                   current: Dict[str, Any], today: str) -> Dict[str, int]:
        """按实际扫描轮次推进计数并返回达到阈值的关键项。

        每轮真实扫描中该项仍存在则计数 +1，关机/停用期不扫描即不累计；
        旧版按自然日记录（形如 2026-09-08）自动迁移为首轮计数 1。
        """
        stale = {}
        for key in current:
            full_key = f"{prefix}:{key}"
            count = self._calc_days(days_data, full_key, today)
            count += 1
            if count >= self._days:
                stale[key] = count
            else:
                days_data[full_key] = count
        return stale

    @staticmethod
    def _calc_days(days_data: dict, full_key: str, today: str) -> int:
        """计算某项已连续出现的扫描轮次数。

        存储值为整数计数；兼容旧版日期字符串记录（迁移为 1 次）。
        """
        raw = days_data.get(full_key)
        if raw is None:
            return 0
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 1

    # ── 处置动作 ──────────────────────────────────────────────

    def _handle_no_seed(self, path: str, days: int, handle: bool) -> Optional[Dict[str, Any]]:
        """处置达到天数的孤儿源文件。"""
        action = self._no_seed_action
        text = f"孤儿源文件 {path}（连续 {days} 次扫描仍无做种任务）"
        if action == "notify" or not handle:
            logger.warning(f"检测到孤儿源文件：{path}，连续 {days} 次扫描")
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
        text = f"无效做种 {dl}：{name}（连续 {days} 次扫描仍文件丢失/报错）"
        if action == "notify" or not handle:
            logger.warning(f"检测到无效做种：{dl} / {name}，连续 {days} 次扫描")
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

    def _handle_red(self, item: Dict[str, Any], days: int,
                    handle: bool,
                    healthy_roots: Optional[List[str]] = None,
                    ) -> Optional[Dict[str, Any]]:
        """处置达到天数的红种做种任务。

        删除种子时联动判定源文件归属：该源文件仍被其它正常任务覆盖则仅删任务
        保留文件；无任何其它正常做种任务覆盖时删除任务并连同源文件一起删除。
        healthy_roots 由结算阶段传入，已排除本轮红种/无效任务的根路径。
        """
        action = self._red_action
        dl = item.get("dl", "")
        name = item.get("name", "")
        hash_value = item.get("hash", "")
        root = item.get("root", "")
        text = f"红种做种 {dl}：{name}（连续 {days} 次扫描 tracker 仍通告失败）"
        if action != "delete" or not handle:
            logger.warning(f"检测到红种做种：{dl} / {name}，连续 {days} 次扫描")
            return {"time": datetime.now().strftime("%m-%d %H:%M"), "text": f"检测到 {text}"}
        if not self._allow_delete:
            logger.warning(f"红种做种 {dl} / {name} 已达标但未开启删除开关，跳过处置")
            return {"time": datetime.now().strftime("%m-%d %H:%M"),
                    "text": f"达标未处置 {text}"}
        if not hash_value:
            return None
        # 源文件是否仍被其它正常做种任务覆盖：正常任务集合由结算阶段计算并传入
        delete_file = True
        if healthy_roots and root and root.rstrip("/") in healthy_roots:
            delete_file = False
        try:
            services = self._downloader_helper.get_services(name_filters=[dl])
            instance = getattr(services.get(dl), "instance", None) if services else None
            if instance is None:
                logger.warning(f"下载器 {dl} 不可用，无法删除任务")
                return None
            instance.delete_torrents(delete_file, [hash_value])
            logger.info(
                f"已删除红种做种任务：{dl} / {name}（delete_file={delete_file}）"
            )
            return {"time": datetime.now().strftime("%m-%d %H:%M"),
                    "text": (f"已删除 {'任务及源文件 ' if delete_file else '任务 '}{text}")}
        except Exception as err:
            logger.error(f"删除红种做种任务失败 {dl} / {name}：{err}")
            return {"time": datetime.now().strftime("%m-%d %H:%M"),
                    "text": f"删除失败 {text}：{err}"}

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
        red = report.get("red") or []
        unavailable = report.get("unavailable") or []
        # 单项超过 5 个只报汇总计数，避免消息过长；少量时逐条列出
        if no_seed:
            lines.append(f"孤儿源文件 {len(no_seed)} 个（无做种任务）")
            if len(no_seed) <= 5:
                for item in no_seed:
                    lines.append(f"  - {item.get('path', '')}")
            else:
                lines.append("  - 数量较多，不逐条推送，请到插件页查看明细")
        if invalid:
            lines.append(f"无效做种任务 {len(invalid)} 个（文件丢失/报错）")
            if len(invalid) <= 5:
                for item in invalid:
                    lines.append(f"  - [{item.get('dl', '')}] {item.get('name', '')}")
            else:
                lines.append("  - 数量较多，不逐条推送，请到插件页查看明细")
        if red:
            lines.append(f"红种做种任务 {len(red)} 个（tracker 全部通告失败）")
            if len(red) <= 5:
                for item in red:
                    lines.append(f"  - [{item.get('dl', '')}] {item.get('name', '')}")
            else:
                lines.append("  - 数量较多，不逐条推送，请到插件页查看明细")
        if unavailable:
            lines.append(f"下载器异常 {len(unavailable)} 个（已跳过，未做任何处置）")
            for item in unavailable[:5]:
                lines.append(f"  - {item.get('name', '')}: {item.get('err', '')[:80]}")
        if handled_list:
            done = [h for h in handled_list if str(h.get("text", "")).startswith(("已", "达标未"))]
            if done:
                lines.append(f"处置动作：{len(done)} 项")
                for h in done[-3:]:
                    lines.append(f"  - {h.get('text', '')[:100]}")
        if not lines:
            lines.append("本轮检测正常，无孤儿文件、无无效做种、无红种做种、下载器全部在线。")
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
