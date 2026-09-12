"""渠道用户自动建号插件。

背景：MoviePilot 只按「渠道 userid → 已绑定用户」解析 Agent 身份。其他渠道账号（微信、
钉钉、Telegram 等）首次发消息时，若没有绑定任何 MoviePilot 用户，Agent 会拒绝全部工具调用
（连只读查询都拒绝），普通命令还会提示「只有管理员才有权限执行此命令」。

本插件通过 `message_parser` 模块钩子（在宿主解析消息之前执行）识别本次消息的渠道与用户 id，
发现该用户尚无绑定账号时，按渠道 userid 自动创建**普通用户**并完成绑定，使其能正常使用
查询、搜索、订阅等功能；随后仍然返回 None，把消息原样交回宿主解析，不改变任何原有行为。

设计约束（通用性）：
- 不依赖具体渠道：按渠道类型尝试解析，另带通用字段兜底，任何渠道的消息都走同一条路径。
- 只做「查 + 建 + 绑定」：不修改任何已有用户、不授予管理员权限。
- 绝不阻断：解析或建号失败只记日志并返回 None。
- 可关闭、可限流：开关与渠道白名单可配，另有每轮/每小时建号上限防止异常刷量。
"""
import json
import re
import secrets
import time
import xml.dom.minidom
from typing import Any, Dict, List, Optional, Tuple

from app.db.user_oper import UserOper
from app.plugins import _PluginBase
from app.schemas.types import NotificationChannel
from app.sdk.logging import logger

# 渠道类型 → 用户 settings 里的绑定键（与宿主身份解析使用的键保持一致）
CHANNEL_BINDING_KEYS: Dict[str, str] = {
    "wechat": "wechat_userid",
    "wechatclawbot": "wechatclawbot_userid",
    "telegram": "telegram_userid",
    "feishu": "feishu_userid",
    "slack": "slack_userid",
    "discord": "discord_userid",
    "qq": "qq_userid",
    "vocechat": "vocechat_userid",
    "synologychat": "synologychat_userid",
}

# 渠道类型 → 通知渠道枚举（用于给新用户发提示）
CHANNEL_ENUMS: Dict[str, NotificationChannel] = {
    "wechat": NotificationChannel.Wechat,
    "wechatclawbot": NotificationChannel.WechatClawBot,
    "telegram": NotificationChannel.Telegram,
    "feishu": NotificationChannel.Feishu,
    "slack": NotificationChannel.Slack,
    "discord": NotificationChannel.Discord,
    "qq": NotificationChannel.QQ,
    "vocechat": NotificationChannel.VoceChat,
    "synologychat": NotificationChannel.SynologyChat,
    "dingtalk": NotificationChannel.DingTalk,
}

# 渠道类型 → 高置信度的用户 id 字段路径（按优先级）
CHANNEL_USERID_KEYS: Dict[str, Tuple[Tuple[str, ...], ...]] = {
    "wechat": (("FromUserName",), ("body", "from", "userid"), ("from", "userid")),
    "wechatclawbot": (("body", "from", "userid"), ("from", "userid"), ("wechat_userid",)),
    "telegram": (("message", "from", "id"), ("from", "id"),
                 ("callback_query", "from", "id"), ("edited_message", "from", "id")),
    "feishu": (("event", "sender", "sender_id", "user_id"),
               ("event", "sender", "sender_id", "open_id"),
               ("event", "operator", "operator_id", "open_id"),
               ("sender", "sender_id", "user_id"), ("open_id",), ("user_id",)),
    "slack": (("event", "user"), ("user",), ("event", "bot_id")),
    "discord": (("d", "author", "id"), ("author", "id"), ("d", "user_id")),
    "qq": (("d", "author", "id"), ("author", "id"), ("d", "user_openid"), ("user_openid",)),
    "vocechat": (("from_uid",), ("from_user", "uid"), ("user_id",)),
    "synologychat": (("user_id",), ("username",)),
}

# 通用兜底字段名：以上逐渠道路径都没命中时，按这些键名递归查找第一个字符串
GENERIC_USERID_KEYS: Tuple[str, ...] = (
    "FromUserName", "from_user_id", "fromuserid",
    "userid", "userId", "user_id", "UserID",
    "sender_id", "senderId", "senderStaffId", "staffId",
    "from_uid", "unionid", "open_id", "openid", "openId",
)

# 明显不是用户 id 的取值（避免把机器人自身或会话 id 当成用户）
EXCLUDED_KEYS: Tuple[str, ...] = (
    "ToUserName", "to_user_id", "chat_id", "chatid", "chatId",
    "conversation_id", "conversationId", "message_id", "msgId", "msg_id",
)

DEFAULT_PERMISSIONS = {
    "discovery": True,
    "search": True,
    "subscribe": True,
    "manage": False,
}

# 插件数据键
DATA_CREATED = "created_users"
DATA_CACHE = "seen_bindings"
DATA_RATE = "rate_window"


class ChannelUserBootstrap(_PluginBase):
    """渠道用户自动建号插件。"""

    plugin_name = "渠道用户自动建号Q自用版"
    plugin_desc = "其他渠道账号首次发消息时，自动按渠道 userid 创建 MoviePilot 普通用户并完成绑定，使其能正常使用查询、搜索、订阅。"
    plugin_icon = "https://raw.githubusercontent.com/q10710/MoviePilot-Plugins/main/icons/channeluserbootstrap.png"
    plugin_version = "1.0.2"
    plugin_label = "系统设置"
    plugin_author = "Q"
    author_url = "https://github.com/q10710"
    plugin_config_prefix = "channeluserbootstrap_"
    plugin_order = 45
    auth_level = 1

    _enabled = False
    _notify_user = True
    _channels: List[str] = []
    # 每小时最多新建账号数，防止异常刷量
    _hourly_limit = 20
    # 用户数据访问（宿主用户表）
    _user_oper = None

    def init_plugin(self, config: dict = None) -> None:
        """根据配置初始化插件。"""
        self._enabled = False
        self._notify_user = True
        self._channels = []
        self._hourly_limit = 20
        self._user_oper = UserOper()
        if not config:
            return
        self._enabled = bool(config.get("enabled"))
        self._notify_user = bool(config.get("notify_user", True))
        raw_channels = str(config.get("channels") or "")
        self._channels = [item.strip().lower() for item in re.split(r"[,\s]+", raw_channels) if item.strip()]
        try:
            limit = int(config.get("hourly_limit") or 20)
        except (TypeError, ValueError):
            limit = 20
        self._hourly_limit = max(limit, 1)
        if self._enabled:
            logger.info(
                f"渠道用户自动建号已启用：渠道范围="
                f"{self._channels or '全部'}，上限={self._hourly_limit}/小时，"
                f"新用户提示={'开' if self._notify_user else '关'}"
            )

    def get_state(self) -> bool:
        """返回插件启用状态。"""
        return self._enabled

    def get_module(self) -> Dict[str, Any]:
        """向宿主注册模块方法：消息解析前置钩子。"""
        return {"message_parser": self.parse_and_bootstrap}

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """本插件不提供远程命令。"""
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """返回插件 API：最近自动创建的账号清单。"""
        return [
            {
                "path": "/created",
                "endpoint": self.api_created_users,
                "methods": ["GET"],
                "auth": "bear",
                "summary": "查看自动创建的账号",
            }
        ]

    def get_form(self) -> Tuple[Optional[List[dict]], Dict[str, Any]]:
        """返回插件配置表单与默认值。"""
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                            "hint": "启用后，未绑定账号的渠道用户首次发消息会自动建号",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify_user",
                                            "label": "给新用户发送提示",
                                            "hint": "建号成功后在该渠道回复一条提示，告知可直接重新发送指令",
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
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "channels",
                                            "label": "生效渠道（留空=全部）",
                                            "placeholder": "wechat,telegram,feishu",
                                            "hint": "按通知渠道类型过滤；留空表示所有接入渠道都生效",
                                            "persistent-hint": True,
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
                                            "model": "hourly_limit",
                                            "label": "每小时建号上限",
                                            "type": "number",
                                            "hint": "防止异常刷量；达到上限后本轮只记录日志不建号",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "notify_user": True,
            "channels": "",
            "hourly_limit": 20,
        }

    def get_page(self) -> Optional[List[dict]]:
        """返回插件详情页：自动创建的账号清单。"""
        created = self.get_data(DATA_CREATED) or {}
        items = [
            {
                "time": info.get("time", ""),
                "channel": info.get("channel", ""),
                "userid": info.get("userid", ""),
                "username": info.get("username", ""),
            }
            for info in list(created.values())[-50:]
        ]
        return [
            {
                "component": "VCard",
                "content": [
                    {"component": "VCardTitle", "props": {"title": "渠道用户自动建号"}},
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "variant": "tonal",
                                    "text": (
                                        f"状态：{'启用' if self._enabled else '未启用'}\n"
                                        f"生效渠道：{'、'.join(self._channels) if self._channels else '全部'}\n"
                                        f"每小时上限：{self._hourly_limit} 个\n"
                                        f"累计自动创建：{len(created)} 个账号"
                                    ),
                                },
                            }
                        ],
                    },
                ],
            },
            {
                "component": "VCard",
                "content": [
                    {"component": "VCardTitle", "props": {"title": "最近创建"}},
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VDataTable",
                                "props": {
                                    "headers": [
                                        {"title": "时间", "key": "time"},
                                        {"title": "渠道", "key": "channel"},
                                        {"title": "渠道用户ID", "key": "userid"},
                                        {"title": "账号", "key": "username"},
                                    ],
                                    "items": items,
                                    "itemsPerPage": 20,
                                },
                            }
                        ],
                    },
                ],
            },
        ]

    def stop_service(self) -> None:
        """本插件无需常驻服务。"""
        return None

    async def api_created_users(self, apikey: str = "") -> Dict[str, Any]:
        """返回自动创建的账号清单。"""
        return {"success": True, "data": self.get_data(DATA_CREATED) or {}}

    # ── 核心逻辑 ──────────────────────────────────────────────

    def parse_and_bootstrap(self, source: Optional[str] = None, body: Any = None,
                            form: Any = None, args: Any = None, **kwargs) -> None:
        """消息解析前置钩子：识别渠道用户并按需建号，始终返回 None 交回宿主解析。"""
        if not self._enabled:
            return None
        try:
            userid, channel_type = self._extract_userid(source=source, body=body, args=args)
            if not userid or not channel_type:
                logger.debug(
                    f"渠道用户自动建号：未能识别渠道用户（source={source}），"
                    f"若为加密回调请检查渠道密钥配置"
                )
                return None
            if self._channels and channel_type not in self._channels:
                return None
            self._ensure_user(channel_type, userid, source=source)
        except Exception as err:
            logger.error(f"渠道用户自动建号处理失败：{err}")
        return None

    def _extract_userid(self, source: Optional[str], body: Any,
                        args: Any = None) -> Tuple[Optional[str], Optional[str]]:
        """从消息原始内容中提取 (渠道用户ID, 渠道类型)。

        先按明文解析（JSON / XML）；企业微信等渠道使用加密回调时，报文是密文，
        此处按渠道密钥解密后再次解析，保证加密渠道同样能识别用户。
        """
        channel_type = self._detect_channel_type(source)
        payload = self._load_payload(body)
        if payload is not None:
            channel_type = channel_type or self._infer_channel_type(payload)
            userid = self._collect_userid(payload, channel_type, source=source)
            if userid:
                return userid, channel_type or "unknown"
        decrypted = self._decrypt_payload(body, args)
        if decrypted is not None:
            payload = self._load_payload(decrypted)
            if payload is not None:
                # 能解密的只有企业微信回调，渠道类型据此确定为 wechat
                channel_type = channel_type or self._infer_channel_type(payload) or "wechat"
                userid = self._collect_userid(payload, channel_type, source=source)
                if userid:
                    return userid, channel_type
        return None, None

    @classmethod
    def _infer_channel_type(cls, payload: Any) -> Optional[str]:
        """在缺少 source 参数时，按报文结构推断渠道类型。

        真实渠道回调未必带 source（企业微信回调就是 source=None），
        只靠 source 反查会把渠道判成 unknown，导致写入的绑定键不正确。
        这里按各渠道报文的显著特征做保守推断，识别不出则返回 None。
        """
        if not isinstance(payload, dict):
            return None
        # 企业微信 app 模式：XML 里带 FromUserName，或密文回调带 Encrypt
        if "FromUserName" in payload or "Encrypt" in payload:
            return "wechat"
        # 企业微信 bot 模式：{"body": {"from": {"userid": ...}}}
        body = payload.get("body")
        if isinstance(body, dict):
            if isinstance(body.get("from"), dict) and body["from"].get("userid"):
                return "wechat"
            if body.get("from") and body.get("msgtype"):
                return "wechat"
        # Telegram：message/edited_message/callback_query 下带 from.id
        for key in ("message", "edited_message", "callback_query", "channel_post"):
            node = payload.get(key)
            if isinstance(node, dict) and isinstance(node.get("from"), dict) and node["from"].get("id") is not None:
                return "telegram"
        # 飞书：header + event.sender
        if "header" in payload and isinstance(payload.get("event"), dict):
            sender = payload["event"].get("sender")
            if isinstance(sender, dict):
                return "feishu"
        # Slack：event.user 或顶层 user
        event = payload.get("event")
        if isinstance(event, dict) and event.get("user"):
            return "slack"
        if payload.get("type") == "event_callback" and payload.get("user"):
            return "slack"
        # 钉钉：senderStaffId / senderId
        if payload.get("senderStaffId") or payload.get("senderId"):
            return "dingtalk"
        # 企业微信智能机器人（clawbot）：wechat_userid 字段
        if payload.get("wechat_userid"):
            return "wechatclawbot"
        return None

    def _collect_userid(self, payload: Any, channel_type: Optional[str],
                        source: Optional[str] = None) -> Optional[str]:
        """按渠道专用字段与通用字段依次尝试取出用户ID。"""
        candidates: List[Any] = []
        if channel_type and channel_type in CHANNEL_USERID_KEYS:
            for path in CHANNEL_USERID_KEYS[channel_type]:
                candidates.append(self._get_by_path(payload, path))
        candidates.append(self._find_by_keys(payload, GENERIC_USERID_KEYS))
        for candidate in candidates:
            value = self._normalize_userid(candidate, source=source)
            if value:
                return value
        return None

    def _decrypt_payload(self, body: Any, args: Any) -> Optional[str]:
        """按渠道密钥尝试解密加密回调报文，返回明文 XML 字符串或 None。

        企业微信的接收回调是密文（<xml> 内为 Encrypt 字段），宿主的解密发生在
        消息模块内部；插件先于宿主执行，因此需要自行解密才能拿到用户ID。
        对每个已配置的同类渠道客户端依次尝试，任一个解密成功即返回。
        """
        if not body or args is None:
            return None
        signature = self._arg_value(args, "msg_signature")
        timestamp = self._arg_value(args, "timestamp")
        nonce = self._arg_value(args, "nonce")
        if not signature or not timestamp or not nonce:
            return None
        payload_bytes = body if isinstance(body, (bytes, bytearray)) else str(body).encode("utf-8")
        for conf in self._channel_configs("wechat"):
            token = str(conf.get("WECHAT_TOKEN") or "").strip()
            aes_key = str(conf.get("WECHAT_ENCODING_AESKEY") or "").strip()
            corpid = str(conf.get("WECHAT_CORPID") or "").strip()
            if not token or not aes_key or not corpid:
                continue
            try:
                from app.adapters.external.wechat import WXBizMsgCrypt

                crypt = WXBizMsgCrypt(sToken=token, sEncodingAESKey=aes_key, sReceiveId=corpid)
                ret, plain = crypt.DecryptMsg(
                    sPostData=bytes(payload_bytes),
                    sMsgSignature=str(signature),
                    sTimeStamp=str(timestamp),
                    sNonce=str(nonce),
                )
            except Exception as err:
                logger.debug(f"渠道用户自动建号：解密回调报文失败：{err}")
                continue
            if ret == 0 and plain:
                return plain.decode("utf-8", errors="replace") if isinstance(plain, (bytes, bytearray)) else str(plain)
        return None

    def _channel_configs(self, channel_type: str) -> List[Dict[str, Any]]:
        """返回指定渠道类型下所有通知客户端的配置字典。"""
        try:
            clients = self.systemconfig.get("Notifications") or []
        except Exception as err:
            logger.debug(f"渠道用户自动建号：读取通知配置失败：{err}")
            return []
        result: List[Dict[str, Any]] = []
        for item in clients:
            if not isinstance(item, dict):
                continue
            if str(item.get("type") or "").strip().lower() != channel_type:
                continue
            config = item.get("config")
            if isinstance(config, dict):
                result.append(config)
        return result

    @staticmethod
    def _arg_value(args: Any, key: str) -> Optional[str]:
        """从查询参数（dict 或 QueryParams）中安全取值。"""
        try:
            value = args.get(key)
        except Exception:
            return None
        return str(value).strip() if value else None

    @staticmethod
    def _load_payload(body: Any) -> Optional[Any]:
        """把消息原始内容解析为可检索结构（JSON 优先，其次 XML）。"""
        if body is None:
            return None
        if isinstance(body, (dict, list)):
            return body
        if isinstance(body, (bytes, bytearray)):
            try:
                text = bytes(body).decode("utf-8", errors="replace")
            except Exception:
                return None
        else:
            text = str(body)
        text = (text or "").strip()
        if not text:
            return None
        if text[0] in "{[":
            try:
                data = json.loads(text)
            except Exception:
                return None
            # 兼容被转义成字符串的 JSON
            for _ in range(3):
                if isinstance(data, str) and data.strip()[:1] in "{[":
                    try:
                        data = json.loads(data)
                    except Exception:
                        break
                else:
                    break
            return data
        if text.startswith("<"):
            try:
                root = xml.dom.minidom.parseString(text).documentElement

                def to_obj(node) -> Dict[str, Any]:
                    """把 XML 节点转换为便于检索的字典。"""
                    result: Dict[str, Any] = {}
                    for child in node.childNodes:
                        if child.nodeType != child.ELEMENT_NODE:
                            continue
                        values = [
                            item.data
                            for item in child.childNodes
                            if item.nodeType in (item.TEXT_NODE, item.CDATA_SECTION_NODE)
                        ]
                        text_value = "".join(values).strip()
                        if text_value:
                            result.setdefault(child.tagName, text_value)
                        else:
                            result.setdefault(child.tagName, to_obj(child))
                    return result

                return to_obj(root)
            except Exception:
                return None
        return None

    def _detect_channel_type(self, source: Optional[str]) -> Optional[str]:
        """按通知客户端名称反查渠道类型。"""
        name = str(source or "").strip()
        if not name:
            return None
        try:
            clients = self.systemconfig.get("Notifications") or []
        except Exception as err:
            logger.debug(f"渠道用户自动建号：读取通知配置失败：{err}")
            return None
        for item in clients:
            if not isinstance(item, dict):
                continue
            if str(item.get("name") or "").strip() == name:
                return str(item.get("type") or "").strip().lower() or None
        return None

    @staticmethod
    def _get_by_path(payload: Any, path: Tuple[str, ...]) -> Optional[Any]:
        """按字段路径逐层取值，取不到返回 None。"""
        current = payload
        for key in path:
            if isinstance(current, dict):
                if key in current:
                    current = current[key]
                else:
                    return None
            elif isinstance(current, list):
                matched = None
                for item in current:
                    if isinstance(item, dict) and key in item:
                        matched = item[key]
                        break
                if matched is None:
                    return None
                current = matched
            else:
                return None
        return current

    @classmethod
    def _find_by_keys(cls, payload: Any, keys: Tuple[str, ...]) -> Optional[Any]:
        """递归查找指定字段名，返回第一个命中的字符串值。"""
        if isinstance(payload, dict):
            for key in keys:
                if key in payload:
                    value = payload[key]
                    found = cls._first_string(value)
                    if found:
                        return found
            for key, value in payload.items():
                if key in EXCLUDED_KEYS:
                    continue
                found = cls._find_by_keys(value, keys)
                if found:
                    return found
        elif isinstance(payload, list):
            for item in payload:
                found = cls._find_by_keys(item, keys)
                if found:
                    return found
        return None

    @classmethod
    def _first_string(cls, value: Any) -> Optional[str]:
        """在嵌套结构里取出第一个像用户 id 的字符串。"""
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, (int,)) and value > 0:
            return str(value)
        if isinstance(value, dict):
            for item in value.values():
                found = cls._first_string(item)
                if found:
                    return found
        if isinstance(value, list):
            for item in value:
                found = cls._first_string(item)
                if found:
                    return found
        return None

    @staticmethod
    def _normalize_userid(candidate: Any, source: Optional[str] = None) -> Optional[str]:
        """校验候选用户ID是否合理（避免把会话ID、机器人ID当成用户）。"""
        if candidate is None or isinstance(candidate, (dict, list, bool)):
            return None
        value = str(candidate).strip()
        if not value or len(value) > 128:
            return None
        if any(char.isspace() for char in value):
            return None
        if source and value == str(source).strip():
            return None
        return value

    def _ensure_user(self, channel_type: str, userid: str, source: Optional[str] = None) -> Optional[str]:
        """确保渠道用户存在且已绑定，必要时创建普通用户。"""
        binding_key = CHANNEL_BINDING_KEYS.get(channel_type, f"{channel_type}_userid")
        if self._resolve_existing(binding_key, userid):
            return None
        if not self._allow_new_user():
            logger.warning(f"渠道用户自动建号：已达每小时上限 {self._hourly_limit}，本轮跳过 {channel_type}/{userid}")
            return None
        username, created = self._create_user(binding_key, userid)
        if not username:
            return None
        if not created:
            # 并发请求已建好同一账号：仅复用，不再记录与通知
            return username
        self._record_created(channel_type, userid, username)
        logger.info(
            f"渠道用户自动建号：{channel_type}/{userid} → {username}"
            f"（普通用户，权限：探索/搜索/订阅）"
        )
        self._notify_new_user(channel_type, userid, username)
        return username

    def _resolve_existing(self, binding_key: str, userid: str) -> Optional[str]:
        """判断该渠道用户是否已有绑定或同名账号。"""
        cache = self.get_data(DATA_CACHE) or {}
        cached = cache.get(f"{binding_key}:{userid}")
        if cached:
            return str(cached)
        try:
            users = self._user_oper.list()
        except Exception as err:
            logger.debug(f"渠道用户自动建号：读取用户列表失败：{err}")
            return None
        for user in users:
            settings = getattr(user, "settings", None) or {}
            if getattr(user, "is_active", True) and str(settings.get(binding_key) or "") == userid:
                self._remember_binding(binding_key, userid, user.name)
                return user.name
            if str(getattr(user, "name", "")) == userid:
                self._remember_binding(binding_key, userid, user.name)
                return user.name
        return None

    def _remember_binding(self, binding_key: str, userid: str, username: str) -> None:
        """缓存已确认的绑定，避免每条消息都全量查询用户表。"""
        cache = self.get_data(DATA_CACHE) or {}
        cache[f"{binding_key}:{userid}"] = username
        if len(cache) > 500:
            cache = dict(list(cache.items())[-500:])
        self.save_data(DATA_CACHE, cache)

    def _allow_new_user(self) -> bool:
        """按每小时上限限制建号频率。"""
        window = self.get_data(DATA_RATE) or {}
        now = time.time()
        started = float(window.get("start") or 0)
        count = int(window.get("count") or 0)
        if not started or now - started >= 3600:
            window = {"start": now, "count": 0}
            count = 0
        if count >= self._hourly_limit:
            return False
        window["count"] = count + 1
        self.save_data(DATA_RATE, window)
        return True

    def _create_user(self, binding_key: str, userid: str) -> Tuple[Optional[str], bool]:
        """按渠道用户ID创建普通用户并写入绑定，返回 (用户名, 是否本次新建)。"""
        name = self._unique_name(userid)
        try:
            from app.application.security.token import get_password_hash

            password = get_password_hash(secrets.token_urlsafe(24))
            self._user_oper.add(
                name=name,
                hashed_password=password,
                is_active=True,
                is_superuser=False,
                permissions=dict(DEFAULT_PERMISSIONS),
                settings={binding_key: userid, "nickname": ""},
            )
        except Exception as err:
            # 并发回调（同一用户短时间连发多条）可能同时走到建号：唯一约束冲突时
            # 说明另一个请求已建好，直接复查并复用，不当作错误。
            if self._is_duplicate_error(err):
                existing = self._resolve_existing(binding_key, userid)
                if existing:
                    logger.debug(f"渠道用户自动建号：{name} 已由并发请求创建，复用现有账号")
                    return existing, False
            logger.error(f"渠道用户自动建号：创建用户失败 {name}：{err}")
            return None, False
        self._remember_binding(binding_key, userid, name)
        return name, True

    @staticmethod
    def _is_duplicate_error(err: Exception) -> bool:
        """判断异常是否属于「用户名已存在」的唯一约束冲突。"""
        text = str(err).lower()
        return (
            "uniqueviolation" in text
            or "duplicate key" in text
            or "already exists" in text
            or "unique constraint" in text
        )

    def _unique_name(self, base: str) -> str:
        """生成不冲突的用户名：base、base-2、base-3 …"""
        try:
            existing = {str(getattr(user, "name", "")) for user in self._user_oper.list()}
        except Exception:
            existing = set()
        if base not in existing:
            return base
        index = 2
        while f"{base}-{index}" in existing:
            index += 1
        return f"{base}-{index}"

    def _record_created(self, channel_type: str, userid: str, username: str) -> None:
        """记录自动创建的账号，供详情页展示。"""
        created = self.get_data(DATA_CREATED) or {}
        created[f"{channel_type}:{userid}"] = {
            "time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "channel": channel_type,
            "userid": userid,
            "username": username,
        }
        if len(created) > 200:
            created = dict(list(created.items())[-200:])
        self.save_data(DATA_CREATED, created)

    def _notify_new_user(self, channel_type: str, userid: str, username: str) -> None:
        """给新用户发送一条建号提示（失败只记日志）。"""
        if not self._notify_user:
            return
        channel = CHANNEL_ENUMS.get(channel_type)
        if not channel:
            return
        try:
            self.post_message(
                channel=channel,
                title="已自动为你创建账号",
                text=(
                    f"检测到你是首次使用：已按渠道用户ID自动创建账号「{username}」，"
                    f"并完成绑定。\n请重新发送刚才的指令即可使用（支持查询、搜索、订阅）。"
                ),
                userid=userid,
            )
        except Exception as err:
            logger.debug(f"渠道用户自动建号：新用户提示发送失败：{err}")
