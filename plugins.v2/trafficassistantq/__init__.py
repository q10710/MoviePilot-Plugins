import copy
import hashlib
import json
import threading
import uuid
from dataclasses import asdict, fields
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

import pytz
from app.sdk.network import SitesHelper
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from ruamel.yaml import YAMLError

from app.sdk.config import settings
from app.sdk.events import Event, eventmanager
from app.sdk.plugins import PluginManager
from app.db.oper.site import SiteOper
from app.db.oper.systemconfig import SystemConfigOper
from app.sdk.logging import logger
from app.plugins import _PluginBase
from app.sdk.scheduler import update_plugin_job
from app.schemas import NotificationType
from app.schemas.types import EventType, SystemConfigKey

from .trafficconfig import BaseConfig, TrafficConfig

lock = threading.Lock()

# 刷流任务身份/运行态字段：最终配置记忆与同步时不会被覆盖
BRUSH_TASK_IDENTITY_KEYS = ("id", "name", "site_id", "enabled")
# 最终配置模板持久化数据键
DATA_KEY_BRUSH_TEMPLATE = "taq_brush_template"
# 刷流任务指纹快照持久化数据键
DATA_KEY_BRUSH_SNAPSHOTS = "taq_brush_snapshots"


class TrafficAssistantQ(_PluginBase):
    """站点流量管理Q改版：基于官方站点流量管理，联动站点刷流，分享率低于下限时按最终配置自动新建/同步刷流任务并启动。"""

    # 插件名称
    plugin_name = "站点流量管理Q自用版"
    # 插件描述
    plugin_desc = "自动管理流量，保障站点分享率。低于分享率下限时，按最终配置自动新建或同步该站刷流任务并启动；高于上限自动暂停。最终配置记忆最后一次手动改动的刷流任务规则，任务被全部删除后仍可重建。基于官方 TrafficAssistant v2.0.0 改造。"
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/q10710/MoviePilot-Plugins/main/icons/trafficassistantq.png"
    # 插件版本
    plugin_version = "2.0.2"
    # 插件作者
    plugin_author = "q1071091473"
    # 作者主页
    author_url = "https://github.com/q10710"
    # 插件配置项ID前缀
    plugin_config_prefix = "trafficassistantq_"
    # 加载顺序
    plugin_order = 19
    # 可使用的用户级别
    auth_level = 2

    # region 私有属性

    pluginmanager = None
    siteshelper = None
    siteoper = None
    systemconfig = None

    # 流量管理配置
    _traffic_config = TrafficConfig()
    # 插件是否需要热加载
    _plugin_reload_if_need = False

    # 定时器
    _scheduler = None
    # 退出事件
    _event = threading.Event()

    # endregion

    def init_plugin(self, config: dict = None):
        self.pluginmanager = PluginManager()
        self.siteshelper = SitesHelper()
        self.siteoper = SiteOper()
        self.systemconfig = SystemConfigOper()

        if not config:
            return

        result, reason = self.__validate_and_fix_config(config=config)

        if not result and not self._traffic_config:
            self.__update_config_if_error(config=config, error=reason)
            return

        if self._traffic_config.onlyonce:
            self._traffic_config.onlyonce = False
            self.update_config(config=config)

            logger.info("立即运行一次站点流量管理服务")
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(self.traffic, 'date',
                                    run_date=datetime.now(
                                        tz=pytz.timezone(settings.TZ)
                                    ) + timedelta(seconds=3),
                                    name="站点流量管理")

            if self._scheduler.get_jobs():
                # 启动服务
                self._scheduler.print_jobs()
                self._scheduler.start()

        self.__update_config()

    def get_state(self) -> bool:
        """返回插件是否已启用，未完成配置时保持布尔类型。"""
        return bool(self._traffic_config and self._traffic_config.enabled)

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        定义远程控制命令
        :return: 命令关键字、事件、描述、附带数据
        """
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        return [
            {
                'component': 'VForm',
                'content': [
                        {
                            'component': 'VRow',
                            'content': [
                                {
                                    'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                            'hint': '开启后插件将处于激活状态',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'notify',
                                            'label': '发送通知',
                                            'hint': '是否在特定事件发生时发送通知',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
                                            'hint': '插件将立即运行一次',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 12
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'multiple': True,
                                            'chips': True,
                                            'clearable': True,
                                            'model': 'sites',
                                            'label': '站点列表',
                                            'items': self.__get_site_options(),
                                            'hint': '选择参与配置的站点',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'brush_plugin',
                                            'label': '站点刷流插件',
                                            'items': self.__get_plugin_options(),
                                            'hint': '选择参与配置的刷流插件',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enable_site_config',
                                            'label': '站点独立配置',
                                            'hint': '启用站点独立配置',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'site_config_dialog',
                                            'label': '打开站点配置窗口',
                                            'hint': '点击弹出窗口以修改站点配置',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VCronField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '执行周期',
                                            'placeholder': '5位cron表达式',
                                            'hint': '使用cron表达式指定执行周期，如 0 8 * * *',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'ratio_lower_limit',
                                            'label': '分享率下限',
                                            'type': 'number',
                                            "min": "0",
                                            'hint': '设置最低分享率阈值',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'ratio_upper_limit',
                                            'label': '分享率上限',
                                            'type': 'number',
                                            "min": "0",
                                            'hint': '设置最高分享率阈值',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                        {
                            'component': 'VRow',
                            'content': [
                                {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'add_to_subscription_if_above',
                                            'label': '添加订阅站点',
                                            'hint': '分享率大于上限时自动添加到订阅站点',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'add_to_search_if_above',
                                            'label': '添加搜索站点',
                                            'hint': '分享率大于上限时自动添加到搜索站点',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'disable_auto_brush_if_above',
                                            'label': '停止刷流',
                                            'hint': '分享率大于上限时自动暂停该站刷流任务（不删除、不改规则）',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'remove_from_subscription_if_below',
                                            'label': '移除订阅站点',
                                            'hint': '分享率小于等于下限时自动从订阅中移除站点',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'remove_from_search_if_below',
                                            'label': '移除搜索站点',
                                            'hint': '分享率小于等于下限时自动从搜索中移除站点',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enable_auto_brush_if_below',
                                            'label': '开启刷流',
                                            'hint': '分享率小于等于下限时自动按最终配置新建/同步该站刷流任务并启动',
                                            'persistent-hint': True
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    # {
                    #     'component': 'VRow',
                    #     'content': [
                    #         {
                    #             'component': 'VCol',
                    #             'props': {
                    #                 'cols': 12,
                    #                 'md': 4
                    #             },
                    #             'content': [
                    #                 {
                    #                     'component': 'VSwitch',
                    #                     'props': {
                    #                         'model': 'send_alert_if_below',
                    #                         'label': '发送预警',
                    #                         'hint': '分享率小于等于下限时发送预警通知',
                    #                         'persistent-hint': True
                    #                     }
                    #                 }
                    #             ]
                    #         },
                    #     ]
                    # },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'error',
                                            'variant': 'tonal',
                                            'text': '警告：本插件仍在完善阶段，可能会导致站点流量异常，分享率降低等，'
                                                    '严重甚至导致站点封号，请慎重使用'
                                        }
                                    }
                                ]
                                },
                            ]
                        },
                        {
                            "component": "VDialog",
                            "props": {
                                "model": "site_config_dialog",
                                "max-width": "65rem",
                                "overlay-class": "v-dialog--scrollable v-overlay--scroll-blocked",
                                "content-class": "v-card v-card--density-default v-card--variant-elevated rounded-t"
                            },
                            "content": [
                                {
                                    "component": "VCard",
                                    "props": {
                                        "title": "设置站点配置"
                                    },
                                    "content": [
                                        {
                                            "component": "VDialogCloseBtn",
                                            "props": {
                                                "model": "site_config_dialog"
                                            }
                                        },
                                        {
                                            "component": "VCardText",
                                            "props": {},
                                            "content": [
                                                {
                                                    'component': 'VRow',
                                                    'content': [
                                                        {
                                                            'component': 'VCol',
                                                            'props': {
                                                                'cols': 12,
                                                            },
                                                            'content': [
                                                                {
                                                                    'component': 'VAceEditor',
                                                                    'props': {
                                                                        'modelvalue': 'site_config_str',
                                                                        'lang': 'yaml',
                                                                        'theme': 'monokai',
                                                                        'style': 'height: 30rem',
                                                                    }
                                                                }
                                                            ]
                                                        }
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        },
                        {
                            'component': 'VRow',
                            'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'error',
                                            'variant': 'tonal',
                                            'text': '警告：本插件依赖站点刷流插件，请提前安装对应插件中进行相关配置，'
                                                    '否则可能导致开启站点刷流后，分享率降低或命中H&R种子，严重甚至导致站点封号，请慎重使用'
                                        }
                                    }
                                ]
                            },
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                },
                                'content': [
                                    {
                                        'component': 'VAlert',
                                        'props': {
                                            'type': 'error',
                                            'variant': 'tonal',
                                            'text': '警告：本插件依赖站点数据统计插件，请提前安装对应插件中进行相关配置，'
                                                    '否则可能导致无法获取到分享率等信息，从而影响后续站点流量管理'
                                        }
                                    }
                                ]
                            },
                        ]
                    }
                ]
            }
            ], {
                "enabled": False,
                "onlyonce": False,
                "notify": True,
                "enable_site_config": False,
                "site_config_dialog": False,
                "site_config_str": self.__get_demo_config()
            }

    def get_page(self) -> List[dict]:
        return None

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        [{
            "id": "服务ID",
            "name": "服务名称",
            "trigger": "触发器：cron/interval/date/CronTrigger.from_crontab()",
            "func": self.xxx,
            "kwargs": {} # 定时器参数
        }]
        """

        if not self._traffic_config:
            return []

        if self._traffic_config.enabled and self._traffic_config.cron:
            return [{
                "id": "TrafficAssistantQ",
                "name": "站点流量管理Q改版服务",
                "trigger": CronTrigger.from_crontab(self._traffic_config.cron),
                "func": self.traffic,
                "kwargs": {}
            }]
        return []

    def stop_service(self):
        """
        退出插件
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown()
                    self._event.clear()
                self._scheduler = None
        except Exception as e:
            print(str(e))

    @eventmanager.register(EventType.SiteRefreshed)
    def traffic(self, event: Event = None):
        """
        主要负责管理站点的流量
        通过获取站点统计信息，依据统计信息的成功获取与否执行相应的流量管理操作或记录错误
        """
        if event:
            event_data = event.event_data
            # 所有站点数据刷新完成即 site_id 为 *，才触发后续服务
            if not event_data or event_data.get("site_id") != "*":
                return
            else:
                logger.info("站点数据刷新完成，立即运行一次站点流量管理服务")

        with lock:
            traffic_config = self._traffic_config
            success, reason = self.__validate_config(traffic_config=traffic_config, force=True)
            if not success:
                err_msg = f"配置异常，原因：{reason}"
                logger.error(err_msg)
                self.__send_message(title="站点流量管理", message=err_msg)
                return

            result = self.__get_site_statistics()
            if result.get("success"):
                site_statistics = result.get("data")
                logger.info(f"数据获取成功：{site_statistics}")

                manage_results = self.__auto_traffic(traffic_config=traffic_config, site_statistics=site_statistics)
                aggregated_messages = []  # 初始化一个列表来聚合消息内容

                for site_name, (outcome, stat_time) in manage_results.items():
                    message = f"站点：{site_name} (数据日期：{stat_time})\n{outcome}\n————————————————————"
                    logger.info(message)
                    aggregated_messages.append(message)  # 将每个消息添加到列表中

                # 将所有聚合的消息一次性发送
                if aggregated_messages:
                    full_message = "\n".join(aggregated_messages)
                    self.__send_message(title="站点流量管理", message=full_message)
            else:
                error_msg = result.get("err_msg", "站点流量管理发生异常，请检查日志")
                logger.error(error_msg)
                self.__send_message(title="站点流量管理", message=error_msg)

    def __auto_traffic(self, traffic_config: TrafficConfig, site_statistics: dict):
        """根据提供的站点统计信息自动管理各站点的流量"""
        results = {}
        self._plugin_reload_if_need = False
        brush_plugin_id = traffic_config.brush_plugin
        # 先核对刷流任务是否有新的手动变动，刷新“最终配置”模板记忆
        self.__refresh_brush_template(plugin_id=brush_plugin_id)
        for site_id, site in traffic_config.site_infos.items():
            site_name = site.name
            logger.info(f"正在准备对站点 {site_name} 进行流量管理")
            results[site_name] = self.__manage_site_traffic(traffic_config=traffic_config, site_id=site_id,
                                                            site_name=site_name, site_statistics=site_statistics)
        if self._plugin_reload_if_need:
            self.__reload_plugin(plugin_id=brush_plugin_id)
            self._plugin_reload_if_need = False
            # 固化快照基线，避免插件自身写入被误判为用户改动
            self.__persist_brush_baseline(plugin_id=brush_plugin_id)
        return results

    def __manage_site_traffic(self, traffic_config: TrafficConfig, site_id: int, site_name: str,
                              site_statistics: dict) -> [str, str]:
        """管理单个站点的流量，根据站点的统计数据进行不同的处理"""
        site_stat = site_statistics.get(site_name)
        if not site_stat:
            error_msg = "统计数据不存在，跳过分析"
            logger.warning(error_msg)
            return error_msg, "N/A"

        stat_time = site_stat.get("statistic_time", "N/A")
        logger.info(f"数据日期：{stat_time}")
        if not site_stat.get("success"):
            error_msg = f"{site_stat.get('err_msg')}，跳过分析"
            logger.warning(error_msg)
            return error_msg, stat_time

        site_traffic_config = traffic_config.get_site_config(site_name=site_name)
        process_result = self.__process_site_traffic(traffic_config=site_traffic_config, site_id=site_id,
                                                     site_stat=site_stat)
        return process_result, stat_time

    def __process_site_traffic(self, traffic_config: BaseConfig, site_id: int, site_stat: dict) -> str:
        """根据站点的流量配置和统计信息处理站点流量"""
        ratio_str = site_stat.get("ratio")
        if ratio_str is None:
            error_msg = "分享率：N/A，跳过分析"
            logger.warning(error_msg)
            return error_msg

        try:
            ratio = float(ratio_str)
        except ValueError:
            error_msg = f"分享率无效：{ratio_str}，跳过分析"
            logger.warning(error_msg)
            return error_msg

        if ratio == 0.0:
            error_msg = "分享率：0，跳过分析"
            logger.warning(error_msg)
            return error_msg

        if ratio <= traffic_config.ratio_lower_limit:
            return self.__handle_traffic(traffic_config=traffic_config, site_id=site_id, ratio=ratio, is_low=True)

        if ratio > traffic_config.ratio_upper_limit:
            return self.__handle_traffic(traffic_config=traffic_config, site_id=site_id, ratio=ratio, is_low=False)

        return (f"分享率：{ratio} ({traffic_config.ratio_lower_limit} - {traffic_config.ratio_upper_limit})\n"
                f"- 分享率符合预期，无需调整")

    def __handle_traffic(self, traffic_config: BaseConfig, site_id: int, ratio: float, is_low: bool) -> str:
        """处理流量情况，可以适用于高低流量情况"""
        threshold_type = "≤" if is_low else ">"
        threshold_value = traffic_config.ratio_lower_limit if is_low else traffic_config.ratio_upper_limit
        traffic_summary = f"分享率：{ratio} ({threshold_type}{threshold_value})"
        actions = []

        any_action_taken = False  # 初始化操作跟踪标志

        # 处理搜索和订阅站点
        search_condition = (
            traffic_config.remove_from_search_if_below if is_low else traffic_config.add_to_search_if_above)
        if search_condition:
            success, action_msg = self.__update_search_sites(site_id=site_id, remove=is_low)
            actions.append(f"- {action_msg}")
            if success:
                any_action_taken = True  # 更新操作执行标志

        subscription_condition = (
            traffic_config.remove_from_subscription_if_below if is_low else traffic_config.add_to_subscription_if_above)
        if subscription_condition:
            success, action_msg = self.__update_subscription_sites(site_id=site_id, remove=is_low)
            actions.append(f"- {action_msg}")
            if success:
                any_action_taken = True  # 更新操作执行标志

        # 处理刷流站点
        brush_condition = (
            traffic_config.enable_auto_brush_if_below if is_low else traffic_config.disable_auto_brush_if_above)
        if brush_condition:
            success, action_msg = self.__update_brush_sites(site_id=site_id, enable=is_low,
                                                            plugin_id=self._traffic_config.brush_plugin)
            actions.append(f"- {action_msg}")
            if success:
                any_action_taken = True  # 更新操作执行标志
                self._plugin_reload_if_need = True  # 标记需要进行插件的热加载

        if not any_action_taken:
            actions.clear()
            actions.append("- 配置项符合预期，无需调整")

        return "\n".join([traffic_summary] + actions)

    @staticmethod
    def __update_site_list(site_id: int, site_list: list, remove: bool, description: str) -> [bool, str]:
        """通用方法来添加或移除站点"""
        action_performed = False
        action_msg = f"{description}站点：无需调整"
        if not remove:
            if site_id not in site_list:
                site_list.append(site_id)
                action_performed = True
                action_msg = f"{description}站点：已添加"
        else:
            if site_id in site_list:
                site_list.remove(site_id)
                action_performed = True
                action_msg = f"{description}站点：已移除"
        return action_performed, action_msg

    def __update_search_sites(self, site_id: int, remove: bool) -> [bool, str]:
        """更新搜索站点列表，根据需要添加或移除站点"""
        indexer_sites = self.systemconfig.get(key=SystemConfigKey.IndexerSites) or []
        action_performed, action_msg = self.__update_site_list(site_id=site_id, site_list=indexer_sites, remove=remove,
                                                               description="搜索")
        logger.info(action_msg)
        if action_performed:
            self.systemconfig.set(key=SystemConfigKey.IndexerSites, value=indexer_sites)
        return action_performed, action_msg

    def __update_subscription_sites(self, site_id: int, remove: bool) -> [bool, str]:
        """更新订阅站点列表，根据需要添加或移除站点"""
        rss_sites = self.systemconfig.get(key=SystemConfigKey.RssSites) or []
        action_performed, action_msg = self.__update_site_list(site_id=site_id, site_list=rss_sites, remove=remove,
                                                               description="订阅")
        logger.info(action_msg)
        if action_performed:
            self.systemconfig.set(key=SystemConfigKey.RssSites, value=rss_sites)
        return action_performed, action_msg

    def __update_brush_sites(self, site_id: int, enable: bool, plugin_id: str) -> [bool, str]:
        """按刷流插件配置契约更新目标站点的自动刷流状态。
        低于下限(enable=True)：无任务则按最终配置新建并启动，有任务则同步为最终配置后启动；
        高于上限(enable=False)：只停用已有任务，不删除任务、不改动规则。"""
        plugin_config = self.get_config(plugin_id=plugin_id)
        if not plugin_config:
            action_msg = "刷流站点：获取插件配置失败"
            logger.warning(action_msg)
            return False, action_msg

        tasks = plugin_config.get("tasks")
        if isinstance(tasks, list):
            config_needs_update, actions = self.__update_brush_tasks(
                plugin_config=plugin_config,
                site_id=site_id,
                enable=enable,
            )
            if config_needs_update:
                self.update_config(config=plugin_config, plugin_id=plugin_id)
                logger.info("已写入刷流插件配置")
            return config_needs_update, "，".join(actions)

        # 旧版 brushsites 列表模型：仅维护站点列表与全局开关，不做自动创建
        return self.__update_brush_legacy(plugin_config=plugin_config, site_id=site_id, enable=enable)

    def __update_brush_legacy(self, plugin_config: dict, site_id: int, enable: bool) -> Tuple[bool, str]:
        """兼容旧版 brushsites 列表模型：仅维护站点列表与全局开关，不做自动创建"""
        actions = []
        config_needs_update = False
        plugin_enabled = plugin_config.get("enabled", False)

        if enable and not plugin_enabled:
            plugin_config["enabled"] = True
            action_msg = "刷流插件：已启用"
            logger.info(action_msg)
            actions.append(action_msg)
            config_needs_update = True

        brush_sites = plugin_config.get("brushsites", [])
        action_performed, action_msg = self.__update_site_list(site_id=site_id, site_list=brush_sites,
                                                               remove=not enable,
                                                               description="刷流")
        logger.info(action_msg)
        actions.append(action_msg)
        if action_performed:
            plugin_config["brushsites"] = brush_sites
            config_needs_update = True

        if config_needs_update:
            self.update_config(config=plugin_config, plugin_id=plugin_id)

        return config_needs_update, "，".join(actions)

    def __update_brush_tasks(self, plugin_config: dict, site_id: int, enable: bool) -> Tuple[bool, List[str]]:
        """tasks 模型：按最终配置新建/同步任务并启停目标站点的全部刷流任务"""
        actions = []
        config_needs_update = False
        tasks = plugin_config.get("tasks") or []
        matching_tasks = [
            task for task in tasks
            if isinstance(task, dict) and task.get("site_id") == site_id
        ]

        if enable:
            template = self.__get_brush_template()
            if not template:
                if matching_tasks:
                    # 理论上有任务即有模板，此处兜底：仅启动不覆盖，避免误伤
                    actions.append(f"刷流任务：站点 {site_id} 已有任务但无最终配置模板，仅启动不覆盖配置")
                    for task in matching_tasks:
                        if not bool(task.get("enabled", True)):
                            task["enabled"] = True
                            config_needs_update = True
                else:
                    return False, "刷流任务：无可用最终配置模板，无法自动创建（请先在站点刷流中建立一个任务作为最终配置基线）"
            else:
                if matching_tasks:
                    task = matching_tasks[0]
                    changed = self.__apply_brush_template(task=task, template=template)
                    if not bool(task.get("enabled", True)):
                        task["enabled"] = True
                        changed = True
                    action_msg = ("刷流任务：已按最终配置同步并启用"
                                  if changed else "刷流任务：已启用（配置与最终配置一致）")
                    actions.append(f"{action_msg}「{task.get('name') or site_id}」")
                    config_needs_update = config_needs_update or changed
                else:
                    task = self.__build_brush_task(template=template, site_id=site_id)
                    tasks.append(task)
                    actions.append(f"刷流任务：已按最终配置新建并启用「{task.get('name')}」")
                    config_needs_update = True

            if not plugin_config.get("enabled", False):
                plugin_config["enabled"] = True
                actions.append("刷流插件：已启用")
                config_needs_update = True
        else:
            for task in matching_tasks:
                if bool(task.get("enabled", True)):
                    task["enabled"] = False
                    config_needs_update = True
                    actions.append(f"刷流任务：已暂停「{task.get('name') or site_id}」")
            if not actions:
                actions.append("刷流任务：无需调整")

        for action_msg in actions:
            logger.info(action_msg)
        return config_needs_update, actions

    def __get_brush_template(self) -> dict:
        """读取持久化的最终配置模板数据（含 config 规则与来源信息）"""
        template = self.get_data(DATA_KEY_BRUSH_TEMPLATE) or {}
        if not isinstance(template, dict) or not template.get("config"):
            return {}
        return template

    def __refresh_brush_template(self, plugin_id: str):
        """核对刷流任务相对上次基线是否有变动，将最后一个被改动/新增的任务固化为最终配置模板。
        插件自身写入的任务已通过基线快照隔离，不会被误判为用户改动；任务被全部删除时保留模板记忆。"""
        if not plugin_id:
            return
        try:
            plugin_config = self.get_config(plugin_id=plugin_id)
        except Exception as e:
            logger.warning(f"读取刷流插件配置失败，跳过最终配置模板刷新：{e}")
            return
        if not plugin_config:
            return

        tasks = plugin_config.get("tasks")
        if not isinstance(tasks, list):
            return

        old_snapshots = self.get_data(DATA_KEY_BRUSH_SNAPSHOTS) or {}
        if not isinstance(old_snapshots, dict):
            old_snapshots = {}
        old_template = self.get_data(DATA_KEY_BRUSH_TEMPLATE) or {}
        if not isinstance(old_template, dict):
            old_template = {}

        fingerprints = {}
        changed_task = None
        for task in tasks:
            if not isinstance(task, dict) or not task.get("id"):
                continue
            task_id = task["id"]
            fingerprints[task_id] = self.__brush_task_fingerprint(task)
            # 顺序遍历后保留的即为“最后”一个发生变动的任务
            if old_snapshots.get(task_id) != fingerprints[task_id]:
                changed_task = task

        removed_count = len(set(old_snapshots.keys()) - set(fingerprints.keys()))
        template_changed = False
        if changed_task is not None:
            old_template = self.__make_brush_template(changed_task)
            template_changed = True
            logger.info(f"检测到刷流任务「{changed_task.get('name') or changed_task.get('id')}」"
                        f"配置变动，已更新最终配置模板")

        if removed_count:
            logger.info(f"检测到 {removed_count} 个刷流任务被删除，最终配置模板记忆已保留")

        snapshots_changed = (set(old_snapshots.keys()) != set(fingerprints.keys())) or any(
            old_snapshots.get(key) != fp for key, fp in fingerprints.items())
        if template_changed or snapshots_changed:
            if template_changed:
                self.save_data(DATA_KEY_BRUSH_TEMPLATE, old_template)
                logger.info("最终配置模板已持久化记忆")
            self.save_data(DATA_KEY_BRUSH_SNAPSHOTS, fingerprints)

    def __persist_brush_baseline(self, plugin_id: str):
        """把刷流插件当前全部任务规则固化为指纹快照，避免插件自身写入被误判为用户改动"""
        if not plugin_id:
            return
        plugin_config = self.get_config(plugin_id=plugin_id) or {}
        tasks = plugin_config.get("tasks")
        if not isinstance(tasks, list):
            return
        fingerprints = {}
        for task in tasks:
            if isinstance(task, dict) and task.get("id"):
                fingerprints[task["id"]] = self.__brush_task_fingerprint(task)
        self.save_data(DATA_KEY_BRUSH_SNAPSHOTS, fingerprints)
        logger.info("刷流任务快照基线已更新")

    @staticmethod
    def __brush_task_fingerprint(task: dict) -> str:
        """计算刷流任务的规则指纹：忽略身份字段与启停状态，只对规则字段取哈希"""
        rule = {key: value for key, value in task.items() if key not in BRUSH_TASK_IDENTITY_KEYS}
        dump = json.dumps(rule, ensure_ascii=False, sort_keys=True, default=str)
        return hashlib.sha256(dump.encode("utf-8")).hexdigest()

    def __make_brush_template(self, task: dict) -> dict:
        """把指定任务的全量规则固化为最终配置模板（剔除身份与启停字段，另存来源信息）"""
        rule = {key: copy.deepcopy(value) for key, value in task.items() if key not in BRUSH_TASK_IDENTITY_KEYS}
        return {
            "config": rule,
            "source_task_id": task.get("id"),
            "source_site_id": task.get("site_id"),
            "source_task_name": task.get("name"),
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

    def __apply_brush_template(self, task: dict, template: dict) -> bool:
        """把最终配置规则同步到目标任务，不覆盖身份字段与启停状态；返回是否存在字段变化"""
        rule = (template or {}).get("config") or {}
        changed = False
        for key, value in rule.items():
            if key in BRUSH_TASK_IDENTITY_KEYS:
                continue
            if key not in task or task.get(key) != value:
                task[key] = copy.deepcopy(value)
                changed = True
        return changed

    def __build_brush_task(self, template: dict, site_id: int) -> dict:
        """按最终配置规则生成指定站点的新刷流任务（任务ID/站点/名称单独赋值并默认启用）"""
        rule = copy.deepcopy((template or {}).get("config") or {})
        rule["id"] = uuid.uuid4().hex
        rule["site_id"] = site_id
        rule["name"] = f"{self.__get_site_name_by_id(site_id=site_id) or site_id}刷流"
        rule["enabled"] = True
        return rule

    def __get_site_name_by_id(self, site_id: int) -> str:
        """根据站点 ID 获取站点名称"""
        try:
            site_info = self.siteoper.get(site_id)
            if site_info:
                return site_info.name
        except Exception as e:
            logger.warning(f"获取站点 {site_id} 名称失败：{e}")
        return ""

    def __reload_plugin(self, plugin_id: str):
        logger.info(f"准备热加载插件: {plugin_id}")

        # 加载插件到内存
        try:
            self.pluginmanager.reload_plugin(plugin_id)
            logger.info(f"成功热加载插件: {plugin_id} 到内存")
        except Exception as e:
            logger.error(f"失败热加载插件: {plugin_id} 到内存. 错误信息: {e}")
            return

        # 注册插件服务
        try:
            update_plugin_job(plugin_id)
            logger.info(f"成功热加载插件到插件服务: {plugin_id}")
        except Exception as e:
            logger.error(f"失败热加载插件到插件服务: {plugin_id}. 错误信息: {e}")
            return

        logger.info(f"已完成插件热加载: {plugin_id}")

    def __get_site_statistics(self) -> dict:
        """获取站点统计数据"""

        def is_data_valid(data):
            """检查数据是否有效"""
            return data is not None and "ratio" in data and not data.get("err_msg")

        traffic_config = self._traffic_config
        site_infos = traffic_config.site_infos
        current_day = datetime.now(tz=pytz.timezone(settings.TZ)).date()
        previous_day = current_day - timedelta(days=1)
        result = {"success": True, "data": {}}

        # 尝试获取当天和前一天的数据
        current_data = {data.name: data for data in
                        (self.siteoper.get_userdata_by_date(date=str(current_day)) or [])}
        previous_day_data = {data.name: data for data in
                             (self.siteoper.get_userdata_by_date(date=str(previous_day)) or [])}

        if not current_data and not previous_day_data:
            err_msg = f"{current_day} 和 {previous_day}，均没有获取到有效的数据，请检查"
            logger.warning(err_msg)
            result["success"] = False
            result["err_msg"] = err_msg
            return result

        # 检查每个站点的数据是否有效
        all_sites_failed = True
        for site_id, site in site_infos.items():
            site_name = site.name
            site_current_data = current_data.get(site_name)
            site_current_data = site_current_data.to_dict() if site_current_data else {}
            site_previous_data = previous_day_data.get(site_name)
            site_previous_data = site_previous_data.to_dict() if site_previous_data else {}

            if is_data_valid(site_current_data):
                result["data"][site_name] = {**site_current_data, "success": True,
                                             "statistic_time": str(current_day)}
                all_sites_failed = False
            else:
                if is_data_valid(site_previous_data):
                    result["data"][site_name] = {**site_previous_data, "success": True,
                                                 "statistic_time": str(previous_day)}
                    logger.info(f"站点 {site_name} 使用了 {previous_day} 的数据")
                    all_sites_failed = False
                else:
                    err_msg = site_previous_data.get("err_msg", "无有效数据")
                    result["data"][site_name] = {"err_msg": err_msg, "success": False,
                                                 "statistic_time": str(previous_day)}
                    logger.warning(f"{site_name} 前一天的数据也无效，错误信息：{err_msg}")

        # 如果所有站点的数据都无效，则标记全局失败
        if all_sites_failed:
            err_msg = f"{current_day} 和 {previous_day}，所有站点的数据获取均失败，无法继续站点流量管理服务"
            logger.warning(err_msg)
            result["success"] = False
            result["err_msg"] = err_msg

        return result

    def __send_message(self, title: str, message: str):
        """发送消息"""
        if self._traffic_config.notify:
            self.post_message(mtype=NotificationType.Plugin, title=f"【{title}】", text=message)

    def __validate_config(self, traffic_config: TrafficConfig, force: bool = False, check_plugin_installed: bool = True) \
            -> (bool, str):
        """
        验证配置是否有效
        """
        if not traffic_config.enabled and not force:
            return True, "插件未启用，无需进行验证"

        # 检查站点列表是否为空
        if not traffic_config.sites:
            return False, "站点列表不能为空"

        if self.__has_brush_action_enabled(traffic_config=traffic_config):
            if not traffic_config.brush_plugin:
                return False, "已启用停止/开启刷流，站点刷流插件不能为空"
            if check_plugin_installed:
                result, message = self.__check_required_plugin_installed(plugin_id=traffic_config.brush_plugin)
                if not result:
                    return False, message

        result, message = self.__validate_ratio_config(traffic_config=traffic_config)
        if not result:
            return result, message

        if traffic_config.enable_site_config:
            for site_name, site_config in traffic_config.site_configs.items():
                result, message = self.__validate_ratio_config(traffic_config=site_config)
                if not result:
                    return False, f"站点 {site_name} {message}"

        return True, "所有配置项都有效"

    @staticmethod
    def __validate_ratio_config(traffic_config: BaseConfig) -> Tuple[bool, str]:
        """
        校验分享率阈值配置，确保后续站点动作不会基于无效阈值执行。
        """
        if traffic_config.ratio_lower_limit <= 0 or traffic_config.ratio_upper_limit <= 0:
            return False, "分享率必须大于0"

        if traffic_config.ratio_upper_limit < traffic_config.ratio_lower_limit:
            return False, "分享率上限必须大于等于下限"

        return True, "分享率配置有效"

    @staticmethod
    def __has_brush_action_enabled(traffic_config: TrafficConfig) -> bool:
        """
        判断全局或站点独立配置中是否启用了刷流开关动作。
        """
        if traffic_config.enable_auto_brush_if_below or traffic_config.disable_auto_brush_if_above:
            return True
        if not traffic_config.enable_site_config:
            return False
        return any(
            site_config.enable_auto_brush_if_below or site_config.disable_auto_brush_if_above
            for site_config in traffic_config.site_configs.values()
        )

    def __validate_and_fix_config(self, config: dict = None) -> [bool, str]:
        """
        检查并修正配置值
        """
        if not config:
            return False, ""

        try:
            # 使用字典推导来提取所有字段，并用config中的值覆盖默认值
            traffic_config = TrafficConfig(
                **{field.name: config.get(field.name, getattr(TrafficConfig, field.name, None))
                   for field in fields(TrafficConfig)})

            result, reason = self.__validate_config(traffic_config=traffic_config, check_plugin_installed=False)
            if result:
                # 过滤掉已删除的站点并保存
                if traffic_config.sites:
                    site_id_to_public_status = {site.get("id"): site.get("public") for site in
                                                self.siteshelper.get_indexers()}
                    traffic_config.sites = [
                        site_id for site_id in traffic_config.sites
                        if site_id in site_id_to_public_status and not site_id_to_public_status[site_id]
                    ]

                    site_infos = {}
                    for site_id in traffic_config.sites:
                        site_info = self.siteoper.get(site_id)
                        if site_info:
                            site_infos[site_id] = site_info
                    traffic_config.site_infos = site_infos

                self._traffic_config = traffic_config
                return True, ""
            else:
                self._traffic_config = None
                return result, reason
        except YAMLError as e:
            self._traffic_config = None
            logger.error(e)
            return False, str(e)
        except Exception as e:
            self._traffic_config = None
            logger.error(e)
            return False, str(e)

    def __update_config_if_error(self, config: dict = None, error: str = None):
        """异常时停用插件并保存配置"""
        if config:
            if config.get("enabled", False) or config.get("onlyonce", False):
                config["enabled"] = False
                config["onlyonce"] = False
                self.__log_and_notify_error(
                    f"配置异常，已停用站点流量管理，原因：{error}" if error else "配置异常，已停用站点流量管理，请检查")
            self.update_config(config)

    def __update_config(self):
        """保存配置"""
        if not self._traffic_config.site_config_str:
            self._traffic_config.site_config_str = self.__get_demo_config()
        config_mapping = asdict(self._traffic_config)
        del config_mapping["site_infos"]
        del config_mapping["site_configs"]
        del config_mapping["statistic_plugin"]
        self.update_config(config_mapping)

    def __log_and_notify_error(self, message):
        """
        记录错误日志并发送系统通知
        """
        logger.error(message)
        self.systemmessage.put(message, title="站点流量管理")

    def __get_site_options(self):
        """获取当前可选的站点"""
        site_options = [{"title": site.get("name"), "value": site.get("id")}
                        for site in self.siteshelper.get_indexers()]
        return site_options

    def __get_plugin_options(self) -> List[dict]:
        """获取插件选项列表"""
        # 获取运行的插件选项
        running_plugins = self.pluginmanager.get_running_plugin_ids()

        # 需要检查的插件名称
        filter_plugins = {"BrushFlow", "BrushFlowLowFreq"}

        # 获取本地插件列表
        local_plugins = self.pluginmanager.get_local_plugins()

        # 初始化插件选项列表
        plugin_options = []

        # 从本地插件中筛选出符合条件的插件
        for local_plugin in local_plugins:
            if local_plugin.id in running_plugins and local_plugin.id in filter_plugins:
                plugin_options.append({
                    "title": f"{local_plugin.plugin_name} v{local_plugin.plugin_version}",
                    "value": local_plugin.id,
                    "name": local_plugin.plugin_name
                })

        # 重新编号，保证显示为 1. 2. 等
        for index, option in enumerate(plugin_options, start=1):
            option["title"] = f"{index}. {option['title']}"

        return plugin_options

    def __check_required_plugin_installed(self, plugin_id: str) -> (bool, str):
        """
        检查指定的依赖插件是否已安装
        """
        plugin_names = {
            "SiteStatistic": "站点数据统计",
            "BrushFlow": "站点刷流",
            "BrushFlowLowFreq": "站点刷流（低频版）"
        }

        plugin_name = plugin_names.get(plugin_id, "未知插件")

        # 获取本地插件列表
        local_plugins = self.pluginmanager.get_local_plugins()

        # 检查指定的插件是否已启用
        plugin = next((p for p in local_plugins if p.id == plugin_id and p.installed), None)
        if not plugin:
            return False, f"{plugin_name}未安装"

        return True, f"{plugin_name}已安装"

    @staticmethod
    def __get_demo_config() -> str:
        """
        获取站点独立配置示例。
        """
        return """####### 配置说明 BEGIN #######
# 1. 此配置用于为不同站点覆盖流量管理配置，站点名称必须与站点列表中的名称一致。
# 2. 配置项通过数组形式组织，每个站点配置以“-”开头。
# 3. 未填写的字段会继承上方全局配置；如需关闭某个全局开启的动作，可显式配置为 false。
####### 配置说明 END #######

- # 站点名称，用于标识适用于哪个站点
  site_name: '站点1'
  # 分享率下限，低于或等于该值时执行低分享率动作
  ratio_lower_limit: 1.0
  # 分享率上限，高于该值时执行高分享率动作
  ratio_upper_limit: 5.0
  # 低于下限时是否从订阅站点中移除
  remove_from_subscription_if_below: true
  # 低于下限时是否从搜索站点中移除
  remove_from_search_if_below: true
  # 低于下限时是否开启自动刷流
  enable_auto_brush_if_below: true
  # 高于上限时是否增加到订阅站点
  add_to_subscription_if_above: true
  # 高于上限时是否增加到搜索站点
  add_to_search_if_above: true
  # 高于上限时是否关闭自动刷流
  disable_auto_brush_if_above: true

- # 未填写的字段将继承全局配置
  site_name: '站点2'
  ratio_lower_limit: 2.0
  ratio_upper_limit: 7.5"""
