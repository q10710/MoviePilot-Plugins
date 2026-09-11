from pathlib import Path
from threading import Event

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import datetime
import pytz

from enum import Enum
from typing import Any, Dict, List, Optional, TypedDict

from app.chain.tmdb import TmdbChain
from app.schemas.types import MediaSource, MediaType
from app import schemas
from app.chain.download import DownloadChain
from app.chain.media import MediaChain
from app.chain.subscribe import SubscribeChain
from app.db.subscribe_oper import SubscribeOper
from app.core.config import settings
from app.log import logger
from app.plugins import _PluginBase
from app.chain.mediaserver import MediaServerChain
from app.helper.mediaserver import MediaServerHelper


class HistoryStatus(Enum):
    UNKNOW = "未知状态"
    ALL_EXIST = "全部存在"
    ADDED_RSS = "已加订阅"
    NO_EXIST = "存在缺失"
    FAILED = "获取失败"


class HistoryDataType(Enum):
    ALL_EXIST = "全部存在"
    ADDED_RSS = "已加订阅"
    NO_EXIST = "存在缺失"
    FAILED = "失败记录"
    ALL = "所有记录"
    LATEST = "最新6条记录"
    NOT_ALL_NO_EXIST = "已有季缺失"


class NoExistAction(Enum):
    ONLY_HISTORY = "仅检查记录"
    ADD_SUBSCRIBE = "添加到订阅"
    SET_ALL_EXIST = "标记为存在"


class Icons(Enum):
    STATISTICS = "icon_statistics"
    WARNING = "icon_warning"
    BUG_REMOVE = "icon_bug_remove"
    GLASSES = "icon_3d_glasses"
    ADD_SCHEDULE = "icon_add_schedule"
    TARGET = "icon_target"


# 媒体服务器条目中代表「电视剧」的类型取值：英文来自 Emby/Jellyfin/Plex，中文来自 MoviePilot 存储
TV_ITEM_TYPES = {"Series", "series", "show", "Show", "电视剧", "剧集"}
# TMDB 身份来源标识：新版 MediaServerItem 用 media_source + media_id 描述媒体身份
TMDB_MEDIA_SOURCE = "themoviedb"


class EpisodeNoExistInfo(TypedDict, total=False):
    # 季
    season: Optional[int]

    # 失剧集列表
    episode_no_exist: Optional[List[int]]

    # 总集数
    episode_total: int


class TvNoExistInfo(TypedDict):
    """
    电视剧媒体信息
    """

    title: str
    year: str
    path: str

    # TMDB ID
    tmdbid: int

    # 海报地址
    poster_path: str
    # 评分
    vote_average: float | str
    # 最后发行日期
    last_air_date: str

    season_episode_no_exist_info: Dict[str, EpisodeNoExistInfo]


default_poster_path = "/assets/no-image-CweBJ8Ee.jpeg"


def create_tv_no_exist_info(
    title="未知",
    year="未知",
    path="未知",
    last_air_date="未知",
    tmdbid=0,
    vote_average=0.0,
    poster_path=default_poster_path,
    season_episode_no_exist_info: Optional[
        Dict[str, EpisodeNoExistInfo]
    ] = None,
) -> TvNoExistInfo:
    logger.debug(
        f"season_episode_no_exist_info: {season_episode_no_exist_info}"
    )
    return TvNoExistInfo(
        title=title,
        year=year,
        path=path,
        tmdbid=tmdbid,
        poster_path=poster_path,
        vote_average=vote_average,
        last_air_date=last_air_date,
        season_episode_no_exist_info=season_episode_no_exist_info or {},
    )


class HistoryDetail(TypedDict):
    exist_status: Optional[str]
    tv_no_exist_info: Optional[TvNoExistInfo]
    last_update: Optional[str]
    last_update_full: Optional[str]


class ExtendedHistoryDetail(HistoryDetail):
    unique: Optional[str]


class History(TypedDict):
    item_unique_flags: List[str]
    details: Dict[str, HistoryDetail]


class EpisodeNoExistQ(_PluginBase):
    # 插件名称
    plugin_name = "缺失集数订阅Q改版"
    # 插件描述
    plugin_desc = ("订阅媒体库缺失集数的电视剧；适配新版 MoviePilot 的 MediaServerItem（已无 tmdbid 字段），"
                   "改用 media_source + media_id 识别 TMDB 身份，并兼容中文媒体类型。基于官方缺失集数订阅 v2.0.5 改造。")
    # 插件图标
    plugin_icon = "https://raw.githubusercontent.com/q10710/MoviePilot-Plugins/main/icons/episodenoexistq.png"
    # 插件版本
    plugin_version = "2.1.1"
    # 插件作者
    plugin_author = "Q"
    # 作者主页
    author_url = "https://github.com/q10710"
    # 插件配置项ID前缀
    plugin_config_prefix = "episodenoexistq_"
    # 加载顺序
    plugin_order = 6
    # 可使用的用户级别
    auth_level = 1

    # 退出事件
    _event = Event()

    # 私有属性
    _subChain: SubscribeChain
    _subOper: SubscribeOper
    _mediachain = MediaChain
    _tmdbChain: TmdbChain
    _dlChain: DownloadChain

    _msChain: MediaServerChain
    _msHelper: MediaServerHelper

    _plugin_id = "EpisodeNoExist"
    _scheduler = None

    _enabled: bool = False
    _cron: str = ""
    _onlyonce: bool = False
    _clear: bool = False
    _clearflag: bool = False

    _only_season_exist: bool = True

    _history_type: str = HistoryDataType.LATEST.value
    _no_exist_action: str = NoExistAction.ONLY_HISTORY.value
    _save_path_replaces: List[str] = []
    _whitelist_librarys: List[str] = []
    _whitelist_media_servers: List[str] = []

    def init_plugin(self, config: dict[str, Any] | None = None):
        self._subChain = SubscribeChain()
        self._subOper = SubscribeOper()
        self._mediaChain = MediaChain()
        self._tmdbChain = TmdbChain()

        self._msChain = MediaServerChain()
        self._msHelper = MediaServerHelper()

        if config:
            self._enabled = config.get("enabled", False)
            self._onlyonce = config.get("onlyonce", False)
            self._cron = (
                config.get("cron", "").strip()
                if config.get("cron", "").strip()
                else ""
            )

            self._clear = config.get("clear", False)

            self._only_season_exist = config.get("only_season_exist", True)

            self._no_exist_action = config.get(
                "no_exist_action", NoExistAction.ONLY_HISTORY.value
            )

            self._history_type = config.get(
                "history_type", HistoryDataType.LATEST.value
            )
            _save_path_replaces = config.get("save_path_replaces", "")
            if _save_path_replaces and isinstance(_save_path_replaces, str):
                self._save_path_replaces = _save_path_replaces.split("\n")
            else:
                self._save_path_replaces = []

            _whitelist_librarys = config.get("whitelist_librarys", "")
            if _whitelist_librarys and isinstance(_whitelist_librarys, str):
                self._whitelist_librarys = _whitelist_librarys.split(",")
            else:
                self._whitelist_librarys = []

            _whitelist_media_servers = config.get(
                "whitelist_media_servers", ""
            )
            if _whitelist_media_servers and isinstance(
                _whitelist_media_servers, str
            ):
                self._whitelist_media_servers = _whitelist_media_servers.split(
                    ","
                )
            else:
                self._whitelist_media_servers = []

        # 停止现有任务
        self.stop_service()

        # 启动服务
        if self._enabled or self._onlyonce:
            if self._onlyonce:
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                logger.info(f"{self.plugin_name}服务启动, 立即运行一次")
                self._scheduler.add_job(
                    func=self.__refresh,
                    trigger="date",
                    run_date=datetime.datetime.now(
                        tz=pytz.timezone(settings.TZ)
                    )
                    + datetime.timedelta(seconds=3),
                )

                if self._scheduler.get_jobs():
                    # 启动服务
                    self._scheduler.print_jobs()
                    self._scheduler.start()

            if self._onlyonce or self._clear:
                # 记录缓存清理标志
                self._clearflag = self._clear

                # 关闭清理缓存
                self._clear = False
                # 关闭一次性开关
                self._onlyonce = False

                # 保存配置
                self.__update_config()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        """
        获取插件API
        [{
            "path": "/xx",
            "endpoint": self.xxx,
            "methods": ["GET", "POST"],
            "summary": "API说明"
        }]
        """
        return [
            {
                "path": "/delete_history",
                "endpoint": self.delete_history,
                "methods": ["GET"],
                "summary": f"删除 {self.plugin_name} 检查记录",
            },
            {
                "path": "/set_all_exist_history",
                "endpoint": self.set_all_exist_history,
                "methods": ["GET"],
                "summary": f"标记 {self.plugin_name} 存在记录",
            },
            {
                "path": "/add_subscribe_history",
                "endpoint": self.add_subscribe_history,
                "methods": ["GET"],
                "summary": f"订阅 {self.plugin_name} 缺失记录",
            },
        ]

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
        if self._enabled and self._cron:
            return [
                {
                    "id": "EpisodeNoExist",
                    "name": f"{self.plugin_name}",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.__refresh,
                    "kwargs": {},
                }
            ]
        elif self._enabled:
            return [
                {
                    "id": "EpisodeNoExist",
                    "name": f"{self.plugin_name}",
                    "trigger": CronTrigger.from_crontab("0 8 * * *"),
                    "func": self.__refresh,
                    "kwargs": {},
                }
            ]
        return []

    def __refresh(self):
        self.__get_mediaserver_tv_info()

    def __get_mediaservers(self):
        """
        获取媒体服务器
        """

        mediaservers = self._msHelper.get_services()
        logger.info(f"获取到媒体服务器: {mediaservers}")
        if not mediaservers:
            logger.warn("未获取到媒体服务器")
            return

        return mediaservers

    def __get_mediaserver_tv_info(self) -> None:
        """
        获取媒体库电视剧数据
        """
        logger.info("开始获取媒体库电视剧数据 ...")
        if self._clearflag:
            logger.info("清理检查记录")
            self.save_data("history", "")
            self._clearflag = False
            _history = None
        else:
            _history = self.get_data("history")

        history: Dict[str, Any] = (
            _history if _history else {"item_unique_flags": [], "details": {}}
        )

        # 添加检查记录
        def __append_history(
            item_unique_flag: str,
            exist_status: HistoryStatus,
            tv_no_exist_info: TvNoExistInfo | Dict[str, Any] | None = None,
        ):

            current_time = datetime.datetime.now(tz=pytz.timezone(settings.TZ))

            history["item_unique_flags"].append(item_unique_flag)

            history["details"][item_unique_flag] = {
                "exist_status": exist_status.value,
                "tv_no_exist_info": (
                    tv_no_exist_info if tv_no_exist_info else None
                ),
                "last_update": current_time.strftime("%m-%d %H:%M"),
                "last_update_full": current_time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            logger.info(
                f"添加检查记录: {item_unique_flag}: {history['details'][item_unique_flag]}"
            )

            self.save_data("history", history)

        mediaservers = self.__get_mediaservers()
        if not mediaservers:
            return

        # # 白名单, 只获取黑名单外指定的媒体库
        logger.info(
            f"媒体服务器名称白名单: {self._whitelist_media_servers if self._whitelist_media_servers else '全部'}"
        )
        logger.info(f"媒体库白名单: {self._whitelist_librarys}")

        item_unique_flags = history.get("item_unique_flags", [])
        logger.debug(f"item_unique_flags: {item_unique_flags}")

        # 遍历媒体服务器
        for mediaserver in mediaservers:
            logger.debug(f"mediaserver: {mediaserver}")
            if not mediaserver:
                continue
            if (
                self._whitelist_media_servers
                and mediaserver not in self._whitelist_media_servers
            ):
                logger.info(
                    f"【{mediaserver}】不在媒体服务器名称白名单内, 跳过"
                )
                continue
            logger.info(f"开始获取媒体库 {mediaserver} 的数据 ...")

            __item_count = 0
            librarys = self._msChain.librarys(mediaserver)
            for library in librarys:
                logger.debug(f"媒体库名：{library.name}")
                if library.name not in self._whitelist_librarys:
                    continue
                logger.info(
                    f"正在获取 {mediaserver} 媒体库 {library.name} ..."
                )
                logger.debug(f"library.id: {library.id}")

                if not library.id:
                    logger.debug("未获取到Library ID, 跳过获取缺失集数")
                    continue

                library_items = self._msChain.items(mediaserver, library.id)
                if not library_items:
                    logger.debug("未获取到媒体库items信息, 跳过获取缺失集数")
                    continue

                for item in library_items:
                    # if __item_count >= 30:
                    #     break
                    __item_count += 1

                    if not item:
                        logger.debug("未获取到Item媒体信息, 跳过获取缺失集数")
                        continue

                    if not item.item_id:
                        logger.debug("未获取到Item ID, 跳过获取缺失集数")
                        continue

                    item_title = (
                        item.title
                        or item.original_title
                        or f"ItemID: {item.item_id}"
                    )

                    item_unique_flag = f"{mediaserver}_{item.library}_{item.item_id}_{item_title}"

                    if item_unique_flag in item_unique_flags:
                        logger.info(f"【{item_title}】已处理过, 跳过")
                        continue

                    logger.info(f"正在获取 {item_title} ...")

                    seasoninfo = {}

                    # 类型：兼容媒体服务器返回的英文类型（Series/show）与 MoviePilot 存储的中文类型（电视剧/电影）
                    item_type = (
                        MediaType.TV.value
                        if str(item.item_type or "") in TV_ITEM_TYPES
                        else MediaType.MOVIE.value
                    )
                    if item_type == MediaType.MOVIE.value:
                        logger.warn(
                            f"【{item_title}】为{MediaType.MOVIE.value}, 跳过"
                        )
                        continue
                    # TMDB 身份：新版 MediaServerItem 已移除 tmdbid 字段，改用 media_source + media_id
                    item_tmdbid = self.__resolve_tmdbid(item)
                    if item_type == MediaType.TV.value and item_tmdbid:
                        # 查询剧集信息
                        espisodes_info = (
                            self._msChain.episodes(mediaserver, item.item_id)
                            or []
                        )
                        logger.debug(
                            f"获取到媒体库【{item_title}】季集信息:{espisodes_info}"
                        )
                        for episode_info in espisodes_info:
                            seasoninfo[episode_info.season] = (
                                episode_info.episodes
                            )

                    # 插入数据（pydantic v2 的 dict() 已废弃，优先用 model_dump）
                    item_dict = self.__to_dict(item)
                    item_dict["seasoninfo"] = seasoninfo
                    item_dict["item_type"] = item_type
                    # 兼容下游按 tmdbid 取值：统一注入解析后的 TMDB ID
                    item_dict["tmdbid"] = item_tmdbid

                    logger.info(f"获到媒体库【{item_title}】数据：{item_dict}")

                    is_add_subscribe_success, tv_no_exist_info = (
                        self.__get_item_no_exist_info(item_dict)
                    )

                    if is_add_subscribe_success and tv_no_exist_info:
                        if not tv_no_exist_info[
                            "season_episode_no_exist_info"
                        ]:
                            logger.info(
                                f"【{item_title}】所有季集均已存在/订阅"
                            )
                            __append_history(
                                item_unique_flag=item_unique_flag,
                                exist_status=HistoryStatus.ALL_EXIST,
                                tv_no_exist_info=tv_no_exist_info,
                            )
                        else:
                            logger.info(
                                f"【{item_title}】缺失集数信息：{tv_no_exist_info}"
                            )

                            if (
                                self._no_exist_action
                                == NoExistAction.ADD_SUBSCRIBE.value
                            ):
                                logger.info("开始订阅缺失集数")
                                is_add_subscribe_success = (
                                    self.__add_subscribe_by_tv_no_exist_info(
                                        tv_no_exist_info, item_unique_flag
                                    )
                                )
                                if is_add_subscribe_success:
                                    __append_history(
                                        item_unique_flag=item_unique_flag,
                                        exist_status=HistoryStatus.ADDED_RSS,
                                        tv_no_exist_info=tv_no_exist_info,
                                    )
                                else:
                                    logger.warn(
                                        f"订阅【{item_title}】失败, 仅记录缺失集数"
                                    )
                                    __append_history(
                                        item_unique_flag=item_unique_flag,
                                        exist_status=HistoryStatus.NO_EXIST,
                                        tv_no_exist_info=tv_no_exist_info,
                                    )
                            elif (
                                self._no_exist_action
                                == NoExistAction.SET_ALL_EXIST.value
                            ):
                                logger.debug("将缺失季集标记为存在")
                                __append_history(
                                    item_unique_flag=item_unique_flag,
                                    exist_status=HistoryStatus.ALL_EXIST,
                                    tv_no_exist_info=tv_no_exist_info,
                                )

                            else:
                                logger.debug("仅记录缺失集数")
                                __append_history(
                                    item_unique_flag=item_unique_flag,
                                    exist_status=HistoryStatus.NO_EXIST,
                                    tv_no_exist_info=tv_no_exist_info,
                                )
                    else:
                        logger.warn(f"【{item_title}】获取缺失集数信息失败")
                        __append_history(
                            item_unique_flag=item_unique_flag,
                            exist_status=HistoryStatus.FAILED,
                            tv_no_exist_info=tv_no_exist_info,
                        )

                logger.info(
                    f"{mediaserver} 媒体库 {library.name} 获取数据完成"
                )

        logger.info(
            f"媒体库缺失集数据获取完成, 已处理媒体数量: {len(item_unique_flags)}"
        )

    @staticmethod
    def __to_dict(item: Any) -> dict[str, Any]:
        """把媒体服务器条目转成 dict，兼容 pydantic v2 与旧版对象。

        pydantic v2 中 dict() 已废弃（model_dump 取代），SQLAlchemy 模型则提供 to_dict()；
        这里按 model_dump → dict → to_dict 顺序尝试，避免因主程序依赖升级而中断。
        """
        for method in ("model_dump", "dict", "to_dict"):
            func = getattr(item, method, None)
            if callable(func):
                try:
                    data = func()
                    if isinstance(data, dict):
                        return data
                except Exception as err:
                    logger.debug(f"条目转字典失败（{method}）：{err}")
        return dict(item) if isinstance(item, dict) else {}

    @staticmethod
    def __resolve_tmdbid(item: Any) -> Optional[int]:
        """从媒体服务器条目解析 TMDB ID。

        新版 MoviePilot 的 MediaServerItem 已移除 tmdbid 字段，改为 media_source + media_id；
        为兼容两种版本，这里优先读旧字段，其次从 media_source/media_id 推导，
        并兼容 media_id 为数字字符串、以及 media_source 为枚举/字符串两种形态。
        """
        legacy = getattr(item, "tmdbid", None)
        if legacy:
            try:
                return int(legacy)
            except (TypeError, ValueError):
                pass
        media_source = getattr(item, "media_source", None)
        if media_source is not None:
            media_source = str(
                getattr(media_source, "value", media_source) or ""
            ).strip().lower()
        if media_source and media_source != TMDB_MEDIA_SOURCE:
            # 非 TMDB 身份（如豆瓣）无法用于 TMDB 季集查询
            return None
        media_id = getattr(item, "media_id", None)
        if media_id is None and isinstance(item, dict):
            media_id = item.get("media_id")
        if not media_id:
            return None
        try:
            return int(str(media_id).strip())
        except (TypeError, ValueError):
            return None

    def __get_item_no_exist_info(
        self, item_dict: dict[str, Any]
    ) -> tuple[bool, TvNoExistInfo]:
        """
        获取缺失集数
        """

        title = item_dict.get("title") or item_dict.get("original_title")

        tv_no_exist_info = create_tv_no_exist_info(
            title=title or "",
            year=item_dict.get("year", ""),
            path=item_dict.get("path", ""),
        )

        logger.debug(
            f" tv_no_exist_info create_tv_no_exist_info: {tv_no_exist_info}"
        )

        tmdbid: Optional[int] = None
        raw_tmdbid = item_dict.get("tmdbid")
        if raw_tmdbid:
            try:
                tmdbid = int(raw_tmdbid)
            except (TypeError, ValueError):
                tmdbid = None
        if not tmdbid:
            logger.debug(
                f"【{item_dict.get('title')}】未获取到TMDBID, 跳过获取缺失集数"
            )
            return False, tv_no_exist_info

        tv_no_exist_info["tmdbid"] = tmdbid
        # tv_no_exist_info.tmdbid = tmdbid

        mtype = item_dict.get("item_type")
        if not mtype:
            logger.debug(f"【{title}】未获取到媒体类型, 跳过获取缺失集数")
            return False, tv_no_exist_info
        if mtype != MediaType.TV.value:
            logger.debug(f"【{title}】媒体类型不为电视剧, 跳过获取缺失集数")
            return False, tv_no_exist_info

        # 添加不存在的季集信息
        def __append_season_info(
            season: int,
            episode_no_exist: List[int],
            episode_total: int,
        ):
            logger.debug(
                f"添加【{title}】第【{season}】季缺失集：{episode_no_exist}"
            )
            __season_info: EpisodeNoExistInfo = {
                "season": season,
                "episode_no_exist": episode_no_exist,
                "episode_total": episode_total,
            }

            logger.debug(
                f"【{title}】第【{season}】季缺失集信息：{__season_info}"
            )

            tv_no_exist_info["season_episode_no_exist_info"][
                str(season)
            ] = __season_info

            logger.debug(
                f"【{title}】缺失季集数的电视剧信息：{tv_no_exist_info}"
            )

        exist_season_info = item_dict.get("seasoninfo") or {}

        logger.debug(f"【{title}】在媒体库已有季集信息：{exist_season_info}")
        logger.debug(
            f"【{title}】开始获取媒体信息 mtype：{mtype}, tmdbid：{tmdbid}"
        )

        # 获取媒体信息
        tmdbinfo = self._mediaChain.recognize_media(
            mtype=MediaType.TV,
            media_source=MediaSource.TMDB,
            media_id=str(tmdbid),
        )

        if tmdbinfo:
            tv_no_exist_info["poster_path"] = (
                tmdbinfo.poster_path
                or tv_no_exist_info.get("poster_path", default_poster_path)
            )

            tv_no_exist_info["vote_average"] = (
                tmdbinfo.vote_average
                or tv_no_exist_info.get("vote_average", 0.0)
            )

            tv_no_exist_info["last_air_date"] = (
                tmdbinfo.last_air_date
                or tv_no_exist_info.get("last_air_date", "未知")
            )

            tmdbinfo_seasons = tmdbinfo.seasons.items()
            if not tmdbinfo_seasons:
                logger.debug(
                    f"【{title}】未获取到TMDB季集信息, 跳过获取缺失集数"
                )
                return False, tv_no_exist_info

            if not exist_season_info and not self._only_season_exist:
                logger.debug(f"【{title}】全部季不存在, 添加全部季集数")
                # 全部季不存在
                for season, _ in tmdbinfo_seasons:
                    filted_episodes = self.__filter_episodes(tmdbid, season)
                    if not filted_episodes:
                        logger.debug(
                            f"【{title}】第【{season}】季未获取到TMDB集数信息, 跳过"
                        )
                        continue
                    # 该季总集数
                    episode_total = len(filted_episodes)

                    # 判断用户是否已经添加订阅
                    if self._subOper.exists(MediaSource.TMDB, str(tmdbid), season=season):
                        logger.info(
                            f"【{title}】第【{season}】季已存在订阅, 跳过"
                        )
                        continue
                    __append_season_info(
                        season=season,
                        episode_no_exist=[],
                        episode_total=episode_total,
                    )
            else:
                logger.debug(f"【{title}】检查每季缺失的集")
                # 检查每季缺失的季集
                for season, _ in tmdbinfo_seasons:
                    filted_episodes = self.__filter_episodes(tmdbid, season)
                    logger.debug(
                        f"【{title}】第【{season}】季在TMDB的集数信息: {filted_episodes}"
                    )
                    if not filted_episodes:
                        logger.debug(
                            f"【{title}】第【{season}】季未获取到TMDB集数信息, 跳过"
                        )
                        continue
                    # 该季总集数
                    episode_total = len(filted_episodes)

                    # 该季已存在的集, 选项仅检查已有季缺失未开启时添加全部季
                    exist_episode = exist_season_info.get(season)
                    logger.debug(
                        f"【{title}】第【{season}】季在媒体库已存在的集数信息: {exist_episode}"
                    )
                    if exist_episode:
                        logger.debug(
                            f"查找【{title}】第【{season}】季缺失集集数"
                        )
                        # 按TMDB集数查找缺失集
                        lack_episode = list(
                            set(filted_episodes).difference(set(exist_episode))
                        )

                        if not lack_episode:
                            logger.debug(
                                f"【{title}】第【{season}】季全部集存在"
                            )
                            # 该季全部集存在, 不添加季集信息
                            continue

                        # 判断用户是否已经添加订阅
                        if self._subOper.exists(MediaSource.TMDB, str(tmdbid), season=season):
                            logger.info(
                                f"【{title}】第【{season}】季已存在订阅, 跳过"
                            )
                            continue
                        # 添加不存在的季集信息
                        __append_season_info(
                            season=season,
                            episode_no_exist=lack_episode,
                            episode_total=episode_total,
                        )
                    else:
                        logger.debug(f"【{title}】第【{season}】季全集不存在")
                        # 判断用户是否已经添加订阅
                        if self._subOper.exists(MediaSource.TMDB, str(tmdbid), season=season):
                            logger.info(
                                f"【{title}】第【{season}】季已存在订阅, 跳过"
                            )
                            continue
                        # 该季全集不存在, 选项仅检查已有季缺失未开启时添加全部集
                        if not self._only_season_exist:
                            __append_season_info(
                                season=season,
                                episode_no_exist=[],
                                episode_total=episode_total,
                            )

            logger.debug(f"【{title}】季集信息: {tv_no_exist_info}")

            # 存在不完整的剧集
            if tv_no_exist_info["season_episode_no_exist_info"]:
                logger.debug("媒体库中已存在部分剧集")
                return True, tv_no_exist_info

            # 全部存在
            logger.debug(f"【{title}】所有季集均已存在/订阅")
            return True, tv_no_exist_info

        else:
            logger.debug(f"【{title}】未获取到TMDB信息, 跳过获取缺失集数")
            return False, tv_no_exist_info

    def __filter_episodes(self, tmdbid, season):
        # 电视剧某季所有集
        episodes_info = self._tmdbChain.tmdb_episodes(
            tmdbid=tmdbid, season=season
        )

        episodes = []
        # 遍历集，筛选当前日期发布的剧集
        current_time = datetime.datetime.now(tz=pytz.timezone(settings.TZ))
        for episode in episodes_info:
            if episode and episode.air_date:
                # 将 air_date 字符串转换为 datetime 对象
                air_date = datetime.datetime.strptime(
                    episode.air_date, "%Y-%m-%d"
                )
                __episode_name = (
                    f"【TMDBID: {tmdbid}】第 {season}季 {episode.name}"
                )
                # 比较两个日期
                if air_date.date() < current_time.date():
                    episodes.append(episode.episode_number)
                else:
                    logger.debug(
                        f"{__episode_name} air_date: {episode.air_date} 发布时间比现在晚, 不添加进集统计"
                    )

        logger.debug(f"筛选后的集数::: {episodes}")

        return episodes

    def __update_config(self):
        """
        更新配置
        """
        __config = {
            "enabled": self._enabled,
            "cron": self._cron,
            "onlyonce": self._onlyonce,
            "clear": self._clear,
            "only_season_exist": self._only_season_exist,
            "history_type": self._history_type,
            "no_exist_action": self._no_exist_action,
            "save_path_replaces": "\n".join(
                map(str, self._save_path_replaces)
            ),
            "whitelist_librarys": ",".join(map(str, self._whitelist_librarys)),
            "whitelist_media_servers": ",".join(
                map(str, self._whitelist_media_servers)
            ),
        }
        logger.info(f"更新配置 {__config}")
        self.update_config(__config)

    def stop_service(self):
        """
        停止服务
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

    @staticmethod
    def __remove_history_by_unique(historys, unique: str):

        historys["item_unique_flags"] = [
            item for item in historys["item_unique_flags"] if item != unique
        ]

        if unique in historys["details"]:
            del historys["details"][unique]
            return True, historys
        else:
            logger.warn(f"unique: {unique} 不在历史记录里")
            return False, historys

    def __checke_and_add_subscribe(
        self,
        title: str,
        year: str,
        tmdbid: int,
        season: int,
        save_path: str | None = None,
        total_episode: int | None = None,
    ):
        title_season = f"{title} ({year}) 第 {season} 季"
        logger.info(f"开始检查 {title_season} 是否已添加订阅")

        save_path_replaced = None
        if self._save_path_replaces and save_path:
            for _save_path_replace in self._save_path_replaces:
                replace_list = [
                    part.strip()
                    for part in _save_path_replace.split(":")
                    if part.strip()
                ]
                if len(replace_list) < 2:
                    continue
                _lib_path_str, _save_path_str = replace_list[:2]
                logger.debug(f"替换路径: {_lib_path_str} -> {_save_path_str}")
                if _lib_path_str in save_path:
                    save_path_parent_str = str(Path(save_path).parent)
                    save_path_replaced = save_path_parent_str.replace(
                        _lib_path_str, _save_path_str
                    )
                    logger.info(
                        f"{title_season} 的下载路径替换为: {save_path_replaced}"
                    )
                    break

        # 判断用户是否已经添加订阅
        if self._subOper.exists(MediaSource.TMDB, str(tmdbid), season=season):
            logger.info(f"{title_season} 订阅已存在")
            return True

        logger.info(f"开始添加订阅: {title_season}")

        if not isinstance(season, int):
            try:
                season = int(season)
            except ValueError:
                logger.warn("season 无法转换为整数")

        # 添加订阅
        is_add_success, msg = self._subChain.add(
            title=title,
            year=year,
            mtype=MediaType.TV,
            media_source=MediaSource.TMDB,
            media_id=str(tmdbid),
            season=season,
            exist_ok=True,
            username=self.plugin_name,
            save_path=save_path_replaced,
            total_episode=total_episode,
        )
        logger.debug(f"添加订阅 {title_season} 结果: {is_add_success}, {msg}")
        if not is_add_success:
            logger.warn(f"添加订阅 {title_season} 失败: {msg}")
            return False
        logger.info(f"已添加订阅: {title_season}")
        return True

    @staticmethod
    def __update_exist_status_by_unique(
        historys, unique: str, new_status: str
    ):
        if unique in historys["details"]:
            historys["details"][unique]["exist_status"] = new_status
            logger.info(f"更新检查记录 {unique} 状态为: {new_status}")
            return True, historys
        else:
            logger.warn(f"unique: {unique} 不在历史记录里")
            return False, historys

    def __add_subscribe_by_tv_no_exist_info(
        self, tv_no_exist_info: TvNoExistInfo, unique: str
    ):

        title = tv_no_exist_info["title"]
        year = tv_no_exist_info["year"]
        tmdbid = tv_no_exist_info["tmdbid"]
        save_path = tv_no_exist_info["path"]
        season_episode_no_exist_info = tv_no_exist_info[
            "season_episode_no_exist_info"
        ]

        if (
            not title
            or not year
            or not tmdbid
            or not season_episode_no_exist_info
        ):
            logger.warn(f"unique: {unique} 季集信息不完整, 跳过订阅")
            return False

        season_keys = season_episode_no_exist_info.keys()

        for season in season_keys:
            total_episode = None
            # 尝试直接获取值
            season_info = season_episode_no_exist_info.get(season)

            if season_info:
                total_episode = season_info.get("episode_total")
                episode_no_exist = season_info.get("episode_no_exist")
                if not episode_no_exist:
                    logger.info(
                        f"【{title}】第 {season} 季所有集均缺失,  仅添加已有季选项为: {self._only_season_exist}"
                    )
                    # if self._history_type == HistoryDataType.NOT_ALL_NO_EXIST:
                    if self._only_season_exist:
                        logger.info(f"跳过订阅:【{title}】第 {season} 季")
                        continue
                    else:
                        logger.info(f"添加订阅:【{title}】第 {season} 季")

                else:
                    logger.info(
                        f"【{title}】第 {season} 季缺失集数: {episode_no_exist}, 将添加订阅"
                    )

            __season_int: int | None = None
            if not isinstance(season, int):
                try:
                    __season_int = int(season)
                except ValueError:
                    logger.warn("season 无法转换为整数")
                    return False
            else:
                __season_int = season

            is_add_subscribe_success = self.__checke_and_add_subscribe(
                title=title,
                year=year,
                tmdbid=tmdbid,
                season=__season_int,
                save_path=save_path,
                total_episode=total_episode,
            )
            if not is_add_subscribe_success:
                return False

        return True

    def __add_subscribe_by_unique(self, historys, unique: str):

        if unique in historys["details"]:
            tv_no_exist_info = historys["details"][unique]["tv_no_exist_info"]
            is_add_subscribe_success = (
                self.__add_subscribe_by_tv_no_exist_info(
                    tv_no_exist_info, unique
                )
            )
            if is_add_subscribe_success:
                is_update_exist_status_success, historys = (
                    self.__update_exist_status_by_unique(
                        historys=historys,
                        unique=unique,
                        new_status=HistoryStatus.ADDED_RSS.value,
                    )
                )
                return is_update_exist_status_success, historys
            else:
                return False, historys

        else:
            logger.warn(f"unique: {unique} 不在历史记录里")
            return False, historys

    def delete_history(self, key: str, apikey: str):
        """
        删除同步检查记录
        """
        logger.info(f"开始删除检查记录: {key}")
        if apikey != settings.API_TOKEN:
            logger.warn("API密钥错误")
            return schemas.Response(success=False, message="API密钥错误")
        # 检查记录
        historys = self.get_data("history")
        if not historys:
            logger.warn("未找到检查记录")
            return schemas.Response(success=False, message="未找到检查记录")

        is_success, historys = EpisodeNoExist.__remove_history_by_unique(
            historys, key
        )

        if is_success:
            logger.info(f"删除检查记录 {key} 成功")
            self.save_data("history", historys)
            return schemas.Response(success=True, message="删除成功")
        else:
            logger.warn(f"删除检查记录 {key} 失败")
            return schemas.Response(success=False, message="删除失败")

    def add_subscribe_history(self, key: str, apikey: str):
        """
        订阅缺失检查记录
        """
        logger.info(f"开始订阅检查记录: {key}")
        if apikey != settings.API_TOKEN:
            logger.warn("API密钥错误")
            return schemas.Response(success=False, message="API密钥错误")
        # 检查记录
        historys = self.get_data("history")
        if not historys:
            logger.warn("未找到检查记录")
            return schemas.Response(success=False, message="未找到检查记录")

        is_success, historys = self.__add_subscribe_by_unique(historys, key)
        if is_success:
            logger.info(f"添加 {key} 订阅成功")
            self.save_data("history", historys)
            return schemas.Response(success=True, message="订阅成功")
        else:
            logger.warn(f"添加 {key} 订阅失败")
            return schemas.Response(success=False, message="订阅失败")

    def set_all_exist_history(self, key: str, apikey: str):
        """
        标记存在检查记录
        """
        logger.info(f"开始标记存在检查记录: {key}")
        if apikey != settings.API_TOKEN:
            logger.warn("API密钥错误")
            return schemas.Response(success=False, message="API密钥错误")
        # 检查记录
        historys = self.get_data("history")
        if not historys:
            logger.warn("未找到检查记录")
            return schemas.Response(success=False, message="未找到检查记录")

        is_success, historys = EpisodeNoExist.__update_exist_status_by_unique(
            historys, key, HistoryStatus.ALL_EXIST.value
        )
        if is_success:
            logger.info(f"标记存在 {key} 成功")
            self.save_data("history", historys)
            return schemas.Response(success=True, message="标记存在成功")
        else:
            logger.warn(f"标记存在 {key} 失败")
            return schemas.Response(success=False, message="标记存在失败")

    def get_form(self) -> tuple[list[dict[str, Any]], dict[str, Any]]:
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
                                            "model": "only_season_exist",
                                            "label": "仅检查已有季缺失",
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
                                            "model": "clear",
                                            "label": "清理检查记录",
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
                                            "model": "onlyonce",
                                            "label": "立即运行一次",
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
                                            "model": "cron",
                                            "label": "执行周期",
                                            "placeholder": "5位cron表达式, 留空自动",
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
                                            "model": "history_type",
                                            "label": "历史数据类型",
                                            "items": [
                                                {
                                                    "title": f"{HistoryDataType.LATEST.value}",
                                                    "value": f"{HistoryDataType.LATEST.value}",
                                                },
                                                {
                                                    "title": f"{HistoryDataType.NO_EXIST.value}",
                                                    "value": f"{HistoryDataType.NO_EXIST.value}",
                                                },
                                                {
                                                    "title": f"{HistoryDataType.NOT_ALL_NO_EXIST.value}",
                                                    "value": f"{HistoryDataType.NOT_ALL_NO_EXIST.value}",
                                                },
                                                {
                                                    "title": f"{HistoryDataType.ALL_EXIST.value}",
                                                    "value": f"{HistoryDataType.ALL_EXIST.value}",
                                                },
                                                {
                                                    "title": f"{HistoryDataType.ADDED_RSS.value}",
                                                    "value": f"{HistoryDataType.ADDED_RSS.value}",
                                                },
                                                {
                                                    "title": f"{HistoryDataType.FAILED.value}",
                                                    "value": f"{HistoryDataType.FAILED.value}",
                                                },
                                                {
                                                    "title": f"{HistoryDataType.ALL.value}",
                                                    "value": f"{HistoryDataType.ALL.value}",
                                                },
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
                                            "model": "no_exist_action",
                                            "label": "缺失处理方式",
                                            "items": [
                                                {
                                                    "title": f"{NoExistAction.ONLY_HISTORY.value}",
                                                    "value": f"{NoExistAction.ONLY_HISTORY.value}",
                                                },
                                                {
                                                    "title": f"{NoExistAction.ADD_SUBSCRIBE.value}",
                                                    "value": f"{NoExistAction.ADD_SUBSCRIBE.value}",
                                                },
                                                {
                                                    "title": f"{NoExistAction.SET_ALL_EXIST.value}",
                                                    "value": f"{NoExistAction.SET_ALL_EXIST.value}",
                                                },
                                            ],
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
                                "props": {"cols": 12, "md": 12},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "whitelist_librarys",
                                            "label": "电视剧媒体库白名单",
                                            "placeholder": "*必填, 多个名称用英文逗号分隔",
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
                                "props": {"cols": 12, "md": 12},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "whitelist_media_servers",
                                            "label": "媒体服务器名称白名单",
                                            "placeholder": "留空默认全部, 多个名称用英文逗号分隔: emby,embyA,embyB,jellyfin,plex",
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
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "save_path_replaces",
                                            "label": "下载路径替换, 一行一个",
                                            "placeholder": "将媒体库电视剧的路径替换为下载路径, 用英文冒号作为分割。不输入则按默认下载路径处理。\n例如将'/media/library/tv/上载新生 (2020)'的下载路径设置为'/downloads/tv', 则输入 /media/library:/downloads",
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
            "cron": "",
            "onlyonce": False,
            "only_season_exist": True,
            "clear": False,
            "history_type": HistoryDataType.LATEST.value,
            "save_path_replaces": "",
            "no_exist_action": NoExistAction.ONLY_HISTORY.value,
            "whitelist_media_servers": "",
            "whitelist_librarys": "",
        }

    def __get_action_buttons_content(self, unique: str | None, status: str):
        if not unique:
            return []
        action_buttons = {
            "add_subscribe_history": {
                "component": "VBtn",
                "props": {
                    "class": "text-primary flex-grow",
                    "variant": "tonal",
                    "style": "height: 100%",
                },
                "events": {
                    "click": {
                        "api": "plugin/EpisodeNoExist/add_subscribe_history",
                        "method": "get",
                        "params": {
                            "key": f"{unique}",
                            "apikey": settings.API_TOKEN,
                        },
                    }
                },
                "text": "订阅缺失",
            },
            "set_all_exist_history": {
                "component": "VBtn",
                "props": {
                    "class": "text-success flex-grow",
                    "style": "height: 100%",
                    "variant": "tonal",
                },
                "events": {
                    "click": {
                        "api": "plugin/EpisodeNoExist/set_all_exist_history",
                        "method": "get",
                        "params": {
                            "key": f"{unique}",
                            "apikey": settings.API_TOKEN,
                        },
                    }
                },
                "text": "标记存在",
            },
            "delete_history": {
                "component": "VBtn",
                "props": {
                    "class": "text-error flex-grow",
                    "style": "height: 100%",
                    "variant": "tonal",
                },
                "events": {
                    "click": {
                        "api": "plugin/EpisodeNoExist/delete_history",
                        "method": "get",
                        "params": {
                            "key": f"{unique}",
                            "apikey": settings.API_TOKEN,
                        },
                    }
                },
                "text": "删除记录",
            },
        }

        action_names = {
            HistoryStatus.NO_EXIST.value: [
                "delete_history",
                "set_all_exist_history",
                "add_subscribe_history",
            ],
            HistoryStatus.ADDED_RSS.value: [
                "delete_history",
                "set_all_exist_history",
            ],
        }.get(status, ["delete_history"])

        action_buttons_list = [
            action_buttons.get(name)
            for name in action_names
            if action_buttons.get(name) is not None
        ]

        return action_buttons_list

    def __get_history_post_content(self, history: ExtendedHistoryDetail):
        def __count_seasons_episodes(
            seasons_episodes_info: Dict[str, EpisodeNoExistInfo],
        ):
            seasons_episodes_info = seasons_episodes_info or {}
            seasons_count = len(seasons_episodes_info.keys())
            episodes_count = 0
            for season in seasons_episodes_info.values():
                episode_no_exist = season.get("episode_no_exist")
                if episode_no_exist:
                    episodes_count += len(episode_no_exist)
                else:
                    episodes_count += season.get("episode_total", 0)
            return seasons_count, episodes_count

        history = history or {}
        time_str = history.get("last_update")

        tv_no_exist_info: TvNoExistInfo = history.get("tv_no_exist_info") or {}  # type: ignore

        title = tv_no_exist_info.get("title", "未知")
        title = title[:8] + "..." if len(title) > 8 else title

        year = tv_no_exist_info.get("year", "未知")
        tmdbid = tv_no_exist_info.get("tmdbid", 0)
        poster = tv_no_exist_info.get("poster_path", default_poster_path)
        vote = tv_no_exist_info.get("vote_average", 0.0)
        last_air_date = tv_no_exist_info.get("last_air_date", "未知")
        season_episode_no_exist_info = tv_no_exist_info.get(
            "season_episode_no_exist_info", {}
        )

        season_no_exist_count, episode_no_exist_count = (
            __count_seasons_episodes(season_episode_no_exist_info)
        )

        _status = history.get("exist_status") or HistoryStatus.UNKNOW.value
        status = _status
        if status == HistoryStatus.NO_EXIST.value:
            status = (
                f"缺失{season_no_exist_count}季, {episode_no_exist_count}集"
            )

        mp_domain = settings.MP_DOMAIN()
        link = f"#/media?mediaid=tmdb:{tmdbid}&type={MediaType.TV.value}"
        if mp_domain:
            if mp_domain.endswith("/"):
                link = f"{mp_domain}{link}"
            else:
                link = f"{mp_domain}/{link}"

        unique = history.get("unique")

        if tmdbid and tmdbid != 0:
            href = f"{link}"
        else:
            href = "#"

        action_buttons_content = self.__get_action_buttons_content(
            unique,
            _status,
        )

        component = {
            "component": "VCard",
            "props": {
                "variant": "tonal",
                "props": {"class": ""},
            },
            "content": [
                {
                    "component": "div",
                    "props": {"class": "flex flex-row"},
                    "content": [
                        {
                            "component": "VImg",
                            "props": {
                                "src": poster,
                                "height": 240,
                                "width": 160,
                                "aspect-ratio": "2/3",
                                "class": "object-cover shadow ring-gray-500 max-w-32",
                                "cover": True,
                                "transition": True,
                                "lazy-src": "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAPAAAACgCAQAAACY0inuAAABB0lEQVR42u3RMREAAAjEMF45M65xwcClEppMlx4XwIAFWIAFWIAFWIABC7AAC7AAC7AAAxZgARZgARZgARZgwAIswAIswAIswIAFWIAFWIAFWIABC7AAC7AAC7AACzBgARZgARZgARZgwAIswAIswAIswIABAxZgARZgARZgAQYswAIswAIswAIMWIAFWIAFWIAFWIABC7AAC7AAC7AAAxZgARZgARZgAQYswAIswAIswAIswIAFWIAFWIAFWIABC7AAC7AAC7AAAzYBsAALsAALsAALMGABFmABFmABFmDAAizAAizAAizAAgxYgAVYgAVYgAUYsAALsAALsAALMGABFmAB1m0LDz+locM0WkgAAAAASUVORK5CYII=",
                            },
                        },
                        {
                            "component": "div",
                            "props": {"class": ""},
                            "content": [
                                {
                                    "component": "VCardTitle",
                                    "props": {
                                        "class": "pt-6 pl-4 pr-4 text-lg whitespace-nowrap",
                                        "style": "width: 12rem",
                                    },
                                    "content": [
                                        {
                                            "component": "a",
                                            "props": {
                                                "href": f"{href}",
                                                "target": "_blank",
                                            },
                                            "text": title,
                                        }
                                    ],
                                },
                                {
                                    "component": "VCardText",
                                    "props": {
                                        "class": "pa-0 pl-4 pr-4 pb-1 whitespace-nowrap"
                                    },
                                    "text": f"状态: {status}",
                                },
                                {
                                    "component": "VCardText",
                                    "props": {
                                        "class": "pa-0 pl-4 pr-4 py-1 whitespace-nowrap"
                                    },
                                    "text": f"年份: {year}",
                                },
                                {
                                    "component": "VCardText",
                                    "props": {
                                        "class": "pa-0 pl-4 pr-4 py-1 whitespace-nowrap"
                                    },
                                    "text": f"评分: {vote}",
                                },
                                {
                                    "component": "VCardText",
                                    "props": {
                                        "class": "pa-0 pl-4 pr-4 py-1 whitespace-nowrap"
                                    },
                                    "text": f"检查: {time_str}",
                                },
                                {
                                    "component": "VCardText",
                                    "props": {
                                        "class": "pa-0 pl-4 pr-4 py-1 whitespace-nowrap"
                                    },
                                    "text": f"最后: {last_air_date}",
                                },
                            ],
                        },
                    ],
                },
                {
                    "component": "VBtnToggle",
                    "props": {
                        "class": "bg-opacity-80 flex flex-row-reverse justify-between items-center flex-nowrap space-x-reverse space-x-4",
                        "variant": "tonal",
                        "rounded": "0",
                    },
                    "content": action_buttons_content,
                },
            ],
        }

        return component

    def __get_historys_posts_content(
        self, historys: List[ExtendedHistoryDetail] | None
    ):

        posts_content = []
        if not historys:
            posts_content = [
                {
                    "component": "div",
                    "text": "暂无数据",
                    "props": {
                        "class": "text-start",
                    },
                }
            ]
        else:
            for history in historys:
                posts_content.append(self.__get_history_post_content(history))

        component = {
            "component": "div",
            "content": [
                {
                    "component": "VCardTitle",
                    "props": {
                        "class": "pt-8 pb-2 px-0 text-base whitespace-nowrap text-center",
                    },
                    "content": [
                        {
                            "component": "span",
                            "text": f"··· {self._history_type} ···",
                        }
                    ],
                },
                {
                    "component": "div",
                    "props": {
                        "class": "flex flex-row flex-wrap gap-4 items-center justify-center",
                    },
                    "content": posts_content,
                },
            ],
        }

        return component

    @staticmethod
    def __get_svg_content(color: str, ds: List[str]):
        def __get_path_content(fill: str, d: str) -> dict[str, Any]:
            return {
                "component": "path",
                "props": {"fill": fill, "d": d},
            }

        path_content = [__get_path_content(color, d) for d in ds]
        component = {
            "component": "svg",
            "props": {
                "class": "icon",
                "viewBox": "0 0 1024 1024",
                "width": "40",
                "height": "40",
            },
            "content": path_content,
        }
        return component

    @staticmethod
    def __get_icon_content():
        color = "#8a8a8a"
        icon_content = {
            Icons.TARGET: EpisodeNoExist.__get_svg_content(
                color,
                [
                    "M512 307.2c-114.688 0-204.8 90.112-204.8 204.8 0 110.592 90.112 204.8 204.8 204.8s204.8-90.112 204.8-204.8-90.112-204.8-204.8-204.8z",
                    "M962.56 471.04H942.08c-20.48-204.8-184.32-372.736-389.12-389.12v-20.48c0-24.576-16.384-40.96-40.96-40.96s-40.96 16.384-40.96 40.96v16.384c-204.8 20.48-372.736 184.32-389.12 393.216h-20.48c-24.576 0-40.96 16.384-40.96 40.96s16.384 40.96 40.96 40.96h16.384c20.48 204.8 184.32 372.736 393.216 393.216v16.384c0 24.576 16.384 40.96 40.96 40.96s40.96-16.384 40.96-40.96V942.08c204.8-20.48 372.736-184.32 393.216-389.12h16.384c24.576 0 40.96-16.384 40.96-40.96s-16.384-40.96-40.96-40.96z m-409.6 389.12v-24.576c0-24.576-16.384-40.96-40.96-40.96s-40.96 16.384-40.96 40.96v24.576c-159.744-20.48-290.816-147.456-307.2-307.2h24.576c24.576 0 40.96-16.384 40.96-40.96s-16.384-40.96-40.96-40.96H163.84c16.384-159.744 147.456-290.816 307.2-307.2v24.576c0 24.576 16.384 40.96 40.96 40.96s40.96-16.384 40.96-40.96V163.84c159.744 20.48 290.816 147.456 307.2 307.2h-24.576c-24.576 0-40.96 16.384-40.96 40.96s16.384 40.96 40.96 40.96h24.576c-16.384 159.744-147.456 290.816-307.2 307.2z",
                ],
            ),
            Icons.ADD_SCHEDULE: EpisodeNoExist.__get_svg_content(
                color,
                [
                    "M611.157333 583.509333h-63.146666v-63.146666c0-20.138667-16.042667-36.181333-35.84-36.181334-20.138667 0-35.84 16.042667-35.84 35.84v63.146667h-63.146667c-19.797333 0-36.181333 16.384-36.181333 36.181333 0.7168 21.128533 16.759467 35.498667 36.181333 36.181334h63.146667v62.805333c0 20.923733 16.759467 35.84 35.84 35.84 19.797333 0 35.84-16.042667 35.84-35.84v-63.146667h63.146666a35.84 35.84 0 1 0 0-71.68z",
                    "M839.338667 145.749333h-13.653334v86.016c0 56.32-45.738667 102.4-102.4 102.4-56.32 0-102.4-46.08-102.4-102.4V145.749333h-217.770666v86.016c0 56.32-46.08 102.4-102.4 102.4-56.661333 0-102.4-46.08-102.4-102.4V145.749333h-13.653334C120.490667 145.749333 68.266667 197.973333 68.266667 262.144v551.594667c0 64.170667 52.224 116.394667 116.394666 116.394666h654.677334c64.170667 0 116.394667-52.224 116.394666-116.394666V262.144c0-64.170667-52.224-116.394667-116.394666-116.394667z m0 716.117334H184.661333c-26.624 0-48.128-21.504-48.128-48.128V402.773333h750.933334v410.965334c0 26.624-21.504 48.128-48.128 48.128z",
                    "M300.612267 265.796267a34.133333 34.133333 0 0 0 34.133333-34.133334V128a34.133333 34.133333 0 1 0-68.266667 0v103.6288a34.133333 34.133333 0 0 0 34.133334 34.133333zM723.3536 265.796267a34.133333 34.133333 0 0 0 34.133333-34.133334V128a34.133333 34.133333 0 1 0-68.266666 0v103.6288a34.133333 34.133333 0 0 0 34.133333 34.133333z",
                ],
            ),
            Icons.BUG_REMOVE: EpisodeNoExist.__get_svg_content(
                color,
                [
                    "M945.000296 566.802963c-25.486222-68.608-91.211852-79.530667-144.19437-72.855704a464.402963 464.402963 0 0 0-29.316741-101.148444c20.366222-8.343704 48.279704-12.136296 70.731852 14.487704a37.925926 37.925926 0 0 0 57.912889-49.000297c-51.655111-61.060741-117.94963-53.589333-164.636445-32.426666a333.482667 333.482667 0 0 0-72.021333-78.696297c2.654815-11.377778 4.399407-23.021037 4.399408-35.157333 0-19.683556-4.020148-38.305185-10.695112-55.675259 10.467556-10.960593 30.644148-25.979259 61.705482-23.058963a37.660444 37.660444 0 0 0 41.339259-34.17126 37.925926 37.925926 0 0 0-34.133333-41.339259 145.294222 145.294222 0 0 0-113.246815 36.560593A153.182815 153.182815 0 0 0 513.137778 56.888889c-36.408889 0-69.404444 13.160296-95.876741 34.285037a145.59763 145.59763 0 0 0-109.37837-33.450667 37.925926 37.925926 0 1 0 7.205926 75.548445 73.007407 73.007407 0 0 1 55.902814 17.597629A154.737778 154.737778 0 0 0 358.4 212.005926c0 12.212148 1.782519 23.969185 4.475259 35.384889A334.051556 334.051556 0 0 0 290.512593 326.807704c-46.800593-21.845333-114.194963-30.492444-166.646519 31.478518a37.925926 37.925926 0 0 0 57.912889 49.000297c23.134815-27.382519 52.261926-22.641778 72.969481-13.615408a464.213333 464.213333 0 0 0-28.975407 100.655408c-53.475556-7.395556-120.832 2.768593-146.773333 72.438518a37.925926 37.925926 0 1 0 71.111111 26.43437c10.24-27.534222 44.259556-27.230815 68.532148-23.134814-0.644741 33.374815 1.137778 64.891259 9.253926 106.192592-38.456889 10.884741-81.768296 39.405037-101.793185 103.461926a37.925926 37.925926 0 0 0 72.438518 22.603852c11.150222-35.65037 32.768-48.810667 49.682963-53.551407 47.900444 129.024 148.555852 218.339556 265.102222 218.339555 116.280889 0 216.746667-88.936296 264.798815-217.467259 16.535704 5.271704 36.712296 18.659556 47.369482 52.679111a37.888 37.888 0 1 0 72.400592-22.603852c-19.569778-62.691556-61.44-91.401481-99.252148-102.779259 8.305778-42.059852 10.012444-73.500444 9.367704-107.254519 24.007111-3.678815 55.978667-3.109926 65.877333 23.514074a37.925926 37.925926 0 1 0 71.111111-26.396444z m-321.308444 69.973333c14.791111 14.791111 14.791111 39.063704 0 53.854815a38.039704 38.039704 0 0 1-53.475556 0l-56.888889-56.888889-56.888888 56.888889a38.456889 38.456889 0 0 1-53.854815 0c-14.791111-14.791111-14.791111-39.063704 0-53.854815l56.888889-56.888889-56.888889-56.888888a37.774222 37.774222 0 0 1 0-53.475556c14.791111-14.791111 39.063704-14.791111 53.854815 0l56.888888 56.888889 56.888889-56.888889a37.774222 37.774222 0 0 1 53.475556 0c14.791111 14.791111 14.791111 38.684444 0 53.475556l-56.888889 56.888888 56.888889 56.888889z"
                ],
            ),
            Icons.WARNING: EpisodeNoExist.__get_svg_content(
                color,
                [
                    "M965.316923 727.276308l-319.015385-578.953846c-58.171077-106.299077-210.944-106.023385-268.996923 0l-318.621538 579.347692c-56.359385 102.636308 18.116923 227.643077 134.695385 227.643077h637.243076c116.184615 0 191.172923-124.416 134.695385-228.036923z m-453.316923 26.781538c-24.812308 0-44.504615-20.086154-44.504615-44.504615 0-24.812308 19.692308-44.898462 44.504615-44.898462a44.701538 44.701538 0 0 1 0 89.403077z m57.501538-361.156923l-20.873846 170.929231c-1.575385 19.298462-17.329231 33.870769-36.627692 33.870769s-35.446154-14.572308-37.021538-33.870769l-20.48-170.929231c-3.150769-33.870769 23.630769-63.015385 57.501538-63.015385 29.932308 0 57.501538 21.582769 57.501538 63.015385z"
                ],
            ),
            Icons.GLASSES: EpisodeNoExist.__get_svg_content(
                color,
                [
                    "M1028.096 503.808L815.104 204.8c-8.192-12.288-20.48-16.384-32.768-16.384h-126.976c-24.576 0-40.96 20.48-40.96 40.96 0 24.576 20.48 40.96 40.96 40.96h102.4l131.072 184.32H143.36l135.168-188.416h102.4c24.576 0 40.96-16.384 40.96-40.96s-16.384-40.96-40.96-40.96H253.952c-16.384 0-24.576 8.192-32.768 16.384L8.192 499.712c0 8.192-8.192 32.768-8.192 53.248v188.416c0 53.248 45.056 94.208 98.304 94.208h266.24c53.248 0 94.208-40.96 94.208-94.208v-188.416-12.288h122.88V741.376c0 53.248 40.96 94.208 98.304 94.208h266.24c53.248 0 94.208-40.96 94.208-94.208v-188.416c0-16.384-8.192-40.96-12.288-49.152zM376.832 716.8c0 20.48-16.384 40.96-40.96 40.96H122.88c-20.48 0-40.96-20.48-40.96-40.96v-135.168c0-24.576 20.48-40.96 40.96-40.96H335.872c24.576 0 40.96 16.384 40.96 40.96v135.168z m581.632 0c0 20.48-16.384 40.96-40.96 40.96H704.512c-20.48 0-40.96-20.48-40.96-40.96v-135.168c0-24.576 20.48-40.96 40.96-40.96h212.992c24.576 0 40.96 16.384 40.96 40.96v135.168z",
                ],
            ),
            Icons.STATISTICS: EpisodeNoExist.__get_svg_content(
                color,
                [
                    "M471.04 270.336V20.48c-249.856 20.48-450.56 233.472-450.56 491.52 0 274.432 225.28 491.52 491.52 491.52 118.784 0 229.376-40.96 315.392-114.688L655.36 708.608c-40.96 28.672-94.208 45.056-139.264 45.056-135.168 0-245.76-106.496-245.76-245.76 0-114.688 81.92-217.088 200.704-237.568z",
                    "M552.96 20.48v249.856C655.36 286.72 737.28 368.64 753.664 471.04h249.856C983.04 233.472 790.528 40.96 552.96 20.48zM712.704 651.264l176.128 176.128c65.536-77.824 106.496-172.032 114.688-274.432h-249.856c-8.192 36.864-20.48 69.632-40.96 98.304z",
                ],
            ),
        }
        return icon_content

    @staticmethod
    def __get_historys_statistic_content(
        title: str, value: str, icon_name: Icons
    ) -> dict[str, Any]:
        icon_content = EpisodeNoExist.__get_icon_content().get(icon_name, "")
        total_elements = {
            "component": "VCard",
            "props": {
                "variant": "tonal",
                "style": "width: 10rem;",
            },
            "content": [
                {
                    "component": "VCardText",
                    "props": {
                        "class": "d-flex align-center",
                    },
                    "content": [
                        icon_content,
                        {
                            "component": "div",
                            "props": {
                                "class": "ml-2",
                            },
                            "content": [
                                {
                                    "component": "span",
                                    "props": {"class": "text-caption"},
                                    "text": f"{title}",
                                },
                                {
                                    "component": "div",
                                    "props": {
                                        "class": "d-flex align-center flex-wrap"
                                    },
                                    "content": [
                                        {
                                            "component": "span",
                                            "props": {"class": "text-h6"},
                                            "text": f"{value}",
                                        }
                                    ],
                                },
                            ],
                        },
                    ],
                }
            ],
        }
        return total_elements

    def __get_historys_statistics_content(
        self,
        historys_total,
        historys_no_exist_total,
        historys_fail_total,
        historys_all_exist_total,
        historys_added_rss_total,
        history_not_all_no_exist_total,
    ):

        # 数据统计
        data_statistics = [
            {
                "title": "总处理",
                "value": f"{historys_total}部",
                "icon_name": Icons.STATISTICS,
            },
            {
                "title": "存在缺失",
                "value": f"{historys_no_exist_total}部",
                "icon_name": Icons.WARNING,
            },
            {
                "title": "已有季缺失",
                "value": f"{history_not_all_no_exist_total}部",
                "icon_name": Icons.TARGET,
            },
            {
                "title": "未识别",
                "value": f"{historys_fail_total}部",
                "icon_name": Icons.BUG_REMOVE,
            },
            {
                "title": "全部存在",
                "value": f"{historys_all_exist_total}部",
                "icon_name": Icons.GLASSES,
            },
            {
                "title": "已订阅",
                "value": f"{historys_added_rss_total}部",
                "icon_name": Icons.ADD_SCHEDULE,
            },
        ]

        content = list(
            map(
                lambda s: EpisodeNoExist.__get_historys_statistic_content(
                    title=str(s["title"]),
                    value=str(s["value"]),
                    icon_name=Icons(s["icon_name"]),
                ),
                data_statistics,
            )
        )

        component = {
            "component": "VRow",
            "props": {"class": "flex flex-row justify-center flex-wrap gap-6"},
            "content": content,
        }
        return component

    def get_page(self) -> List[Dict[str, Any]]:
        """
        拼装插件详情页面, 需要返回页面配置, 同时附带数据
        """

        # 查询检查记录
        historys = self.get_data("history")

        if not historys:
            return [
                {
                    "component": "div",
                    "text": "暂无数据",
                    "props": {
                        "class": "text-center",
                    },
                }
            ]

        details = historys.get("details", {})

        def sort_history(history_list):
            history_list.sort(
                key=lambda x: x["last_update_full"], reverse=True
            )

        history_failed: List[ExtendedHistoryDetail] = []
        history_all_exist: List[ExtendedHistoryDetail] = []
        history_added_rss: List[ExtendedHistoryDetail] = []
        history_no_exist: List[ExtendedHistoryDetail] = []
        history_all: List[ExtendedHistoryDetail] = []

        # 字典将exist_status映射到相应的列表
        status_to_list = {
            HistoryStatus.FAILED.value: history_failed,
            HistoryStatus.ADDED_RSS.value: history_added_rss,
            HistoryStatus.ALL_EXIST.value: history_all_exist,
            HistoryStatus.NO_EXIST.value: history_no_exist,
        }

        for key, item in details.items():
            item_with_key = item.copy()
            item_with_key["unique"] = key
            history_all.append(item_with_key)

            # 根据exist_status分类项目
            target_list = status_to_list.get(item["exist_status"])
            if target_list is not None:
                target_list.append(item_with_key)

        # 对所有列表排序
        sort_history(history_all)
        sort_history(history_failed)
        sort_history(history_all_exist)
        sort_history(history_added_rss)
        sort_history(history_no_exist)

        # 根据_history_type确定使用的列表
        history_type_to_list = {
            HistoryDataType.FAILED.value: history_failed,
            HistoryDataType.ADDED_RSS.value: history_added_rss,
            HistoryDataType.ALL_EXIST.value: history_all_exist,
            HistoryDataType.NO_EXIST.value: history_no_exist,
            HistoryDataType.ALL.value: history_all,
        }

        def __get_season_episode_no_exist_info(
            _history: ExtendedHistoryDetail,
        ):
            _tv_no_exist_info = _history.get("tv_no_exist_info")
            if not _tv_no_exist_info:
                return []
            _no_exist_info = _tv_no_exist_info.get(
                "season_episode_no_exist_info"
            )
            if not _no_exist_info:
                return []

            _values = _no_exist_info.values()
            return _values

        history_not_all_no_exist = [
            history
            for history in history_no_exist
            if any(
                season_info.get("episode_no_exist")
                for season_info in __get_season_episode_no_exist_info(history)
            )
        ]

        if self._history_type == HistoryDataType.NOT_ALL_NO_EXIST.value:
            historys_in_type = history_not_all_no_exist
        else:
            historys_in_type = history_type_to_list.get(
                self._history_type, history_all[:6]
            )

        historys_posts_content = self.__get_historys_posts_content(
            historys_in_type
        )

        # 统计数据
        item_unique_flags = historys.get("item_unique_flags", [])
        historys_total = len(item_unique_flags)
        historys_no_exist_total = len(history_no_exist)
        historys_fail_total = len(history_failed)
        historys_added_rss_total = len(history_added_rss)
        historys_all_exist_total = len(history_all_exist)
        historys_all_exist_total = len(history_all_exist)
        history_not_all_no_exist_total = len(history_not_all_no_exist)
        historys_statistics_content = self.__get_historys_statistics_content(
            historys_total=historys_total,
            historys_no_exist_total=historys_no_exist_total,
            historys_fail_total=historys_fail_total,
            historys_all_exist_total=historys_all_exist_total,
            historys_added_rss_total=historys_added_rss_total,
            history_not_all_no_exist_total=history_not_all_no_exist_total,
        )

        # 拼装页面
        return [
            {
                "component": "div",
                "content": [
                    historys_statistics_content,
                    historys_posts_content,
                ],
            },
        ]
