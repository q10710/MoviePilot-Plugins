"""上映/开播日期兜底回查：识别结果缺日期时按外部ID回查 TMDB。

imdb、豆瓣等来源的识别结果不携带 TMDB 的季播出日期（imdb 连影片上映日期也不携带），
上映前暂停判据会把这类订阅永久判为「日期未知」。本模块只做只读的日期补齐：
调用方优先使用识别结果已有的日期，缺失时才调用这里，用 IMDb / TVDB 外部ID
到 TMDB 精确回查；任何失败都返回 None，由调用方保持原有的「日期未知」处理。
"""
from datetime import date
from typing import Optional, Tuple

from app.sdk.logging import logger

from .media import parse_date


def _external_ids(subscribe, mediainfo) -> Tuple[str, str]:
    """收集可用于 TMDB 回查的外部ID；订阅本身是 imdb/tvdb 来源时以其原生ID兜底。"""
    imdb_id = str(getattr(mediainfo, "imdb_id", "") or "").strip()
    tvdb_id = str(getattr(mediainfo, "tvdb_id", "") or "").strip()
    source = str(getattr(subscribe, "media_source", "") or "").strip().lower()
    media_id = str(getattr(subscribe, "media_id", "") or "").strip()
    if not imdb_id and source == "imdb" and media_id.startswith("tt"):
        imdb_id = media_id
    if not tvdb_id and source == "tvdb" and media_id.isdigit():
        tvdb_id = media_id
    return imdb_id, tvdb_id


def _movie_air_date(movie_obj, candidate: dict, tmdb_id) -> Optional[date]:
    """取电影上映日期，优先用 find 结果自带字段，缺失时再查详情。"""
    air = parse_date(candidate.get("release_date"))
    if air:
        return air
    if not tmdb_id:
        return None
    detail = movie_obj.details(tmdb_id, append_to_response="")
    return parse_date((detail or {}).get("release_date"))


def _tv_air_date(tv_obj, candidate: dict, season: Optional[int]) -> Optional[date]:
    """取剧集指定季的开播日期；该季无排期时回落到整剧首播日期。"""
    tmdb_id = candidate.get("id")
    if tmdb_id:
        detail = tv_obj.details(tmdb_id, append_to_response="")
        if detail:
            if season is not None:
                for item in detail.get("seasons") or []:
                    if item.get("season_number") == season:
                        air = parse_date(item.get("air_date"))
                        if air:
                            return air
            air = parse_date(detail.get("first_air_date"))
            if air:
                return air
    return parse_date(candidate.get("first_air_date"))


def lookup_tmdb_air_date(subscribe, mediainfo, season: Optional[int] = None,
                         is_movie: bool = False) -> Optional[date]:
    """按 IMDb / TVDB 外部ID回查 TMDB 上映或指定季开播日期；不可用时返回 None。"""
    imdb_id, tvdb_id = _external_ids(subscribe, mediainfo)
    if not imdb_id and not tvdb_id:
        return None
    try:
        # 延迟导入主程序 TMDB 客户端：沿用其域名、代理、语言与缓存配置。
        from app.modules.themoviedb.tmdbv3api import Find, Movie, TV

        finder = Find()
        found = (
            finder.find_by_imdb_id(imdb_id)
            if imdb_id
            else finder.find_by_tvdb_id(tvdb_id)
        )
        if not found:
            return None

        if is_movie:
            for candidate in found.get("movie_results") or []:
                air = _movie_air_date(Movie(), candidate, candidate.get("id"))
                if air:
                    return air
            return None

        for candidate in found.get("tv_results") or []:
            air = _tv_air_date(TV(), candidate, season)
            if air:
                return air
        return None
    except Exception as err:
        logger.warning(
            f"上映日期回查失败：{getattr(mediainfo, 'title', '') or subscribe.name}"
            f"（imdb={imdb_id or '-'} tvdb={tvdb_id or '-'}）：{err}"
        )
        return None
