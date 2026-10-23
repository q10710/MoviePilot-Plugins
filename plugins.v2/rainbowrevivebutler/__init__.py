import csv
import io
import json
import math
import re
import time
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pytz
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi.responses import HTMLResponse, Response

from app.core.config import settings
from app.core.event import eventmanager, Event
from app.db.site_oper import SiteOper
from app.helper.downloader import DownloaderHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType


def tier_of(seeders: Optional[int]) -> int:
    """档位（HH 同款）：0-1人=0（高价值）· 2-3人=1 · 4-5人=2 · 6+人=99（摊薄）。"""
    n = seeders or 1
    if n <= 1:
        return 0
    if n <= 3:
        return 1
    if n <= 5:
        return 2
    return 99


class RainbowReviveButler(_PluginBase):
    """
    彩虹岛复活区管家 Q自用版

    基于六个橙子「憨憨保种区管家」(HHClubButler) 架构复刻，致谢原作者 SixOrg。
    彩虹岛（CHDBits）复活区玩法适配：复活区全景 / 免费到期处置 / 复活推送 / 做种到期提醒。

    彩虹岛复活区机制（2026-10 实测）：
    - 发布超 14 天、做种人数低于 10 的种子，会员花 3W 做种积分可列入复活区；
    - 列入后享 7 天免费下载（列表带精确到期时间戳），免盒子（SeedBox）备案规则，
      仍限速 100M/s；无结算、无做种时长考核（与憨憨合约制不同）。
    - 复活区当前无 H3/H5 HR 标记，但保留解析（站点制度 10 月新实施，防御未来打标）。

    核心处置逻辑：巡检/守护线程将下载器中「仍在下载」的任务与复活区列表比对，
    免费到期（+5 分钟缓冲）仍未下完 → HR 种转人工，非 HR 种自动删除任务（可选含文件）；
    到期前 warn_hours 小时预警；预计完不成（按当前速率 ETA 超过剩余免费时长）提前删。

    频率拆分：到期处置由后台守护线程高频执行（默认 5 分钟一次，到期即删）；
    全景/复活推送/自动下载由 Cron 巡检（默认 2 小时一次）驱动。
    """

    plugin_name = "彩虹岛复活区管家Q自用版"
    plugin_desc = "基于六个橙子「憨憨保种区管家」(HHClubButler) 架构复刻，致谢原作者 SixOrg。\n复活区全景 / 免费到期处置 / 复活推送 / 换种优选 / 做种到期提醒"
    plugin_icon = "https://raw.githubusercontent.com/q10710/MoviePilot-Plugins/main/icons/rainbowrevivebutler.png"
    plugin_version = "1.7.6"
    plugin_label = "资源站点"
    plugin_author = "Q"
    author_url = "https://github.com/q10710"
    plugin_config_prefix = "rainbowrevivebutler_"
    plugin_order = 20
    auth_level = 1

    _enabled: bool = False
    _onlyonce: bool = False
    _notify: bool = False
    _cron: str = "0 */2 * * *"
    _site_name: str = "彩虹岛"
    _downloaders: List[str] = []
    _warn_hours: float = 2.0
    _delete_files: bool = True
    _early_delete: bool = True
    _low_seeder_max: int = 3
    _dispose_minutes: float = 5.0
    _auto_clean_days: int = 0
    # 复活自动下载
    _auto_download: bool = False
    _push_downloader: str = ""
    _save_path: str = ""
    _tag: str = ""
    _target_volume: float = 0.0
    _mode: str = "incremental"

    _running: bool = False
    _scheduler: Optional[BackgroundScheduler] = None
    _last_result: str = "尚未运行"
    _cron_hours: float = 0.0
    _last_credit_delta: Optional[float] = None

    _last_overview: Optional[dict] = None
    _last_overview_ts: float = 0.0
    _last_dispose_ts: float = 0.0
    _guard_thread: Optional[threading.Thread] = None
    _guard_stop: Optional[threading.Event] = None

    _GRACE_SEC: int = 300  # 免费到期后缓冲，防时钟偏差误删

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #
    def init_plugin(self, config: dict = None):
        """读取配置并启动服务。"""
        self.stop_service()
        config = config or {}
        self._enabled = bool(config.get("enabled"))
        self._onlyonce = bool(config.get("onlyonce"))
        self._notify = bool(config.get("notify"))
        self._cron = str(config.get("cron") or "0 */2 * * *")
        _cm = re.search(r"\*/(\d+)", self._cron)
        self._cron_hours = float(_cm.group(1)) if _cm else 0.0
        self._site_name = str(config.get("site_name") or "彩虹岛")
        dls = config.get("downloaders")
        self._downloaders = [str(x) for x in dls if str(x).strip()] if isinstance(dls, list) else []
        try:
            _wh = config.get("warn_hours", 2)
            self._warn_hours = max(0.0, float(_wh if _wh not in ("", None) else 2))
        except (TypeError, ValueError):
            self._warn_hours = 2.0
        self._delete_files = bool(config.get("delete_files", True))
        self._early_delete = bool(config.get("early_delete", True))
        try:
            self._low_seeder_max = int(config.get("low_seeder_max", 3) or 0)
        except (TypeError, ValueError):
            self._low_seeder_max = 3
        try:
            self._dispose_minutes = max(1.0, float(config.get("dispose_minutes", 5) or 5))
        except (TypeError, ValueError):
            self._dispose_minutes = 5.0
        try:
            self._auto_clean_days = max(0, min(365, int(config.get("auto_clean_days", 0) or 0)))
        except (TypeError, ValueError):
            self._auto_clean_days = 0
        self._auto_download = bool(config.get("auto_download", False))
        self._push_downloader = str(config.get("push_downloader") or "")
        self._save_path = str(config.get("save_path") or "")
        self._tag = str(config.get("tag") or "")
        try:
            self._target_volume = max(0.0, float(config.get("target_volume", 0) or 0))
        except (TypeError, ValueError):
            self._target_volume = 0.0
        self._mode = config.get("mode") or "incremental"
        if self._mode not in ("incremental", "wash"):
            self._mode = "incremental"

        self._dispose_lock = threading.RLock()
        self._start_guard_thread()

        if self._onlyonce:
            self._onlyonce = False
            _cfg = self.get_config() or {}
            if not isinstance(_cfg, dict):
                _cfg = {}
            _cfg.update({
                "enabled": self._enabled, "onlyonce": False, "notify": self._notify,
                "cron": self._cron, "site_name": self._site_name,
                "downloaders": self._downloaders, "warn_hours": self._warn_hours,
                "delete_files": self._delete_files, "early_delete": self._early_delete,
                "low_seeder_max": self._low_seeder_max, "dispose_minutes": self._dispose_minutes,
                "auto_clean_days": self._auto_clean_days,
                "auto_download": self._auto_download, "push_downloader": self._push_downloader,
                "save_path": self._save_path, "tag": self._tag, "target_volume": self._target_volume,
                "mode": self._mode,
            })
            self.update_config(_cfg)
            try:
                # MP v3 时区属性是 settings.TZ（无 settings.TIMEZONE），与原作者 HHClubButler 一致
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                self._scheduler.add_job(
                    func=self.run, trigger="date", id="RainbowReviveButlerOnce",
                    run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                    name="彩虹岛复活区管家（立即运行）")
                if not self._scheduler.running:
                    self._scheduler.start()
            except Exception as e:
                logger.error(f"彩虹岛复活区管家：一次性任务调度失败：{e}")

        logger.info(
            f"彩虹岛复活区管家配置生效：启用={self._enabled} 通知={self._notify} "
            f"巡检={self._cron} 处置守护={self._dispose_minutes:g}min 站点={self._site_name} "
            f"下载器={self._downloaders} 预警={self._warn_hours:g}h 删文件={self._delete_files} "
            f"提前删={self._early_delete} 复活阈值={self._low_seeder_max}人 "
            f"自动下载={self._auto_download}(→{self._push_downloader or '-'}, "
            f"体积上限={self._target_volume:g}GB 模式={self._mode}")

    def get_state(self) -> bool:
        """返回插件启用状态。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """聊天命令：/rainbow_rescue 立即触发巡检。"""
        return [{
            "cmd": "/rainbow_rescue",
            "event": EventType.PluginAction,
            "desc": "彩虹岛复活区管家巡检",
            "category": "插件命令",
            "data": {"action": "rainbow_revive_run"}
        }]

    @eventmanager.register(EventType.PluginAction)
    def event_handler(self, event: Event) -> None:
        """接收 /rainbow_rescue 命令：异步触发巡检（与 api_run 同路径）。

        分身限制：MP 插件命令的 action 来自静态 get_command，无法按实例路由——
        若创建分身（plugin.clone），一条命令会触发本体与全部分身一起巡检；
        数据/配置本身按实例隔离不串。
        """
        if not event or not event.event_data:
            return
        if event.event_data.get("action") != "rainbow_revive_run":
            return
        if self._running:
            logger.info("彩虹岛复活区管家正在运行中，跳过命令触发")
            return
        threading.Thread(target=self.run, daemon=True).start()

    def stop_service(self):
        """停止调度器与守护线程。"""
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error(f"彩虹岛复活区管家：停止调度器失败：{e}")
        try:
            if self._guard_stop:
                self._guard_stop.set()
            if self._guard_thread and self._guard_thread.is_alive():
                self._guard_thread.join(timeout=3)
        except Exception as e:
            logger.error(f"彩虹岛复活区管家：停止守护线程失败：{e}")
        finally:
            self._guard_stop = None
            self._guard_thread = None

    def get_service(self) -> List[Dict[str, Any]]:
        """注册全景/复活/自动下载巡检（到期处置由守护线程负责）。"""
        if not self.get_state():
            return []
        try:
            trigger = CronTrigger.from_crontab(self._cron)
        except Exception as e:
            logger.error(f"彩虹岛复活区管家：cron 无效（{self._cron}）：{e}，回退 0 */2 * * *")
            trigger = CronTrigger.from_crontab("0 */2 * * *")
        return [{
            "id": "RainbowReviveButler",
            "name": "彩虹岛复活区管家巡检",
            "trigger": trigger,
            "func": self.run,
            "kwargs": {}
        }]

    # ------------------------------------------------------------------ #
    # 站点层：凭证零配置（自动读 MP 站点管理），页面解析
    # ------------------------------------------------------------------ #
    def _get_mp_site(self):
        """按站名/域名匹配 MP 站点管理中的彩虹岛。"""
        try:
            kw = (self._site_name or "").strip().lower()
            for site in SiteOper().list_active():
                name = (site.name or "").lower()
                url = (site.url or "").lower()
                domain = (site.domain or "").lower()
                if (kw and kw in name) or "ptchdbits" in url or "ptchdbits" in domain:
                    return site
        except Exception as e:
            logger.error(f"彩虹岛复活区管家：读取 MP 站点配置失败：{e}")
        return None

    def _get_site_cookie(self) -> str:
        """自动获取站点 Cookie。"""
        site = self._get_mp_site()
        return (site.cookie or "") if site else ""

    def _get_site_url(self) -> str:
        """自动获取站点 URL。"""
        site = self._get_mp_site()
        return (site.url or "").rstrip("/") if site else ""

    def _session(self, cookie: str) -> requests.Session:
        """构建带 Cookie 的会话。"""
        s = requests.Session()
        s.headers["User-Agent"] = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
        for part in cookie.split(";"):
            if "=" in part:
                k, v = part.split("=", 1)
                s.cookies.set(k.strip(), v.strip())
        return s

    @staticmethod
    def _get_with_retry(session, url, timeout=30, retries=2, backoff=(3, 5)):
        """带重试的 GET（HH 同款）：网络异常/5xx/429 重试并退避；
        其余状态码原样返回交调用方按现状处理，不破坏既有优雅降级路径。"""
        last_exc = None
        r = None
        for i in range(retries + 1):
            try:
                r = session.get(url, timeout=timeout)
                if r.status_code == 429 or r.status_code >= 500:
                    last_exc = None  # 服务端瞬时错误：记住响应等退避重试
                else:
                    return r
            except Exception as e:
                last_exc = e
                r = None
            if i < retries:
                wait = backoff[i] if i < len(backoff) else 5
                time.sleep(wait)
        if last_exc is not None:
            raise last_exc
        return r

    @staticmethod
    def _parse_ts(text: str) -> Optional[float]:
        """把站点时间（Asia/Shanghai）解析为 epoch 秒。"""
        try:
            return pytz.timezone("Asia/Shanghai").localize(
                datetime.strptime(text.strip(), "%Y-%m-%d %H:%M:%S")).timestamp()
        except Exception:
            return None

    @staticmethod
    def _norm_title(s) -> str:
        """标题归一化（与任务名比对用）。"""
        if not s:
            return ""
        return re.sub(r"[^0-9a-z一-鿿]", "", str(s).lower())

    def _fuzzy_match_norm(self, norm: str, by_norm_long: Dict[str, dict]) -> Optional[dict]:
        """模糊标题兜底（HH 同款互含，窄用）：norm 与站点 norm 均≥15 字符且互含时匹配；
        仅唯一命中才返回（防多义误配），多义/无命中返回 None。"""
        if not norm or len(norm) < 15:
            return None
        hit_sd = None
        hits = 0
        for tnorm, sd in by_norm_long.items():
            if len(tnorm) >= 15 and (norm in tnorm or tnorm in norm):
                hits += 1
                if hits > 1:
                    return None
                hit_sd = sd
        return hit_sd

    @staticmethod
    def _parse_size_gb(text: str) -> float:
        """把 '46.36<br />GB' 这类单元格解析为 GB。"""
        m = re.search(r">([\d.]+)<br\s*/?>\s*(GB|MB|TB|KB)", text, re.I)
        if not m:
            m = re.search(r"([\d.]+)\s*(GB|MB|TB|KB)", text, re.I)
        if not m:
            return 0.0
        val = float(m.group(1))
        unit = m.group(2).upper()
        return {"TB": val * 1024, "GB": val, "MB": val / 1024, "KB": val / 1024 / 1024}.get(unit, 0.0)

    def _parse_revive_page(self, html: str) -> List[dict]:
        """解析复活区列表页，提取种子行字段。"""
        seeds: List[dict] = []
        seen = set()
        for m in re.finditer(r"(?<!user)details\.php\?id=(\d+)", html):
            tid = m.group(1)
            if tid in seen:
                continue
            start = html.rfind("<tr", 0, m.start())
            tbl_end = html.find("</table>", m.end())
            if start == -1 or tbl_end == -1:
                continue
            end = html.find("</tr>", tbl_end)
            if end == -1:
                continue
            row = html[start:end]
            if "rowfollow" not in row:
                continue
            seen.add(tid)
            tm = re.search(r'<a title="([^"]+)"\s+href="details\.php\?id=' + tid, row)
            title = tm.group(1).strip() if tm else ""
            size_gb = self._parse_size_gb(row)
            sm = re.search(r'<span class="(red|green)">(\d+)</span>', row)
            seeders = int(sm.group(2)) if sm else 0
            lm = re.search(r'#leechers">(\d+)</a>', row)
            leechers = int(lm.group(1)) if lm else 0
            is_free = "pro_free" in row
            free_until = None
            if is_free:
                fm = re.search(r"限时\s*<span title=\"([^\"]+)\"", row)
                if fm:
                    free_until = self._parse_ts(fm.group(1))
            plain = re.sub(r"<[^>]+>", " ", row)
            hr = bool(re.search(r"\bH[35]\b", plain))
            if not title:
                continue
            seeds.append({
                "tid": tid, "title": title, "norm": self._norm_title(title),
                "size_gb": round(size_gb, 2), "seeders": seeders, "leechers": leechers,
                "is_free": is_free, "free_until": free_until, "hr": hr,
            })
        return seeds

    def _fetch_revive_list(self, session: requests.Session, base: str) -> Tuple[List[dict], int]:
        """拉取复活区全部分页（上限 10 页防失控）；首页空时重试一次防瞬时限流。"""
        all_seeds: List[dict] = []
        seen = set()
        page = 0
        for attempt in range(2):
            all_seeds = []
            seen = set()
            for page in range(10):
                url = f"{base}/renewtorrents.php?page={page}" if page else f"{base}/renewtorrents.php"
                try:
                    r = self._get_with_retry(session, url, timeout=30)
                    if r.status_code != 200:
                        break
                except Exception as e:
                    if page == 0:
                        raise
                    logger.warning(f"彩虹岛复活区管家：第 {page} 页拉取失败：{e}")
                    break
                seeds = self._parse_revive_page(r.text)
                new = [s for s in seeds if s["tid"] not in seen]
                for s in new:
                    seen.add(s["tid"])
                all_seeds.extend(new)
                if len(new) == 0:
                    break
                time.sleep(0.5)  # 页间限速（HH 同款），防站点限流
            if all_seeds or attempt == 1:
                break
            # 首页拿到 0 个种子：可能是站点瞬时限流/返回登录页，等 5 秒重试一次
            logger.warning("彩虹岛复活区管家：复活区列表为空，5 秒后重试一次（防瞬时限流）")
            time.sleep(5)
        return all_seeds, page + 1

    def _fetch_my_seeding_ids(self, session: requests.Session, base: str) -> Optional[set]:
        """spstate=1 视图 = 我在做种的复活区种子。

        返回 None 表示拉取失败（与「真没做种」的空集区分），调用方据此中止自动下载防重复。
        """
        ids: set = set()
        try:
            for page in range(5):
                url = base + "/renewtorrents.php?spstate=1" + (f"&page={page}" if page else "")
                r = self._get_with_retry(session, url, timeout=30)
                # (?<!user) 排除 userdetails.php?id= 用户链接（其尾部与 details.php?id= 同形，会误抓页头/上传者 ID）
                found = set(re.findall(r"(?<!user)details\.php\?id=(\d+)", r.text))
                new = found - ids
                if not new:
                    break
                ids |= new
                time.sleep(0.5)  # 页间限速（HH 同款），防站点限流
            return ids
        except Exception as e:
            logger.warning(f"彩虹岛复活区管家：拉取我的做种视图失败：{e}")
            return None

    def _get_site_uid(self, session: requests.Session, base: str) -> Optional[str]:
        """从首页解析当前登录用户 UID（做种组合接口需要）。"""
        try:
            r = self._get_with_retry(session, base + "/", timeout=20)
            m = re.search(r"userdetails\.php\?id=(\d+)", r.text)
            return m.group(1) if m else None
        except Exception as e:
            logger.warning(f"彩虹岛复活区管家：解析 UID 失败：{e}")
            return None

    @staticmethod
    def _parse_size_cell(text: str) -> float:
        """把 '1.13<br />GB' 这类单元格解析为 GB（做种组合用，兼容 MB/KB/TB）。"""
        m = re.search(r">([\d.]+)<br\s*/?>\s*(GB|MB|TB|KB)", text, re.I)
        if not m:
            m = re.search(r"([\d.]+)\s*(GB|MB|TB|KB)", text, re.I)
        if not m:
            return 0.0
        val = float(m.group(1))
        unit = m.group(2).upper()
        return {"TB": val * 1024, "GB": val, "MB": val / 1024, "KB": val / 1024 / 1024}.get(unit, 0.0)

    @staticmethod
    def _parse_data_size(text: str) -> float:
        """把 '2.13<br />MB' 上传量解析为 GB。"""
        return RainbowReviveButler._parse_size_cell(text)

    def _fetch_site_stats(self, session: requests.Session, base: str) -> dict:
        """从页头解析账户级数据：做种积分/魔力/分享率/上传/下载/当前做种数。"""
        try:
            r = self._get_with_retry(session, base + "/", timeout=20)
            h = r.text
            def grab(pat, default=None):
                m = re.search(pat, h)
                return m.group(1).replace(",", "") if m else default
            stats = {
                "credit": float(grab(r"做种积分[^0-9]{0,10}([\d][\d,.]*)", 0) or 0),
                "magic": float(grab(r"魔力值.*?[:：]\s*([\d,]+)", 0) or 0),
                "ratio": float(grab(r"分享率[:：][^0-9]*?([\d.]+)", 0) or 0),
                "uploaded_tb": self._parse_updown(grab(r"上传量[:：][^0-9]*?([\d.]+\s*[TGM]B)")),
                "downloaded_tb": self._parse_updown(grab(r"下载量[:：][^0-9]*?([\d.]+\s*[TGM]B)")),
                "seeding_now": int(grab(r"当前活动[:：].*?arrowup[^>]*>\s*(\d+)", 0) or 0),
                "leeching_now": int(grab(r"当前活动[:：].*?arrowdown[^>]*>\s*(\d+)", 0) or 0),
                "hr": int(grab(r"H&R[:：][^0-9]*?(\d+)", 0) or 0),
            }
            return stats
        except Exception as e:
            logger.warning(f"彩虹岛复活区管家：解析账户统计失败：{e}")
            return {}

    @staticmethod
    def _parse_updown(text: Optional[str]) -> float:
        """把 '6.531 TB' 解析为 TB。"""
        if not text:
            return 0.0
        m = re.search(r"([\d.]+)\s*([TGM])B", text, re.I)
        if not m:
            return 0.0
        val = float(m.group(1))
        unit = m.group(2).upper()
        return {"T": val, "G": val / 1024, "M": val / 1024 / 1024}.get(unit, 0.0)

    def _fetch_seeding_portfolio(self, session: requests.Session, base: str) -> List[dict]:
        """取全量做种组合（getusertorrentlistajax.php），每条含大小/做种数/上传贡献/下载/分享率。"""
        uid = self._get_site_uid(session, base)
        if not uid:
            return []
        try:
            r = self._get_with_retry(session, base + f"/getusertorrentlistajax.php?userid={uid}&type=seeding", timeout=30)
            h = r.text
        except Exception as e:
            logger.warning(f"彩虹岛复活区管家：拉取做种组合失败：{e}")
            return []
        out: List[dict] = []
        # AJAX 做种列表是扁平 <tr>...</tr>（无嵌套 torrentname 表），直接按整行正则切分
        for row in re.findall(r"<tr>(?:(?!</tr>).)*?details\.php\?id=\d+(?:(?!</tr>).)*?</tr>", h, re.S):
            if "rowfollow" not in row:
                continue
            tm_tid = re.search(r'details\.php\?id=(\d+)', row)
            tds = re.findall(r"<td[^>]*>(?:(?!</td>).)*?</td>", row, re.S)
            if len(tds) < 8 or not tm_tid:
                continue
            def cell(i):
                return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", tds[i])).strip()
            tm = re.search(r'title="([^"]+)"', tds[1])
            size_gb = self._parse_size_cell(tds[2])
            try:
                seeders = int(cell(3) or 0)
            except ValueError:
                seeders = 0
            upload_gb = self._parse_data_size(tds[5])
            try:
                ratio = float(re.sub(r"[^0-9.]", "", cell(7)) or 0)
            except ValueError:
                ratio = 0.0
            title = tm.group(1) if tm else ""
            if not title:
                continue
            out.append({
                "tid": tm_tid.group(1), "title": title, "size_gb": round(size_gb, 2),
                "seeders": seeders, "upload_gb": round(upload_gb, 3), "ratio": ratio,
            })
        return out

    # ------------------------------------------------------------------ #
    # 下载器层
    # ------------------------------------------------------------------ #
    def _get_manage_services(self) -> dict:
        """获取配置的（受管控）下载器服务。"""
        if not self._downloaders:
            return {}
        out = {}
        try:
            all_sv = DownloaderHelper().get_services()
        except Exception:
            all_sv = None
        for name in self._downloaders:
            sv = all_sv.get(name) if all_sv else None
            if sv is None:
                try:
                    got = DownloaderHelper().get_services(name_filters=[name])
                    sv = got.get(name) if got else None
                except Exception:
                    sv = None
            if sv and getattr(sv, "instance", None):
                out[name] = sv
        return out

    def _get_push_service(self):
        """获取推送（自动下载）下载器服务。"""
        if not self._push_downloader:
            return None
        try:
            sv = DownloaderHelper().get_services(name_filters=[self._push_downloader])
            sv = sv.get(self._push_downloader) if sv else None
            if sv and getattr(sv, "instance", None):
                return sv
        except Exception:
            pass
        return None

    @staticmethod
    def _tf(t, *names, default=None):
        """跨 qb/tr 读任务字段（dict 或对象）。"""
        for n in names:
            v = t.get(n) if isinstance(t, dict) else getattr(t, n, None)
            if v is not None:
                return v
        return default

    @staticmethod
    def _thash(t) -> str:
        """读任务 hash（qb=hash / tr=hashString）。"""
        if isinstance(t, dict):
            return str(t.get("hash") or t.get("hashString") or "")
        return str(getattr(t, "hash", None) or getattr(t, "hashString", "") or "")

    def _get_site_domain(self) -> str:
        """取站点主机名（如 ptchdbits.co），供下载器 comment/tracker 过滤用。"""
        try:
            from urllib.parse import urlparse
            host = urlparse(self._get_site_url() or "").hostname or ""
            return host.lower()
        except Exception:
            return ""

    @staticmethod
    def _extract_seed_id_from_comment(comment: str) -> str:
        """从下载器种子 comment 提取站点种子ID（形如 .../details.php?id=12345）。

        通用正则（不限域名）：NexusPHP 站点常把详情页 URL 写进种子 comment，
        提取到的数字即站点 tid，可作精确匹配主键。未命中返回空串。
        """
        try:
            m = re.search(r"details\.php\?id=(\d+)", comment or "", re.I)
            return m.group(1) if m else ""
        except Exception:
            return ""

    def _get_downloader_seed_map(self, svcs: dict) -> Dict[str, dict]:
        """从受管下载器种子 comment 提取 {种子ID(tid): {hash, name, downloader, progress, speed, eta, save_path, state}}，零站点请求。

        TR：一次 RPC 带 comment 字段；QB：先按站点 tracker 过滤，再定向查 properties 的 comment
        （避免对全部种子逐个发 properties 请求）。comment 未带 tid 的站点返回空映射，
        调用方回退标题匹配，零回归。
        """
        out: Dict[str, dict] = {}
        domain = self._get_site_domain()
        for dl_name, sv in (svcs or {}).items():
            dl_type = ""
            try:
                dl_type = str(getattr(sv, "type", "") or getattr(getattr(sv, "config", None), "type", "") or "").lower()
            except Exception:
                dl_type = ""
            try:
                if "transmission" in dl_type:
                    trc = getattr(sv.instance, "trc", None)
                    if trc is None:
                        continue
                    tts = trc.get_torrents(arguments=["id", "name", "hashString", "comment", "percentDone",
                                                      "rateDownload", "eta", "downloadDir", "status"])
                    for t in tts or []:
                        try:
                            sid = self._extract_seed_id_from_comment(str(t.get("comment") or ""))
                            if sid:
                                out.setdefault(sid, {
                                    "hash": str(t.get("hashString") or ""),
                                    "name": str(t.get("name") or ""),
                                    "downloader": dl_name,
                                    "progress": float(t.get("percentDone") or 0),
                                    "speed": int(t.get("rateDownload") or 0),
                                    "eta": int(t.get("eta") or 0),
                                    "save_path": str(t.get("downloadDir") or ""),
                                    "state": str(t.get("status") or ""),
                                })
                        except Exception:
                            continue
                elif "qbittorrent" in dl_type:
                    qbc = getattr(sv.instance, "qbc", None)
                    if qbc is None:
                        continue
                    if not domain:
                        continue  # 站点域名缺失时跳过 comment 查询（防对全部种子发 properties 请求风暴），回退标题匹配
                    infos = qbc.torrents_info()
                    for it in infos or []:
                        try:
                            tracker = str(it.get("tracker") or "").lower()
                            if domain and domain not in tracker:
                                continue  # 非本站 tracker 不查 comment（省请求）
                            h = str(it.get("hash") or "")
                            if not h:
                                continue
                            comment = ""
                            try:
                                p = qbc.torrents_properties(torrent_hash=h)
                                comment = str(p.get("comment") or "") if isinstance(p, dict) \
                                    else str(getattr(p, "comment", "") or "")
                            except Exception:
                                comment = ""
                            sid = self._extract_seed_id_from_comment(comment)
                            if sid:
                                out.setdefault(sid, {
                                    "hash": h,
                                    "name": str(it.get("name") or ""),
                                    "downloader": dl_name,
                                    "progress": float(it.get("progress") or 0),
                                    "speed": int(it.get("dlspeed") or 0),
                                    "eta": int(it.get("eta") or 0),
                                    "save_path": str(it.get("save_path") or ""),
                                    "state": str(it.get("state") or ""),
                                })
                        except Exception:
                            continue
            except Exception as e:
                logger.warning(f"彩虹岛复活区管家：下载器 {dl_name} comment 提取种子ID失败：{e}")
                continue
        return out

    def _clean_stale_downloads(self, seeds: List[dict]) -> int:
        """清理僵尸任务：本站 tracker 或标题命中复活列表 + 未完成 + 超 auto_clean_days 天 → 连文件删除。

        仅在 auto_clean_days>0 时被调用；按下载器分组批量删，删除后各自复核计数。
        """
        if not self._auto_clean_days or self._auto_clean_days <= 0:
            return 0
        svcs = self._get_manage_services()
        if not svcs:
            return 0
        domain = self._get_site_domain()
        try:
            site_titles = {self._norm_title(s.get("title") or "") for s in (seeds or [])}
        except Exception:
            site_titles = set()
        now = time.time()
        threshold = self._auto_clean_days * 86400
        total_deleted = 0
        per_dl: Dict[str, int] = {}
        for name, sv in svcs.items():
            try:
                torrents, error = sv.instance.get_torrents()
            except Exception:
                continue
            if error or not torrents:
                continue
            stale = []
            for t in torrents:
                try:
                    tname = str(self._tf(t, "name", default="") or "")
                    progress = self._tf(t, "progress", "percentDone", default=0) or 0
                    added = self._tf(t, "added_on", "added_time", "added_date", default=0) or 0
                    thash = self._thash(t)
                    tracker = str(self._tf(t, "tracker", "trackerList", default="") or "").lower()
                except Exception:
                    continue
                is_site = bool(domain and domain in tracker)
                if not is_site and site_titles:
                    n = self._norm_title(tname)
                    if n and n in site_titles:
                        is_site = True
                if not is_site:
                    continue
                try:
                    progress = float(progress)
                except (TypeError, ValueError):
                    progress = 1.0
                if progress > 1.0:
                    progress /= 100.0
                if progress >= 1.0:
                    continue  # 已完成（在保）不动
                try:
                    if hasattr(added, "timestamp"):
                        added = added.timestamp()
                    added = float(added)
                except (TypeError, ValueError, OSError, OverflowError):
                    added = 0.0
                if added <= 0:
                    continue
                if now - added >= threshold and thash:
                    stale.append((thash, tname))
            if not stale:
                continue
            ids = [th for th, _ in stale]
            try:
                sv.instance.delete_torrents(delete_file=self._delete_files, ids=ids)
                # 复核：仍存在的不算已删
                ts, err = sv.instance.get_torrents()
                remain = set()
                if not err and ts:
                    remain = {self._thash(x) for x in ts}
                done = sum(1 for th in ids if th not in remain)
                total_deleted += done
                per_dl[name] = done
            except Exception as e:
                logger.warning(f"彩虹岛复活区管家：僵尸任务清理（下载器 {name}）失败：{e}")
        if per_dl:
            detail = "，".join(f"{k} {v} 个" for k, v in per_dl.items())
            logger.info(f"彩虹岛复活区管家：僵尸清理——删除 {total_deleted} 个超 {self._auto_clean_days} 天未完成任务"
                        f"（含文件={self._delete_files}，{detail}）")
        return total_deleted

    # ------------------------------------------------------------------ #
    # 核心：到期处置（守护线程与巡检共用）
    # ------------------------------------------------------------------ #
    def _has_incomplete_tasks(self) -> bool:
        """受管控下载器里是否存在未完成（下载中）任务——决定守护线程是否值得拉站点。"""
        services = self._get_manage_services()
        if not services:
            return False
        for dl_name, sv in services.items():
            try:
                torrents, error = sv.instance.get_torrents()
                if error:
                    # 查询失败保守认为有任务（走完整流程），防下载器瞬断时到期处置静默失效
                    return True
                if not torrents:
                    continue
                for t in torrents:
                    progress = float(self._tf(t, "progress", "percentDone", default=0) or 0)
                    if progress < 1.0:
                        return True
            except Exception:
                # 查不清时保守认为有（走完整流程）
                return True
        return False

    def _dispose_pass(self, notify: bool = True, quick: bool = False) -> dict:
        """一轮到期处置：拉列表 → 比对下载器未完成任务 → 到期删/预警/提前删。

        quick=True（守护线程专用）：先查下载器有无未完成任务，没有则跳过站点抓取，
        避免每 5 分钟空拉 3 页给站点造成压力（曾触发过一次站点限流返回空列表）。
        quick=False（cron 巡检）：始终全量抓取（看板需要数据）。
        返回统计 dict。异常向上抛由调用方处理。
        """
        if not self._dispose_lock.acquire(blocking=False):
            # 另一轮处置进行中：跳过本轮，防 dedupe/日志并发丢写与双通知
            # skipped 标记让巡检方区分「跳过」与「真空」，不覆盖看板快照
            return {"seeds": [], "pages": 0, "deleted": [], "hr_skipped": [],
                    "warned": [], "early_deleted": [], "delete_failed": [], "skipped": True}
        try:
            return self._dispose_pass_locked(notify=notify, quick=quick)
        finally:
            self._dispose_lock.release()

    def _dispose_pass_locked(self, notify: bool = True, quick: bool = False) -> dict:
        base = self._get_site_url()
        cookie = self._get_site_cookie()
        if not base or not cookie:
            raise Exception("未找到彩虹岛站点配置或 Cookie（请在站点管理中配置）")
        if quick and not self._has_incomplete_tasks():
            return {"seeds": [], "pages": 0, "deleted": [], "hr_skipped": [],
                    "warned": [], "early_deleted": [], "delete_failed": []}
        session = self._session(cookie)
        seeds, pages = self._fetch_revive_list(session, base)
        if not seeds:
            raise Exception("复活区列表为空（Cookie 失效或页面结构变化）")
        by_norm: Dict[str, dict] = {}
        for s in seeds:
            by_norm.setdefault(s["norm"], s)

        now = time.time()
        warn_sec = self._warn_hours * 3600
        dedupe = self.get_data("notify_dedupe") or {}
        cutoff = now - 30 * 86400
        dedupe = {k: v for k, v in dedupe.items() if isinstance(v, (int, float)) and v > cutoff}

        deleted: List[dict] = []
        hr_skipped: List[dict] = []
        warned: List[dict] = []
        early_deleted: List[dict] = []
        delete_failed: List[dict] = []

        services = self._get_manage_services()
        for dl_name, sv in services.items():
            try:
                torrents, error = sv.instance.get_torrents()
            except Exception as e:
                logger.warning(f"彩虹岛复活区管家：下载器 {dl_name} 查询失败：{e}")
                continue
            if error or torrents is None:
                continue
            for t in torrents:
                try:
                    tname = str(self._tf(t, "name", default="") or "")
                    seed = by_norm.get(self._norm_title(tname))
                    if not seed:
                        continue
                    progress = float(self._tf(t, "progress", "percentDone", default=0) or 0)
                    thash = self._thash(t)
                    if progress >= 1.0:
                        continue  # 已完成在做种，不碰
                    free_until = seed.get("free_until")
                    if not free_until:
                        continue
                    remain_sec = free_until - now
                    if remain_sec <= -self._GRACE_SEC:
                        # 免费已到期（+缓冲）：非 HR 删，HR 转人工
                        if seed.get("hr"):
                            key = f"hr:{seed['tid']}:{int(free_until)}"
                            if key not in dedupe:
                                dedupe[key] = now
                                hr_skipped.append(seed)
                            continue
                        if not self._recheck_incomplete(sv, thash):
                            continue
                        ok = self._delete_task(sv, thash, dl_name)
                        entry = {"time": int(now), "tid": seed["tid"], "title": seed["title"],
                                 "size_gb": seed["size_gb"], "progress": round(progress * 100, 1),
                                 "downloader": dl_name, "action": "expired_delete", "ok": ok}
                        self._append_disposal(entry)
                        if ok:
                            deleted.append(seed)
                        else:
                            fkey = f"delfail:{seed['tid']}:{int(free_until)}"
                            if fkey not in dedupe:
                                dedupe[fkey] = now
                                delete_failed.append(seed)
                        logger.info(f"彩虹岛复活区管家：到期删除 {seed['title'][:50]}（{dl_name}，进度 {progress*100:.0f}%）成功={ok}")
                        continue
                    if 0 < remain_sec <= warn_sec:
                        key = f"warn:{seed['tid']}:{int(free_until)}"
                        if key not in dedupe:
                            dedupe[key] = now
                            warned.append({**seed, "progress_pct": round(progress * 100, 1),
                                           "remain_min": round(remain_sec / 60)})
                        if self._early_delete:
                            speed = float(self._tf(t, "dlspeed", "rateDownload", "downloadSpeed", default=0) or 0)
                            size_b = float(self._tf(t, "size", "totalSize", default=0) or 0)
                            if speed > 0 and size_b > 0:
                                eta = size_b * (1 - progress) / speed
                                if eta > remain_sec and not seed.get("hr"):
                                    if not self._recheck_incomplete(sv, thash):
                                        continue
                                    ok = self._delete_task(sv, thash, dl_name)
                                    entry = {"time": int(now), "tid": seed["tid"], "title": seed["title"],
                                             "size_gb": seed["size_gb"], "progress": round(progress * 100, 1),
                                             "downloader": dl_name, "action": "early_delete", "ok": ok}
                                    self._append_disposal(entry)
                                    if ok:
                                        early_deleted.append(seed)
                                    else:
                                        fkey = f"delfail:{seed['tid']}:{int(free_until)}"
                                        if fkey not in dedupe:
                                            dedupe[fkey] = now
                                            delete_failed.append(seed)
                                    logger.info(f"彩虹岛复活区管家：预计完不成提前删 {seed['title'][:50]}（ETA {eta/60:.0f}min > 剩余 {remain_sec/60:.0f}min）成功={ok}")
                except Exception as e:
                    logger.warning(f"彩虹岛复活区管家：处理任务出错：{e}")
                    continue

        self.save_data("notify_dedupe", dedupe)
        stats = {"seeds": seeds, "pages": pages, "deleted": deleted, "hr_skipped": hr_skipped,
                 "warned": warned, "early_deleted": early_deleted, "delete_failed": delete_failed}
        if notify and self._notify:
            self._notify_disposal(deleted, hr_skipped, warned, early_deleted, delete_failed)
        return stats

    def _recheck_incomplete(self, sv, thash: str) -> bool:
        """删除前复核：任务仍存在且未完成才返回 True；查询失败返回 False（本轮不删）。"""
        try:
            ts, err = sv.instance.get_torrents(ids=thash)
            if err or not ts:
                return False
            return float(self._tf(ts[0], "progress", "percentDone", default=0) or 0) < 1.0
        except Exception:
            return False

    def _delete_task(self, sv, thash: str, dl_name: str) -> bool:
        """删除下载器任务（可选含数据文件），复核不存在才算成功。"""
        try:
            sv.instance.delete_torrents(delete_file=self._delete_files, ids=[thash])
            ts, err = sv.instance.get_torrents(ids=thash)
            if err:
                logger.warning(f"彩虹岛复活区管家：任务 {thash[:12]} 删除后复核查询失败，按未删除处理（{dl_name}）")
                return False
            if ts:
                logger.warning(f"彩虹岛复活区管家：任务 {thash[:12]} 删除后仍存在（{dl_name}）")
                return False
            return True
        except Exception as e:
            logger.error(f"彩虹岛复活区管家：删除任务 {thash[:12]} 失败（{dl_name}）：{e}")
            return False

    def _append_disposal(self, entry: dict):
        """记录处置日志（上限 200 条）。持锁读改写，防守护线程/巡检并发丢条目。"""
        try:
            with self._dispose_lock:
                log = self.get_data("disposal_log") or []
                if not isinstance(log, list):
                    log = []
                log.append(entry)
                self.save_data("disposal_log", log[-200:])
        except Exception as e:
            logger.warning(f"彩虹岛复活区管家：写处置日志失败：{e}")

    def _notify_disposal(self, deleted, hr_skipped, warned, early_deleted, delete_failed=None):
        """到期处置相关通知（逐类一条，不刷屏）。"""
        try:
            if deleted or early_deleted:
                lines = [f"  {s['title'][:60]}（{s['size_gb']:.1f}GB）" for s in (deleted + early_deleted)[:15]]
                more = len(deleted) + len(early_deleted) - 15
                if more > 0:
                    lines.append(f"  …等共 {len(deleted)+len(early_deleted)} 个")
                self.post_message(
                    title=f"【彩虹岛复活区管家】免费到期已删除 {len(deleted)+len(early_deleted)} 个任务",
                    text=("删除含数据文件" if self._delete_files else "仅删任务") + "，避免转为计费下载：\n" + "\n".join(lines))
            if hr_skipped:
                lines = [f"  {s['title'][:60]}" for s in hr_skipped[:10]]
                self.post_message(
                    title=f"【彩虹岛复活区管家】{len(hr_skipped)} 个 HR 种到期未删，转人工",
                    text="HR 种不自动删除，请手动决定：\n" + "\n".join(lines))
            if delete_failed:
                try:
                    lines = [f"  {s['title'][:60]}（{s['size_gb']:.1f}GB）" for s in delete_failed[:10]]
                    self.post_message(
                        title=f"【彩虹岛复活区管家】{len(delete_failed)} 个到期删除失败，请人工处理",
                        text="下载器拒绝或任务锁定，任务仍在计费下载：\n" + "\n".join(lines))
                except Exception:
                    pass
            if warned:
                lines = [f"  {s['title'][:50]}（进度 {s.get('progress_pct', '?')}%，剩 {s.get('remain_min', '?')} 分钟）"
                         for s in warned[:10]]
                self.post_message(
                    title=f"【彩虹岛复活区管家】{len(warned)} 个免费下载即将到期",
                    text=f"到期后仍未下完将自动删除（{self._warn_hours:g} 小时预警）：\n" + "\n".join(lines))
        except Exception as e:
            logger.error(f"彩虹岛复活区管家：发送通知失败：{e}")

    # ------------------------------------------------------------------ #
    # 巡检：全景 + 复活推送 + 做种到期提醒 + 自动下载
    # ------------------------------------------------------------------ #
    def run(self):
        """巡检入口（Cron 驱动）。"""
        if self._running:
            logger.info("彩虹岛复活区管家正在运行中，跳过本次触发")
            return
        self._running = True
        try:
            self._do_run()
        except Exception as e:
            logger.error(f"彩虹岛复活区管家运行异常：{e}", exc_info=True)
            self._last_result = f"运行失败：{e}"
            self._save_log([f"运行异常：{e}"])
            if self._notify:
                try:
                    self.post_message(title="【彩虹岛复活区管家】运行异常", text=str(e))
                except Exception:
                    pass
        finally:
            self._running = False

    def _do_run(self):
        """巡检主逻辑：到期处置 + 全景 + 复活 + 做种到期提醒 + 自动下载。"""
        t0 = time.time()
        stats = self._dispose_pass(notify=self._notify)
        if stats.get("skipped"):
            self._last_result = f"{datetime.now().strftime('%m-%d %H:%M')} 上一轮处置进行中，本轮跳过"
            logger.info(f"彩虹岛复活区管家：{self._last_result}")
            self._save_log([f"跳过：{self._last_result}"])
            return
        seeds = stats["seeds"]
        base = self._get_site_url()
        session = self._session(self._get_site_cookie())
        my_ids_raw = self._fetch_my_seeding_ids(session, base)
        # None=拉取失败（与空集区分）：本轮中止自动下载，防止把手动保种的复活种重复下载
        my_ids_ok = my_ids_raw is not None
        my_ids = my_ids_raw if my_ids_ok else set()
        if not my_ids_ok:
            logger.warning("彩虹岛复活区管家：我的做种视图拉取失败，本轮中止自动下载（防重复下载手动保种）")
        acct_stats = self._fetch_site_stats(session, base)
        portfolio = self._fetch_seeding_portfolio(session, base)
        # 做种积分增速
        try:
            prev = self.get_data("credit_sample") or {}
            cur_credit = acct_stats.get("credit", 0)
            if prev.get("credit") and prev.get("ts") and cur_credit:
                dt_h = (time.time() - prev["ts"]) / 3600
                if 0.01 < dt_h < 24 * 7:
                    acct_stats["credit_rate"] = round((cur_credit - prev["credit"]) / dt_h, 1)
                self._last_credit_delta = round(cur_credit - prev["credit"], 1)
            if cur_credit:
                self.save_data("credit_sample", {"credit": cur_credit, "ts": time.time()})
                self._append_credit_sample(
                    cur_credit,
                    magic=acct_stats.get("magic", 0) or 0,
                    downloaded_gb=(acct_stats.get("downloaded_tb", 0) or 0) * 1024,
                    ratio=acct_stats.get("ratio", 0) or 0,
                    uploaded_gb=(acct_stats.get("uploaded_tb", 0) or 0) * 1024)
        except Exception:
            pass
        portfolio_stats = self._analyze_portfolio(portfolio)
        now = time.time()
        warn_sec = self._warn_hours * 3600

        with self._dispose_lock:
            # 去重表读改写持锁（与守护线程到期处置共用锁，防并发丢键导致重复推送）
            dedupe = self.get_data("notify_dedupe") or {}
            # 做种到期提醒（只提醒不动作）
            seeding_expiring = []
            for s in seeds:
                if s["tid"] in my_ids and s.get("free_until"):
                    remain = s["free_until"] - now
                    if 0 < remain <= warn_sec:
                        key = f"seedexp:{s['tid']}:{int(s['free_until'])}"
                        if key not in dedupe:
                            dedupe[key] = now
                            seeding_expiring.append({**s, "remain_min": round(remain / 60)})
            # 复活：免费 + 做种人数低
            deal_seeds = []
            if self._low_seeder_max > 0:
                for s in seeds:
                    if s["is_free"] and s["seeders"] <= self._low_seeder_max and s.get("free_until") and s["free_until"] > now:
                        key = f"deal:{s['tid']}:{int(s['free_until'])}"
                        if key not in dedupe:
                            dedupe[key] = now  # 写入去重，同一免费窗口只推一次（否则每轮巡检重复推送）
                            deal_seeds.append(s)
            self.save_data("notify_dedupe", dedupe)

        # 自动下载复活（默认关）：HH 同款优选（增量/换种循环淘汰）
        downloaded: List[dict] = []
        evicted: List[dict] = []
        self._target_reached = None   # (committed, target) 达标标记 → 摘要显示「已达目标」
        self._dl_aborted = False      # 做种视图失败中止标记 → 摘要追加「已中止自动下载」
        self._last_filtered_n = 0
        self._last_fail_reasons = []
        if self._auto_download and not my_ids_ok:
            self._dl_aborted = True
            self._save_log(["跳过自动下载：我的做种视图拉取失败（防重复下载手动保种）"])
        if self._auto_download and self._low_seeder_max > 0 and my_ids_ok:
            # 候选池 = 免费且在窗口内、做种≤阈值的全部复活种子（预算跨轮复算，HH 同款每轮重选）
            pool = [s for s in seeds
                    if s["is_free"] and s.get("free_until") and s["free_until"] > now
                    and s["seeders"] <= self._low_seeder_max]
            if pool:
                downloaded, evicted = self._auto_download_deals(seeds, pool, my_ids, now)

        # 僵尸任务清理（HH 同款，auto_clean_days>0 时启用）：本站未完成且超N天连文件删
        cleaned_stale = 0
        if self._auto_clean_days and self._auto_clean_days > 0:
            try:
                cleaned_stale = self._clean_stale_downloads(seeds) or 0
            except Exception as e:
                logger.warning(f"彩虹岛复活区管家：僵尸任务清理异常：{e}")

        if self._notify:
            if cleaned_stale:
                try:
                    self.post_message(
                        title=f"【彩虹岛复活区管家】清理超时未完成 {cleaned_stale} 个",
                        text=(f"自动清理阈值 {self._auto_clean_days} 天，本站未完成任务连同数据文件一并删除。\\n"
                              f"处置明细见工作台「运行日志」与「处置日志」。"))
                except Exception:
                    pass
            if seeding_expiring:
                try:
                    lines = [f"  {s['title'][:50]}（剩 {s.get('remain_min', '?')} 分钟）" for s in seeding_expiring[:10]]
                    self.post_message(
                        title=f"【彩虹岛复活区管家】{len(seeding_expiring)} 个我在做种的复活种免费将结束",
                        text="免费结束后新下载骤减、上传收益下降，去留请自行判断：\n" + "\n".join(lines))
                except Exception:
                    pass
            if deal_seeds and not self._auto_download:
                try:
                    lines = ["──────────────",
                             f"复活区新发现 {len(deal_seeds)} 个候选（免费 · 做种 ≤ {self._low_seeder_max} 人）",
                             "仅提醒，未加入下载器",
                             "──────────────"]
                    for s in deal_seeds[:10]:
                        t = (s.get("title") or "").strip()
                        t = t if len(t) <= 44 else t[:44] + "…"
                        remain_h = round((s.get("free_until", 0) - time.time()) / 3600)
                        lines.append(f"  · {t}（{s['size_gb']:.1f}GB · 做种 {s['seeders']} · 剩 {remain_h}h）")
                    if len(deal_seeds) > 10:
                        lines.append(f"  …等共 {len(deal_seeds)} 个")
                    self.post_message(
                        title=f"【彩虹岛复活区管家】发现 {len(deal_seeds)} 个复活区免费种子（未下载）",
                        text="\n".join(lines))
                except Exception:
                    pass

        # 概览缓存
        free_active = sum(1 for s in seeds if s["is_free"] and s.get("free_until") and s["free_until"] > now)
        self._last_overview = {
            "ts": now, "total": len(seeds), "pages": stats["pages"], "free_active": free_active,
            "my_seeding": len(my_ids), "deleted": len(stats["deleted"]),
            "early": len(stats["early_deleted"]), "warned": len(stats["warned"]),
            "auto_downloaded": len(downloaded), "seeds": seeds, "my_ids": sorted(my_ids),
            "stats": acct_stats, "portfolio": portfolio[:200], "portfolio_stats": portfolio_stats,
            "last_gain": self._last_credit_delta, "ran_at": now,
        }
        self._last_overview_ts = now
        _suffix = ""
        if self._target_reached:
            _suffix = f" | 已达目标（{self._target_reached[0]:.0f}/{self._target_reached[1]:.0f} GB）"
        elif self._dl_aborted:
            _suffix = " | 已中止自动下载"
        self._last_result = (
            f"复活区 {len(seeds)} 种 | 免费 {free_active} | 我的做种 {len(my_ids)} | "
            f"到期删 {len(stats['deleted'])} | 下载 {len(downloaded)} | 淘汰 {len(evicted)}{_suffix}")
        logger.info(
            f"彩虹岛复活区管家：{datetime.now().strftime('%m-%d %H:%M')} 巡检：复活区 {len(seeds)} 种（免费中 {free_active}），"
            f"我的做种 {len(my_ids)}，到期删 {len(stats['deleted'])}，提前删 {len(stats['early_deleted'])}，"
            f"预警 {len(stats['warned'])}，自动下载 {len(downloaded)}，淘汰 {len(evicted)}，耗时 {time.time()-t0:.1f}s")
        # 持久运行日志（工作台弹窗可查历史巡检摘要）
        try:
            _log_lines = [
                f"巡检完成：复活区 {len(seeds)} 种 | 免费 {free_active} | 我的做种 {len(my_ids)}",
                f"处置：到期删 {len(stats['deleted'])} | 提前删 {len(stats['early_deleted'])} | 预警 {len(stats['warned'])}"
                + (f" | 僵尸清理 {cleaned_stale}" if cleaned_stale else ""),
            ]
            for s in downloaded[:10]:
                _log_lines.append(
                    f"下载：{s.get('title', '')[:60]}（{s.get('size_gb', 0):.1f}GB → {self._push_downloader or '—'}）")
            for rsn in (self._last_fail_reasons or [])[:5]:
                _log_lines.append(f"下载失败：{rsn}")
            for s in evicted[:10]:
                _log_lines.append(f"淘汰：{s.get('title', '')[:60]}（{s.get('downloader') or '—'}）")
            if self._dl_aborted:
                _log_lines.append("本轮已中止自动下载（我的做种视图拉取失败）")
            _log_lines.append(f"耗时 {time.time()-t0:.1f}s")
            self._save_log(_log_lines)
        except Exception as e:
            logger.warning(f"彩虹岛复活区管家：写运行日志失败：{e}")

    def _auto_download_deals(self, seeds: List[dict], pool: List[dict], my_ids: set, now: float) -> Tuple[List[dict], List[dict]]:
        """HH 同款优选：总保种上限内选种推送；换种（wash）放不下时踢低档位腾位。

        范围限制（用户口径「只删复活区设置的目录种子」）：淘汰对象仅限受管/推送下载器内
        与复活区列表标题匹配、且配置了保存路径时必须位于该目录下的已完成任务；
        HR 种跳过不删；删除走 _delete_task（删后复核）。
        """
        out: List[dict] = []
        evicted: List[dict] = []
        try:
            sv_push = self._get_push_service()
            if not sv_push and not self._push_downloader:
                # 未配置推送下载器 → 回退管理下载器第一个（HH 语义），避免自动下载静默跳过
                _mgmt_fb = self._get_manage_services() or {}
                if _mgmt_fb:
                    self._push_downloader = next(iter(_mgmt_fb))
                    logger.info(f"彩虹岛复活区管家：未配置推送下载器，回退管理下载器第一个 {self._push_downloader}")
                    sv_push = _mgmt_fb[self._push_downloader]
            if not sv_push:
                logger.warning("彩虹岛复活区管家：自动下载未配置有效推送下载器，跳过")
                return out, evicted
            scan = self._get_manage_services()
            if self._push_downloader and self._push_downloader not in scan:
                scan[self._push_downloader] = sv_push
            if not scan:
                logger.warning("彩虹岛复活区管家：无可用下载器服务，跳过优选")
                return out, evicted

            by_norm: Dict[str, dict] = {}
            by_tid: Dict[str, dict] = {}
            for s in seeds:
                by_norm.setdefault(s["norm"], s)
                if s.get("tid"):
                    by_tid[str(s["tid"])] = s
            # seed_id 精确匹配（HH 同款）：从下载器 comment 提取站点 tid→infohash 映射，
            # 命中则按 tid 精确对应复活种子，未命中回退标题匹配（comment 未带 tid 时零回归）。
            hash_to_seed: Dict[str, dict] = {}
            try:
                for _sid, _info in (self._get_downloader_seed_map(scan) or {}).items():
                    _sd = by_tid.get(str(_sid))
                    _h = str(_info.get("hash") or "")
                    if _sd is not None and _h:
                        hash_to_seed[_h] = _sd
            except Exception as e:
                logger.warning(f"彩虹岛复活区管家：seed_id 映射构建失败，回退标题匹配：{e}")

            # 1) 扫描下载器：已有任务（去重）/ 当前在保（复活区已完成）/ 在途（未完成）
            existing: set = set()
            current: List[dict] = []
            in_flight_gb = 0.0
            want_dir = (self._save_path or "").rstrip("/")
            for dl_name, sv in scan.items():
                try:
                    torrents, error = sv.instance.get_torrents()
                except Exception as e:
                    logger.warning(f"彩虹岛复活区管家：优选读取下载器 {dl_name} 失败：{e}")
                    continue
                if error or not torrents:
                    continue
                for t in torrents:
                    _th0 = self._thash(t)
                    seed = hash_to_seed.get(_th0) if _th0 else None
                    norm = seed["norm"] if seed is not None else self._norm_title(self._tf(t, "name", default=""))
                    if not norm:
                        continue
                    existing.add(norm)
                    if seed is None:
                        seed = by_norm.get(norm)
                    # 目录范围对在途/已完成统一生效（防在途占预算、下完又不计入的口径漂移）
                    if want_dir:
                        tpath = str(self._tf(t, "save_path", "downloadDir", "download_dir", default="") or "")
                        if not tpath:
                            continue  # 配置了目录但任务目录读不到 → 不纳入（不删不动、不计体积）
                        if not (tpath.rstrip("/") + "/").startswith(want_dir + "/"):
                            continue  # 不在复活区设置的目录 → 不纳入
                    if seed is None and want_dir:
                        # 模糊标题兜底（窄用）：仅复活目录内任务、精确匹配未命中时才尝试；
                        # 互含唯一匹配才生效（多义/无命中返回 None 保持原行为），
                        # 命中后把站点 norm 加入去重集，防同一资源被重复下载
                        seed = self._fuzzy_match_norm(norm, by_norm)
                        if seed is None:
                            continue
                        existing.add(seed["norm"])
                        logger.info(f"彩虹岛复活区管家：模糊匹配命中 {norm[:40]} → {seed['title'][:40]}")
                    if seed is None:
                        continue
                    progress = float(self._tf(t, "progress", "percentDone", default=0) or 0)
                    if progress < 1.0:
                        # 在途（下载中）计入已承诺体积（HH 同款：体积基准含在途）
                        in_flight_gb += seed["size_gb"]
                        continue
                    thash = self._thash(t)
                    current.append({
                        "title": seed["title"], "norm": seed["norm"], "size": seed["size_gb"],
                        "seeders": seed.get("seeders", 1), "hr": seed.get("hr"),
                        "tid": seed["tid"], "downloader": dl_name, "hash": thash,
                        "evictable": bool(thash),
                    })
            # 站点侧我在做种但不在复活目录内的（手动/其它客户端/其它目录）：
            # 口径（2026-10-23 用户拍板「复活目录和影视目录分开算」）：只做去重（不重复下载），
            # 不计入本插件的目录预算——500GB 目标只约束插件管理的复活目录，手动保种不占额度、也不可淘汰。
            cur_norms = {c["norm"] for c in current}
            for s in seeds:
                if s["tid"] in my_ids and s["norm"] not in cur_norms:
                    existing.add(s["norm"])
                    cur_norms.add(s["norm"])

            current_gb = sum(c["size"] for c in current)
            self._last_current_gb = current_gb
            self._last_in_flight_gb = in_flight_gb

            # 2) 目标体积与剩余额度（HH 同款；0=不限）
            target = self._target_volume
            committed_gb = current_gb + in_flight_gb
            if target and target > 0:
                if committed_gb >= target - 1e-6 and self._mode != "wash":
                    logger.info(f"彩虹岛复活区管家：当前保种+在途已达标（{committed_gb:.1f} GB ≥ 目标 {target:g} GB），本次不推不删")
                    self._target_reached = (committed_gb, target)
                    return out, evicted
                eff_target: Optional[float] = max(0.0, target - committed_gb)
                logger.info(f"彩虹岛复活区管家：目标体积（总保种上限）{target:g} GB，当前 {current_gb:.1f}，"
                            f"在途 {in_flight_gb:.1f}，剩余可增 {eff_target:.1f} GB")
            else:
                eff_target = None  # 0=不限

            # 3) 候选池：排除已在下载器、已在做种；按 norm 去重防同名资源重复下载
            cands: List[dict] = []
            cand_norms: set = set()
            filtered_in_dl = 0
            for s in pool:
                if s.get("norm") in existing:
                    filtered_in_dl += 1   # 已在下载器 → 计入「已过滤 N 个」通知口径
                    continue
                if (not s["norm"] or s["norm"] in cand_norms or s["tid"] in my_ids):
                    continue
                cand_norms.add(s["norm"])
                cands.append(dict(s, size=s["size_gb"]))
            self._last_filtered_n = filtered_in_dl
            if self._mode == "wash" and current:
                res = self._optimize_wash(cands, current, in_flight_gb)
            else:
                res = self._optimize_incremental(cands, eff_target)

            # 4) 换种淘汰（HH 同款：先算后删；HR 闸 + 目录范围 + 删后复核）
            for v in res.get("del_seeds", []):
                if not v.get("evictable") or not v.get("hash"):
                    continue
                sv = scan.get(v.get("downloader") or "")
                if not sv:
                    continue
                ok = self._delete_task(sv, str(v["hash"]), str(v.get("downloader") or ""))
                self._append_disposal({"time": int(now), "tid": v.get("tid"), "title": v.get("title"),
                                       "size_gb": v.get("size", 0.0), "progress": 100.0,
                                       "downloader": v.get("downloader"), "action": "wash_evict", "ok": ok})
                if ok:
                    evicted.append(v)
                logger.info(f"彩虹岛复活区管家：换种淘汰 {str(v.get('title', ''))[:50]}"
                            f"（{v.get('downloader')}，做种 {v.get('seeders')} 人，{v.get('size', 0.0):.1f}GB）成功={ok}")

            # 5) 推送选中的候选
            base = self._get_site_url()
            cookie = self._get_site_cookie()
            fail_count = 0
            for s in res.get("picked", []):
                if s["norm"] in existing or s["tid"] in my_ids:
                    continue
                url = f"{base}/download.php?id={s['tid']}"
                try:
                    # add_torrent 返回 (成功标志, 任务标识列表)，失败时 (False, [])
                    # 修复跨盘/自定义保存路径不生效：qBittorrent 开启自动种子管理时会忽略传入的
                    # save_path，导致填了其他盘的路径仍落在默认盘。对 qBittorrent 显式关闭自动管理
                    # （ignore_category_check=False），使下载目录严格按"保存路径"配置生效；
                    # Transmission 无此问题，透传即可。
                    _dl_l = ""
                    try:
                        _dl_l = str(getattr(sv_push, "type", "")
                                    or getattr(getattr(sv_push, "config", None), "type", "") or "").lower()
                    except Exception:
                        _dl_l = ""
                    if "qbittorrent" in _dl_l:
                        result = sv_push.instance.add_torrent(
                            content=url, cookie=cookie,
                            download_dir=(self._save_path or None),
                            tag=(self._tag or None), ignore_category_check=False)
                    else:
                        result = sv_push.instance.add_torrent(
                            content=url, cookie=cookie,
                            download_dir=(self._save_path or None),
                            tag=(self._tag or None))
                    ok = result[0] if isinstance(result, tuple) else bool(result)
                    if not ok:
                        fail_count += 1
                        self._last_fail_reasons.append(f"{s['title'][:40]}：下载器拒绝（返回 {str(result)[:80]}）")
                        logger.warning(f"彩虹岛复活区管家：自动下载被下载器拒绝 {s['title'][:40]}（返回 {result}）")
                        time.sleep(1)  # 推送间隔（HH 同款），防打爆下载器/站点
                        continue
                    out.append(s)
                    existing.add(s["norm"])  # 推送成功即入清单，防同 norm 后续候选重复推送
                    logger.info(f"彩虹岛复活区管家：自动下载 {s['title'][:50]}（{s['size_gb']:.1f}GB → {self._push_downloader}）")
                except Exception as e:
                    fail_count += 1
                    self._last_fail_reasons.append(f"{s['title'][:40]}：{str(e)[:120]}")
                    logger.error(f"彩虹岛复活区管家：自动下载失败 {s['title'][:40]}：{e}")
                time.sleep(1)  # 推送间隔（HH 同款），防打爆下载器/站点

            # HH 同款优选完成汇总通知（有新增或淘汰才发，防 2 小时一轮刷屏）
            if self._notify and (out or evicted):
                try:
                    self._notify_optimize(current, out, evicted, res, in_flight_gb, fail_count)
                except Exception as e:
                    logger.error(f"彩虹岛复活区管家：发送优选完成通知失败：{e}")
        except Exception as e:
            logger.error(f"彩虹岛复活区管家：自动下载流程异常：{e}")
        return out, evicted

    # ------------------------------------------------------------------ #
    # HH 同款优选：增量（不删保种）/ 换种（wash，删低档位补高档位）
    # ------------------------------------------------------------------ #
    @staticmethod
    def _tier_sort_key(s: dict):
        """候选排序（HH 同款）：档位高（摊薄重）优先、同档位大体积优先（-size 降序）。"""
        return (tier_of(s.get("seeders", 1)), -s.get("size", 0.0))

    def _optimize_incremental(self, seeds: List[dict], eff_target: Optional[float]) -> dict:
        """HH 同款增量优选：不删在保，按档位高→低在剩余额度内补齐；eff_target=None 表示 0=不限。"""
        if eff_target is not None and eff_target <= 0:
            logger.info("彩虹岛复活区管家：当前保种体积已达目标，无需新增")
            return {"picked": [], "del_seeds": []}
        candidates = sorted(seeds, key=RainbowReviveButler._tier_sort_key)
        picked: List[dict] = []
        used = 0.0
        for s in candidates:
            if eff_target is None or used + s["size"] <= eff_target + 1e-6:
                picked.append(s)
                used += s["size"]
        cap_txt = "不限" if eff_target is None else f"{eff_target:.1f} GB"
        logger.info(f"彩虹岛复活区管家：增量优选：剩余额度 {cap_txt}，按档位高→低补 {len(picked)} 个，新增 {used:.1f} GB")
        return {"picked": picked, "del_seeds": []}

    def _optimize_wash(self, candidates: List[dict], current: List[dict], in_flight_gb: float) -> dict:
        """HH 同款换种优选：在目标体积（总保种上限，含在途）内，候选放不下时踢比候选
        档位更低的在保种子腾位（低档优先、小体积优先、放不下回滚）；只踢可淘汰
        （复活区目录内）且非 HR 的种子。不因单纯超线而删——仅在有候选需要位置时腾位。
        """
        cap = self._target_volume
        if not cap or cap <= 0:
            # 0=不限：不淘汰、新增不受限（按「0=不限」文案语义）
            return {"picked": list(candidates), "del_seeds": []}
        cur_norms = {c["norm"] for c in current}
        final: List[dict] = list(current)
        total_gb = sum(s.get("size", 0.0) for s in final) + in_flight_gb
        new_cands = [s for s in candidates if s.get("norm") not in cur_norms]
        new_cands.sort(key=RainbowReviveButler._tier_sort_key)
        del_map: Dict[str, dict] = {}
        for cand in new_cands:
            csize = cand.get("size", 0.0)
            if csize > cap:
                continue
            # 放得下直接加
            if total_gb + csize <= cap + 1e-6:
                final.append(cand)
                total_gb += csize
                continue
            # 放不下：只踢比候选档位更低的种子（HR/不可淘汰跳过）
            cand_tier = tier_of(cand.get("seeders", 1))
            victims = [s for s in final
                       if tier_of(s.get("seeders", 1)) > cand_tier
                       and (s.get("norm") or s.get("title", "")) not in del_map
                       and s.get("evictable") and not s.get("hr")]
            victims.sort(key=lambda s: (-tier_of(s.get("seeders", 1)), s.get("size", 0.0)))
            evicted_round: List[dict] = []
            added = False
            for victim in victims:
                final.remove(victim)
                evicted_round.append(victim)
                total_gb -= victim.get("size", 0.0)
                if total_gb + csize <= cap + 1e-6:
                    final.append(cand)
                    total_gb += csize
                    for v in evicted_round:
                        del_map[v.get("norm") or v.get("title", "")] = v
                    added = True
                    break
            if not added:
                # 回滚本次尝试踢掉的种子
                for v in evicted_round:
                    final.append(v)
                    total_gb += v.get("size", 0.0)
        del_seeds = list(del_map.values())
        add_seeds = [s for s in final if s.get("norm") not in cur_norms]
        keep_count = len(final) - len(add_seeds)
        logger.info(f"彩虹岛复活区管家：换种优选（体积上限 {cap:g} GB，含在途 {in_flight_gb:.1f} GB）："
                    f"构建 {len(final)} 个（保留当前 {keep_count} + 新增 {len(add_seeds)}），"
                    f"删除低档位 {len(del_seeds)} 个，最终占用 {total_gb:.1f} GB")
        for s in add_seeds[:8]:
            logger.info(f"彩虹岛复活区管家：  + 换种新增 {s.get('title', '')[:50]} | {s.get('size', 0.0):.1f} GB | 当前做种 {s.get('seeders', '?')} 人")
        for s in del_seeds[:8]:
            logger.info(f"彩虹岛复活区管家：  - 换种淘汰 {s.get('title', '')[:50]} | {s.get('size', 0.0):.1f} GB | 当前做种 {s.get('seeders', '?')} 人")
        return {"picked": add_seeds, "del_seeds": del_seeds, "keep_count": keep_count}

    def _notify_optimize(self, current: List[dict], picked_ok: List[dict],
                         evicted: List[dict], res: dict, in_flight_gb: float, fail_count: int):
        """HH 同款优选完成汇总通知：一条式（在保状态 / 目标体积 / 做种分布 / 上次收益 + 新增与删除明细）。"""
        mode_name = "换种" if self._mode == "wash" else "增量"
        ev_norms = {v.get("norm") for v in evicted}
        final = [c for c in current if c.get("norm") not in ev_norms]
        seen = {c.get("norm") for c in final}
        for s in picked_ok:
            if s.get("norm") not in seen:
                final.append({"size": s.get("size_gb", 0.0), "seeders": s.get("seeders", 0)})
                seen.add(s.get("norm"))
        final_count = len(final)
        final_gb = sum(c.get("size", 0.0) for c in final)
        lines = ["──────────────", f"在保种子：{final_count} 个 / {final_gb:.1f} GB"]
        if self._target_volume and self._target_volume > 0:
            remain = max(0.0, self._target_volume - final_gb - in_flight_gb)
            lines.append(f"目标体积：{self._target_volume:.0f} GB（在途 {in_flight_gb:.1f} GB · 剩余可增 {remain:.1f} GB）")
        else:
            lines.append("目标体积：不限")
        dist = {"0-1人": {"count": 0, "gb": 0.0}, "2-3人": {"count": 0, "gb": 0.0}, "4+人": {"count": 0, "gb": 0.0}}
        for c in final:
            n = c.get("seeders", 0)
            k = "0-1人" if n <= 1 else ("2-3人" if n <= 3 else "4+人")
            dist[k]["count"] += 1
            dist[k]["gb"] += c.get("size", 0.0)
        lines.append("做种人数分布")
        for k in ("0-1人", "2-3人", "4+人"):
            lines.append(f"  {k}  {dist[k]['count']}个  {dist[k]['gb']:.1f} GB")
        if isinstance(self._last_credit_delta, (int, float)) and self._last_overview_ts:
            ran = datetime.fromtimestamp(self._last_overview_ts, pytz.timezone("Asia/Shanghai")).strftime("%m-%d %H:%M")
            lines.append(f"上次收益：+{self._last_credit_delta:.0f}（{ran}）")
        lines.append("──────────────")
        if picked_ok:
            total_new = sum(s.get("size_gb", 0.0) for s in picked_ok)
            lines.append(f"新增 {len(picked_ok)} 个种子：+{total_new:.1f} GB")
            for s in picked_ok[:5]:
                t = (s.get("title") or "").strip()
                lines.append(f"  · {t if len(t) <= 36 else t[:36] + '…'}")
            if len(picked_ok) > 5:
                lines.append(f"  …等共 {len(picked_ok)} 个")
            if fail_count:
                lines.append(f"⚠️ 推送失败：{fail_count} 个（成功 {len(picked_ok)}）")
            else:
                lines.append(f"推送成功：{len(picked_ok)}/{len(picked_ok) + fail_count}")
        else:
            lines.append("无需新增下载")
            if getattr(self, "_last_filtered_n", 0):
                lines.append(f"已过滤 {self._last_filtered_n} 个已在下载器的种子")
        if evicted:
            lines.append(f"删除低档位种子：{len(evicted)} 个")
            for v in evicted[:5]:
                t = (v.get("title") or "").strip()
                lines.append(f"  - {t if len(t) <= 32 else t[:32] + '…'}（做种{v.get('seeders', '?')}人/{v.get('size', 0.0):.0f}GB）")
            if len(evicted) > 5:
                lines.append(f"  …等共 {len(evicted)} 个")
        self.post_message(title=f"【彩虹岛复活区管家】{mode_name}优选完成", text="\n".join(lines))

    # ------------------------------------------------------------------ #
    # 守护线程：高频到期处置
    # ------------------------------------------------------------------ #
    def _start_guard_thread(self):
        """启动到期处置守护线程（stop Event 以参数传入，防止配置重载后旧线程读到新 Event 永不退出）。"""
        try:
            self._guard_stop = threading.Event()
            self._guard_thread = threading.Thread(
                target=self._guard_loop, args=(self._guard_stop,),
                daemon=True, name="RainbowReviveButler-Dispose")
            self._guard_thread.start()
        except Exception as e:
            logger.error(f"彩虹岛复活区管家：启动守护线程失败：{e}")

    def _guard_loop(self, stop_event):
        """守护线程主循环：每 dispose_minutes 跑一轮到期处置（到期即删的核心）。"""
        while True:
            try:
                if stop_event.is_set():
                    return
                if self._enabled:
                    wait = self._dispose_minutes * 60 - (time.time() - self._last_dispose_ts)
                    if wait <= 0:
                        try:
                            self._dispose_pass(notify=self._notify, quick=True)
                        except Exception as e:
                            logger.warning(f"彩虹岛复活区管家：守护处置轮次异常：{e}")
                        self._last_dispose_ts = time.time()
                        wait = self._dispose_minutes * 60
                    stop_event.wait(min(wait, 60.0) if wait > 0 else 60)
                else:
                    stop_event.wait(60)
            except Exception:
                try:
                    stop_event.wait(60)
                except Exception:
                    return

    def _refresh_overview(self, manual: bool = False):
        """只刷站点数据快照（只读、无处置动作），供页面/仪表盘用。

        除复活区列表外，还取账户级数据（做种积分/魔力/分享率/上传下载/当前做种数）
        与全量做种组合（getusertorrentlistajax），并跨次轮询做种积分算增速。
        """
        try:
            base = self._get_site_url()
            cookie = self._get_site_cookie()
            if not base or not cookie:
                return
            session = self._session(cookie)
            seeds, pages = self._fetch_revive_list(session, base)
            my_ids = self._fetch_my_seeding_ids(session, base)
            stats = self._fetch_site_stats(session, base)
            portfolio = self._fetch_seeding_portfolio(session, base)
            now = time.time()
            # 做种积分增速：存最近两次采样，算每小时增速
            credit_rate = None
            try:
                prev = self.get_data("credit_sample") or {}
                cur_credit = stats.get("credit", 0)
                if prev.get("credit") and prev.get("ts") and cur_credit:
                    dt_h = (now - prev["ts"]) / 3600
                    if 0.01 < dt_h < 24 * 7:
                        credit_rate = round((cur_credit - prev["credit"]) / dt_h, 1)
                # 注意：此处只读采样不覆写——采样更新仅由巡检 _do_run 负责，
                # 否则面板/仪表盘每次刷新都重置时间戳，增速永远算不出
            except Exception:
                pass
            stats["credit_rate"] = credit_rate
            # 做种组合分析
            portfolio_stats = self._analyze_portfolio(portfolio)
            # 防清空：列表被拉空（站点返登录页/瞬时故障）或做种视图拉取失败（None）时
            # 保留上次概况，不用空数据覆盖（HH error/degraded 保留上次同款语义）
            if my_ids is None or (not seeds and (self._last_overview or {}).get("seeds")):
                logger.warning(
                    f"彩虹岛复活区管家：概况刷新跳过（复活区列表 {len(seeds)} 条，"
                    f"做种视图{'拉取失败' if my_ids is None else len(my_ids)}），保留上次数据")
                return
            self._last_overview = {
                "ts": now, "total": len(seeds), "pages": pages,
                "free_active": sum(1 for s in seeds if s["is_free"] and s.get("free_until") and s["free_until"] > now),
                "my_seeding": len(my_ids),
                "deleted": (self._last_overview or {}).get("deleted", 0),
                "early": (self._last_overview or {}).get("early", 0),
                "warned": (self._last_overview or {}).get("warned", 0),
                "auto_downloaded": (self._last_overview or {}).get("auto_downloaded", 0),
                "seeds": seeds, "my_ids": sorted(my_ids),
                "stats": stats, "portfolio": portfolio[:200], "portfolio_stats": portfolio_stats,
            }
            self._last_overview_ts = now
            logger.info(f"彩虹岛复活区管家：概况已刷新（复活区 {len(seeds)} 种，我的做种 {len(my_ids)}，"
                        f"做种组合 {len(portfolio)} 种，做种积分 {stats.get('credit', 0):.0f}，增速 {credit_rate}/h）")
        except Exception as e:
            logger.warning(f"彩虹岛复活区管家：概况刷新失败（{'手动' if manual else '自动'}）：{e}")

    @staticmethod
    def _analyze_portfolio(portfolio: List[dict]) -> dict:
        """分析做种组合：总体积、档位分布、高价值/低价值种子。"""
        if not portfolio:
            return {}
        total_gb = sum(p.get("size_gb", 0) for p in portfolio)
        t_low = [p for p in portfolio if p.get("seeders", 0) <= 1]
        t_mid = [p for p in portfolio if 2 <= p.get("seeders", 0) <= 3]
        t_high = [p for p in portfolio if p.get("seeders", 0) >= 4]
        # 高价值：做种人数少（NexusPHP 做种积分奖励低做种）+ 体积大
        valuable = sorted(portfolio, key=lambda p: (p.get("seeders", 99), -p.get("size_gb", 0)))[:10]
        # 低价值：做种人数极多（收益被摊薄）
        crowded = sorted(portfolio, key=lambda p: -p.get("seeders", 0))[:10]
        return {
            "count": len(portfolio), "total_gb": round(total_gb, 1),
            "tier_low": len(t_low), "tier_mid": len(t_mid), "tier_high": len(t_high),
            "low_gb": round(sum(p.get("size_gb", 0) for p in t_low), 1),
            "valuable": valuable, "crowded": crowded,
        }

    @staticmethod
    def _refresh_note(last_ts: float = 0.0, hours: float = 2.0) -> str:
        """刷新状态说明文案。"""
        if not last_ts:
            return "尚未拉取数据"
        mins = (time.time() - last_ts) / 60
        if mins >= 60:
            h, m = divmod(int(mins), 60)
            ago = f"{h} 小时 {m} 分钟前" if m else f"{h} 小时前"
        else:
            ago = f"{int(mins)} 分钟前"
        if hours > 0:
            return f"每 {hours:g} 小时自动刷新 · 上次更新 {ago}"
        return f"上次更新 {ago}"

    @staticmethod
    def _revive_dist(ov: dict) -> Tuple[dict, int, float]:
        """我在复活区的做种分布（HH 同款三档：0-1/2-3/4+），按当前做种人数分档。"""
        seeds = ov.get("seeds", []) or []
        my_ids = set(ov.get("my_ids", []) or [])
        dist = {"0-1人": {"count": 0, "gb": 0.0}, "2-3人": {"count": 0, "gb": 0.0}, "4+人": {"count": 0, "gb": 0.0}}
        count = 0
        for s in seeds:
            if s.get("tid") not in my_ids:
                continue
            n = s.get("seeders", 0)
            k = "0-1人" if n <= 1 else ("2-3人" if n <= 3 else "4+人")
            dist[k]["count"] += 1
            dist[k]["gb"] += s.get("size_gb", 0.0) or 0.0
            count += 1
        return dist, count, sum(x["gb"] for x in dist.values())

    @staticmethod
    def _overview_html(ov: dict, last_ts: float = 0.0, hours: float = 0.0) -> str:
        """照原作者版式：主题自适应 table 布局，保种视角 4 格 + 我的做种人数分布条（HH 同款）。"""
        if not ov or not ov.get("total"):
            return ('<div style="padding:12px 14px;border-radius:10px;'
                    'border:1px solid rgba(var(--v-theme-on-surface),.08);'
                    'background:rgba(var(--v-theme-surface),.55);'
                    'color:rgba(var(--v-theme-on-surface),.72);font-size:12px;">'
                    '彩虹岛复活区管家：未获取到数据（请确认插件已启用、站点 Cookie 有效）</div>')
        stats = ov.get("stats", {}) or {}
        dist, count, total_gb = RainbowReviveButler._revive_dist(ov)
        C = {"0-1人": "#22c55e", "2-3人": "#3b82f6", "4+人": "#ef4444"}
        t_gb = total_gb or 1.0
        ON = "rgb(var(--v-theme-on-surface))"
        ON60 = "rgba(var(--v-theme-on-surface),.6)"
        ON40 = "rgba(var(--v-theme-on-surface),.4)"

        def pct(k):
            return (dist[k]["gb"] / t_gb * 100.0) if t_gb else 0.0

        gain = ov.get("last_gain")
        gain_txt = f'+{gain:.0f}' if isinstance(gain, (int, float)) else '—'
        credit = stats.get("credit", 0) or 0

        def td(label, num, sub):
            return (f'<td style="text-align:center;padding:4px 2px;width:25%;vertical-align:top;">'
                    f'<div style="font-size:10.5px;color:{ON60};white-space:nowrap;">{label}</div>'
                    f'<div style="font-size:17px;font-weight:700;color:{ON};line-height:1.4;white-space:nowrap;">{num}'
                    f'<span style="font-size:10px;font-weight:400;color:{ON40};"> {sub}</span></div></td>')

        cells = ('<table style="width:100%;border-collapse:collapse;table-layout:fixed;margin:2px 0 6px;"><tr>'
                 + td("🌱 我的做种", f'{count}', "个")
                 + td("💾 总体积", f'{total_gb:.1f}', "GB")
                 + td("⭐ 上次收益", gain_txt, "")
                 + td("🌟 当前积分", f'{credit:,.0f}', "")
                 + '</tr></table>')
        ran = ov.get("ran_at") or last_ts
        ran_txt = (datetime.fromtimestamp(ran, pytz.timezone("Asia/Shanghai")).strftime("%m-%d %H:%M")
                   if ran else "")
        dist_title = ('<table style="width:100%;border-collapse:collapse;table-layout:fixed;margin:2px 0 2px;"><tr>'
                      f'<td style="text-align:left;font-size:10.5px;color:{ON60};">做种人数分布'
                      + (f' · 上次巡检 {ran_txt}' if ran_txt else '')
                      + '</td></tr></table>')
        bar = ('<table style="width:100%;border-collapse:collapse;table-layout:fixed;margin:6px 0 6px;"><tr>'
               f'<td style="height:10px;width:{pct("0-1人"):.2f}%;background:{C["0-1人"]};border-radius:5px 0 0 5px;"></td>'
               f'<td style="height:10px;width:{pct("2-3人"):.2f}%;background:{C["2-3人"]};"></td>'
               f'<td style="height:10px;width:{pct("4+人"):.2f}%;background:{C["4+人"]};border-radius:0 5px 5px 0;"></td>'
               '</tr></table>')
        legend = ('<table style="width:100%;border-collapse:collapse;table-layout:fixed;font-size:10.5px;color:{ON60};"><tr>'
                  f'<td style="text-align:left;white-space:nowrap;"><span style="display:inline-block;width:7px;height:7px;border-radius:2px;background:{C["0-1人"]};margin-right:3px;"></span>0-1人 {dist["0-1人"]["count"]}个 {dist["0-1人"]["gb"]:.1f}GB</td>'
                  f'<td style="text-align:center;white-space:nowrap;"><span style="display:inline-block;width:7px;height:7px;border-radius:2px;background:{C["2-3人"]};margin-right:3px;"></span>2-3人 {dist["2-3人"]["count"]}个 {dist["2-3人"]["gb"]:.0f}GB</td>'
                  f'<td style="text-align:right;white-space:nowrap;"><span style="display:inline-block;width:7px;height:7px;border-radius:2px;background:{C["4+人"]};margin-right:3px;"></span>4+人 {dist["4+人"]["count"]}个 {dist["4+人"]["gb"]:.1f}GB</td>'
                  '</tr></table>')
        return f'<div style="background:transparent;">{cells}{dist_title}{bar}{legend}</div>'

    def _overview_form_cards(self) -> dict:
        """配置表单顶部数据面板（照原作者 HHClubButler：4 格 + 分布条 + 刷新行 + 最近运行状态）。"""
        if not self._last_overview or not self._last_overview.get("total"):
            return {"component": "VCard", "props": {"flat": True},
                    "content": [{"component": "VCardText", "props": {},
                                 "text": "尚未运行或未获取到数据（启用插件或刷新概况后此处显示最近概况）"}]}
        ov = self._last_overview
        dist, count, total_gb = self._revive_dist(ov)
        c = count or 1
        c01 = dist["0-1人"]["count"] / c * 100.0
        c23 = dist["2-3人"]["count"] / c * 100.0
        c45 = dist["4+人"]["count"] / c * 100.0

        def stat_col(label, num, sub=""):
            return {"component": "VCol", "props": {"cols": 6, "md": 3},
                    "content": [
                        {"component": "div",
                         "props": {"style": "text-align:center;font-size:11px;color:rgba(var(--v-theme-on-surface),.6);white-space:nowrap;"},
                         "text": label},
                        {"component": "div",
                         "props": {"style": "text-align:center;font-size:17px;font-weight:700;white-space:nowrap;"},
                         "text": f"{num} {sub}".strip()},
                    ]}

        def legend_col(key, align):
            color_map = {"0-1人": "#22c55e", "2-3人": "#3b82f6", "4+人": "#ef4444"}
            sq = color_map.get(key, "#999")
            justify = "flex-start" if align == "left" else ("flex-end" if align == "right" else "center")
            return {"component": "VCol", "props": {"cols": 12, "md": 4},
                    "content": [{"component": "div",
                                 "props": {"style": f"display:flex;align-items:center;justify-content:{justify};font-size:11px;color:rgba(var(--v-theme-on-surface),.6);white-space:nowrap;"},
                                 "content": [
                                     {"component": "div", "props": {"style": f"width:8px;height:8px;background:{sq};margin-right:4px;flex-shrink:0;"}, "text": ""},
                                     {"component": "span", "props": {}, "text": f'{key} {dist[key]["count"]}个 {dist[key]["gb"]:.1f}GB'},
                                 ]}]}

        gain = ov.get("last_gain")
        gain_txt = f"+{gain:.0f}" if isinstance(gain, (int, float)) else "—"
        stats = ov.get("stats", {}) or {}
        credit = stats.get("credit", 0) or 0
        ran = ov.get("ran_at") or self._last_overview_ts
        ran_txt = (datetime.fromtimestamp(ran, pytz.timezone("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M")
                   if ran else "")
        return {"component": "VCard", "props": {"flat": True},
                "content": [
                    {"component": "VCardText", "props": {"class": "pt-2"},
                     "content": [
                         {"component": "VRow", "content": [
                             stat_col("🌱 我的做种", count, "个"),
                             stat_col("💾 总体积", f"{total_gb:.1f}", "GB"),
                             stat_col("⭐ 上次收益", gain_txt, ""),
                             stat_col("🌟 当前积分", f"{credit:,.0f}", ""),
                         ]},
                         {"component": "div",
                          "props": {"style": "margin-top:8px;margin-bottom:4px;font-size:11px;color:rgba(var(--v-theme-on-surface),.6);"},
                          "text": "做种人数分布" + (f" · 上次巡检 {ran_txt}" if ran_txt else "")},
                         {"component": "div",
                          "props": {"style": "width:100%;display:flex;border-radius:4px;overflow:hidden;"},
                          "content": [
                              {"component": "div", "props": {"style": f"height:8px;width:{c01:.2f}%;background:#22c55e;"}, "text": ""},
                              {"component": "div", "props": {"style": f"height:8px;width:{c23:.2f}%;background:#3b82f6;"}, "text": ""},
                              {"component": "div", "props": {"style": f"height:8px;width:{c45:.2f}%;background:#ef4444;"}, "text": ""},
                          ]},
                         {"component": "VRow", "props": {"no-gutters": True}, "content": [
                             legend_col("0-1人", "left"),
                             legend_col("2-3人", "center"),
                             legend_col("4+人", "right"),
                         ]},
                         {"component": "VRow", "props": {"class": "d-flex align-center", "no-gutters": True, "style": "margin-top:8px;"},
                          "content": [
                              {"component": "VCol", "props": {"cols": "auto"},
                               "content": [{"component": "div",
                                            "props": {"style": "font-size:10.5px;color:rgba(var(--v-theme-on-surface),.4);"},
                                            "text": self._refresh_note(self._last_overview_ts, self._cron_hours)}]},
                              {"component": "VCol", "props": {"cols": True},
                               "content": [{"component": "div",
                                            "props": {"style": "font-size:10.5px;color:rgba(var(--v-theme-on-surface),.4);text-align:right;"},
                                            "text": "左下角 [查看数据] 内置 [立即刷新] 及 [复活区管理器]"}]},
                          ]},
                     ]},
                    {"component": "VAlert", "props": {"type": "info", "variant": "tonal",
                                                     "text": f"最近运行状态：{self._last_result}"}}
                ]}

    def _get_overview_cached_or_refresh(self) -> dict:
        """取缓存概况，超过 2h 或缺失时同步刷新。"""
        if self._last_overview and time.time() - self._last_overview_ts < 2 * 3600:
            return self._last_overview
        self._refresh_overview(manual=True)
        return self._last_overview or {}

    # ------------------------------------------------------------------ #
    # API
    # ------------------------------------------------------------------ #
    def get_api(self) -> List[Dict[str, Any]]:
        """插件 API。"""
        return [{
            "path": "/run", "endpoint": self.api_run, "methods": ["POST"],
            "auth": "apikey", "summary": "立即巡检（含到期处置+复活+自动下载）",
        }, {
            "path": "/result", "endpoint": self.api_result, "methods": ["GET"],
            "auth": "apikey", "summary": "最近一次巡检结果",
        }, {
            "path": "/refresh", "endpoint": self.api_refresh, "methods": ["GET"],
            "auth": "apikey", "summary": "只刷概况（无处置动作）",
        }, {
            "path": "/data", "endpoint": self.api_data, "methods": ["GET"],
            "auth": "apikey", "summary": "复活区数据与处置日志",
        }, {
            "path": "/export", "endpoint": self.api_export, "methods": ["GET"],
            "auth": "apikey", "summary": "导出复活区列表 CSV",
        }, {
            "path": "/panel", "endpoint": self.api_panel, "methods": ["GET"],
            "auth": "apikey", "summary": "复活区工作台",
        }, {
            "path": "/stats", "endpoint": self.api_stats_page, "methods": ["GET"],
            "auth": "apikey", "summary": "做种积分统计表独立页",
        }, {
            "path": "/level", "endpoint": self.api_level_page, "methods": ["GET"],
            "auth": "apikey", "summary": "数据与等级独立页",
        }, {
            "path": "/delete_seed", "endpoint": self.api_delete_seed, "methods": ["POST"],
            "auth": "apikey", "summary": "按种子ID删除下载器任务（工作台批量删）",
        }]

    def api_run(self):
        """立即运行巡检（后台线程）。"""
        if self._running:
            return {"success": False, "message": "正在运行中，请稍候"}
        threading.Thread(target=self.run, daemon=True).start()
        return {"success": True, "message": "巡检已启动，完成后通知"}

    def api_result(self):
        """最近一次巡检结果（状态查询，只读）。"""
        return {"success": True, "result": self._last_result,
                "running": bool(self._running),
                "ran_at": (self._last_overview or {}).get("ran_at")}

    def api_refresh(self):
        """立即刷新概况快照（只读）。返回 MP 标准信封 {success,message,data}，否则前端提示「服务器返回了无效响应」。"""
        self._refresh_overview(manual=True)
        ov = self._last_overview or {}
        if self._last_overview_ts > 0:
            return {"success": True, "message": "概况已刷新",
                    "data": {"total": ov.get("total", 0), "my_seeding": ov.get("my_seeding", 0)}}
        return {"success": False, "message": "概况刷新失败（站点不可达或 Cookie 失效）", "data": None}

    def api_data(self):
        """返回概况 + 种子列表 + 处置日志（工作台数据源）。"""
        ov = self._get_overview_cached_or_refresh()
        seeds = ov.get("seeds", [])
        my_ids = set(ov.get("my_ids", []))
        now = time.time()
        # 下载器任务详情合并（面板下载器/任务状态列）：进度/速度/ETA/路径/归属
        dl_detail: Dict[str, dict] = {}
        try:
            svcs = self._get_manage_services()
            if svcs:
                for _sid, _info in (self._get_downloader_seed_map(svcs) or {}).items():
                    dl_detail[str(_sid)] = {
                        "downloader": _info.get("downloader"),
                        "progress": round(float(_info.get("progress") or 0) * 100, 1),
                        "speed": int(_info.get("speed") or 0),
                        "eta": int(_info.get("eta") or 0),
                        "save_path": _info.get("save_path") or "",
                        "state": _info.get("state") or "",
                    }
        except Exception as e:
            logger.warning(f"彩虹岛复活区管家：合并下载器任务详情失败：{e}")
        out = []
        for s in seeds:
            remain = int((s.get("free_until") or 0) - now)
            d = dl_detail.get(str(s["tid"]))
            out.append({
                "tid": s["tid"], "title": s["title"], "size_gb": s["size_gb"],
                "seeders": s["seeders"], "leechers": s["leechers"],
                "is_free": s["is_free"], "free_until": s.get("free_until"),
                "remain_sec": remain, "expired": bool(s.get("free_until") and s["free_until"] <= now),
                "hr": s.get("hr", False), "mine": s["tid"] in my_ids,
                "dl": d.get("downloader") if d else None,
                "dl_progress": d.get("progress") if d else None,
                "dl_speed": d.get("speed") if d else None,
                "dl_eta": d.get("eta") if d else None,
                "dl_path": d.get("save_path") if d else None,
                "dl_state": d.get("state") if d else None,
            })
        out.sort(key=lambda x: (not x["is_free"], x["remain_sec"]))
        return {
            "success": True,
            "overview": {k: ov.get(k) for k in
                         ("ts", "total", "pages", "free_active", "my_seeding",
                          "deleted", "early", "warned", "auto_downloaded")},
            "stats": ov.get("stats", {}),
            "portfolio_stats": ov.get("portfolio_stats", {}),
            "portfolio": ov.get("portfolio", []),
            "last_result": self._last_result, "seeds": out,
            "disposal_log": list(reversed(self.get_data("disposal_log") or []))[:50],
            "run_log": self._read_run_log(100),
            "cfg": {"delete_files": self._delete_files, "early_delete": self._early_delete,
                    "low_seeder_max": self._low_seeder_max, "auto_download": self._auto_download},
        }

    def _save_log(self, lines: List[str]):
        """持久运行日志：带时间戳追加写入 run_log.txt，并裁剪保留最近 300 行。"""
        try:
            if not lines:
                return
            path = self.get_data_path()
            if not path.exists():
                path.mkdir(parents=True, exist_ok=True)
            f = path / "run_log.txt"
            old = []
            if f.exists():
                try:
                    old = f.read_text(encoding="utf-8").splitlines()
                except Exception:
                    old = []
            ts = datetime.now().strftime("%Y-%m-%d %H:%M")
            new = [f"[{ts}] {x}" for x in lines]
            f.write_text("\n".join((old + new)[-300:]) + "\n", encoding="utf-8")
        except Exception as e:
            logger.error(f"彩虹岛复活区管家：保存运行日志失败：{e}")

    def _read_run_log(self, limit: int = 100) -> List[str]:
        """读取持久运行日志最后 limit 行（工作台运行日志弹窗数据源）。"""
        try:
            f = self.get_data_path() / "run_log.txt"
            if not f.exists():
                return []
            return f.read_text(encoding="utf-8").splitlines()[-limit:]
        except Exception:
            return []

    def api_export(self):
        """导出复活区列表为 CSV。"""
        ov = self._get_overview_cached_or_refresh()
        seeds = ov.get("seeds", [])
        my_ids = set(ov.get("my_ids", []))
        now = time.time()
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["种子ID", "种子名称", "大小GB", "做种人数", "下载人数", "免费状态", "剩余秒", "我做种", "HR"])
        for s in seeds:
            remain = int((s.get("free_until") or 0) - now)
            w.writerow([s["tid"], s["title"], s["size_gb"], s["seeders"], s["leechers"],
                        "免费中" if (s["is_free"] and remain > 0) else ("已过期" if s.get("free_until") else "-"),
                        remain, "是" if s["tid"] in my_ids else "", "是" if s.get("hr") else ""])
        return Response(
            content="\ufeff" + buf.getvalue(), media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": "attachment; filename=rainbow_revive_seeds.csv"})

    def api_delete_seed(self, tid: str = None):
        """按种子 ID 删除其在下载器中的任务（工作台批量删除用）。"""
        if not tid:
            return {"success": False, "message": "缺少 tid 参数"}
        ov = self._get_overview_cached_or_refresh()
        seed = next((s for s in ov.get("seeds", []) if s["tid"] == str(tid)), None)
        if not seed:
            return {"success": False, "message": f"复活区未找到种子 {tid}"}
        norm = self._norm_title(seed["title"])
        services = self._get_manage_services()
        for dl_name, sv in services.items():
            try:
                ts, err = sv.instance.get_torrents()
                if err or not ts:
                    continue
                del_ids = [self._thash(t) for t in ts
                           if self._norm_title(self._tf(t, "name", default="")) == norm]
                if not del_ids:
                    continue
                sv.instance.delete_torrents(delete_file=self._delete_files, ids=del_ids)
                # 删除后复核：全部不存在才算成功（qb/tr 删除内部可能吞异常返回 False）
                try:
                    ts2, err2 = sv.instance.get_torrents(ids=del_ids)
                    leftover = len(ts2 or []) if not err2 else len(del_ids)
                except Exception:
                    leftover = len(del_ids)
                ok = leftover == 0
                self._append_disposal({"time": int(time.time()), "tid": seed["tid"], "title": seed["title"],
                                       "size_gb": seed["size_gb"], "progress": 0, "downloader": dl_name,
                                       "action": "manual_delete", "ok": ok})
                if not ok:
                    return {"success": False, "message": f"{dl_name} 删除未生效（仍剩 {leftover} 个任务），请重试"}
                # 删除成功后本地回写概况（不发站点请求）：列表剔除该种子并重算计数
                try:
                    if isinstance(self._last_overview, dict) and self._last_overview.get("seeds"):
                        _tid = seed["tid"]
                        _ov = self._last_overview
                        _ov["seeds"] = [x for x in _ov["seeds"] if x.get("tid") != _tid]
                        _ov["total"] = len(_ov["seeds"])
                        _now_ts = time.time()
                        _ov["free_active"] = sum(
                            1 for x in _ov["seeds"]
                            if x.get("is_free") and x.get("free_until") and x["free_until"] > _now_ts)
                        if isinstance(_ov.get("my_ids"), (list, set)):
                            _ov["my_ids"] = sorted(t for t in _ov["my_ids"] if t != _tid)
                            _ov["my_seeding"] = len(_ov["my_ids"])
                        _ov["ts"] = _now_ts
                except Exception:
                    pass
                return {"success": True, "message": f"已从 {dl_name} 删除 {len(del_ids)} 个任务"}
            except Exception as e:
                logger.error(f"彩虹岛复活区管家：手动删除失败 {seed['title'][:40]}（{dl_name}）：{e}")
        return {"success": False, "message": "下载器中未找到匹配任务"}

    def api_panel(self):
        """工作台单文件 HTML 页。"""
        apikey = getattr(settings, "API_TOKEN", "") or ""
        return HTMLResponse(content=self._build_panel_html(apikey))

    # ------------------------------------------------------------------ #
    # 页面（Vuetify JSON）
    # ------------------------------------------------------------------ #
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """配置表单（照原作者结构：顶部数据面板 + 响应式栅格 + VCronField）。"""
        downloaders = []
        try:
            for config in DownloaderHelper().get_configs().values():
                downloaders.append({"title": config.name, "value": config.name})
        except Exception as e:
            logger.warning(f"彩虹岛复活区管家：获取下载器列表失败：{e}")
        # 目标体积 hint 实时数值（HH 同款）：已做种/在途来自最近一次优选巡检
        cur_gb = getattr(self, "_last_current_gb", None)
        infl_gb = getattr(self, "_last_in_flight_gb", 0.0) or 0.0
        if cur_gb is not None:
            target_hint = f"含复活区已做种 {cur_gb:.1f} GB + 在途 {infl_gb:.1f} GB，0=不限"
        else:
            target_hint = "含复活区已做种 + 在途总体积，0=不限（巡检运行后显示实时数值）"
        form = [{
            "component": "VForm",
            "content": [
                {**self._overview_form_cards()},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 3},
                     "content": [{"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 3},
                     "content": [{"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即运行一次"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 3},
                     "content": [{"component": "VSwitch", "props": {"model": "notify", "label": "发送通知"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 3},
                     "content": [{"component": "VSwitch", "props": {"model": "delete_files", "label": "删除时含数据文件"}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VCronField", "props": {
                         "model": "cron", "label": "执行周期",
                         "placeholder": "0 */2 * * *（每2小时）"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VTextField", "props": {
                         "model": "dispose_minutes", "label": "到期处置间隔（分钟）", "type": "number",
                         "hint": "守护线程频率，越小越接近「到期即删」", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VTextField", "props": {
                         "model": "warn_hours", "label": "到期前预警（小时）", "type": "number"}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VTextField", "props": {
                         "model": "site_name", "label": "站点名", "placeholder": "彩虹岛",
                         "hint": "自动匹配站点管理中的彩虹岛（ptchdbits）", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VTextField", "props": {
                         "model": "low_seeder_max", "label": "复活做种阈值（人）", "type": "number",
                         "hint": "免费且做种≤N 推送；0=关闭复活", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VTextField", "props": {
                         "model": "auto_clean_days", "label": "僵尸任务清理（天）", "type": "number",
                         "hint": "本站未完成且超N天的任务连文件删除；0=关闭", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VSelect", "props": {
                         "model": "downloaders", "label": "管理下载器（多选）",
                         "items": downloaders, "multiple": True, "chips": True,
                         "hint": "统计/处置/优选/删除覆盖所选下载器", "persistent-hint": True}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VSwitch", "props": {
                         "model": "early_delete", "label": "预计完不成提前删",
                         "hint": "按当前速率 ETA 超过剩余免费时长即提前删", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VSwitch", "props": {
                         "model": "auto_download", "label": "复活自动下载",
                         "hint": "低做种免费种子自动推送下载器", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VSelect", "props": {
                         "model": "push_downloader", "label": "推送下载器（单选）",
                         "items": downloaders, "hint": "新增任务的下载器，默认=管理下载器第一个", "persistent-hint": True}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 6},
                     "content": [{"component": "VSelect", "props": {
                         "model": "mode", "label": "优选模式",
                         "items": [
                             {"title": "增量优选（不删保种&补齐体积）", "value": "incremental"},
                             {"title": "换种优选（删低档位&补高档位）", "value": "wash"}
                         ]}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 6},
                     "content": [{"component": "VTextField", "props": {
                         "model": "target_volume", "label": "目标体积（GB，总保种上限）", "type": "number",
                         "hint": target_hint, "persistent-hint": True}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 6},
                     "content": [{"component": "VTextField", "props": {
                         "model": "save_path", "label": "保存路径（跟随推送下载器）",
                         "placeholder": "如 /downloads/下载/复活",
                         "hint": "换种淘汰只动保存在此目录内的复活区种子；留空则只按复活区匹配", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 6},
                     "content": [{"component": "VTextField", "props": {
                         "model": "tag", "label": "自定义标签", "placeholder": "如 彩虹岛保种"}}]},
                ]},
            ],
        }]
        defaults = {
            "enabled": False, "onlyonce": False, "notify": True,
            "cron": "0 */2 * * *", "site_name": "彩虹岛", "downloaders": [],
            "warn_hours": 2, "delete_files": True, "early_delete": True,
            "low_seeder_max": 3, "dispose_minutes": 5, "auto_clean_days": 0,
            "auto_download": False, "push_downloader": "", "save_path": "",
            "tag": "", "target_volume": 0, "mode": "incremental",
        }
        return form, defaults

    def get_page(self) -> Optional[List[dict]]:
        """插件详情页（照原作者 HHClubButler 五段式：刷新行 / 概览卡 / 运行状态 / 工作台入口）。"""
        apikey = getattr(settings, "API_TOKEN", "") or ""
        try:
            self._get_overview_cached_or_refresh()
        except Exception as e:
            logger.error(f"彩虹岛复活区管家：详情页刷新概况失败：{e}")
        link_style = ('display:inline-flex;align-items:center;gap:6px;padding:7px 18px;background:rgba(66,165,245,.12);'
                      'color:#42A5F5;text-decoration:none;border-radius:4px;font-size:13px;font-weight:500;border:1px solid rgba(66,165,245,.3);')
        _pid = self.__class__.__name__   # 分身（plugin.clone）下用实例 ID，页面不打到源实例 API
        panel_link = (
            '<div style="padding:10px 0;display:flex;align-items:center;gap:10px;">'
            f'<a href="/api/v1/plugin/{_pid}/stats?apikey={apikey}" target="_blank" '
            f'style="{link_style}">📊 做种积分统计</a>'
            f'<a href="/api/v1/plugin/{_pid}/level?apikey={apikey}" target="_blank" '
            f'style="{link_style}">📈 数据与等级</a>'
            f'<a href="/api/v1/plugin/{_pid}/panel?apikey={apikey}" target="_blank" '
            f'style="{link_style}">🖥️ 复活区管理器</a>'
            '<span style="font-size:11px;color:rgba(var(--v-theme-on-surface),.45);">独立工作台 · 批量删除 · 搜索 · 档位 · 筛选 · 列自定义 · 多皮肤等功能</span>'
            '</div>'
        )
        return [{
            "component": "VRow", "props": {"density": "compact", "no-gutters": True},
            "content": [
                {"component": "VCol", "props": {"cols": 12, "md": 12},
                 "content": [
                     {"component": "VRow", "props": {"class": "d-flex justify-space-between align-center", "no-gutters": True},
                      "content": [
                          {"component": "div", "html": (
                              '<div style="font-size:10.5px;color:rgba(var(--v-theme-on-surface),.4);'
                              'margin-right:10px;white-space:nowrap;">'
                              + self._refresh_note(self._last_overview_ts, self._cron_hours)
                              + '</div>')},
                          {"component": "div", "html": '<a style="cursor:pointer;color:#42A5F5;text-decoration:none;white-space:nowrap;">🔄 立即刷新</a>',
                           "events": {"click": {"api": f"plugin/{_pid}/refresh", "method": "get",
                                                "params": {"apikey": apikey}}}}
                      ]},
                     {"component": "div", "html": self._overview_html(
                         self._last_overview or {}, self._last_overview_ts, self._cron_hours)},
                     {"component": "VAlert", "props": {"type": "info", "variant": "tonal",
                                                      "text": f"最近运行状态：{self._last_result}"}},
                     {"component": "div", "html": panel_link},
                 ]},
            ]
        }]

    def get_dashboard_meta(self) -> Optional[List[Dict[str, str]]]:
        """仪表盘元数据。"""
        return [{"key": "revive", "name": "彩虹岛复活区管家"}]

    def get_dashboard(self, key: str, **kwargs):
        """首页仪表盘（照原作者三元组格式）：账户统计 + 复活区档位 + 做种组合。"""
        if key and key != "revive":
            return None
        try:
            ov = self._get_overview_cached_or_refresh()
        except Exception as e:
            logger.error(f"彩虹岛复活区管家：仪表盘概况获取失败：{e}")
            ov = {}
        elements = [
            {"component": "div",
             "props": {"style": "text-align:left;font-size:10.5px;color:rgba(var(--v-theme-on-surface),.4);margin-bottom:6px;white-space:nowrap;"},
             "text": self._refresh_note(self._last_overview_ts, 2.0)},
            {"component": "div", "html": self._overview_html(
                ov, self._last_overview_ts, self._cron_hours)},
        ]
        return ({"cols": 12, "md": 6},
                {"refresh": 60, "title": "彩虹岛复活区管家", "border": True},
                elements)

    # ------------------------------------------------------------------ #
    # 工作台（单文件 HTML，移植 HHClubButler 全套交互）
    # ------------------------------------------------------------------ #
    def _append_credit_sample(self, credit: float, magic: float = 0.0, downloaded_gb: float = 0.0,
                              ratio: float = 0.0, uploaded_gb: float = 0.0):
        """记录做种积分/魔力/下载量/分享率/上传量采样（供统计表算日增量、月度表与等级页昨日增量/升阶预估），
        上限 5000 条、400 天自清（覆盖近12个月月度表）。"""
        if not credit:
            return
        try:
            hist = self.get_data("credit_history") or []
            if not isinstance(hist, list):
                hist = []
            now = time.time()
            hist.append({"ts": now, "credit": float(credit), "magic": float(magic or 0),
                         "dl_gb": float(downloaded_gb or 0), "ratio": float(ratio or 0),
                         "up_gb": float(uploaded_gb or 0)})
            hist = [h for h in hist if isinstance(h, dict) and h.get("ts", 0) > now - 400 * 86400]
            self.save_data("credit_history", hist[-5000:])
        except Exception as e:
            logger.warning(f"彩虹岛复活区管家：记录做种积分采样失败：{e}")

    # 彩虹岛（CHDBits/NexusPHP）站点等级规则（faq.php 实测，2026-10-23）：
    # (class_key, 中文名, 注册周数, 下载量GB, 分享率, 魔力值W)
    LEVEL_RULES = [
        ("User", "未烧", 0, 0, 0.0, 0),
        ("PowerUser", "感冒", 5, 200, 2.0, 8),
        ("Elite", "发热", 10, 500, 3.0, 15),
        ("Crazy", "低烧", 15, 800, 4.0, 30),
        ("Insane", "中烧", 20, 999, 5.0, 65),
        ("Veteran", "高烧", 25, 1500, 6.0, 100),
        ("Extreme", "烧糊涂", 30, 2048, 7.0, 220),
        ("Ultimate", "走火入魔", 40, 3072, 8.0, 350),
        ("NexusMaster", "骨灰", 52, 4096, 10.0, 500),
    ]

    def _yesterday_deltas_data(self) -> dict:
        """基于 credit_history 采样算昨日增量（credit/magic/dl_gb），无样本返回 None 项。"""
        out = {"credit": None, "magic": None, "dl_gb": None, "up_gb": None}
        try:
            hist = self.get_data("credit_history") or []
            if not isinstance(hist, list) or len(hist) < 2:
                return out
            hist = [h for h in hist if isinstance(h, dict) and h.get("ts")]
            if len(hist) < 2:
                return out
            hist.sort(key=lambda h: h.get("ts", 0))
            last = hist[-1]
            # 找 ts <= 最新-24h 内最靠近的一条（容忍 20~36h 前的采样）
            target = last.get("ts", 0) - 86400
            cand = None
            for h in reversed(hist[:-1]):
                if target - 10800 <= h.get("ts", 0) <= target + 21600:
                    cand = h
                    break
            if cand is None:
                return out
            for k_src, k_dst in (("credit", "credit"), ("magic", "magic"), ("dl_gb", "dl_gb"), ("up_gb", "up_gb")):
                if isinstance(last.get(k_src), (int, float)) and isinstance(cand.get(k_src), (int, float)):
                    out[k_dst] = round(float(last[k_src]) - float(cand[k_src]), 2)
            return out
        except Exception:
            return out

    def _level_upgrade_info(self, prof: dict, stats: dict) -> Optional[dict]:
        """当前等级 → 下一级四维条件核对与预计升阶天数（基于昨日采样增量差分）。"""
        try:
            uc = str(prof.get("user_class") or "").lower()
            idx = 0
            # 两遍匹配：先精确等值，再子串兜底。
            # 单遍子串会让 "user" in "poweruser" 先命中 User 规则，把 PowerUser 误判为未烧。
            for i, (key, _, _, _, _, _) in enumerate(self.LEVEL_RULES):
                if key.lower() == uc:
                    idx = i
                    break
            else:
                for i, (key, _, _, _, _, _) in enumerate(self.LEVEL_RULES):
                    kl = key.lower()
                    if kl in uc or uc in kl:
                        idx = i
                        break
            if idx >= len(self.LEVEL_RULES) - 1:
                return {"maxed": True, "cur": self.LEVEL_RULES[idx][1], "idx": idx}
            _, cur_cn, _, _, _, _ = self.LEVEL_RULES[idx]
            nkey, nn_cn, n_weeks, n_dl_gb, n_ratio, n_magic_w = self.LEVEL_RULES[idx + 1]

            # 当前值
            try:
                reg_dt = datetime.strptime(str(prof.get("reg_date") or ""), "%Y-%m-%d %H:%M:%S")
                reg_days = max(0.0, (datetime.now() - reg_dt).total_seconds() / 86400)
            except Exception:
                reg_days = None
            dl_gb = (stats.get("downloaded_tb") or 0) * 1024
            ratio = stats.get("ratio") or 0
            magic = stats.get("magic") or 0

            deltas = self._yesterday_deltas_data()
            conds = []
            unknown = False
            days_list = []
            # 注册周数（确定性）
            if reg_days is None:
                conds.append({"k": "注册", "ok": False, "txt": "未知注册时间"})
                unknown = True
            else:
                need_days = n_weeks * 7
                rem_days = max(0.0, need_days - reg_days)
                ok = rem_days <= 0
                conds.append({"k": "注册", "ok": ok,
                              "txt": (f"{reg_days/7:.1f}周 ≥ {n_weeks}周" if ok
                                      else f"{reg_days/7:.1f}/{n_weeks}周 · 差 {math.ceil(rem_days)}天")})
                days_list.append(rem_days)
            # 下载量
            ok = dl_gb >= n_dl_gb
            if ok:
                conds.append({"k": "下载", "ok": True, "txt": f"{dl_gb:.0f}G ≥ {n_dl_gb}G"})
            else:
                g = deltas.get("dl_gb")
                if g and g > 0:
                    d = math.ceil((n_dl_gb - dl_gb) / g)
                    conds.append({"k": "下载", "ok": False, "txt": f"{dl_gb:.0f}/{n_dl_gb}G · 按昨日 {g:.0f}G/日约 {d}天"})
                    days_list.append(d)
                else:
                    conds.append({"k": "下载", "ok": False, "txt": f"{dl_gb:.0f}/{n_dl_gb}G · 需手动下载"})
                    unknown = True
            # 分享率
            ok = ratio >= n_ratio
            if ok:
                conds.append({"k": "分享率", "ok": True, "txt": f"{ratio:.2f} ≥ {n_ratio:g}"})
            else:
                conds.append({"k": "分享率", "ok": False, "txt": f"{ratio:.2f}/{n_ratio:g} · 需提升"})
                unknown = True
            # 魔力值
            n_magic = n_magic_w * 10000
            ok = magic >= n_magic
            if ok:
                conds.append({"k": "魔力", "ok": True, "txt": f"{magic/10000:.1f}W ≥ {n_magic_w}W"})
            else:
                g = deltas.get("magic")
                if g and g > 0:
                    d = math.ceil((n_magic - magic) / g)
                    conds.append({"k": "魔力", "ok": False, "txt": f"{magic/10000:.1f}/{n_magic_w}W · 按昨日 {g/10000:.1f}W/日约 {d}天"})
                    days_list.append(d)
                else:
                    conds.append({"k": "魔力", "ok": False, "txt": f"{magic/10000:.1f}/{n_magic_w}W · 采样积累中"})
                    unknown = True

            if all(c["ok"] for c in conds):
                days_txt = "条件已达标，等待站点自动升级"
            elif days_list and not unknown:
                days_txt = f"预计约 {math.ceil(max(days_list))} 天"
            elif days_list:
                top = math.ceil(max(days_list))
                days_txt = ("自动条件已达标，其余条件需手动满足" if top <= 0
                            else f"最快约 {top} 天（另有条件需手动）")
            else:
                days_txt = "采样积累中，暂无法预估"
            return {"maxed": False, "cur": cur_cn, "next": nn_cn, "next_key": nkey,
                    "conds": conds, "days_txt": days_txt, "idx": idx,
                    "next2": (self.LEVEL_RULES[idx + 2][1] if idx + 2 < len(self.LEVEL_RULES) else None),
                    "yday": deltas}
        except Exception as e:
            logger.warning(f"彩虹岛复活区管家：升阶预估计算失败：{e}")
            return None

    def _fetch_profile(self, session: requests.Session, base: str) -> dict:
        """从档案页解析账户资料：加入日期/最近动向/邀请人/HP/BT时间比率/做种时间/下载时间。"""
        uid = self._get_site_uid(session, base)
        if not uid:
            return {}
        try:
            r = self._get_with_retry(session, base + f"/userdetails.php?id={uid}", timeout=25)
            t = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", r.text))

            def g(pat, default=""):
                m = re.search(pat, t)
                return m.group(1).strip() if m else default

            um = re.search(r"class=['\"](\w+)_Name['\"]><b>([^<]+)</b>", r.text)
            # 头像：优先 class 含 avatar 的 img，其次 src 含 avatar 的 img；解析为绝对地址
            avatar = ""
            try:
                am = re.search(r'<img[^>]+class=["\'][^"\']*avatar[^"\']*["\'][^>]*>', r.text, re.I)
                src_m = None
                if am:
                    src_m = re.search(r'(?:data-src|src)=["\']([^"\']+)["\']', am.group(0), re.I)
                if not src_m:
                    src_m = re.search(r'<img[^>]+(?:data-src|src)=["\']([^"\']*avatar[^"\']*)["\']', r.text, re.I)
                if src_m:
                    u = src_m.group(1).strip()
                    if u.startswith("//"):
                        u = "https:" + u
                    elif u.startswith("/"):
                        u = base + u
                    elif not re.match(r"https?://", u, re.I):
                        u = base.rstrip("/") + "/" + u
                    if re.match(r"https?://", u, re.I):
                        avatar = u
            except Exception:
                avatar = ""
            return {
                "uid": uid,
                "username": um.group(2).strip() if um else "-",
                "user_class": um.group(1) if um else "Member",
                "avatar": avatar,
                "reg_date": g(r"加入日期\s*([\d-]+ [\d:]+)"),
                "last_active": g(r"最近动向\s*([\d-]+ [\d:]+)"),
                "inviter": g(r"邀请人\s*(\S+)", "-"),
                "hp": g(r"HP\s+(\d+)\s+BT时间"),
                "bt_ratio": g(r"做种/下载时间比率\s*[:：]\s*([\d.]+)"),
                "seed_time": g(r"做种时间\s*[:：]\s*([\d天h:.]+?)(?:\s|下载|&nbsp)"),
                "dl_time": g(r"下载时间\s*[:：]\s*([\d天h:.]+?)(?:\s|性别|&nbsp|$)"),
            }
        except Exception as e:
            logger.warning(f"彩虹岛复活区管家：解析档案页失败：{e}")
            return {}

    def _fetch_task_stats(self, session: requests.Session, base: str) -> dict:
        """从任务页解析彩虹岛原生任务系统全站统计（各档完成/失败/比例）。"""
        try:
            r = self._get_with_retry(session, base + "/selfassess.php", timeout=25)
            t = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", r.text))
            tiers = {}
            for name in ("Master", "Ultimate", "Extreme", "Veteran", "Insane"):
                m = re.search(name + r"：完成(\d+)、失败(\d+)、完成比例([\d.]+)%", t)
                if m:
                    tiers[name] = {"done": int(m.group(1)), "fail": int(m.group(2)),
                                   "pct": float(m.group(3))}
            m = re.search(r"任务系统当前人数：(\d+)人", t)
            return {"tiers": tiers, "busy": int(m.group(1)) if m else 0}
        except Exception as e:
            logger.warning(f"彩虹岛复活区管家：解析任务统计失败：{e}")
            return {}

    def api_stats_page(self, days: Optional[int] = None, persist: Optional[int] = None):
        """做种积分统计独立页（完整移植原作者「结算统计表」模板：渐变底/ECharts柱状图/双仪表/网格表/月表）。

        范围持久化（HH monthly_stats_range 同款语义）：?persist=N 仅回写不渲染；渲染优先取 ?days=N，
        否则读上次存档（默认 15）——切换后刷新不再回落。
        """
        _rng = self.get_data("stats_range") or {}
        _saved_days = _rng.get("days") if isinstance(_rng, dict) else None
        try:
            _saved_days = int(_saved_days or 15)
        except Exception:
            _saved_days = 15
        if persist is not None:
            _pd = max(1, min(30, int(persist)))
            self.save_data("stats_range", {"days": _pd})
            return HTMLResponse(content="")
        view_days = max(1, min(30, int(days))) if days else _saved_days
        hist = self.get_data("credit_history") or []
        tz_sh = pytz.timezone("Asia/Shanghai")
        # 按自然日聚合采样（日表只看近 40 天；聚合键用完整日期保证 12月/1月 跨年排序正确，展示仍用月-日）
        now_ts = time.time()
        daily_hist = [h for h in hist if isinstance(h, dict) and h.get("ts", 0) > now_ts - 40 * 86400]
        days: Dict[str, dict] = {}
        for h in daily_hist:
            if not h.get("credit"):
                continue
            dt = datetime.fromtimestamp(h["ts"], tz_sh)
            key = dt.strftime("%Y-%m-%d")
            d = days.setdefault(key, {"label": dt.strftime("%m-%d"),
                                      "first": h["credit"], "last": h["credit"], "n": 0})
            d["last"] = h["credit"]
            d["n"] += 1
        ordered = sorted(days.items())  # 完整日期键 → 跨年顺序正确
        cum = 0.0
        records = []
        for key, d in ordered:
            prev_last = records[-1]["last"] if records else None
            delta = (d["last"] - prev_last) if prev_last is not None else None
            cum += delta or 0
            records.append({"date": d["label"], "first": d["first"], "last": d["last"],
                            "delta": delta, "n": d["n"], "cum": round(cum, 1)})
        cur = records[-1]["last"] if records else 0
        cum30 = round(sum((r["delta"] or 0) for r in records[-30:]), 1)
        # 近12个月收益月度表：全量采样按自然月聚合；月收益 = 月末采样 − 上月末采样（首月按 末−首）
        tz_sh = pytz.timezone("Asia/Shanghai")
        month_map: Dict[str, dict] = {}
        for h in sorted((x for x in hist if isinstance(x, dict) and x.get("credit")),
                        key=lambda x: x.get("ts", 0)):
            mk = datetime.fromtimestamp(h["ts"], tz_sh).strftime("%Y-%m")
            m = month_map.setdefault(mk, {"first": h["credit"], "last": h["credit"], "n": 0,
                                          "first_ts": h["ts"]})
            m["last"] = h["credit"]
            m["n"] += 1
        monthly = []
        prev_last = None
        cum_m = 0.0
        _all_mk = sorted(month_map)
        _now_mk = datetime.now(tz_sh).strftime("%Y-%m")
        for mk in _all_mk:
            d = month_map[mk]
            delta = (d["last"] - prev_last) if prev_last is not None else (d["last"] - d["first"])
            cum_m += delta
            # 部分月标注（HH 月表同款语义）：当前月=进行中；最早月首采样不在 1 日=自 m/d 起
            note = ""
            if mk == _now_mk:
                note = "进行中"
            elif mk == _all_mk[0]:
                _fd = datetime.fromtimestamp(d.get("first_ts", 0), tz_sh)
                if _fd.day > 1:
                    note = f"自{_fd.month}/{_fd.day}起"
            monthly.append({"month": mk, "first": d["first"], "last": d["last"],
                            "delta": round(delta, 1), "n": d["n"],
                            "cum": round(cum_m, 1), "note": note})
            prev_last = d["last"]
        monthly = monthly[-12:]
        payload = json.dumps({"records": records, "current": cur, "cum30": cum30,
                              "monthly": monthly, "days": view_days,
                              "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
                             ensure_ascii=False)
        html = _STATS_HTML.replace("__DATA__", payload)
        return HTMLResponse(content=html)

    def api_level_page(self):
        """数据与等级独立页（完整移植原作者「数据与等级」模板：头像卡/KPI带/进度条/雷达/阶梯）。"""
        try:
            base = self._get_site_url()
            cookie = self._get_site_cookie()
            session = self._session(cookie)
            prof = self._fetch_profile(session, base)
            task = self._fetch_task_stats(session, base)
            stats = self._fetch_site_stats(session, base)
        except Exception as e:
            logger.error(f"彩虹岛复活区管家：数据与等级抓取失败：{e}")
            return HTMLResponse(content=(
                '<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8"><title>数据与等级</title>'
                '<style>body{background:#0d1321;color:#e8edf7;font-family:"PingFang SC",sans-serif;'
                'padding:40px;text-align:center}.card{max-width:520px;margin:0 auto;background:#111c33;'
                'border:1px solid #1e3357;border-radius:14px;padding:30px}</style></head><body><div class="card">'
                f'<div style="font-size:16px;font-weight:700;margin-bottom:10px;">数据与等级 · 获取失败</div>'
                f'<div style="color:#f87171;line-height:1.8;">{str(e)}</div>'
                '<div style="margin-top:14px;font-size:12px;color:#7d8db1;">请检查站点 Cookie 是否有效</div>'
                '</div></body></html>'))

        username = prof.get("username", "-")
        uid = prof.get("uid", "-")
        cls = prof.get("user_class", "Member")
        meta = (f"UID {uid} · 加入 {prof.get('reg_date', '—')} · 最近动向 {prof.get('last_active', '—')}"
                f" · 邀请人 {prof.get('inviter', '—')}")
        lvline = (f"HP {prof.get('hp', '—')} · 分享率 {stats.get('ratio', 0):.3f}"
                  f" · BT比率 {prof.get('bt_ratio', '—')} · 做种时间 {prof.get('seed_time', '—')}")

        # KPI 带（原模板 8 卡结构 + 原作者 SVG 图标）
        credit = stats.get("credit", 0) or 0
        rate = stats.get("credit_rate")
        yd = self._yesterday_deltas_data()
        upg = self._level_upgrade_info(prof, stats)
        def _kpi(ic, k, v, s, cls=""):
            return (f'<div class="kpi"><div class="ic">{ic}</div>'
                    f'<div class="k">{k}</div><div class="v {cls}">{v}</div>'
                    f'<div class="s">{s}</div></div>')
        kpis = (
            _kpi(_IC_UP, "上传量", f"{stats.get('uploaded_tb', 0):.3f} TB", "累计上传", "green")
            + _kpi(_IC_DOWN, "下载量", f"{stats.get('downloaded_tb', 0):.3f} TB", "累计下载", "blue")
            + _kpi(_IC_RATIO, "分享率", f"{stats.get('ratio', 0):.3f}", "上传 ÷ 下载", "green")
            + _kpi(_IC_PTS, "做种积分", f"{credit:,.0f}", f"+{rate:.0f}/h" if rate else "巡检采样增速", "blue")
            + _kpi(_IC_BEAN, "魔力值", _fmt_wan(stats.get("magic", 0)), f"余额 {stats.get('magic', 0):,.0f}", "bean")
            + _kpi(_IC_ZAP_G, "当前做种", f"{stats.get('seeding_now', 0)}", "个任务在做种")
            + _kpi(_IC_ZAP_B, "HP", prof.get("hp", "—"), "H&R 容量点数")
            + _kpi(_IC_TREND, "H&R", f"{stats.get('hr', 0)}", "违规扣点")
            + _kpi(_IC_PTS, "昨日积分增量",
                   (f"+{yd['credit']:,.0f}" if yd.get("credit") is not None else "积累中"),
                   (f"魔力 +{yd['magic']:,.0f}" if yd.get("magic") is not None else "跨日采样差分"),
                   "green")
            + _kpi(_IC_UP, "昨日下载增量",
                   (f"+{yd['dl_gb']:,.0f} GB" if yd.get("dl_gb") is not None else "积累中"),
                   "跨日采样差分", "blue")
            + _kpi(_IC_TREND, "昨日上传增量",
                   (f"+{yd['up_gb']:,.0f} GB" if yd.get("up_gb") is not None else "积累中"),
                   "跨日采样差分", "green")
        )

        # 进度条卡：任务系统五档完成率（替代原「下一级达标进度」）
        tiers = task.get("tiers") or {}
        tier_order = [("Insane", "疯狂"), ("Veteran", "资深"), ("Extreme", "极限"), ("Ultimate", "终极"), ("Master", "骨灰")]
        bars = ""
        for en, cn in tier_order:
            t = tiers.get(en)
            if not t:
                continue
            pct = t["pct"]
            color = "#2ee6a8" if pct >= 90 else ("#fbbf24" if pct >= 80 else "#f87171")
            bars += (f'<div class="bar"><div class="r">'
                     f'<span class="need">{en} {cn} · 完成 {t["done"]:,} / 失败 {t["fail"]:,}</span>'
                     f'<span class="val" style="color:{color}">{pct:.2f}%</span></div>'
                     f'<div class="track"><div class="fill" style="width:{pct:.1f}%;background:{color}"></div></div></div>')

        # 雷达：四维（上传/下载/分享率/做种积分）归一化
        axes = [("上传", min(100.0, stats.get("uploaded_tb", 0) / 10 * 100)),
                ("下载", min(100.0, stats.get("downloaded_tb", 0) / 10 * 100)),
                ("分享率", min(100.0, stats.get("ratio", 0) / 5 * 100)),
                ("做种积分", min(100.0, credit / 500000 * 100))]
        radar_svg = _radar_svg(axes)
        avg_pct = (sum(t["pct"] for t in tiers.values()) / len(tiers)) if tiers else 0

        # 阶梯卡：任务五档
        steps = ""
        max_done = max((tiers.get(en, {}).get("pct", 0) for en, _ in tier_order), default=0)
        for en, cn in tier_order:
            t = tiers.get(en)
            if not t:
                continue
            cls = "done" if t["pct"] >= 90 else ("cur" if t["pct"] == max_done else "")
            tag = f'<span class="tagcur">当前最高</span>' if t["pct"] == max_done and max_done > 0 else ""
            steps += (f'<div class="step {cls}">{tag}<div class="en">{en}</div>'
                      f'<div class="cn">{cn} {t["pct"]:.1f}%</div></div>')

        # 下方双卡：做种组合 + 账户附加
        ps = self._get_overview_cached_or_refresh().get("portfolio_stats", {}) or {}
        next_cards = (
            f'<div class="ncard"><div class="tt"><div class="t1">💎 做种组合</div>'
            f'<div class="t2">高价值(0-1人) {ps.get("tier_low", 0)} · 中(2-3人) {ps.get("tier_mid", 0)} · 摊薄(4+人) {ps.get("tier_high", 0)}</div></div>'
            f'<div class="dd"><b>{ps.get("count", 0)}</b> 颗<br>{ps.get("total_gb", 0):.0f} GB</div></div>'
            f'<div class="ncard"><div class="tt"><div class="t1">📋 任务系统</div>'
            f'<div class="t2">全站当前 {task.get("busy", 0)} 人 · 五档平均完成率</div></div>'
            f'<div class="dd"><b>{avg_pct:.1f}%</b></div></div>')
        # 升阶预估卡（FAQ 等级规则 + 采样差分）
        if upg:
            if upg.get("maxed"):
                upg_card = (f'<div class="ncard"><div class="tt"><div class="t1">🏆 等级升阶</div>'
                            f'<div class="t2">当前 {upg.get("cur", "-")} · 已是最高等级</div></div>'
                            f'<div class="dd"><b>MAX</b></div></div>')
            else:
                cond_html = "".join(
                    f'<div style="display:flex;justify-content:space-between;gap:10px;font-size:12px;'
                    f'padding:2px 0;"><span style="color:{("#2ee6a8" if c["ok"] else "#f87171")}">'
                    f'{"✓" if c["ok"] else "✗"} {c["k"]}</span>'
                    f'<span style="color:#9fb0cf;text-align:right;">{_esc(c["txt"])}</span></div>'
                    for c in upg.get("conds", []))
                upg_card = (f'<div class="ncard" style="flex:1 1 100%;width:100%;">'
                            f'<div class="tt"><div class="t1">⬆️ 等级升阶 · {upg.get("cur", "-")} → {upg.get("next", "-")}</div>'
                            f'<div class="t2">{upg.get("days_txt", "")}'
                            + (f' · 再下一级：{upg["next2"]}' if upg.get("next2") else "")
                            + '</div></div>'
                            f'<div style="margin-top:6px;">{cond_html}</div></div>')
            next_cards = next_cards + upg_card

        # 站点账号等级阶梯（LEVEL_RULES 9 级，当前等级高亮；idx 来自升阶预估）
        _acc_idx = (upg or {}).get("idx") or 0
        acc_steps = ""
        for _i, (_k, _cn, *_r) in enumerate(self.LEVEL_RULES):
            _cls = "done" if _i < _acc_idx else ("cur" if _i == _acc_idx else "")
            _tag = '<span class="tagcur">当前等级</span>' if _i == _acc_idx else ""
            acc_steps += (f'<div class="step {_cls}">{_tag}<div class="en">{_k}</div>'
                          f'<div class="cn">{_cn}</div></div>')

        foot = ("数据来源：ptchdbits.co 档案页/任务页/页头实时解析 · "
                "任务系统完成率为全站统计（Master/Ultimate/Extreme/Veteran/Insane）· "
                "做种积分增速由巡检跨次采样计算")
        avatar_url = prof.get("avatar") or ""
        avatar_html = (f'<img src="{_esc(avatar_url)}" alt="" '
                       f'onerror="this.onerror=null;this.style.display=\'none\'">' if avatar_url else _AVATAR_SVG)
        html = (_LEVEL_HTML
                .replace("__AVATAR__", avatar_html)
                .replace("__USERNAME__", _esc(username))
                .replace("__META__", _esc(meta))
                .replace("__BADGE__", _esc(cls))
                .replace("__LVLINE__", _esc(lvline))
                .replace("__KPIS__", kpis)
                .replace("__BARS__", bars)
                .replace("__RADAR__", radar_svg)
                .replace("__UP_PCT__", f"{avg_pct:.0f}")
                .replace("__STEPS__", steps)
                .replace("__ACCLADDER__", acc_steps)
                .replace("__NEXT__", next_cards)
                .replace("__FOOT__", foot))
        return HTMLResponse(content=html)

    def _build_panel_html(self, apikey: str) -> str:
        """工作台页面：15 主题 / 列显隐 / 列排序 / 列宽拖拽 / 搜索 / 筛选 / 批量删除 / 导出。"""
        return (_PANEL_HTML
                .replace("__AK__", json.dumps(apikey))
                .replace("__PID__", self.__class__.__name__)
                .replace("__THEMES__", _THEMES_CSS))


# ---------------------------------------------------------------------- #
# 工作台静态资源（15 主题 CSS + 页面模板），移植自 HHClubButler
# ---------------------------------------------------------------------- #
_THEMES_CSS = r'''
body[data-theme="light"]{--bg:#ffffff;--fg:#1d1d1f;--topbar:#f5f5f7;--border:#e5e5ea;--chip:#e8e8ed;--chip-active:#0071e3;--chip-hover:#d1d1d6;--input-bg:#ffffff;--input-border:#d2d2d7;--btn:#0071e3;--btn-danger:#ff3b30;--btn-ghost:#e8e8ed;--toolbar:#f0f0f2;--picker-bg:#ffffff;--picker-border:#d2d2d7;--th:#f5f5f7;--th-hover:#e8e8ed;--td-border:#f0f0f2;--tr-hover:#f0f5ff;--tr-sel:#0071e3;--footer:#f5f5f7;--footer-border:#e5e5ea;--muted:#86868b;--muted2:#6e6e73}
body[data-theme="cream"]{--bg:#fdf6e3;--fg:#3e2f1c;--topbar:#f5e6c8;--border:#d4c5a0;--chip:#ead9b8;--chip-active:#b8860b;--chip-hover:#e0cf9f;--input-bg:#fff8e0;--input-border:#c4b080;--btn:#b8860b;--btn-danger:#c0392b;--btn-ghost:#ead9b8;--toolbar:#f0e0bc;--picker-bg:#fdf6e3;--picker-border:#c4b080;--th:#f0e0bc;--th-hover:#e8d5a0;--td-border:#e0d0a5;--tr-hover:#f5e6b8;--tr-sel:#8b6914;--footer:#e8d8a8;--footer-border:#d4c5a0;--muted:#8b7355;--muted2:#6b5535}
body[data-theme="sakura"]{--bg:#fff5f7;--fg:#4a2a35;--topbar:#ffe4ec;--border:#f8c8d8;--chip:#ffd6e5;--chip-active:#e91e63;--chip-hover:#ffc2d6;--input-bg:#ffffff;--input-border:#f8b8cc;--btn:#e91e63;--btn-danger:#c0392b;--btn-ghost:#ffd6e5;--toolbar:#ffe0ec;--picker-bg:#fff5f7;--picker-border:#f8b8cc;--th:#ffe0ec;--th-hover:#ffd0e0;--td-border:#f8d0dc;--tr-hover:#ffe4ef;--tr-sel:#ad1457;--footer:#ffe4ec;--footer-border:#f8c8d8;--muted:#a06878;--muted2:#804858}
body[data-theme="mint"]{--bg:#f0faf4;--fg:#1f3a2e;--topbar:#d8f0e2;--border:#b8dcc4;--chip:#c8ecd4;--chip-active:#10b981;--chip-hover:#b0e4c2;--input-bg:#ffffff;--input-border:#a8d4b8;--btn:#10b981;--btn-danger:#e53e3e;--btn-ghost:#c8ecd4;--toolbar:#e0f2e8;--picker-bg:#f0faf4;--picker-border:#a8d4b8;--th:#e0f2e8;--th-hover:#d0ead8;--td-border:#d8ecd0;--tr-hover:#e0f5e8;--tr-sel:#059669;--footer:#e0f2e8;--footer-border:#b8dcc4;--muted:#6a9a7e;--muted2:#4a7a5e}
body[data-theme="sky"]{--bg:#f0f7ff;--fg:#1a2a3e;--topbar:#dceafd;--border:#b8d4f0;--chip:#cce4fa;--chip-active:#0ea5e9;--chip-hover:#b8d8f5;--input-bg:#ffffff;--input-border:#a8c8ee;--btn:#0ea5e9;--btn-danger:#e53e3e;--btn-ghost:#cce4fa;--toolbar:#e0eefd;--picker-bg:#f0f7ff;--picker-border:#a8c8ee;--th:#e0eefd;--th-hover:#d0e6f8;--td-border:#d8e8f8;--tr-hover:#e0f0ff;--tr-sel:#0284c7;--footer:#e0eefd;--footer-border:#b8d4f0;--muted:#6a8aaa;--muted2:#4a6a8a}
body[data-theme="lavender"]{--bg:#f6f3ff;--fg:#2e1f4a;--topbar:#e8e0fb;--border:#ccbfe8;--chip:#ddd2f5;--chip-active:#8b5cf6;--chip-hover:#cdbef0;--input-bg:#ffffff;--input-border:#bcade0;--btn:#8b5cf6;--btn-danger:#e53e3e;--btn-ghost:#ddd2f5;--toolbar:#ece6fb;--picker-bg:#f6f3ff;--picker-border:#bcade0;--th:#ece6fb;--th-hover:#e0d6f5;--td-border:#e0d8f0;--tr-hover:#ece4ff;--tr-sel:#7c3aed;--footer:#ece6fb;--footer-border:#ccbfe8;--muted:#8a7aaa;--muted2:#6a5a8a}
body[data-theme="silver"]{--bg:#eef0f2;--fg:#2a2d32;--topbar:#dde1e6;--border:#c0c5cc;--chip:#d4d8dd;--chip-active:#6b7280;--chip-hover:#c8ccd2;--input-bg:#ffffff;--input-border:#b8bdc4;--btn:#6b7280;--btn-danger:#d97706;--btn-ghost:#d4d8dd;--toolbar:#e4e7eb;--picker-bg:#eef0f2;--picker-border:#b8bdc4;--th:#e4e7eb;--th-hover:#d8dce0;--td-border:#dde1e6;--tr-hover:#e2e6ea;--tr-sel:#4b5563;--footer:#dde1e6;--footer-border:#c0c5cc;--muted:#7a8088;--muted2:#5a6068}
body[data-theme="dark"]{--bg:#1e1e1e;--fg:#d4d4d4;--topbar:#252526;--border:#000;--chip:#3a3d41;--chip-active:#0e639c;--chip-hover:#494c50;--input-bg:#3c3c3c;--input-border:#555;--btn:#0e639c;--btn-danger:#c72e2e;--btn-ghost:#3a3d41;--toolbar:#2d2d30;--picker-bg:#252526;--picker-border:#555;--th:#2d2d30;--th-hover:#37373d;--td-border:#333;--tr-hover:#2a2d2e;--tr-sel:#094771;--footer:#000;--footer-border:#000;--muted:#888;--muted2:#aaa}
body[data-theme="graphite"]{--bg:#2c2c2e;--fg:#e5e5e7;--topbar:#1c1c1e;--border:#0a0a0a;--chip:#3a3a3c;--chip-active:#64d2ff;--chip-hover:#48484a;--input-bg:#1c1c1e;--input-border:#48484a;--btn:#64d2ff;--btn-danger:#ff453a;--btn-ghost:#3a3a3c;--toolbar:#1c1c1e;--picker-bg:#2c2c2e;--picker-border:#48484a;--th:#1c1c1e;--th-hover:#3a3a3c;--td-border:#38383a;--tr-hover:#3a3a3c;--tr-sel:#0a84ff;--footer:#000;--footer-border:#000;--muted:#8e8e93;--muted2:#aeaeb2}
body[data-theme="midnight"]{--bg:#191923;--fg:#e0e0ec;--topbar:#10101a;--border:#050508;--chip:#2a2a38;--chip-active:#5e5ce6;--chip-hover:#35354a;--input-bg:#10101a;--input-border:#3a3a4e;--btn:#5e5ce6;--btn-danger:#ff453a;--btn-ghost:#2a2a38;--toolbar:#14141e;--picker-bg:#191923;--picker-border:#3a3a4e;--th:#14141e;--th-hover:#20202e;--td-border:#252535;--tr-hover:#22222e;--tr-sel:#4846c9;--footer:#050508;--footer-border:#000;--muted:#6a6a8a;--muted2:#9a9ab8}
body[data-theme="navy"]{--bg:#0d1b2a;--fg:#c8d6e5;--topbar:#1b263b;--border:#0a1520;--chip:#2c3e5d;--chip-active:#4fc3f7;--chip-hover:#34496b;--input-bg:#1b263b;--input-border:#34496b;--btn:#4fc3f7;--btn-danger:#e74c3c;--btn-ghost:#2c3e5d;--toolbar:#162235;--picker-bg:#0d1b2a;--picker-border:#34496b;--th:#162235;--th-hover:#1f2f4a;--td-border:#1f2d42;--tr-hover:#1a2a42;--tr-sel:#2a6fa0;--footer:#0a1520;--footer-border:#000;--muted:#6a8aa8;--muted2:#8aa8c8}
body[data-theme="plum"]{--bg:#1a0f1f;--fg:#e0c8e8;--topbar:#2a1530;--border:#100a15;--chip:#3a2045;--chip-active:#ce93d8;--chip-hover:#452850;--input-bg:#2a1530;--input-border:#452850;--btn:#ce93d8;--btn-danger:#ef5350;--btn-ghost:#3a2045;--toolbar:#22122a;--picker-bg:#1a0f1f;--picker-border:#452850;--th:#22122a;--th-hover:#2e1838;--td-border:#2e1838;--tr-hover:#2a1830;--tr-sel:#7b1fa2;--footer:#100a15;--footer-border:#000;--muted:#8a6a9a;--muted2:#b090c0}
body[data-theme="charcoal"]{--bg:#23201e;--fg:#e0dcd6;--topbar:#1a1715;--border:#0a0806;--chip:#3a3532;--chip-active:#d4a574;--chip-hover:#45403c;--input-bg:#1a1715;--input-border:#4a4440;--btn:#d4a574;--btn-danger:#e74c3c;--btn-ghost:#3a3532;--toolbar:#1e1a18;--picker-bg:#23201e;--picker-border:#4a4440;--th:#1e1a18;--th-hover:#2a2522;--td-border:#33302c;--tr-hover:#2a2624;--tr-sel:#8a6a44;--footer:#0a0806;--footer-border:#000;--muted:#8a8078;--muted2:#aaa098}
body[data-theme="forest"]{--bg:#0f1f17;--fg:#c8e0d0;--topbar:#162a1f;--border:#0a1510;--chip:#1f3a2a;--chip-active:#66bb6a;--chip-hover:#2a4a35;--input-bg:#162a1f;--input-border:#2a4a35;--btn:#66bb6a;--btn-danger:#ef5350;--btn-ghost:#1f3a2a;--toolbar:#122519;--picker-bg:#0f1f17;--picker-border:#2a4a35;--th:#122519;--th-hover:#1a3525;--td-border:#1a3020;--tr-hover:#1a2e20;--tr-sel:#2e7d32;--footer:#0a1510;--footer-border:#000;--muted:#5a8a6a;--muted2:#8ab89a}
body[data-theme="hc"]{--bg:#000000;--fg:#ffffff;--topbar:#1a1a1a;--border:#404040;--chip:#2a2a2a;--chip-active:#00ffff;--chip-hover:#3a3a3a;--input-bg:#000;--input-border:#888;--btn:#00ffff;--btn-danger:#ff0000;--btn-ghost:#2a2a2a;--toolbar:#1a1a1a;--picker-bg:#000;--picker-border:#888;--th:#1a1a1a;--th-hover:#2a2a2a;--td-border:#333;--tr-hover:#1a1a1a;--tr-sel:#006666;--footer:#000;--footer-border:#404040;--muted:#aaaaaa;--muted2:#cccccc}
'''


def _esc(s):
    """HTML 转义（等级页占位符注入用）。"""
    return (str(s if s is not None else "")
            .replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;"))


def _fmt_wan(n):
    """大数字万化显示（照原作者格式）。"""
    try:
        v = float(n or 0)
    except (TypeError, ValueError):
        return "—"
    if v >= 10000:
        return f"{v/10000:.2f}".rstrip("0").rstrip(".") + "万"
    return f"{v:,.0f}"


# 原作者「数据与等级」页同款 SVG 图标
_IC_UP = '<svg viewBox="0 0 24 24" fill="none" stroke="#2ee6a8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 19V5M5 12l7-7 7 7"/></svg>'
_IC_DOWN = '<svg viewBox="0 0 24 24" fill="none" stroke="#38bdf8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 5v14M5 12l7 7 7-7"/></svg>'
_IC_RATIO = '<svg viewBox="0 0 24 24" fill="none" stroke="#fbbf24" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M7 17L17 7M9 7h8v8"/></svg>'
_IC_PTS = '<svg viewBox="0 0 24 24" fill="none" stroke="#38bdf8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 2l2.4 7.4H22l-6 4.4 2.3 7.2L12 16.7 5.7 21l2.3-7.2-6-4.4h7.6z"/></svg>'
_IC_BEAN = '<svg viewBox="0 0 24 24" fill="none" stroke="#FF8A3C" stroke-width="2"><circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="3.5"/></svg>'
_IC_ZAP_G = '<svg viewBox="0 0 24 24" fill="none" stroke="#2ee6a8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M13 2L4 14h6l-1 8 9-12h-6z"/></svg>'
_IC_ZAP_B = '<svg viewBox="0 0 24 24" fill="none" stroke="#38bdf8" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M13 2L4 14h6l-1 8 9-12h-6z"/></svg>'
_IC_TREND = '<svg viewBox="0 0 24 24" fill="none" stroke="#f87171" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 17l6-6 4 4 8-8M15 7h6v6"/></svg>'


def _radar_svg(axes):
    """账户四维雷达 SVG（照原作者几何：中心 115,112 半径 78，四轴 0-100 归一化，含展开动画）。"""
    import math as _m
    cx, cy, r = 115.0, 112.0, 78.0
    n = len(axes)
    def _pt(i, frac):
        ang = -_m.pi / 2 + i * 2 * _m.pi / n
        return cx + r * frac * _m.cos(ang), cy + r * frac * _m.sin(ang)
    parts = []
    # 网格环
    for ring in (0.25, 0.5, 0.75, 1.0):
        pts = " ".join(f"{x:.1f},{y:.1f}" for i in range(n) for x, y in [_pt(i, ring)])
        parts.append(f'<polygon class="grid-in" points="{pts}" fill="none" stroke="rgba(64,200,255,.14)"/>')
    # 轴线
    for i in range(n):
        x, y = _pt(i, 1.0)
        parts.append(f'<line class="axis-in" x1="{cx}" y1="{cy}" x2="{x:.1f}" y2="{y:.1f}" stroke="rgba(64,200,255,.14)"/>')
    # 数据多边形
    vals = [max(0.05, min(1.0, (v or 0) / 100.0)) for _, v in axes]
    pts = " ".join(f"{x:.1f},{y:.1f}" for i, v in enumerate(vals) for x, y in [_pt(i, v)])
    parts.append(f'<polygon class="poly-data" points="{pts}" fill="rgba(56,217,255,.18)" stroke="#38D9FF" stroke-width="1.8"/>')
    # 顶点 + 标签
    for i, ((lab, v), frac) in enumerate(zip(axes, vals)):
        x, y = _pt(i, frac)
        parts.append(f'<circle class="label-in l{i+1}" cx="{x:.1f}" cy="{y:.1f}" r="3" fill="#38D9FF"/>')
        lx, ly = _pt(i, 1.22)
        parts.append(f'<text class="label-in l{i+1}" x="{lx:.1f}" y="{ly:.1f}" text-anchor="middle" dominant-baseline="middle" font-size="11" fill="#9A8FC0">{lab}</text>')
    return "".join(parts)


_AVATAR_SVG = ('<svg viewBox="0 0 44 44" width="44" height="44">'
               '<defs><linearGradient id="rb" x1="0" y1="0" x2="1" y2="1">'
               '<stop offset="0" stop-color="#FF8A3C"/><stop offset="1" stop-color="#38D9FF"/></linearGradient></defs>'
               '<rect width="44" height="44" rx="10" fill="url(#rb)"/>'
               '<path d="M10 30 L22 14 L34 30 Z" fill="rgba(11,7,20,.82)"/>'
               '<circle cx="22" cy="12" r="3.4" fill="#2ee6a8"/></svg>')

_PANEL_HTML = r'''<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>彩虹岛复活区管家</title><style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:"Segoe UI","PingFang SC",Arial,sans-serif;background:var(--bg);color:var(--fg);font-size:12px;height:100vh;height:100dvh;display:flex;flex-direction:column;overflow:hidden}
__THEMES__
.topbar{background:var(--topbar);padding:8px 12px;display:flex;align-items:center;gap:8px;border-bottom:1px solid var(--border);flex-wrap:wrap;position:relative}
.dot{display:inline-block;width:8px;height:8px;border-radius:0;margin-right:5px;vertical-align:middle}
.chip b{margin-left:3px}
.topbar h1{font-size:14px;color:var(--fg);margin-right:8px}
.chip{background:var(--chip);padding:3px 10px;border-radius:12px;cursor:pointer;font-size:11px;user-select:none;color:var(--fg)}
.chip.active{background:var(--chip-active);color:#fff}
.chip:hover{background:var(--chip-hover)}
.search{margin-left:auto;background:var(--input-bg);border:1px solid var(--input-border);color:var(--fg);padding:5px 10px;border-radius:3px;width:300px;max-width:30vw}
.btn{background:var(--btn);color:#fff;border:none;padding:5px 12px;border-radius:3px;cursor:pointer;font-size:12px}
.btn.danger{background:var(--btn-danger)}
.btn.ghost{background:var(--btn-ghost);color:var(--fg)}
.colpicker{position:absolute;right:12px;top:50px;background:var(--picker-bg);border:1px solid var(--picker-border);border-radius:4px;padding:8px;z-index:100;min-width:160px;display:none;box-shadow:0 4px 16px rgba(0,0,0,0.5);max-height:70vh;overflow:auto}
.colpicker.show{display:block}
.colpicker h4{font-size:11px;color:var(--muted2);margin-bottom:6px;font-weight:normal}
.colpicker label{display:flex;align-items:center;gap:6px;padding:3px 0;cursor:pointer;font-size:12px;color:var(--fg)}
.table-wrap{flex:1;overflow:auto;position:relative}
.status{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);display:flex;flex-direction:column;align-items:center;justify-content:center;gap:12px;font-size:14px;color:var(--fg);text-align:center;max-width:420px;background:var(--bg);padding:28px 36px;border-radius:10px;box-shadow:0 6px 24px rgba(0,0,0,0.12)}
.status .bar{width:240px;height:4px;background:var(--chip);border-radius:2px;overflow:hidden;position:relative}
.status .bar i{position:absolute;top:0;left:0;height:100%;width:38%;background:var(--btn);border-radius:2px;animation:hhslide 1s linear infinite}
@keyframes hhslide{0%{left:-38%}100%{left:100%}}
.status .errmsg{color:var(--btn-danger);font-size:14px;max-width:70%;text-align:center}
.status .retrybtn{background:var(--btn);color:#fff;border:none;padding:7px 22px;border-radius:4px;cursor:pointer;font-size:14px;margin-top:6px}
table{width:100%;border-collapse:separate;border-spacing:0;font-size:12px;table-layout:fixed}
th,td{padding:6px 10px;border-bottom:1px solid var(--td-border);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;position:relative}
th{background:var(--th);color:var(--muted2);text-align:left;font-weight:500;cursor:pointer;user-select:none;position:sticky;top:0;z-index:2}
th:hover{background:var(--th-hover)}
th .sortind{margin-left:4px;font-size:10px}
th .resize{position:absolute;right:-3px;top:0;width:9px;height:100%;cursor:col-resize;background:transparent;z-index:3;touch-action:none}
th .resize:hover{background:var(--chip-active)}
tr:hover td{background:var(--tr-hover)}
tr.sel td{background:var(--tr-sel);color:#fff}
.tier0{color:#22c55e;font-weight:600}.tier1{color:#4da3ff;font-weight:600}.tier2{color:#ff6b6b;font-weight:600}
.free-yes{color:#10b981}.free-soon{color:#f59e0b}.free-exp{color:#9e9e9e}
.mine-y{color:#6366f1;font-weight:600}
.hr-y{color:#ef4444;font-weight:600}
.right{text-align:right;font-variant-numeric:tabular-nums}
.hide{display:none !important}
.footer{background:var(--footer);padding:6px 14px;font-size:11px;color:var(--muted);border-top:1px solid var(--footer-border);display:flex;gap:18px;flex-wrap:wrap}
.themebtn{background:var(--btn-ghost);color:var(--fg);border:1px solid var(--input-border);padding:3px 10px;border-radius:3px;cursor:pointer;font-size:11px}
@media (max-width:720px){.search{width:100%;max-width:none;margin-left:0;order:99}.topbar h1{font-size:13px}}
</style></head><body>
<div class="topbar">
<h1>🌈 复活区管家</h1>
<span class="chip active" data-f="all"><i class="dot" style="background:#9e9e9e"></i>全部 <b id="cnt_all">0</b></span>
<span class="chip" data-f="free"><i class="dot" style="background:#10b981"></i>免费中 <b id="cnt_free">0</b></span>
<span class="chip" data-f="soon"><i class="dot" style="background:#f59e0b"></i>即将到期 <b id="cnt_soon">0</b></span>
<span class="chip" data-f="low"><i class="dot" style="background:#22c55e"></i>低做种 <b id="cnt_low">0</b></span>
<span class="chip" data-f="mine"><i class="dot" style="background:#6366f1"></i>我做种 <b id="cnt_mine">0</b></span>
<span class="chip" data-f="expired"><i class="dot" style="background:#9e9e9e"></i>已过期 <b id="cnt_exp">0</b></span>
<input class="search" id="q" placeholder="搜索名称...">
<button class="btn ghost" id="btnExport">导出</button>
<button class="btn danger" id="btnDel">删除选中</button>
<button class="btn ghost" id="colsBtn">列选择</button>
<select class="themebtn" id="themeSel" title="选择皮肤" style="cursor:pointer;"><option value="light">纯白</option><option value="cream">米黄暖</option><option value="sakura">樱花粉</option><option value="mint">薄荷绿</option><option value="sky">天空蓝</option><option value="lavender">薰衣草紫</option><option value="silver">银灰金属</option><option value="dark">深灰</option><option value="graphite">石墨蓝</option><option value="midnight">午夜蓝</option><option value="navy">深蓝</option><option value="plum">暗紫</option><option value="charcoal">炭灰暖</option><option value="forest">暗夜绿</option><option value="hc">高对比</option></select>
<button class="btn ghost" id="dlBtn">下载器: 全部</button>
<div class="colpicker" id="dlpop"><h4>按下载器筛选（可多选）</h4><label><input type="checkbox" data-dl="__all__" checked>全部</label></div>
<button class="btn ghost" id="btnRefresh">刷新</button>
<button class="btn ghost" id="btnExit">退出</button>
<div class="colpicker" id="colpicker"><h4>勾选要显示的列</h4>
<label><input type="checkbox" data-col="c_tid" checked>种子ID</label>
<label><input type="checkbox" data-col="c_name" checked>种子名称</label>
<label><input type="checkbox" data-col="c_size" checked>大小</label>
<label><input type="checkbox" data-col="c_seed" checked>做种人数</label>
<label><input type="checkbox" data-col="c_leech" checked>下载人数</label>
<label><input type="checkbox" data-col="c_tier" checked>档位</label>
<label><input type="checkbox" data-col="c_free" checked>免费状态</label>
<label><input type="checkbox" data-col="c_remain">剩余时间</label>
<label><input type="checkbox" data-col="c_mine" checked>我做种</label>
<label><input type="checkbox" data-col="c_hr" checked>HR</label>
<label><input type="checkbox" data-col="c_dl" checked>下载器</label>
<label><input type="checkbox" data-col="c_dstat" checked>任务状态</label>
</div>
</div>
<div class="table-wrap"><div id="status" class="status"><span>正在加载复活区数据，请稍候…</span><div class="bar"><i></i></div></div><table id="tbl"><thead><tr id="head"></tr></thead><tbody id="tbody"></tbody></table></div>
<div id="logmodal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.55);z-index:99;" onclick="if(event.target===this)this.style.display='none'">
<div style="background:var(--bg,#1e1e2a);margin:5vh auto;width:86%;max-width:900px;max-height:80vh;border-radius:6px;padding:14px;display:flex;flex-direction:column;gap:8px;border:1px solid var(--input-border,#444);">
<div style="display:flex;justify-content:space-between;align-items:center;color:var(--fg,#eee);"><b>运行日志（最近巡检摘要）</b><button class="btn ghost" onclick="document.getElementById('logmodal').style.display='none'">关闭</button></div>
<pre id="logtxt" style="overflow:auto;max-height:66vh;font-size:11px;line-height:1.7;white-space:pre-wrap;color:var(--fg,#ddd);background:transparent;">加载中…</pre>
</div></div>
<div class="footer">
<span>已选 <b id="seln">0</b> 个 / 共 <b id="totaln">0</b> 个</span>
<span>已选体积 <b id="selgb">0</b> GB</span>
<span>总体积 <b id="totalgb">0</b> GB</span>
<button class="btn ghost" id="btnLog">运行日志</button>
<span id="runinfo" style="margin-left:auto;"></span>
</div>
<script>
var APIKEY=__AK__;
var COLS=[
{k:"c_tid",t:"种子ID",w:"80",r:1,sortk:"tid"},
{k:"c_name",t:"种子名称",w:"300",sortk:"title"},
{k:"c_size",t:"大小",w:"90",r:1,sortk:"size_gb"},
{k:"c_seed",t:"做种",w:"60",r:1,sortk:"seeders"},
{k:"c_leech",t:"下载",w:"60",r:1,sortk:"leechers"},
{k:"c_tier",t:"档位",w:"80",sortk:"seeders"},
{k:"c_free",t:"免费状态",w:"90",sortk:"free_until"},
{k:"c_remain",t:"剩余时间",w:"100",sortk:"remain_sec"},
{k:"c_mine",t:"我做种",w:"70",sortk:"mine"},
{k:"c_hr",t:"HR",w:"50",sortk:"hr"},
{k:"c_dl",t:"下载器",w:"90",sortk:"dl"},
{k:"c_dstat",t:"任务状态",w:"180",sortk:"dl_progress"}
];
var seeds=[],filtered=[],sel={};
var RUNLOG=[];
var curF="all",sortKey="remain_sec",sortDir=1,query="",curDls=[];
function tierOf(n){n=+n||0;return n<=1?0:(n<=3?1:2)}
function fmtSize(gb){if(!isFinite(gb))return"-";gb=+gb;if(gb>=1024)return(gb/1024).toFixed(2)+" TB";return gb.toFixed(2)+" GB"}
function fmtRemain(sec){if(sec==null)return"-";if(sec<=0)return"已过期";var d=Math.floor(sec/86400),h=Math.floor(sec%86400/3600),m=Math.floor(sec%3600/60);return(d?d+"天":"")+h+"小时"+m+"分"}
function esc(s){return String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;")}
function buildHead(){
var h='<th style="width:28px;"><input type="checkbox" id="chkAll"></th>';
for(var i=0;i<COLS.length;i++){var c=COLS[i];var hide=c.visible===false?" hide":"";
var ind="";if(sortKey===c.sortk)ind=sortDir<0?" ▼":" ▲";
h+='<th class="'+(c.r?"right ":"")+(sortKey===c.sortk?"sorted":"")+hide+'" data-col="'+c.k+'" data-sortk="'+c.sortk+'" style="width:'+c.w+'px;">'+c.t+'<span class="sortind">'+ind+'</span><span class="resize"></span></th>';}
h+='<th style="width:60px;">操作</th>';
document.getElementById("head").innerHTML=h;bindHead();
}
function bindHead(){
var dragMoved=false;
document.querySelectorAll("th .resize").forEach(function(hd){hd.onmousedown=function(e){e.preventDefault();e.stopPropagation();
var th=hd.parentElement,sx=e.clientX,sw=th.offsetWidth;dragMoved=false;document.body.style.cursor='col-resize';
function mv(ev){var dx=ev.clientX-sx;if(Math.abs(dx)>1)dragMoved=true;var w=Math.max(40,sw+dx);th.style.width=w+"px";var col=th.dataset.col;if(col)document.querySelectorAll('td[data-col="'+col+'"]').forEach(function(td){td.style.width=w+"px"})}
function persistColW(th){var col=th.dataset.col;if(!col)return;for(var i=0;i<COLS.length;i++)if(COLS[i].k===col){COLS[i].w=parseInt(th.style.width)||COLS[i].w;saveColCfg()}}
function up(ev){document.removeEventListener("mousemove",mv);document.removeEventListener("mouseup",up);document.body.style.cursor='';if(dragMoved&&ev){ev.stopPropagation();ev.preventDefault();persistColW(th)}}
document.addEventListener("mousemove",mv);document.addEventListener("mouseup",up)};
hd.ontouchstart=function(e){e.preventDefault();e.stopPropagation();
var th=hd.parentElement,sx=e.touches[0].clientX,sw=th.offsetWidth;dragMoved=false;document.body.style.cursor='col-resize';
function tmv(ev){var dx=ev.touches[0].clientX-sx;if(Math.abs(dx)>1)dragMoved=true;var w=Math.max(40,sw+dx);th.style.width=w+"px";var col=th.dataset.col;if(col)document.querySelectorAll('td[data-col="'+col+'"]').forEach(function(td){td.style.width=w+"px"})}
function tup(ev){document.removeEventListener("touchmove",tmv);document.removeEventListener("touchend",tup);document.body.style.cursor='';if(dragMoved&&ev){ev.stopPropagation();ev.preventDefault();persistColW(th)}}
document.addEventListener("touchmove",tmv);document.addEventListener("touchend",tup)}});
document.querySelectorAll("th[data-sortk]").forEach(function(th){th.onclick=function(ev){if(dragMoved){dragMoved=false;return}var sk=this.dataset.sortk;if(sortKey===sk){sortDir=-sortDir}else{sortKey=sk;sortDir=(sk==="title"||sk==="tid")?1:-1}doSort();buildHead();render()}});
document.getElementById("chkAll").onchange=function(){var v=this.checked;filtered.forEach(function(s){sel[s.tid]=v});render()};
}
function doSort(){
seeds.sort(function(a,b){
var va=a[sortKey],vb=b[sortKey];
if(typeof va==="string"||typeof vb==="string")return String(va||"").localeCompare(String(vb||""))*sortDir;
return((va||0)-(vb||0))*sortDir;
});
}
function cellVal(c,s){
if(c.k==="c_tid")return s.tid;
if(c.k==="c_name")return '<span title="'+esc(s.title)+'">'+esc(s.title)+"</span>";
if(c.k==="c_size")return fmtSize(s.size_gb);
if(c.k==="c_seed")return s.seeders;
if(c.k==="c_leech")return s.leechers;
if(c.k==="c_tier"){var t=tierOf(s.seeders);return '<span class="'+["tier0","tier1","tier2"][t]+'">'+["0-1人","2-3人","4+人"][t]+"</span>"}
if(c.k==="c_free"){if(s.expired)return'<span class="free-exp">已过期</span>';if(s.is_free)return'<span class="free-yes">免费中</span>';return"-"}
if(c.k==="c_remain"){if(!s.free_until)return"-";var cls=s.expired?"free-exp":(s.remain_sec<=7200?"free-soon":"free-yes");return'<span class="'+cls+'">'+fmtRemain(s.remain_sec)+"</span>"}
if(c.k==="c_mine")return s.mine?'<span class="mine-y">是</span>':"-";
if(c.k==="c_hr")return s.hr?'<span class="hr-y">HR</span>':"-";
if(c.k==="c_dl")return s.dl?esc(s.dl):"-";
if(c.k==="c_dstat"){if(!s.dl)return"-";var pc=Math.round(+s.dl_progress||0);var sp=+s.dl_speed||0;var spd=sp>=1048576?(sp/1048576).toFixed(1)+" MB/s":(sp>=1024?(sp/1024).toFixed(0)+" KB/s":sp+" B/s");var et=+s.dl_eta||0;var eta=(et>0&&et<8640000)?(Math.floor(et/3600)+"h"+Math.floor(et%3600/60)+"m"):"-";return'<span title="'+esc((s.dl_state||"")+" | "+(s.dl_path||""))+'">'+pc+"% · "+spd+" · "+eta+"</span>"}
return "";
}
function render(){
var f=seeds.filter(function(s){
if(curF==="free"&&!(s.is_free&&!s.expired))return false;
if(curF==="soon"&&!(s.is_free&&!s.expired&&s.remain_sec<=7200))return false;
if(curF==="low"&&!(s.seeders<=3))return false;
if(curF==="mine"&&!s.mine)return false;
if(curF==="expired"&&!s.expired)return false;
if(curDls.length&&curDls.indexOf(s.dl||"")<0)return false;
if(query&&(s.title||"").toLowerCase().indexOf(query)<0)return false;
return true});
filtered=f;
var html="";
for(var i=0;i<f.length;i++){var s=f[i];var seled=!!sel[s.tid];
html+='<tr class="'+(seled?"sel":"")+'">';
html+='<td><input type="checkbox" data-idx="'+i+'" '+(seled?"checked":"")+"></td>";
for(var j=0;j<COLS.length;j++){var c=COLS[j];if(c.visible===false)continue;
html+='<td class="'+(c.r?"right ":"")+'" data-col="'+c.k+'">'+cellVal(c,s)+"</td>"}
html+='<td><a style="color:#ff8080;cursor:pointer;" data-del="'+i+'">删除</a></td></tr>'}
if(!f.length)html='<tr><td colspan="20" style="text-align:center;padding:40px;color:#888">无匹配种子</td></tr>';
document.getElementById("tbody").innerHTML=html;
document.querySelectorAll("#tbody input[type=checkbox]").forEach(function(cb){cb.onchange=function(){var i=+this.dataset.idx;sel[filtered[i].tid]=this.checked;renderFooter()}});
document.querySelectorAll("#tbody [data-del]").forEach(function(a){a.onclick=function(){delOne(filtered[+this.dataset.del])}});
renderFooter();
}
function renderFooter(){
var sn=0,sgb=0;seeds.forEach(function(s){if(sel[s.tid]){sn++;sgb+=+s.size_gb||0}});
document.getElementById("seln").textContent=sn;document.getElementById("selgb").textContent=sgb.toFixed(1);
document.getElementById("totaln").textContent=seeds.length;
document.getElementById("totalgb").textContent=seeds.reduce(function(a,b){return a+(+b.size_gb||0)},0).toFixed(1);
var cFree=0,cSoon=0,cLow=0,cMine=0,cExp=0;
seeds.forEach(function(s){if(s.is_free&&!s.expired)cFree++;if(s.is_free&&!s.expired&&s.remain_sec<=7200)cSoon++;if(s.seeders<=3)cLow++;if(s.mine)cMine++;if(s.expired)cExp++});
document.getElementById("cnt_all").textContent=seeds.length;
document.getElementById("cnt_free").textContent=cFree;document.getElementById("cnt_soon").textContent=cSoon;
document.getElementById("cnt_low").textContent=cLow;document.getElementById("cnt_mine").textContent=cMine;
document.getElementById("cnt_exp").textContent=cExp;
}
async function load(){
var st=document.getElementById("status");
function showErr(msg){st.innerHTML='<span class="errmsg">'+esc(msg)+'</span><button class="retrybtn" onclick="load()">重试</button>'}
st.style.display="flex";
st.innerHTML='<span>正在加载复活区数据，请稍候… <b id="el">0</b> 秒</span><div class="bar"><i></i></div>';
var t0=Date.now(),tmr=setInterval(function(){var el=document.getElementById("el");if(el)el.textContent=Math.floor((Date.now()-t0)/1000)},250);
var ctl=(typeof AbortController!=="undefined")?new AbortController():null;
var to=ctl?setTimeout(function(){ctl.abort()},90000):null;
try{
var r=await fetch("/api/v1/plugin/__PID__/data?apikey="+encodeURIComponent(APIKEY),ctl?{signal:ctl.signal}:{});
if(to)clearTimeout(to);
var j=await r.json();
if(!j.success){clearInterval(tmr);showErr(j.message||"数据加载失败");return}
seeds=j.seeds||[];
var dls=[];seeds.forEach(function(s){if(s.dl&&dls.indexOf(s.dl)<0)dls.push(s.dl)});dls.sort();
buildDlPop(dls);
document.getElementById("runinfo").textContent=j.last_result||"";
RUNLOG=j.run_log||[];
doSort();render();clearInterval(tmr);st.style.display="none";
}catch(e){clearInterval(tmr);showErr((e&&e.name==="AbortError")?"加载超时，请稍后重试":"请求失败："+e)}
}
async function delOne(s){
if(!confirm("确认删除种子:\n"+s.title+"\n\n将删除下载器中的对应任务!"))return;
await doDel([s]);
}
async function delSel(){
var arr=seeds.filter(function(s){return sel[s.tid]});
if(!arr.length){alert("未选中任何种子");return}
if(!confirm("确认删除选中的 "+arr.length+" 个种子的下载器任务?"))return;
await doDel(arr);
}
async function doDel(arr){
var okN=0;
for(var i=0;i<arr.length;i++){
try{var r=await fetch("/api/v1/plugin/__PID__/delete_seed?apikey="+encodeURIComponent(APIKEY)+"&tid="+encodeURIComponent(arr[i].tid),{method:"POST"});var j=await r.json();if(j.success){okN++;delete sel[arr[i].tid]}else{alert("删除失败:"+arr[i].title.slice(0,40)+" -> "+(j.message||""))}}
catch(e){alert("删除异常:"+e)}}
if(okN){alert("已删除 "+okN+" 个任务");load()}
}
function doExport(){
var rows=[["种子ID","种子名称","大小GB","做种人数","下载人数","档位","免费状态","剩余秒","我做种","HR"]];
filtered.forEach(function(s){rows.push([s.tid,s.title,s.size_gb,s.seeders,s.leechers,["0-1人","2-3人","4+人"][tierOf(s.seeders)],s.expired?"已过期":(s.is_free?"免费中":"-"),s.remain_sec,s.mine?"是":"",s.hr?"是":""])});
var csv="\ufeff"+rows.map(function(r){return r.map(function(c){return'"'+String(c==null?"":c).replace(/"/g,'""')+'"'})}).join("\n");
var a=document.createElement("a");a.href=URL.createObjectURL(new Blob([csv],{type:"text/csv"}));a.download="rainbow_revive_seeds.csv";a.click();
}
document.getElementById("btnDel").onclick=delSel;
document.getElementById("btnExport").onclick=doExport;
document.getElementById("btnRefresh").onclick=load;
document.getElementById("btnExit").onclick=function(){window.close();setTimeout(function(){try{window.open("","_self");window.close();}catch(e){}},120);setTimeout(function(){try{window.history.back();}catch(e){}},700);};
var dlpop=document.getElementById("dlpop");
function buildDlPop(dls){
var keep=curDls.filter(function(d){return dls.indexOf(d)>=0});
curDls=keep;
var h='<h4>按下载器筛选（可多选）</h4><div style="margin-bottom:4px"><a href="javascript:void 0" style="color:#6366f1;text-decoration:none" id="dlAll">全选</a> · <a href="javascript:void 0" style="color:#6366f1;text-decoration:none" id="dlNone">清空</a></div><label><input type="checkbox" data-dl="__all__"'+(curDls.length?"":" checked")+'>全部</label>';
dls.forEach(function(d){h+='<label><input type="checkbox" data-dl="'+esc(d)+'"'+(curDls.indexOf(d)>=0?" checked":"")+'>'+esc(d)+'</label>'});
if(!dls.length)h+='<label style="color:var(--muted2)">暂无下载器任务</label>';
dlpop.innerHTML=h;
bindDlPop();
var _all=document.getElementById("dlAll"),_none=document.getElementById("dlNone");
if(_all)_all.onclick=function(e){e.stopPropagation();curDls=dls.slice();renderDlBtn(dls);syncDlChecks();render();};
if(_none)_none.onclick=function(e){e.stopPropagation();curDls=[];renderDlBtn(dls);syncDlChecks();render();};
renderDlBtn(dls);
}
function renderDlBtn(dls){
var lbl=!curDls.length?"全部":(curDls.length===1?curDls[0]:"已选 "+curDls.length);
document.getElementById("dlBtn").textContent="下载器: "+lbl;
}
function bindDlPop(){
dlpop.querySelectorAll("input[type=checkbox]").forEach(function(cb){
cb.onchange=function(){
var d=cb.dataset.dl;
if(d==="__all__"){if(cb.checked)curDls=[];else cb.checked=true;
dlpop.querySelectorAll("input[type=checkbox]").forEach(function(x){if(x.dataset.dl!=="__all__")x.checked=false});
}else{
if(cb.checked){if(curDls.indexOf(d)<0)curDls.push(d)}
else{curDls=curDls.filter(function(x){return x!==d})}
var all=dlpop.querySelector('input[data-dl="__all__"]');if(all)all.checked=!curDls.length;
}
var dls=[];seeds.forEach(function(s){if(s.dl&&dls.indexOf(s.dl)<0)dls.push(s.dl)});dls.sort();
renderDlBtn(dls);render();
}};
document.getElementById("dlBtn").onclick=function(e){e.stopPropagation();syncDlChecks();dlpop.classList.toggle("show")};
function syncDlChecks(){
var dls=[];seeds.forEach(function(s){if(s.dl&&dls.indexOf(s.dl)<0)dls.push(s.dl)});
var all=dlpop.querySelector('input[data-dl="__all__"]');if(all)all.checked=!curDls.length;
dlpop.querySelectorAll('input[type=checkbox]:not([data-dl="__all__"])').forEach(function(x){x.checked=curDls.indexOf(x.dataset.dl)>=0});
}
document.addEventListener("click",function(e){if(!dlpop.contains(e.target))dlpop.classList.remove("show")});
document.getElementById("btnLog").onclick=function(){var m=document.getElementById("logmodal");m.style.display="block";document.getElementById("logtxt").textContent=(RUNLOG&&RUNLOG.length)?RUNLOG.join("\n"):"暂无运行日志（首次巡检后生成）"};
document.getElementById("q").oninput=function(){query=this.value.trim().toLowerCase();render()};
document.querySelectorAll(".topbar .chip").forEach(function(c){c.onclick=function(){document.querySelectorAll(".topbar .chip").forEach(function(x){x.classList.remove("active")});this.classList.add("active");curF=this.dataset.f;render()}});
var cp=document.getElementById("colpicker");
var LS_KEY="rrb_colcfg_v1";
function loadColCfg(){try{var raw=localStorage.getItem(LS_KEY);if(!raw)return;var cfg=JSON.parse(raw);if(cfg&&typeof cfg==="object"){for(var i=0;i<COLS.length;i++){var v=cfg[COLS[i].k];if(v===undefined)continue;if(v&&typeof v==="object"){if(v.v!==undefined)COLS[i].visible=v.v;if(v.w)COLS[i].w=String(v.w)}else{COLS[i].visible=v}}}}catch(e){}}
function saveColCfg(){try{var cfg={};for(var i=0;i<COLS.length;i++)cfg[COLS[i].k]={v:COLS[i].visible!==false,w:COLS[i].w};localStorage.setItem(LS_KEY,JSON.stringify(cfg))}catch(e){}}
function syncColChecks(){cp.querySelectorAll("input[type=checkbox]").forEach(function(cb){for(var i=0;i<COLS.length;i++){if(COLS[i].k===cb.dataset.col){cb.checked=COLS[i].visible!==false;break}}})}
document.getElementById("colsBtn").onclick=function(e){e.stopPropagation();syncColChecks();cp.classList.toggle("show")};
document.addEventListener("click",function(e){if(!cp.contains(e.target))cp.classList.remove("show")});
cp.querySelectorAll("input[type=checkbox]").forEach(function(cb){cb.onchange=function(){var col=cb.dataset.col;var def=null;for(var i=0;i<COLS.length;i++)if(COLS[i].k===col)def=COLS[i];if(def){def.visible=cb.checked;saveColCfg();buildHead();render()}}});
loadColCfg();
var THEME_KEY="rrb_theme_v1";
function applyTheme(t){document.body.setAttribute("data-theme",t);try{localStorage.setItem(THEME_KEY,t)}catch(e){}}
try{var lt=localStorage.getItem(THEME_KEY);if(lt)applyTheme(lt);else applyTheme("dark")}catch(e){applyTheme("dark")}
var themeSel=document.getElementById("themeSel");
themeSel.value=document.body.getAttribute("data-theme")||"dark";
themeSel.onchange=function(){applyTheme(this.value)};
buildHead();load();
</script></body></html>'''


# ---------------------------------------------------------------------- #
# 独立工作台页模板（照原作者「结算统计表」「数据与等级」版式整体移植，仅换数据）
# ---------------------------------------------------------------------- #
# ---------------------------------------------------------------------- #
# 独立工作台页模板（照原作者「结算统计表」「数据与等级」版式整体移植，仅换数据）
# ---------------------------------------------------------------------- #
_STATS_HTML = '<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">\n<meta name="viewport" content="width=device-width,initial-scale=1">\n<title>做种积分统计</title><style>\n\n*{box-sizing:border-box;margin:0;padding:0}\nbody{font-family:"Segoe UI","PingFang SC",Arial,sans-serif;color:#F0ECFF;\nbackground:radial-gradient(800px 420px at 6% -8%,rgba(255,64,160,.13),transparent 55%),\nradial-gradient(900px 460px at 94% -6%,rgba(64,200,255,.15),transparent 55%),#0B0714;\nmin-height:100vh;height:100vh;height:100dvh;display:flex;flex-direction:column;overflow:hidden;font-size:12px}\n.wrap{width:100%;margin:0 auto;flex:1;overflow:auto;padding:20px 28px 16px;display:flex;flex-direction:column}\n.head{display:flex;justify-content:space-between;align-items:center;margin-bottom:18px}\n.head .t{font-size:22px;font-weight:700}\n.head .t b{color:#FF8A3C}\n.head .src{font-size:12px;color:#9A8FC0;margin-top:5px}\n.exit{border:1px solid rgba(255,64,160,.35);background:rgba(255,64,160,.06);color:#D9A0CF;font-size:13px;padding:7px 16px;border-radius:10px;cursor:pointer;font-family:inherit}\n.exit:hover{background:rgba(255,64,160,.16)}\n.topcard{background:rgba(16,10,28,.75);border:1px solid rgba(64,200,255,.25);border-radius:18px;padding:16px 18px 12px;\nbox-shadow:0 0 30px rgba(64,200,255,.07);margin-bottom:18px}\n.ct{display:flex;align-items:baseline;gap:10px;margin-bottom:8px}\n.ct .nm{font-size:16px;font-weight:700}.ct .sb{font-size:11px;color:#9A8FC0}\n.stp{display:inline-flex;align-items:center;gap:4px;margin:0 2px;vertical-align:1px}\n.stp button{width:20px;height:20px;line-height:1;border-radius:6px;border:1px solid rgba(64,200,255,.35);background:rgba(64,200,255,.08);color:#38D9FF;cursor:pointer;font-size:13px;padding:0;font-family:inherit}\n.stp button:hover{background:rgba(64,200,255,.22)}\n.stp b{min-width:22px;text-align:center;font-size:14px;color:#F0ECFF}\n.chart{width:100%;height:300px}\n/* 柱状图下方达标三行对齐表格（列与柱严格对齐；窄屏整体横滑） */\n.qwrap{overflow-x:auto;overflow-y:hidden}\n.qinner{min-width:920px}\n.qtbl{display:grid;grid-template-columns:40px 80px repeat(10,minmax(56px,1fr));\npadding-right:14px;border-top:1px solid rgba(64,200,255,.22);margin-top:2px}\n.qc{display:flex;align-items:center;justify-content:center;text-align:center;white-space:nowrap;overflow:hidden;\npadding:5px 1px;border-right:1px solid rgba(64,200,255,.10);border-bottom:1px solid rgba(64,200,255,.10);font-size:11px}\n.qc.ax{border-right:none}\n.qc.rl{justify-content:flex-end;padding-right:8px;font-size:10px;color:#9A8FC0;font-weight:600;letter-spacing:.3px}\n.qc.q{color:#34D399;font-weight:700;font-size:13px}\n.qc.s{color:#38D9FF;font-weight:600;font-size:12px}\n.qc.d{color:#9A8FC0;font-size:10.5px;border-bottom:1px solid rgba(64,200,255,.30)}\n.qc.mute{color:#5d5478}\n.low{display:grid;grid-template-columns:1fr 1.15fr;gap:18px}\n.card{background:rgba(16,10,28,.72);border:1px solid rgba(255,64,160,.28);border-radius:18px;padding:16px 18px;\nbox-shadow:0 0 24px rgba(255,64,160,.07)}\n.duals{position:relative;width:100%;height:330px;display:flex;gap:14px}\n.duals .dbox{position:relative;flex:1 1 0;min-width:0}\n.duals .g{width:100%;height:330px}\n.duals .dlab{position:absolute;left:0;right:0;top:50%;transform:translateY(-50%);text-align:center;pointer-events:none}\n.duals .dlab .r{font-size:14px;color:#9A8FC0;margin-bottom:8px}\n.duals .dlab .v{font-size:26px;font-weight:700;line-height:1.15}\ntable{width:100%;border-collapse:collapse;font-size:12.5px}\nth{text-align:left;font-weight:600;color:#9A8FC0;font-size:11px;padding:7px 10px;border-bottom:1px solid rgba(255,64,160,.16);position:sticky;top:0;background:#150D24}\ntd{padding:8px 10px;border-bottom:1px solid rgba(120,90,220,.10);color:#DCD3F5;font-variant-numeric:tabular-nums}\ntd.d{color:#9A8FC0}\ntd.bean{color:#FF8A3C;font-weight:600}\ntd.pt{color:#38D9FF;font-weight:600}\n.scroll30{height:330px;overflow-y:auto;border-radius:12px;border:1px solid rgba(120,90,220,.20)}\n.scroll30::-webkit-scrollbar{width:8px}.scroll30::-webkit-scrollbar-thumb{background:#4A3A78;border-radius:4px}\n.status{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);display:flex;flex-direction:column;align-items:center;justify-content:center;gap:14px;font-size:14px;color:#F0ECFF;text-align:center;max-width:460px;padding:28px 36px;border-radius:14px;border:1px solid rgba(255,64,160,.25);background:rgba(16,10,28,.85);box-shadow:0 0 30px rgba(255,64,160,.12)}\n.status .bar{width:260px;height:4px;background:rgba(120,90,220,.18);border-radius:2px;overflow:hidden;position:relative}\n.status .bar i{position:absolute;top:0;left:0;height:100%;width:38%;background:linear-gradient(90deg,#FF8A3C,#38D9FF);border-radius:2px;animation:hhslide 1s linear infinite;box-shadow:0 0 8px rgba(255,138,60,.6)}\n@keyframes hhslide{0%{left:-38%}100%{left:100%}}\n.status .err{color:#FF6B6B;font-size:13px;max-width:80%;text-align:center;line-height:1.6}\n.status .retrybtn{background:linear-gradient(90deg,#FF8A3C,#38D9FF);color:#0B0714;border:none;padding:8px 26px;border-radius:8px;cursor:pointer;font-size:13px;margin-top:4px;font-weight:600}\n.warnbar{display:none;margin-bottom:14px;padding:10px 16px;border-radius:12px;background:rgba(255,107,107,.10);border:1px solid rgba(255,107,107,.45);color:#FFB3A0;font-size:12.5px;line-height:1.6;align-items:center}\n.warnbar b{font-weight:700;color:#FF8A6B}\n.warnbar .wbtn{margin-left:14px;background:rgba(255,107,107,.18);border:1px solid rgba(255,107,107,.55);color:#FFD0C0;padding:5px 16px;border-radius:8px;cursor:pointer;font-size:12px;font-family:inherit;flex-shrink:0}\n.warnbar .wbtn:hover{background:rgba(255,107,107,.32)}\n.bodywrap{position:relative;flex:1;display:flex;flex-direction:column}\n.footer{margin-top:16px;font-size:11.5px;color:#9A8FC0;line-height:1.7;text-align:center}\n@media (max-width:720px){\n.wrap{padding:14px 12px 12px}\n.head .t{font-size:18px}\n.head .src{font-size:11px}\n.low{grid-template-columns:1fr}\n.scroll30{overflow-x:auto}\n.chart{height:272px}\n.duals{height:280px}\n.duals .g{height:280px}\n.status{max-width:88vw;padding:22px 16px}\n.status .bar{width:200px}\n}\n\n</style></head><body>\n<div class="wrap">\n<div class="head">\n<div><div class="t">做种<b>积分统计</b></div>\n<div class="src">彩虹岛做种积分巡检采样 · 每轮巡检自动记录 · 按自然日聚合</div></div>\n<button class="exit" title="关闭页面" onclick="window.close()">退出</button>\n</div>\n<div class="bodywrap">\n<div id="status" class="status"><span>正在加载统计数据，请稍候…</span><div class="bar"><i></i></div></div>\n<div id="main" style="display:none">\n<div class="topcard"><div class="ct"><span class="nm">近<span id="dvl">15</span>日做种积分增量</span><span class="stp"><button onclick="adjD(-1)">−</button><button onclick="adjD(1)">＋</button></span><span class="sb">橙柱 = 当日做种积分增量 · 悬停查看当日首末次采样 · 下方网格与柱严格对齐</span></div>\n<div class="qwrap"><div class="qinner">\n<div class="chart" id="mb"></div>\n<div class="qtbl" id="qtbl"></div>\n</div></div></div>\n<div class="low">\n<div class="card"><div class="ct"><span class="nm">做种积分累计</span><span class="sb">左环 = 当前积分 · 右环 = 近30日增量</span></div>\n<div class="duals"><div class="dbox"><div class="g" id="dg1"></div><div class="dlab"><div class="r">当前积分</div><div class="v" id="cvb" style="color:#FF8A3C">—</div></div></div>\n<div class="dbox"><div class="g" id="dg2"></div><div class="dlab"><div class="r">30日增量</div><div class="v" id="cvp" style="color:#38D9FF">—</div></div></div></div></div>\n<div class="card"><div class="ct"><span class="nm">采样明细</span><span class="sb">最新日期在最上方 · 首/末 = 当日首末次采样值</span></div>\n<div class="scroll30" id="tbl"></div></div>\n</div>\n<div class="card" style="margin-top:10px"><div class="ct"><span class="nm">近12个月收益</span><span class="sb">月收益 = 月末采样 − 上月末采样 · 首月按 末采 − 首采</span></div>\n<div class="scroll30" id="mtbl"></div></div>\n</div>\n</div>\n<div class="footer" id="foot">做种积分数据来自插件每轮巡检对页头「做种积分」的采样 · 增量按相邻日末次采样差计算 · 采样积累后方有数据</div>\n</div>\n<script src="https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js"></script>\n<script>\nvar data=__DATA__;\nvar chb=null,chd1=null,chd2=null,R={days:(data.days||15)};\nfunction esc(s){return String(s==null?"":s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;")}\nfunction fmtBig(n){if(n===null||n===undefined)return "—";return Number(n).toLocaleString()}\nfunction adjD(d){R.days=Math.min(30,Math.max(1,(R.days||15)+d));renderAll();try{var u=new URL(location.href);u.searchParams.set("days",R.days);u.searchParams.set("persist","1");fetch(u.toString()).catch(function(){});}catch(e){}}\nfunction renderAll(){\nvar rec=(data.records||[]).slice(-R.days);\ndocument.getElementById(\'dvl\').textContent=R.days;\nvar st=document.getElementById(\'status\');st.style.display=\'none\';\ndocument.getElementById(\'main\').style.display=\'block\';\ndocument.getElementById(\'foot\').textContent=\'数据更新于 \'+(data.updated_at||\'—\')+\' · 做种积分数据来自插件每轮巡检采样 · 增量按相邻日末次采样差计算\';\nrenderMonthly();\nif(typeof echarts===\'undefined\'){renderTable(rec);return;}\nrenderChart(rec);buildQTable(rec);renderGauges();renderTable((data.records||[]));\n}\nfunction renderChart(rec){\nif(chb){chb.dispose();chb=null;}\nchb=echarts.init(document.getElementById(\'mb\'));\nvar x=[],d=[];\nfor(var i=0;i<rec.length;i++){var r=rec[i];x.push(r.date);d.push(r.delta===null||r.delta===undefined?0:Math.round(r.delta));}\nchb.setOption({backgroundColor:\'transparent\',\ntooltip:{trigger:\'axis\',triggerOn:\'click\',renderMode:\'richText\',confine:true,textStyle:{fontSize:11,color:\'#F0ECFF\',lineHeight:15},backgroundColor:\'rgba(21,13,36,.95)\',borderColor:\'rgba(64,200,255,.3)\',padding:[6,8],\nformatter:function(ps){var r=rec[ps[0].dataIndex];var h=\'<b>\'+esc(r.date)+\'</b><br/>当日增量：\'+fmtBig(r.delta)+\'<br/>首采：\'+fmtBig(r.first)+\'<br/>末采：\'+fmtBig(r.last)+\'<br/>采样数：\'+r.n+\'<br/>累计增量：\'+fmtBig(r.cum);return h;}},\ngrid:{left:80,right:14,top:30,bottom:8},\nxAxis:{type:\'category\',data:x,axisTick:{show:false},axisLine:{show:false},axisLabel:{show:false}},\nyAxis:{type:\'value\',axisLabel:{fontSize:10,color:\'#9A8FC0\',formatter:function(v){return v>=10000?(v/10000).toFixed(1).replace(\'.0\',\'\')+\'万\':v}},splitLine:{lineStyle:{color:\'rgba(64,200,255,.12)\'}}},\nseries:[{name:\'当日增量\',type:\'bar\',data:d,itemStyle:{color:\'#FF8A3C\',borderRadius:[4,4,0,0]},\nlabel:{show:true,position:\'top\',fontSize:9,color:\'#FF8A3C\',distance:2,formatter:function(p){return (p.value===null||p.value===undefined||p.value===0)?\'\':fmtBig(p.value)}},labelLayout:{hideOverlap:true}}]});\nchb.resize();\n}\nfunction buildQTable(rec){\nvar tb=document.getElementById(\'qtbl\');if(!tb)return;\nvar n=rec.length;\ntb.style.gridTemplateColumns=\'40px 80px repeat(\'+n+\',minmax(56px,1fr))\';\nvar inner=document.querySelector(\'.qinner\');if(inner)inner.style.minWidth=Math.max(920,134+n*60)+\'px\';\nfunction row(lab,cls,fn){var h=\'<div class="qc ax"></div><div class="qc rl">\'+lab+\'</div>\';for(var i=0;i<n;i++){h+=\'<div class="qc \'+cls+\'">\'+fn(rec[i])+\'</div>\';}return h;}\ntb.innerHTML=\nrow(\'增量\',\'q\',function(r){return (r.delta===null||r.delta===undefined)?\'<span class="mute">—</span>\':fmtBig(Math.round(r.delta));})+\nrow(\'累计\',\'s\',function(r){return fmtBig(r.cum);})+\nrow(\'日期\',\'d\',function(r){return r.date||\'\';});\n}\nfunction renderGauges(){\nif(chd1){chd1.dispose();chd1=null;}if(chd2){chd2.dispose();chd2=null;}\nchd1=echarts.init(document.getElementById(\'dg1\'));\nchd2=echarts.init(document.getElementById(\'dg2\'));\nvar cur=Number(data.current)||0,c30=Number(data.cum30)||0;\ndocument.getElementById(\'cvb\').textContent=fmtBig(Math.round(cur));\ndocument.getElementById(\'cvp\').textContent=\'+\'+fmtBig(Math.round(c30));\nchd1.setOption({backgroundColor:\'transparent\',series:[\n{type:\'gauge\',startAngle:210,endAngle:-30,min:0,max:Math.max(cur,1),radius:\'78%\',center:[\'50%\',\'52%\'],\nprogress:{show:true,width:22,itemStyle:{color:\'#FF8A3C\',shadowColor:\'#FF8A3C\',shadowBlur:16}},\naxisLine:{lineStyle:{width:22,color:[[1,\'rgba(255,138,60,.10)\']]}},\naxisTick:{show:false},splitLine:{show:false},axisLabel:{show:false},pointer:{show:false},detail:{show:false},data:[{value:cur}]}]});\nchd2.setOption({backgroundColor:\'transparent\',series:[\n{type:\'gauge\',startAngle:210,endAngle:-30,min:0,max:Math.max(c30,1),radius:\'78%\',center:[\'50%\',\'52%\'],\nprogress:{show:true,width:22,itemStyle:{color:\'#38D9FF\',shadowColor:\'#38D9FF\',shadowBlur:16}},\naxisLine:{lineStyle:{width:22,color:[[1,\'rgba(56,217,255,.10)\']]}},\naxisTick:{show:false},splitLine:{show:false},axisLabel:{show:false},pointer:{show:false},detail:{show:false},data:[{value:c30}]}]});\nchd1.resize();chd2.resize();\n}\nfunction renderTable(rec){\nvar rows=\'\';\nfor(var j=rec.length-1;j>=0;j--){var r=rec[j];\nrows+=\'<tr><td class="d">\'+esc(r.date)+\'</td><td class="bean">\'+fmtBig(r.delta===null||r.delta===undefined?null:Math.round(r.delta))+\'</td><td class="pt">\'+fmtBig(r.first)+\'</td><td>\'+fmtBig(r.last)+\'</td><td>\'+r.n+\'</td><td>\'+fmtBig(r.cum)+\'</td></tr>\';}\nif(!rows)rows=\'<tr><td colspan="6" style="text-align:center;color:#5d5478;padding:24px;">暂无采样数据（巡检一次记录一条，运行后自动积累）</td></tr>\';\ndocument.getElementById(\'tbl\').innerHTML=\'<table><thead><tr><th>日期</th><th>当日增量</th><th>首采</th><th>末采</th><th>采样数</th><th>累计增量</th></tr></thead><tbody>\'+rows+\'</tbody></table>\';\n}\nfunction renderMonthly(){\nvar mt=data.monthly||[];var rows=\'\';for(var j=mt.length-1;j>=0;j--){var m=mt[j];rows+=\'<tr><td class="d">\'+esc(m.month)+(m.note?\' <i style="color:#a093c8;font-style:normal">\'+esc(m.note)+\'</i>\':\'\')+\'</td><td class="bean">\'+fmtBig(m.delta===null||m.delta===undefined?null:Math.round(m.delta))+\'</td><td class="pt">\'+fmtBig(m.last)+\'</td><td>\'+fmtBig(m.cum==null?null:Math.round(m.cum))+\'</td><td>\'+m.n+\'</td></tr>\';}if(!rows)rows=\'<tr><td colspan="5" style="text-align:center;color:#5d5478;padding:24px;">暂无月度数据（采样按自然月聚合，运行后自动积累）</td></tr>\';document.getElementById(\'mtbl\').innerHTML=\'<table><thead><tr><th>月份</th><th>当月收益</th><th>月末积分</th><th>累计收益</th><th>采样数</th></tr></thead><tbody>\'+rows+\'</tbody></table>\';}\nwindow.addEventListener(\'resize\',function(){if(chb)chb.resize();if(chd1)chd1.resize();if(chd2)chd2.resize();});\nrenderAll();\n</script></body></html>'

_LEVEL_HTML = '<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">\n<meta name="viewport" content="width=device-width,initial-scale=1">\n<title>数据与等级</title><style>\n\n  * { margin:0; padding:0; box-sizing:border-box; }\n  body { background:#0d1321; color:#e8edf7; font-family:"PingFang SC","Microsoft YaHei",sans-serif; padding:10px; }\n  .card { background:#111c33; border:1px solid #1e3357; border-radius:12px; }\n  .head { display:flex; align-items:center; gap:14px; height:64px; padding:0 20px; background:linear-gradient(135deg,#12203a,#0f1830); border-radius:12px; margin-bottom:10px; }\n  .avatar { width:44px; height:44px; border-radius:10px; background:#fff; display:flex; align-items:center; justify-content:center; overflow:hidden; flex-shrink:0; }\n  .avatar img { width:100%; height:100%; }\n  .head .nm { font-size:20px; font-weight:700; }\n  .head .meta { font-size:10px; color:#7d8db1; margin-top:2px; }\n  .head .right { margin-left:auto; text-align:right; }\n  .badge { display:inline-block; background:linear-gradient(135deg,#f59e0b,#d97706); color:#1a1205; font-size:12px; font-weight:700; padding:5px 14px; border-radius:14px; margin-bottom:5px; }\n  .lvline { font-size:10px; color:#9fb3d8; }\n  .band { display:flex; flex-wrap:wrap; gap:10px; margin-bottom:10px; }\n  .kpi { flex:1 1 0; min-width:0; background:#111c33; border:1px solid #1e3357; border-radius:11px; padding:10px 8px; display:flex; flex-direction:column; align-items:center; justify-content:center; gap:3px; text-align:center; min-height:120px; }\n  .kpi .ic { width:34px; height:34px; border-radius:9px; background:#16233f; display:flex; align-items:center; justify-content:center; margin-bottom:2px; }\n  .kpi .ic svg { width:18px; height:18px; }\n  .kpi .k { font-size:10px; color:#7d8db1; line-height:1.25; }\n  .kpi .v { font-size:16px; font-weight:700; white-space:nowrap; line-height:1.15; }\n  .kpi .s { font-size:8.5px; color:#5a6b8c; white-space:nowrap; }\n  .green { color:#2ee6a8; } .blue { color:#38bdf8; } .bean { color:#FF8A3C; }\n  .main { display:flex; gap:10px; margin-bottom:10px; }\n  .progCard { flex:1 1 0; min-width:0; padding:14px 20px; display:flex; flex-direction:column; }\n  .progCard h3 { font-size:13px; color:#9fb3d8; letter-spacing:1px; margin-bottom:12px; }\n  .bar { margin-bottom:16px; flex:1; display:flex; flex-direction:column; justify-content:center; }\n  .bar:last-child { margin-bottom:0; }\n  .bar .r { display:flex; justify-content:space-between; align-items:baseline; gap:10px; font-size:11px; margin-bottom:6px; }\n  .bar .r .need { color:#7d8db1; white-space:nowrap; }\n  .bar .r .val { color:#c6d3ec; font-weight:600; text-align:right; }\n  .miss { color:#f87171; font-weight:700; } .ok { color:#2ee6a8; }\n  .track { height:9px; background:#0a1326; border-radius:5px; overflow:hidden; }\n  .fill { height:100%; border-radius:5px; }\n  .radarCard { width:380px; flex-shrink:0; padding:14px; display:flex; flex-direction:column; }\n  .radarCard h3 { font-size:13px; color:#9fb3d8; letter-spacing:1px; margin-bottom:2px; }\n  .radarWrap { position:relative; flex:1; display:flex; align-items:center; justify-content:center; }\n  .radarWrap svg { width:285px; height:285px; max-width:100%; }\n  .rcenter { position:absolute; left:50%; top:48.5%; transform:translate(-50%,-50%); text-align:center; pointer-events:none; }\n  .rcenter .lv { font-size:26px; font-weight:800; color:#fbbf24; }\n  .rcenter .sub { font-size:10px; color:#7d8db1; }\n  .rextra { text-align:center; font-size:11px; color:#9fb3d8; border-top:1px solid #1e2d4e; padding-top:8px; }\n  .rextra b { color:#f87171; }\n  .ladderCard { padding:12px 20px; margin-bottom:10px; }\n  .ladderCard h3 { font-size:13px; color:#9fb3d8; letter-spacing:1px; margin-bottom:8px; }\n  .ladder { display:flex; align-items:flex-start; }\n  .step { flex:1; text-align:center; position:relative; padding-top:16px; }\n  .step::before { content:\'\'; position:absolute; top:0; left:50%; width:12px; height:12px; border-radius:50%; background:#27334d; z-index:2; transform:translateX(-50%); }\n  .step::after { content:\'\'; position:absolute; top:5px; left:-50%; width:100%; height:2px; background:#1e3357; }\n  .step:first-child::after { display:none; }\n  .step.done::before { background:#2ee6a8; }\n  .step.done::after { background:#2ee6a8; }\n  .step.cur::before { background:#fbbf24; box-shadow:0 0 0 4px rgba(251,191,36,.2); }\n  .step.cur::after { background:linear-gradient(to right,#2ee6a8,#fbbf24); }\n  .step.done .en { color:#2ee6a8; }\n  .step .en { font-size:10.5px; color:#c6d3ec; }\n  .step .cn { font-size:9.5px; color:#5a6b8c; }\n  .step.cur .en { color:#fbbf24; font-weight:700; }\n  .tagcur { position:absolute; top:-16px; left:50%; transform:translateX(-50%); background:#fbbf24; color:#1a1205; font-size:9px; font-weight:700; padding:2px 8px; border-radius:8px; white-space:nowrap; }\n  .next { display:flex; gap:10px; flex-wrap:wrap; }\n  .ncard { flex:1 1 0; min-width:0; background:#0f1a30; border:1px solid #24355a; border-radius:11px; padding:12px 18px; display:flex; align-items:center; gap:14px; min-height:100px; }\n  .ncard .tt { flex:1; min-width:0; }\n  .ncard .t1 { font-size:13px; font-weight:700; }\n  .ncard .t2 { font-size:10px; color:#7d8db1; margin-top:4px; }\n  .ncard .dd { text-align:right; font-size:11px; color:#c6d3ec; white-space:nowrap; }\n  .ncard .dd b { color:#f87171; }\n  .foot { text-align:center; font-size:9px; color:#44567a; margin-top:10px; line-height:1.6; }\n  @keyframes radarExpand { from { transform:scale(0); opacity:0; } to { transform:scale(1); opacity:1; } }\n  @keyframes fadeIn { from { opacity:0; } to { opacity:1; } }\n  @keyframes fadeUp { from { opacity:0; transform:translateY(5px); } to { opacity:1; transform:none; } }\n  .poly-data { transform-box:view-box; transform-origin:115px 112px; animation:radarExpand .9s cubic-bezier(.22,.61,.36,1) .12s; }\n  .grid-in { animation:fadeIn .5s ease-out; }\n  .axis-in { animation:fadeIn .45s ease-out; }\n  .label-in { animation:fadeUp .5s ease-out; }\n  .l1 { animation-delay:.5s; } .l2 { animation-delay:.6s; } .l3 { animation-delay:.7s; } .l4 { animation-delay:.8s; }\n  @media (max-width:1100px){\n    .band { flex-wrap:wrap; }\n    .kpi { flex:1 1 30%; min-height:100px; }\n    .main { flex-direction:column; }\n    .radarCard { width:auto; }\n    .radarWrap { height:300px; }\n    .ladderCard { overflow-x:auto; } .ladder { min-width:760px; }\n    .next { flex-direction:column; }\n  }\n  @media (max-width:560px){\n    .kpi { flex:1 1 45%; }\n    .head { flex-wrap:wrap; height:auto; padding:10px 14px; }\n    .head .right { margin-left:0; text-align:left; }\n  }\n\n</style></head><body>\n<div class="panel">\n  <div class="head card">\n    <div class="avatar">__AVATAR__</div>\n    <div>\n      <div class="nm">__USERNAME__</div>\n      <div class="meta">__META__</div>\n    </div>\n    <div class="right">\n      <div class="badge">__BADGE__</div>\n      <div class="lvline">__LVLINE__</div>\n    </div>\n  </div>\n  <div class="band">__KPIS__</div>\n  <div class="main">\n    <div class="card progCard">\n      <h3>任务系统五档完成率（全站统计）</h3>\n      __BARS__\n    </div>\n    <div class="card radarCard">\n      <h3>账户四维概览</h3>\n      <div class="radarWrap">\n        <svg viewBox="-30 -16 290 262">__RADAR__</svg>\n        <div class="rcenter"><div class="lv">__UP_PCT__%</div><div class="sub">任务平均完成率</div></div>\n      </div>\n      <div class="rextra">四维 = 上传 / 下载 / 分享率 / 做种积分（各自归一化至 0-100）</div>\n    </div>\n  </div>\n  <div class="card ladderCard">\n    <h3>任务系统等级阶梯</h3>\n    <div class="ladder">__STEPS__</div>\n  </div>\n  <div class="card ladderCard"><h3>站点账号等级阶梯</h3><div class="ladder">__ACCLADDER__</div></div><div class="next">__NEXT__</div>\n  <div class="foot">__FOOT__</div>\n<script>\n(function(){var el=document.querySelector(\'.rcenter .lv\');if(!el)return;var end=parseFloat(el.textContent)||0;var dur=1400,st=performance.now();el.textContent=\'0%\';var timer=setInterval(function(){var p=Math.min((performance.now()-st)/dur,1);el.textContent=Math.round(end*(1-Math.pow(1-p,3)))+\'%\';if(p>=1)clearInterval(timer);},16);})();\n</script>\n</div>\n</body>\n</html>'
