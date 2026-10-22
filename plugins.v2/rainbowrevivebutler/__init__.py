import csv
import io
import json
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
from app.db.site_oper import SiteOper
from app.helper.downloader import DownloaderHelper
from app.log import logger
from app.plugins import _PluginBase


class RainbowReviveButler(_PluginBase):
    """
    彩虹岛复活区管家 Q自用版

    基于六个橙子「憨憨保种区管家」(HHClubButler) 架构复刻，致谢原作者 SixOrg。
    彩虹岛（CHDBits）复活区玩法适配：复活区全景 / 免费到期处置 / 捡漏推送 / 做种到期提醒。

    彩虹岛复活区机制（2026-10 实测）：
    - 发布超 14 天、做种人数低于 10 的种子，会员花 3W 做种积分可列入复活区；
    - 列入后享 7 天免费下载（列表带精确到期时间戳），免盒子（SeedBox）备案规则，
      仍限速 100M/s；无结算、无做种时长考核（与憨憨合约制不同）。
    - 复活区当前无 H3/H5 HR 标记，但保留解析（站点制度 10 月新实施，防御未来打标）。

    核心处置逻辑：巡检/守护线程将下载器中「仍在下载」的任务与复活区列表比对，
    免费到期（+5 分钟缓冲）仍未下完 → HR 种转人工，非 HR 种自动删除任务（可选含文件）；
    到期前 warn_hours 小时预警；预计完不成（按当前速率 ETA 超过剩余免费时长）提前删。

    频率拆分：到期处置由后台守护线程高频执行（默认 5 分钟一次，到期即删）；
    全景/捡漏推送/自动下载由 Cron 巡检（默认 2 小时一次）驱动。
    """

    plugin_name = "彩虹岛复活区管家Q自用版"
    plugin_desc = "基于六个橙子「憨憨保种区管家」(HHClubButler) 架构复刻，致谢原作者 SixOrg。\n复活区全景 / 免费到期处置 / 捡漏推送 / 做种到期提醒"
    plugin_icon = "https://raw.githubusercontent.com/q10710/MoviePilot-Plugins/main/icons/rainbowrevivebutler.png"
    plugin_version = "1.3.0"
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
    # 捡漏自动下载
    _auto_download: bool = False
    _push_downloader: str = ""
    _save_path: str = ""
    _tag: str = ""
    _target_volume: float = 0.0

    _running: bool = False
    _scheduler: Optional[BackgroundScheduler] = None
    _last_result: str = "尚未运行"

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
        self._auto_download = bool(config.get("auto_download", False))
        self._push_downloader = str(config.get("push_downloader") or "")
        self._save_path = str(config.get("save_path") or "")
        self._tag = str(config.get("tag") or "")
        try:
            self._target_volume = max(0.0, float(config.get("target_volume", 0) or 0))
        except (TypeError, ValueError):
            self._target_volume = 0.0

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
                "auto_download": self._auto_download, "push_downloader": self._push_downloader,
                "save_path": self._save_path, "tag": self._tag, "target_volume": self._target_volume,
            })
            self.update_config(_cfg)
            try:
                self._scheduler = BackgroundScheduler(timezone=settings.TIMEZONE)
                self._scheduler.add_job(
                    func=self.run, trigger="date", id="RainbowReviveButlerOnce",
                    next_run_time=datetime.now(pytz.timezone(settings.TIMEZONE)) + timedelta(seconds=3))
                if not self._scheduler.running:
                    self._scheduler.start()
            except Exception as e:
                logger.error(f"彩虹岛复活区管家：一次性任务调度失败：{e}")

        logger.info(
            f"彩虹岛复活区管家配置生效：启用={self._enabled} 通知={self._notify} "
            f"巡检={self._cron} 处置守护={self._dispose_minutes:g}min 站点={self._site_name} "
            f"下载器={self._downloaders} 预警={self._warn_hours:g}h 删文件={self._delete_files} "
            f"提前删={self._early_delete} 捡漏阈值={self._low_seeder_max}人 "
            f"自动下载={self._auto_download}(→{self._push_downloader or '-'}, "
            f"上限={self._target_volume:g}GB)")

    def get_state(self) -> bool:
        """返回插件启用状态。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """无远程命令。"""
        return []

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
        """注册全景/捡漏/自动下载巡检（到期处置由守护线程负责）。"""
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
                    r = session.get(url, timeout=30)
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
            if all_seeds or attempt == 1:
                break
            # 首页拿到 0 个种子：可能是站点瞬时限流/返回登录页，等 5 秒重试一次
            logger.warning("彩虹岛复活区管家：复活区列表为空，5 秒后重试一次（防瞬时限流）")
            time.sleep(5)
        return all_seeds, page + 1

    def _fetch_my_seeding_ids(self, session: requests.Session, base: str) -> set:
        """spstate=1 视图 = 我在做种的复活区种子。"""
        try:
            r = session.get(base + "/renewtorrents.php?spstate=1", timeout=30)
            # (?<!user) 排除 userdetails.php?id= 用户链接（其尾部与 details.php?id= 同形，会误抓页头/上传者 ID）
            return set(re.findall(r"(?<!user)details\.php\?id=(\d+)", r.text))
        except Exception as e:
            logger.warning(f"彩虹岛复活区管家：拉取我的做种视图失败：{e}")
            return set()

    def _get_site_uid(self, session: requests.Session, base: str) -> Optional[str]:
        """从首页解析当前登录用户 UID（做种组合接口需要）。"""
        try:
            r = session.get(base + "/", timeout=20)
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
            r = session.get(base + "/", timeout=20)
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
            r = session.get(base + f"/getusertorrentlistajax.php?userid={uid}&type=seeding", timeout=30)
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
                if error or not torrents:
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
        base = self._get_site_url()
        cookie = self._get_site_cookie()
        if not base or not cookie:
            raise Exception("未找到彩虹岛站点配置或 Cookie（请在站点管理中配置）")
        if quick and not self._has_incomplete_tasks():
            return {"seeds": [], "pages": 0, "deleted": [], "hr_skipped": [],
                    "warned": [], "early_deleted": []}
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
                        (deleted if ok else warned).append(seed)
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
                                    (early_deleted if ok else warned).append(seed)
                                    logger.info(f"彩虹岛复活区管家：预计完不成提前删 {seed['title'][:50]}（ETA {eta/60:.0f}min > 剩余 {remain_sec/60:.0f}min）成功={ok}")
                except Exception as e:
                    logger.warning(f"彩虹岛复活区管家：处理任务出错：{e}")
                    continue

        self.save_data("notify_dedupe", dedupe)
        stats = {"seeds": seeds, "pages": pages, "deleted": deleted, "hr_skipped": hr_skipped,
                 "warned": warned, "early_deleted": early_deleted}
        if notify and self._notify:
            self._notify_disposal(deleted, hr_skipped, warned, early_deleted)
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
        """记录处置日志（上限 200 条）。"""
        try:
            log = self.get_data("disposal_log") or []
            if not isinstance(log, list):
                log = []
            log.append(entry)
            self.save_data("disposal_log", log[-200:])
        except Exception as e:
            logger.warning(f"彩虹岛复活区管家：写处置日志失败：{e}")

    def _notify_disposal(self, deleted, hr_skipped, warned, early_deleted):
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
            if warned and not (deleted or early_deleted):
                lines = [f"  {s['title'][:50]}（进度 {s.get('progress_pct', '?')}%，剩 {s.get('remain_min', '?')} 分钟）"
                         for s in warned[:10]]
                self.post_message(
                    title=f"【彩虹岛复活区管家】{len(warned)} 个免费下载即将到期",
                    text=f"到期后仍未下完将自动删除（{self._warn_hours:g} 小时预警）：\n" + "\n".join(lines))
        except Exception as e:
            logger.error(f"彩虹岛复活区管家：发送通知失败：{e}")

    # ------------------------------------------------------------------ #
    # 巡检：全景 + 捡漏推送 + 做种到期提醒 + 自动下载
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
            if self._notify:
                try:
                    self.post_message(title="【彩虹岛复活区管家】运行异常", text=str(e))
                except Exception:
                    pass
        finally:
            self._running = False

    def _do_run(self):
        """巡检主逻辑：到期处置 + 全景 + 捡漏 + 做种到期提醒 + 自动下载。"""
        t0 = time.time()
        stats = self._dispose_pass(notify=self._notify)
        seeds = stats["seeds"]
        base = self._get_site_url()
        session = self._session(self._get_site_cookie())
        my_ids = self._fetch_my_seeding_ids(session, base)
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
            if cur_credit:
                self.save_data("credit_sample", {"credit": cur_credit, "ts": time.time()})
                self._append_credit_sample(cur_credit)
        except Exception:
            pass
        portfolio_stats = self._analyze_portfolio(portfolio)
        now = time.time()
        warn_sec = self._warn_hours * 3600

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
        # 捡漏：免费 + 做种人数低
        deal_seeds = []
        if self._low_seeder_max > 0:
            for s in seeds:
                if s["is_free"] and s["seeders"] <= self._low_seeder_max and s.get("free_until") and s["free_until"] > now:
                    key = f"deal:{s['tid']}:{int(s['free_until'])}"
                    if key not in dedupe:
                        dedupe[key] = now  # 写入去重，同一免费窗口只推一次（否则每轮巡检重复推送）
                        deal_seeds.append(s)
        self.save_data("notify_dedupe", dedupe)

        # 自动下载捡漏（默认关）
        downloaded = []
        if self._auto_download and deal_seeds:
            downloaded = self._auto_download_deals(deal_seeds, my_ids, now)

        if self._notify:
            if seeding_expiring:
                try:
                    lines = [f"  {s['title'][:50]}（剩 {s.get('remain_min', '?')} 分钟）" for s in seeding_expiring[:10]]
                    self.post_message(
                        title=f"【彩虹岛复活区管家】{len(seeding_expiring)} 个我在做种的复活种免费将结束",
                        text="免费结束后新下载骤减、上传收益下降，去留请自行判断：\n" + "\n".join(lines))
                except Exception:
                    pass
            if deal_seeds and not downloaded:
                try:
                    lines = [f"  {s['title'][:60]}（{s['size_gb']:.1f}GB，做种 {s['seeders']}）" for s in deal_seeds[:10]]
                    more = len(deal_seeds) - 10
                    if more > 0:
                        lines.append(f"  …等共 {len(deal_seeds)} 个")
                    self.post_message(
                        title=f"【彩虹岛复活区管家】捡漏：{len(deal_seeds)} 个低做种免费种子",
                        text="免费中且做种人数 ≤ {}，ratio 黄金窗口：\n{}".format(self._low_seeder_max, "\n".join(lines)))
                except Exception:
                    pass
            if downloaded:
                try:
                    lines = [f"  {t['title'][:60]}（{t['size_gb']:.1f}GB）" for t in downloaded[:10]]
                    self.post_message(
                        title=f"【彩虹岛复活区管家】自动下载 {len(downloaded)} 个捡漏种子",
                        text=f"已推送到 {self._push_downloader}：\n" + "\n".join(lines))
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
        }
        self._last_overview_ts = now
        self._last_result = (
            f"{datetime.now().strftime('%m-%d %H:%M')} 巡检：复活区 {len(seeds)} 种（免费中 {free_active}），"
            f"我的做种 {len(my_ids)}，到期删 {len(stats['deleted'])}，提前删 {len(stats['early_deleted'])}，"
            f"预警 {len(stats['warned'])}，自动下载 {len(downloaded)}，耗时 {time.time()-t0:.1f}s")
        logger.info(f"彩虹岛复活区管家：{self._last_result}")

    def _auto_download_deals(self, deal_seeds: List[dict], my_ids: set, now: float) -> List[dict]:
        """把捡漏种子自动推送到下载器（尊重目标体积上限，去重）。"""
        out: List[dict] = []
        try:
            sv = self._get_push_service()
            if not sv:
                logger.warning("彩虹岛复活区管家：自动下载未配置有效推送下载器，跳过")
                return out
            existing = set()
            try:
                ts, err = sv.instance.get_torrents()
                if not err and ts:
                    for t in ts:
                        existing.add(self._norm_title(self._tf(t, "name", default="")))
            except Exception:
                pass
            base = self._get_site_url()
            cookie = self._get_site_cookie()
            total_gb = 0.0
            for s in deal_seeds:
                if s["norm"] in existing or s["tid"] in my_ids:
                    continue
                if self._target_volume > 0 and (total_gb + s["size_gb"]) > self._target_volume:
                    logger.info(f"彩虹岛复活区管家：自动下载已达目标体积上限 {self._target_volume:g}GB，停止")
                    break
                url = f"{base}/download.php?id={s['tid']}"
                try:
                    # add_torrent 返回 (成功标志, 任务标识列表)，失败时 (False, [])
                    result = sv.instance.add_torrent(
                        content=url, cookie=cookie,
                        download_dir=(self._save_path or None),
                        tag=(self._tag or None))
                    ok = result[0] if isinstance(result, tuple) else bool(result)
                    if not ok:
                        logger.warning(f"彩虹岛复活区管家：自动下载被下载器拒绝 {s['title'][:40]}（返回 {result}）")
                        continue
                    out.append(s)
                    total_gb += s["size_gb"]
                    logger.info(f"彩虹岛复活区管家：自动下载 {s['title'][:50]}（{s['size_gb']:.1f}GB → {self._push_downloader}）")
                except Exception as e:
                    logger.error(f"彩虹岛复活区管家：自动下载失败 {s['title'][:40]}：{e}")
        except Exception as e:
            logger.error(f"彩虹岛复活区管家：自动下载流程异常：{e}")
        return out

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
    def _overview_html(ov: dict) -> str:
        """照原作者版式：主题自适应 table 布局，账户统计 4 格 + 复活区档位色条 + 做种组合色条。"""
        if not ov:
            return ('<div style="padding:12px 14px;border-radius:10px;'
                    'border:1px solid rgba(var(--v-theme-on-surface),.08);'
                    'background:rgba(var(--v-theme-surface),.55);'
                    'color:rgba(var(--v-theme-on-surface),.72);font-size:12px;">'
                    '彩虹岛复活区管家：未获取到数据（请确认插件已启用、下载器已选择、站点可访问）</div>')
        stats = ov.get("stats", {}) or {}
        ps = ov.get("portfolio_stats", {}) or {}
        seeds = ov.get("seeds", []) or []
        ON = "rgb(var(--v-theme-on-surface))"
        ON60 = "rgba(var(--v-theme-on-surface),.6)"
        ON40 = "rgba(var(--v-theme-on-surface),.4)"

        def td(label, num, sub):
            return (f'<td style="text-align:center;padding:4px 2px;width:25%;vertical-align:top;">'
                    f'<div style="font-size:10.5px;color:{ON60};white-space:nowrap;">{label}</div>'
                    f'<div style="font-size:17px;font-weight:700;color:{ON};line-height:1.4;white-space:nowrap;">{num}'
                    f'<span style="font-size:10px;font-weight:400;color:{ON40};"> {sub}</span></div></td>')

        # 账户统计 4 格（做种积分/分享率/上传量/当前做种）
        credit = stats.get("credit", 0) or 0
        rate = stats.get("credit_rate")
        credit_sub = f"+{rate:.0f}/h" if rate else ""
        acc_cells = ('<table style="width:100%;border-collapse:collapse;table-layout:fixed;margin:2px 0 6px;"><tr>'
                     + td("🌟 做种积分", f"{credit:,.0f}", credit_sub)
                     + td("📊 分享率", f"{stats.get('ratio', 0):.3f}", "")
                     + td("⬆️ 上传量", f"{stats.get('uploaded_tb', 0):.2f}", "TB")
                     + td("🌿 当前做种", f"{stats.get('seeding_now', 0)}", "个")
                     + '</tr></table>')

        # 复活区档位色条 + 图例（0-1/2-3/4+）
        C = {"low": "#22c55e", "mid": "#3b82f6", "high": "#ef4444"}
        tiers = {"low": 0, "mid": 0, "high": 0}
        for s in seeds:
            n = s.get("seeders", 0)
            tiers["low" if n <= 1 else ("mid" if n <= 3 else "high")] += 1
        t_total = max(1, len(seeds))
        revive_bar = ('<table style="width:100%;border-collapse:collapse;table-layout:fixed;margin:4px 0 2px;"><tr>'
                      f'<td style="height:10px;width:{tiers["low"]/t_total*100:.2f}%;background:{C["low"]};border-radius:5px 0 0 5px;"></td>'
                      f'<td style="height:10px;width:{tiers["mid"]/t_total*100:.2f}%;background:{C["mid"]};"></td>'
                      f'<td style="height:10px;width:{tiers["high"]/t_total*100:.2f}%;background:{C["high"]};border-radius:0 5px 5px 0;"></td>'
                      '</tr></table>')
        revive_legend = (f'<table style="width:100%;border-collapse:collapse;table-layout:fixed;font-size:10.5px;color:{ON60};"><tr>'
                         f'<td style="text-align:left;white-space:nowrap;"><span style="display:inline-block;width:7px;height:7px;border-radius:2px;background:{C["low"]};margin-right:3px;"></span>复活区 0-1人 {tiers["low"]}</td>'
                         f'<td style="text-align:center;white-space:nowrap;"><span style="display:inline-block;width:7px;height:7px;border-radius:2px;background:{C["mid"]};margin-right:3px;"></span>2-3人 {tiers["mid"]}</td>'
                         f'<td style="text-align:right;white-space:nowrap;"><span style="display:inline-block;width:7px;height:7px;border-radius:2px;background:{C["high"]};margin-right:3px;"></span>4+人 {tiers["high"]}</td>'
                         '</tr></table>')
        revive_title = ('<table style="width:100%;border-collapse:collapse;table-layout:fixed;"><tr>'
                        f'<td style="text-align:left;font-size:10.5px;color:{ON60};">复活区 '
                        f'{ov.get("total", 0)} 种 · 免费中 {ov.get("free_active", 0)} · 我做种 {ov.get("my_seeding", 0)}</td>'
                        '</tr></table>')

        # 做种组合色条 + 图例（高价值/中/摊薄）
        seg = ""
        if ps:
            p_low = ps.get("tier_low", 0); p_mid = ps.get("tier_mid", 0); p_high = ps.get("tier_high", 0)
            p_total = max(1, ps.get("count", 1))
            seg = ('<table style="width:100%;border-collapse:collapse;table-layout:fixed;margin:8px 0 2px;"><tr>'
                   f'<td style="height:10px;width:{p_low/p_total*100:.2f}%;background:{C["low"]};border-radius:5px 0 0 5px;"></td>'
                   f'<td style="height:10px;width:{p_mid/p_total*100:.2f}%;background:{C["mid"]};"></td>'
                   f'<td style="height:10px;width:{p_high/p_total*100:.2f}%;background:{C["high"]};border-radius:0 5px 5px 0;"></td>'
                   '</tr></table>'
                   f'<table style="width:100%;border-collapse:collapse;table-layout:fixed;font-size:10.5px;color:{ON60};"><tr>'
                   f'<td style="text-align:left;white-space:nowrap;"><span style="display:inline-block;width:7px;height:7px;border-radius:2px;background:{C["low"]};margin-right:3px;"></span>做种组合 {ps.get("count", 0)}颗 {ps.get("total_gb", 0):.0f}G</td>'
                   f'<td style="text-align:center;white-space:nowrap;"><span style="display:inline-block;width:7px;height:7px;border-radius:2px;background:{C["mid"]};margin-right:3px;"></span>中 {p_mid}</td>'
                   f'<td style="text-align:right;white-space:nowrap;"><span style="display:inline-block;width:7px;height:7px;border-radius:2px;background:{C["high"]};margin-right:3px;"></span>摊薄 {p_high}</td>'
                   '</tr></table>')

        return f'<div style="background:transparent;">{acc_cells}{revive_title}{revive_bar}{revive_legend}{seg}</div>'

    def _overview_form_cards(self) -> dict:
        """配置表单顶部数据面板（照原作者 _overview_form_cards：VCard + 响应式统计格）。"""
        ov = self._last_overview or {}
        stats = ov.get("stats", {}) or {}
        ps = ov.get("portfolio_stats", {}) or {}
        if not ov or not ov.get("total"):
            return {"component": "VCard", "props": {"flat": True},
                    "content": [{"component": "VCardText", "props": {},
                                 "text": "尚未运行或未获取到数据（启用插件或刷新概况后此处显示最近概况）"}]}
        ON60 = "rgba(var(--v-theme-on-surface),.6)"

        def stat_col(label, num, sub=""):
            return {"component": "VCol", "props": {"cols": 6, "md": 3},
                    "content": [
                        {"component": "div",
                         "props": {"style": f"text-align:center;font-size:11px;color:{ON60};white-space:nowrap;"},
                         "text": label},
                        {"component": "div",
                         "props": {"style": "text-align:center;font-size:17px;font-weight:700;white-space:nowrap;"},
                         "text": f"{num} {sub}".strip()},
                    ]}

        credit = stats.get("credit", 0) or 0
        rate = stats.get("credit_rate")
        credit_sub = f"+{rate:.0f}/h" if rate else ""
        stats_row = {"component": "VRow", "props": {"dense": True}, "content": [
            stat_col("🌟 做种积分", f"{credit:,.0f}", credit_sub),
            stat_col("📊 分享率", f"{stats.get('ratio', 0):.3f}"),
            stat_col("⬆️ 上传量", f"{stats.get('uploaded_tb', 0):.2f} TB"),
            stat_col("🌿 当前做种", stats.get("seeding_now", 0), "个"),
        ]}
        revive_row = {"component": "VRow", "props": {"dense": True}, "content": [
            stat_col("🌱 复活区", ov.get("total", 0), "种"),
            stat_col("🎁 免费中", ov.get("free_active", 0), "种"),
            stat_col("🔖 我在做种", ov.get("my_seeding", 0), "个"),
            stat_col("💎 做种组合", ps.get("count", 0), f"颗/{ps.get('total_gb', 0):.0f}G" if ps else ""),
        ]}
        return {"component": "VCard", "props": {"flat": True},
                "content": [{"component": "VCardText", "props": {"class": "py-2"},
                             "content": [stats_row, revive_row]}]}

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
            "auth": "apikey", "summary": "立即巡检（含到期处置+捡漏+自动下载）",
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

    def api_refresh(self):
        """立即刷新概况快照（只读）。"""
        self._refresh_overview(manual=True)
        ov = self._last_overview or {}
        return {"success": True, "total": ov.get("total", 0), "my_seeding": ov.get("my_seeding", 0)}

    def api_data(self):
        """返回概况 + 种子列表 + 处置日志（工作台数据源）。"""
        ov = self._get_overview_cached_or_refresh()
        seeds = ov.get("seeds", [])
        my_ids = set(ov.get("my_ids", []))
        now = time.time()
        out = []
        for s in seeds:
            remain = int((s.get("free_until") or 0) - now)
            out.append({
                "tid": s["tid"], "title": s["title"], "size_gb": s["size_gb"],
                "seeders": s["seeders"], "leechers": s["leechers"],
                "is_free": s["is_free"], "free_until": s.get("free_until"),
                "remain_sec": remain, "expired": bool(s.get("free_until") and s["free_until"] <= now),
                "hr": s.get("hr", False), "mine": s["tid"] in my_ids,
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
            "cfg": {"delete_files": self._delete_files, "early_delete": self._early_delete,
                    "low_seeder_max": self._low_seeder_max, "auto_download": self._auto_download},
        }

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
        form = [{
            "component": "VForm",
            "content": [
                {**self._overview_form_cards()},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 3},
                     "content": [{"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 3},
                     "content": [{"component": "VSwitch", "props": {"model": "notify", "label": "发送通知"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 3},
                     "content": [{"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即运行一次"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 3},
                     "content": [{"component": "VSwitch", "props": {"model": "delete_files", "label": "删除时含数据文件"}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VCronField", "props": {
                         "model": "cron", "label": "执行周期（全景/捡漏/自动下载）",
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
                         "model": "low_seeder_max", "label": "捡漏做种阈值（人）", "type": "number",
                         "hint": "免费且做种≤N 推送；0=关闭捡漏", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VSelect", "props": {
                         "model": "downloaders", "label": "受管控下载器（到期处置）",
                         "items": downloaders, "multiple": True, "chips": True}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VSwitch", "props": {
                         "model": "early_delete", "label": "预计完不成提前删",
                         "hint": "按当前速率 ETA 超过剩余免费时长即提前删", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VSwitch", "props": {
                         "model": "auto_download", "label": "捡漏自动下载",
                         "hint": "低做种免费种子自动推送下载器", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VTextField", "props": {
                         "model": "target_volume", "label": "自动下载体积上限（GB）", "type": "number",
                         "hint": "0=不限", "persistent-hint": True}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VSelect", "props": {
                         "model": "push_downloader", "label": "推送下载器（自动下载）",
                         "items": downloaders, "hint": "捡漏种子加到哪个下载器", "persistent-hint": True}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VTextField", "props": {
                         "model": "save_path", "label": "保存路径（可选）",
                         "placeholder": "留空用下载器默认目录"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 4},
                     "content": [{"component": "VTextField", "props": {
                         "model": "tag", "label": "任务标签（可选）", "placeholder": "留空不打标签"}}]},
                ]},
            ],
        }]
        defaults = {
            "enabled": False, "onlyonce": False, "notify": True,
            "cron": "0 */2 * * *", "site_name": "彩虹岛", "downloaders": [],
            "warn_hours": 2, "delete_files": True, "early_delete": True,
            "low_seeder_max": 3, "dispose_minutes": 5,
            "auto_download": False, "push_downloader": "", "save_path": "",
            "tag": "", "target_volume": 0,
        }
        return form, defaults

    def get_page(self) -> Optional[List[dict]]:
        """插件详情页：数据面板 + QB 风格保种管理列表 + 工作台入口。"""
        apikey = getattr(settings, "API_TOKEN", "") or ""
        panel_url = f"/api/v1/plugin/RainbowReviveButler/panel?apikey={apikey}"
        export_url = f"/api/v1/plugin/RainbowReviveButler/export?apikey={apikey}"
        ov = self._last_overview or {}
        stats = ov.get("stats", {}) or {}
        ps = ov.get("portfolio_stats", {}) or {}

        seeds = ov.get("seeds", []) or []
        my_ids = set(ov.get("my_ids", []))
        now = time.time()
        my_seeds = [s for s in seeds if s["tid"] in my_ids]
        my_seeds.sort(key=lambda s: (s.get("seeders", 0), -s.get("size_gb", 0)))
        rows = []
        for i, s in enumerate(my_seeds, 1):
            n = s.get("seeders", 0)
            tier_c = "#22c55e" if n <= 1 else ("#4da3ff" if n <= 3 else "#ff6b6b")
            tier_t = "0-1人" if n <= 1 else ("2-3人" if n <= 3 else "4人以上")
            remain = int((s.get("free_until") or 0) - now)
            if s.get("free_until") and remain > 0:
                free_html = f'<span style="color:#10b981;">免费中 {remain//3600}h{remain%3600//60}m</span>'
            elif s.get("free_until"):
                free_html = '<span style="color:#f59e0b;">已过期</span>'
            else:
                free_html = "-"
            rows.append(
                {"component": "VRow",
                 "props": {"class": "py-1", "style": "border-bottom:1px solid rgba(var(--v-theme-on-surface),.06);"},
                 "content": [
                     {"component": "VCol", "props": {"cols": 1}, "content":
                         [{"component": "div", "props": {"class": "text-caption",
                           "style": "text-align:center;color:rgba(var(--v-theme-on-surface),.5);"}, "text": str(i)}]},
                     {"component": "VCol", "props": {"cols": 5}, "content":
                         [{"component": "div", "props": {"style": "font-size:12px;word-break:break-all;line-height:1.4;",
                           "title": s["title"]}, "text": s["title"]}]},
                     {"component": "VCol", "props": {"cols": 2}, "content":
                         [{"component": "div", "props": {"style": "text-align:right;font-size:12px;"},
                           "text": f"{s.get('size_gb', 0):.1f} GB"}]},
                     {"component": "VCol", "props": {"cols": 2}, "content":
                         [{"component": "div", "props": {"style": f"text-align:center;color:{tier_c};font-weight:600;font-size:12px;"},
                           "text": f"{tier_t}（{n}）"}]},
                     {"component": "VCol", "props": {"cols": 1}, "content":
                         [{"component": "div", "props": {"style": "text-align:center;font-size:11px;"},
                           "html": free_html}]},
                     {"component": "VCol", "props": {"cols": 1}, "content":
                         [{"component": "VBtn", "props": {"size": "x-small", "color": "error", "variant": "tonal",
                           "icon": "mdi-delete-outline"},
                           "events": {"click": {"api": "plugin/RainbowReviveButler/delete_seed",
                                                "method": "post", "params": {"apikey": apikey, "tid": s["tid"]}}}}]},
                 ]})
        header = {"component": "VRow",
                  "props": {"class": "py-1", "style": "border-bottom:1px solid rgba(var(--v-theme-on-surface),.12);font-weight:600;font-size:11px;color:rgba(var(--v-theme-on-surface),.6);"},
                  "content": [
                      {"component": "VCol", "props": {"cols": 1}, "text": "#"},
                      {"component": "VCol", "props": {"cols": 5}, "text": "种子名称"},
                      {"component": "VCol", "props": {"cols": 2, "style": "text-align:right;"}, "text": "大小"},
                      {"component": "VCol", "props": {"cols": 2, "style": "text-align:center;"}, "text": "做种档位"},
                      {"component": "VCol", "props": {"cols": 1, "style": "text-align:center;"}, "text": "免费"},
                      {"component": "VCol", "props": {"cols": 1}, "text": ""},
                  ]}
        list_block = [header] + (rows or [{"component": "div",
                     "props": {"style": "padding:20px;text-align:center;color:rgba(var(--v-theme-on-surface),.4);font-size:12px;"},
                     "text": "我在复活区暂无做种种子"}])
        links = (
            '<div style="padding:10px 0;display:flex;align-items:center;gap:10px;flex-wrap:wrap;">'
            f'<a href="/api/v1/plugin/RainbowReviveButler/stats?apikey={apikey}" target="_blank" '
            'style="display:inline-flex;align-items:center;gap:6px;padding:7px 18px;background:rgba(66,165,245,.12);'
            'color:#42A5F5;text-decoration:none;border-radius:4px;font-size:13px;font-weight:500;border:1px solid rgba(66,165,245,.3);">'
            '📊 做种积分统计</a>'
            f'<a href="/api/v1/plugin/RainbowReviveButler/level?apikey={apikey}" target="_blank" '
            'style="display:inline-flex;align-items:center;gap:6px;padding:7px 18px;background:rgba(66,165,245,.12);'
            'color:#42A5F5;text-decoration:none;border-radius:4px;font-size:13px;font-weight:500;border:1px solid rgba(66,165,245,.3);">'
            '📈 数据与等级</a>'
            f'<a href="{panel_url}" target="_blank" '
            'style="display:inline-flex;align-items:center;gap:6px;padding:7px 18px;background:rgba(66,165,245,.12);'
            'color:#42A5F5;text-decoration:none;border-radius:4px;font-size:13px;font-weight:500;border:1px solid rgba(66,165,245,.3);">'
            '🖥️ 复活区管理器</a>'
            f'<a href="{export_url}" '
            'style="display:inline-flex;align-items:center;gap:6px;padding:7px 18px;background:rgba(66,165,245,.12);'
            'color:#42A5F5;text-decoration:none;border-radius:4px;font-size:13px;font-weight:500;border:1px solid rgba(66,165,245,.3);">'
            '⬇ 导出 CSV</a>'
            '<span style="font-size:11px;color:rgba(var(--v-theme-on-surface),.45);">独立工作台 · 批量删除 · 搜索 · 档位筛选 · 列自定义 · 多皮肤</span>'
            '</div>')
        # 做种组合分析：高价值榜（做种人数少+体积大）与摊薄榜（做种人数极多）
        portfolio_html = ""
        if ps:
            def _seed_rows(items, color):
                rs = []
                for i, p in enumerate(items, 1):
                    rs.append(
                        f'<div style="display:flex;gap:8px;padding:3px 0;border-bottom:1px solid rgba(var(--v-theme-on-surface),.05);font-size:12px;">'
                        f'<span style="width:20px;color:rgba(var(--v-theme-on-surface),.4);">{i}</span>'
                        f'<span style="flex:1;word-break:break-all;">{p["title"][:70]}</span>'
                        f'<span style="width:70px;text-align:right;">{p.get("size_gb", 0):.1f}G</span>'
                        f'<span style="width:60px;text-align:right;color:{color};font-weight:600;">{p.get("seeders", 0)}人</span>'
                        f'<span style="width:80px;text-align:right;">↑{p.get("upload_gb", 0):.2f}G</span></div>')
                return "".join(rs)
            valuable = ps.get("valuable", [])[:6]
            crowded = ps.get("crowded", [])[:6]
            portfolio_html = (
                f'<div style="margin:6px 0;padding:10px;background:rgba(var(--v-theme-on-surface),.03);border-radius:6px;">'
                f'<div style="font-size:13px;font-weight:600;margin-bottom:6px;">做种组合分析（共 {ps.get("count", 0)} 颗 / {ps.get("total_gb", 0):.0f} GB）</div>'
                f'<div style="font-size:11px;color:rgba(var(--v-theme-on-surface),.5);margin-bottom:4px;">'
                f'高价值(0-1人) {ps.get("tier_low", 0)} 颗 {ps.get("low_gb", 0):.0f}G · 中(2-3人) {ps.get("tier_mid", 0)} · 摊薄(4+人) {ps.get("tier_high", 0)}</div>'
                f'<div style="font-size:12px;font-weight:600;color:#22c55e;margin:6px 0 2px;">💎 高价值种子（做种稀少，收益最高）</div>'
                + _seed_rows(valuable, "#22c55e") +
                f'<div style="font-size:12px;font-weight:600;color:#ff6b6b;margin:8px 0 2px;">🔻 摊薄种子（做种人数多，收益低，可考虑送复活区翻红）</div>'
                + _seed_rows(crowded, "#ff6b6b") +
                '</div>')

        refresh_api = f"/api/v1/plugin/RainbowReviveButler/refresh?apikey={apikey}"
        top_bar = (
            '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:4px;">'
            f'<span style="font-size:10.5px;color:rgba(var(--v-theme-on-surface),.4);">'
            f'{self._refresh_note(self._last_overview_ts, 2.0)}</span>'
            f'<a href="{refresh_api}" target="_blank" style="cursor:pointer;color:#42A5F5;text-decoration:none;'
            'font-size:12px;">🔄 立即刷新</a></div>')

        return [{
            "component": "VRow", "props": {"density": "compact", "no-gutters": True},
            "content": [{
                "component": "VCol", "props": {"cols": 12},
                "content": [
                    {"component": "div", "props": {"html": top_bar}},
                    {"component": "div", "props": {"html": self._overview_html(ov)}},
                    {"component": "VAlert", "props": {"type": "info", "variant": "tonal",
                                                      "text": f"最近运行状态：{self._last_result}"}},
                    {"component": "div", "props": {"html": links}},
                    {"component": "div", "props": {"html": portfolio_html}},
                    {"component": "div", "props": {"class": "text-caption",
                                                   "style": "margin:6px 0 2px;color:rgba(var(--v-theme-on-surface),.6);"},
                     "text": "我在复活区的做种（按做种人数稀少优先）"},
                ] + list_block,
            }],
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
            {"component": "div", "html": self._overview_html(ov)},
        ]
        return ({"cols": 12, "md": 6},
                {"refresh": 60, "title": "彩虹岛复活区管家", "border": True},
                elements)

    # ------------------------------------------------------------------ #
    # 工作台（单文件 HTML，移植 HHClubButler 全套交互）
    # ------------------------------------------------------------------ #
    def _append_credit_sample(self, credit: float):
        """记录做种积分采样（供统计表算日增量），上限 500 条、30 天自清。"""
        if not credit:
            return
        try:
            hist = self.get_data("credit_history") or []
            if not isinstance(hist, list):
                hist = []
            now = time.time()
            hist.append({"ts": now, "credit": float(credit)})
            hist = [h for h in hist if isinstance(h, dict) and h.get("ts", 0) > now - 30 * 86400]
            self.save_data("credit_history", hist[-500:])
        except Exception as e:
            logger.warning(f"彩虹岛复活区管家：记录做种积分采样失败：{e}")

    def _fetch_profile(self, session: requests.Session, base: str) -> dict:
        """从档案页解析账户资料：加入日期/最近动向/邀请人/HP/BT时间比率/做种时间/下载时间。"""
        uid = self._get_site_uid(session, base)
        if not uid:
            return {}
        try:
            r = session.get(base + f"/userdetails.php?id={uid}", timeout=25)
            t = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", r.text))

            def g(pat, default=""):
                m = re.search(pat, t)
                return m.group(1).strip() if m else default

            return {
                "uid": uid,
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
            r = s = session.get(base + "/selfassess.php", timeout=25)
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

    def api_stats_page(self):
        """做种积分统计表独立页（照原作者 monthly_stats 深色版式）。"""
        hist = self.get_data("credit_history") or []
        # 按自然日聚合
        days: Dict[str, dict] = {}
        for h in hist:
            if not isinstance(h, dict) or not h.get("credit"):
                continue
            day = datetime.fromtimestamp(h["ts"]).strftime("%Y-%m-%d")
            d = days.setdefault(day, {"first": h["credit"], "last": h["credit"], "n": 0, "max": h["credit"]})
            d["last"] = h["credit"]
            d["max"] = max(d["max"], h["credit"])
            d["n"] += 1
        ordered = sorted(days.items())[-30:]
        cur = hist[-1]["credit"] if hist else 0
        # 日增量
        deltas = []
        for i, (day, d) in enumerate(ordered):
            prev = ordered[i-1][1]["last"] if i > 0 else None
            d["delta"] = (d["last"] - prev) if prev is not None else None
            if d["delta"] is not None:
                deltas.append(d["delta"])
        today_delta = ordered[-1][1].get("delta") if ordered else None
        week_delta = sum(deltas[-7:]) if deltas else 0
        daily_avg = (sum(deltas) / len(deltas)) if deltas else 0

        rows = ""
        for day, d in reversed(ordered):
            delta = d.get("delta")
            delta_html = (f'<span style="color:#2ee6a8">+{delta:,.0f}</span>' if delta and delta > 0
                          else (f'<span style="color:#f87171">{delta:,.0f}</span>' if delta is not None else "—"))
            rows += (f'<tr><td>{day}</td><td class="num">{d["first"]:,.1f}</td>'
                     f'<td class="num">{d["last"]:,.1f}</td><td class="num">{delta_html}</td>'
                     f'<td class="num">{d["n"]}</td></tr>')
        if not rows:
            rows = '<tr><td colspan="5" style="text-align:center;color:#5d6d8f;padding:24px;">暂无采样数据（巡检一次记录一条，次日可见增量）</td></tr>'

        def kpi(label, val, sub="", color="#e8edf7"):
            return (f'<div class="kpi"><div class="kl">{label}</div>'
                    f'<div class="kv" style="color:{color}">{val}</div>'
                    f'<div class="ks">{sub}</div></div>')

        kpis = (kpi("当前做种积分", f"{cur:,.1f}", "实时", "#fbbf24")
                + kpi("今日增量", f"+{today_delta:,.0f}" if today_delta else "—", "较昨日末次", "#2ee6a8" if today_delta else "#7d8db1")
                + kpi("近7日增量", f"+{week_delta:,.0f}", "累计", "#38bdf8")
                + kpi("日均增量", f"{daily_avg:,.0f}", "全部采样日均", "#c084fc"))

        css = ("*{box-sizing:border-box;margin:0;padding:0}"
               'body{background:#0d1321;color:#e8edf7;font-family:"PingFang SC","Microsoft YaHei",sans-serif;font-size:13px;padding:20px}'
               ".hd{font-size:16px;font-weight:700;margin-bottom:2px}.sub{color:#7d8db1;font-size:11px;margin-bottom:14px}"
               ".kpis{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:16px}"
               ".kpi{flex:1;min-width:140px;background:#111c33;border:1px solid #1e3357;border-radius:10px;padding:10px 14px}"
               ".kl{font-size:11px;color:#7d8db1}.kv{font-size:19px;font-weight:700;margin-top:2px}.ks{font-size:10px;color:#5d6d8f}"
               "table{width:100%;border-collapse:collapse;background:#111c33;border:1px solid #1e3357;border-radius:10px;overflow:hidden}"
               "th{background:#16233f;color:#7d8db1;font-weight:600;font-size:11px;padding:7px 12px;text-align:left}"
               "td{padding:6px 12px;border-top:1px solid #1e3357;font-size:12px}"
               ".num{text-align:right;font-variant-numeric:tabular-nums}"
               'a{color:#38bdf8;text-decoration:none}')
        html = (f'<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">'
                f'<meta name="viewport" content="width=device-width,initial-scale=1">'
                f'<title>做种积分统计</title><style>{css}</style></head><body>'
                f'<div class="hd">🌈 做种积分统计</div>'
                f'<div class="sub">数据来自每轮巡检采样 · 采样 {len(hist)} 条 · '
                f'<a href="javascript:location.reload()">🔄 刷新</a></div>'
                f'<div class="kpis">{kpis}</div>'
                f'<table><thead><tr><th>日期</th><th style="text-align:right">首次</th>'
                f'<th style="text-align:right">末次</th><th style="text-align:right">增量</th>'
                f'<th style="text-align:right">采样数</th></tr></thead><tbody>{rows}</tbody></table>'
                f'</body></html>')
        return HTMLResponse(content=html)

    def api_level_page(self):
        """数据与等级独立页（照原作者 level 深色版式，数据为彩虹岛账户资料）。"""
        base = self._get_site_url()
        cookie = self._get_site_cookie()
        prof = {}
        task = {}
        stats = {}
        try:
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

        def kpi(label, val, sub="", color="#e8edf7"):
            return (f'<div class="kpi"><div class="kl">{label}</div>'
                    f'<div class="kv" style="color:{color}">{val}</div>'
                    f'<div class="ks">{sub}</div></div>')

        credit = stats.get("credit", 0) or 0
        rate = stats.get("credit_rate")
        kpis = (kpi("上传量", f"{stats.get('uploaded_tb', 0):.3f}", "TB", "#2ee6a8")
                + kpi("下载量", f"{stats.get('downloaded_tb', 0):.3f}", "TB", "#38bdf8")
                + kpi("分享率", f"{stats.get('ratio', 0):.3f}", "实际", "#fbbf24")
                + kpi("做种积分", f"{credit:,.0f}", f"+{rate:.0f}/h" if rate else "实时", "#fbbf24")
                + kpi("魔力值", f"{stats.get('magic', 0):,.0f}", "余额", "#FF8A3C")
                + kpi("当前做种", f"{stats.get('seeding_now', 0)}", "个", "#2ee6a8")
                + kpi("H&R", f"{stats.get('hr', 0)}", "违规扣点", "#f87171" if stats.get("hr") else "#7d8db1")
                + kpi("HP", prof.get("hp", "—"), "H&R 容量", "#c084fc"))

        meta = (f'UID {prof.get("uid", "—")} · 加入 {prof.get("reg_date", "—")} · '
                f'最近动向 {prof.get("last_active", "—")} · 邀请人 {prof.get("inviter", "—")}')
        times = (f'BT时间比率 {prof.get("bt_ratio", "—")} · '
                 f'做种时间 {prof.get("seed_time", "—")} · 下载时间 {prof.get("dl_time", "—")}')

        # 任务系统全站统计
        task_html = ""
        tiers = task.get("tiers") or {}
        if tiers:
            trows = "".join(
                f'<tr><td>{name}</td><td class="num">{t["done"]:,}</td>'
                f'<td class="num">{t["fail"]:,}</td>'
                f'<td class="num"><span style="color:#2ee6a8">{t["pct"]}%</span></td></tr>'
                for name, t in tiers.items())
            task_html = (
                f'<div class="sec">任务系统（全站当前 {task.get("busy", 0)} 人）</div>'
                f'<table><thead><tr><th>等级</th><th style="text-align:right">完成</th>'
                f'<th style="text-align:right">失败</th><th style="text-align:right">完成率</th>'
                f'</tr></thead><tbody>{trows}</tbody></table>')

        css = ("*{box-sizing:border-box;margin:0;padding:0}"
               'body{background:#0d1321;color:#e8edf7;font-family:"PingFang SC","Microsoft YaHei",sans-serif;font-size:13px;padding:20px}'
               ".hd{font-size:16px;font-weight:700;margin-bottom:2px}.sub{color:#7d8db1;font-size:11px;margin-bottom:14px}"
               ".kpis{display:flex;gap:10px;flex-wrap:wrap;margin-bottom:14px}"
               ".kpi{flex:1;min-width:130px;background:#111c33;border:1px solid #1e3357;border-radius:10px;padding:10px 14px}"
               ".kl{font-size:11px;color:#7d8db1}.kv{font-size:18px;font-weight:700;margin-top:2px}.ks{font-size:10px;color:#5d6d8f}"
               ".sec{font-size:12px;font-weight:600;color:#7d8db1;margin:14px 0 6px}"
               ".meta{background:#111c33;border:1px solid #1e3357;border-radius:10px;padding:10px 14px;"
               "font-size:12px;color:#a8b8d4;line-height:1.9;margin-bottom:4px}"
               "table{width:100%;border-collapse:collapse;background:#111c33;border:1px solid #1e3357;border-radius:10px;overflow:hidden}"
               "th{background:#16233f;color:#7d8db1;font-weight:600;font-size:11px;padding:7px 12px;text-align:left}"
               "td{padding:6px 12px;border-top:1px solid #1e3357;font-size:12px}"
               ".num{text-align:right;font-variant-numeric:tabular-nums}"
               'a{color:#38bdf8;text-decoration:none}')
        html = (f'<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">'
                f'<meta name="viewport" content="width=device-width,initial-scale=1">'
                f'<title>数据与等级</title><style>{css}</style></head><body>'
                f'<div class="hd">📈 数据与等级</div>'
                f'<div class="sub">彩虹岛账户资料 · <a href="javascript:location.reload()">🔄 刷新</a></div>'
                f'<div class="kpis">{kpis}</div>'
                f'<div class="meta">{meta}<br>{times}</div>'
                f'{task_html}'
                f'</body></html>')
        return HTMLResponse(content=html)

    def _build_panel_html(self, apikey: str) -> str:
        """工作台页面：15 主题 / 列显隐 / 列排序 / 列宽拖拽 / 搜索 / 筛选 / 批量删除 / 导出。"""
        return (_PANEL_HTML
                .replace("__AK__", json.dumps(apikey))
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
<button class="btn ghost" id="btnRefresh">刷新</button>
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
</div>
</div>
<div class="table-wrap"><div id="status" class="status"><span>正在加载复活区数据，请稍候…</span><div class="bar"><i></i></div></div><table id="tbl"><thead><tr id="head"></tr></thead><tbody id="tbody"></tbody></table></div>
<div class="footer">
<span>已选 <b id="seln">0</b> 个 / 共 <b id="totaln">0</b> 个</span>
<span>已选体积 <b id="selgb">0</b> GB</span>
<span>总体积 <b id="totalgb">0</b> GB</span>
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
{k:"c_hr",t:"HR",w:"50",sortk:"hr"}
];
var seeds=[],filtered=[],sel={};
var curF="all",sortKey="remain_sec",sortDir=1,query="";
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
function up(ev){document.removeEventListener("mousemove",mv);document.removeEventListener("mouseup",up);document.body.style.cursor='';if(dragMoved&&ev){ev.stopPropagation();ev.preventDefault()}}
document.addEventListener("mousemove",mv);document.addEventListener("mouseup",up)};
hd.ontouchstart=function(e){e.preventDefault();e.stopPropagation();
var th=hd.parentElement,sx=e.touches[0].clientX,sw=th.offsetWidth;dragMoved=false;document.body.style.cursor='col-resize';
function tmv(ev){var dx=ev.touches[0].clientX-sx;if(Math.abs(dx)>1)dragMoved=true;var w=Math.max(40,sw+dx);th.style.width=w+"px";var col=th.dataset.col;if(col)document.querySelectorAll('td[data-col="'+col+'"]').forEach(function(td){td.style.width=w+"px"})}
function tup(ev){document.removeEventListener("touchmove",tmv);document.removeEventListener("touchend",tup);document.body.style.cursor='';if(dragMoved&&ev){ev.stopPropagation();ev.preventDefault()}}
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
return "";
}
function render(){
var f=seeds.filter(function(s){
if(curF==="free"&&!(s.is_free&&!s.expired))return false;
if(curF==="soon"&&!(s.is_free&&!s.expired&&s.remain_sec<=7200))return false;
if(curF==="low"&&!(s.seeders<=3))return false;
if(curF==="mine"&&!s.mine)return false;
if(curF==="expired"&&!s.expired)return false;
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
var r=await fetch("/api/v1/plugin/RainbowReviveButler/data?apikey="+encodeURIComponent(APIKEY),ctl?{signal:ctl.signal}:{});
if(to)clearTimeout(to);
var j=await r.json();
if(!j.success){clearInterval(tmr);showErr(j.message||"数据加载失败");return}
seeds=j.seeds||[];
document.getElementById("runinfo").textContent=j.last_result||"";
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
try{var r=await fetch("/api/v1/plugin/RainbowReviveButler/delete_seed?apikey="+encodeURIComponent(APIKEY)+"&tid="+encodeURIComponent(arr[i].tid),{method:"POST"});var j=await r.json();if(j.success){okN++;delete sel[arr[i].tid]}else{alert("删除失败:"+arr[i].title.slice(0,40)+" -> "+(j.message||""))}}
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
document.getElementById("q").oninput=function(){query=this.value.trim().toLowerCase();render()};
document.querySelectorAll(".topbar .chip").forEach(function(c){c.onclick=function(){document.querySelectorAll(".topbar .chip").forEach(function(x){x.classList.remove("active")});this.classList.add("active");curF=this.dataset.f;render()}});
var cp=document.getElementById("colpicker");
var LS_KEY="rrb_colcfg_v1";
function loadColCfg(){try{var raw=localStorage.getItem(LS_KEY);if(!raw)return;var cfg=JSON.parse(raw);if(cfg&&typeof cfg==="object"){for(var i=0;i<COLS.length;i++){if(cfg[COLS[i].k]!==undefined)COLS[i].visible=cfg[COLS[i].k]}}}catch(e){}}
function saveColCfg(){try{var cfg={};for(var i=0;i<COLS.length;i++)cfg[COLS[i].k]=COLS[i].visible!==false;localStorage.setItem(LS_KEY,JSON.stringify(cfg))}catch(e){}}
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
