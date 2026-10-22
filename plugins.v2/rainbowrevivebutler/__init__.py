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
from bs4 import BeautifulSoup
from fastapi.responses import HTMLResponse

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
      仍限速 100M/s；无结算、无做种时长考核（与憨憨合约制不同）；
    - 7 天免费到期后种子不移出列表，仅失去免费标记；
    - 复活区当前无 H3/H5 HR 标记，但保留解析（站点制度 10 月新实施，防御未来打标）。

    核心处置逻辑：巡检时将下载器中「仍在下载」的任务与复活区列表比对，
    免费到期（+5 分钟缓冲）仍未下完 → HR 种转人工，非 HR 种自动删除任务（可选含文件）；
    到期前 warn_hours 小时预警；预计完不成（按当前速率 ETA 超过剩余免费时长）提前删。
    """

    plugin_name = "彩虹岛复活区管家Q自用版"
    plugin_desc = "基于六个橙子「憨憨保种区管家」(HHClubButler) 架构复刻，致谢原作者 SixOrg。\n复活区全景 / 免费到期处置 / 捡漏推送 / 做种到期提醒"
    plugin_icon = "https://raw.githubusercontent.com/q10710/MoviePilot-Plugins/main/icons/rainbowrevivebutler.png"
    plugin_version = "1.0.0"
    plugin_label = "资源站点"
    plugin_author = "Q"
    author_url = "https://github.com/q10710"
    plugin_config_prefix = "rainbowrevivebutler_"
    plugin_order = 20
    auth_level = 1

    _enabled: bool = False
    _onlyonce: bool = False
    _notify: bool = False
    _cron: str = "*/15 * * * *"
    _site_name: str = "彩虹岛"
    _downloaders: List[str] = []
    _warn_hours: float = 2.0
    _delete_files: bool = True
    _early_delete: bool = True
    _low_seeder_max: int = 3

    _running: bool = False
    _scheduler: Optional[BackgroundScheduler] = None
    _last_result: str = "尚未运行"

    _last_overview: Optional[dict] = None
    _last_overview_ts: float = 0.0
    _overview_thread: Optional[threading.Thread] = None
    _overview_stop: Optional[threading.Event] = None

    # 免费到期后的固定缓冲（秒）：防站点时间戳与本机时钟微小偏差导致边界误删
    _GRACE_SEC: int = 300

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
        self._cron = str(config.get("cron") or "*/15 * * * *")
        self._site_name = str(config.get("site_name") or "彩虹岛")
        dls = config.get("downloaders")
        if isinstance(dls, list):
            self._downloaders = [str(x) for x in dls if str(x).strip()]
        else:
            self._downloaders = []
        try:
            self._warn_hours = max(0.0, float(config.get("warn_hours", 2) or 0))
        except (TypeError, ValueError):
            self._warn_hours = 2.0
        self._delete_files = bool(config.get("delete_files", True))
        self._early_delete = bool(config.get("early_delete", True))
        try:
            self._low_seeder_max = int(config.get("low_seeder_max", 3) or 0)
        except (TypeError, ValueError):
            self._low_seeder_max = 3

        self._start_overview_thread()

        if self._onlyonce:
            self._onlyonce = False
            # update_config 为整段替换语义：读-改-写保留全部字段
            _cfg = self.get_config() or {}
            if not isinstance(_cfg, dict):
                _cfg = {}
            _cfg.update({
                "enabled": self._enabled,
                "onlyonce": False,
                "notify": self._notify,
                "cron": self._cron,
                "site_name": self._site_name,
                "downloaders": self._downloaders,
                "warn_hours": self._warn_hours,
                "delete_files": self._delete_files,
                "early_delete": self._early_delete,
                "low_seeder_max": self._low_seeder_max,
            })
            self.update_config(_cfg)
            try:
                self._scheduler = BackgroundScheduler(timezone=settings.TIMEZONE)
                self._scheduler.add_job(
                    func=self.run, trigger="date",
                    id="RainbowReviveButlerOnce",
                    next_run_time=datetime.now(pytz.timezone(settings.TIMEZONE)) + timedelta(seconds=3))
                if not self._scheduler.running:
                    self._scheduler.start()
            except Exception as e:
                logger.error(f"彩虹岛复活区管家：一次性任务调度失败：{e}")

        logger.info(
            f"彩虹岛复活区管家配置生效：启用={self._enabled} 通知={self._notify} "
            f"巡检={self._cron} 站点={self._site_name} 下载器={self._downloaders} "
            f"预警={self._warn_hours:g}h 删文件={self._delete_files} 提前删={self._early_delete} "
            f"捡漏阈值={self._low_seeder_max}人")

    def get_state(self) -> bool:
        """返回插件启用状态。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """无远程命令。"""
        return []

    def stop_service(self):
        """停止调度器与后台线程。"""
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error(f"彩虹岛复活区管家：停止调度器失败：{e}")
        try:
            if self._overview_stop:
                self._overview_stop.set()
            if self._overview_thread and self._overview_thread.is_alive():
                self._overview_thread.join(timeout=3)
        except Exception as e:
            logger.error(f"彩虹岛复活区管家：停止概况线程失败：{e}")
        finally:
            self._overview_stop = None
            self._overview_thread = None

    def get_service(self) -> List[Dict[str, Any]]:
        """注册巡检定时任务。"""
        if not self.get_state():
            return []
        try:
            trigger = CronTrigger.from_crontab(self._cron)
        except Exception as e:
            logger.error(f"彩虹岛复活区管家：cron 无效（{self._cron}）：{e}，回退 */15")
            trigger = CronTrigger.from_crontab("*/15 * * * *")
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

    def _get_site_uid(self, session: requests.Session, base: str) -> Optional[str]:
        """从首页解析当前用户 UID。"""
        try:
            r = session.get(base + "/", timeout=20)
            m = re.search(r"userdetails\.php\?id=(\d+)", r.text)
            return m.group(1) if m else None
        except Exception as e:
            logger.error(f"彩虹岛复活区管家：解析 UID 失败：{e}")
            return None

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
    def _gb(size_bytes: float) -> float:
        """字节转 GB。"""
        return (size_bytes or 0) / 1024 / 1024 / 1024

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
        for m in re.finditer(r"details\.php\?id=(\d+)", html):
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
                continue  # 页头等非种子行
            seen.add(tid)
            # 标题
            tm = re.search(r'<a title="([^"]+)"\s+href="details\.php\?id=' + tid, row)
            title = tm.group(1).strip() if tm else ""
            # 大小
            size_gb = self._parse_size_gb(row)
            # 做种人数（span red/green）
            sm = re.search(r'<span class="(red|green)">(\d+)</span>', row)
            seeders = int(sm.group(2)) if sm else 0
            # 下载人数
            lm = re.search(r'#leechers">(\d+)</a>', row)
            leechers = int(lm.group(1)) if lm else 0
            # 免费标记与精确到期时间
            is_free = "pro_free" in row
            free_until = None
            if is_free:
                fm = re.search(r"限时\s*<span title=\"([^\"]+)\"", row)
                if fm:
                    free_until = self._parse_ts(fm.group(1))
            # HR 标记（当前复活区无 H3/H5，防御性解析）
            plain = re.sub(r"<[^>]+>", " ", row)
            hr = bool(re.search(r"\bH[35]\b", plain))
            # 存活/发布时间
            bm = re.search(r'<td class="rowfollow nowrap"><span title="(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"', row)
            birth = self._parse_ts(bm.group(1)) if bm else None
            if not title:
                continue
            seeds.append({
                "tid": tid, "title": title, "norm": self._norm_title(title),
                "size_gb": round(size_gb, 2), "seeders": seeders, "leechers": leechers,
                "is_free": is_free, "free_until": free_until, "hr": hr, "birth": birth,
            })
        return seeds

    def _fetch_revive_list(self, session: requests.Session, base: str) -> Tuple[List[dict], int]:
        """拉取复活区全部分页（上限 10 页防失控）。"""
        all_seeds: List[dict] = []
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
        return all_seeds, page + 1

    def _fetch_my_seeding_ids(self, session: requests.Session, base: str) -> set:
        """spstate=1 视图 = 我在做种的复活区种子。"""
        try:
            r = session.get(base + "/renewtorrents.php?spstate=1", timeout=30)
            return set(re.findall(r"details\.php\?id=(\d+)", r.text))
        except Exception as e:
            logger.warning(f"彩虹岛复活区管家：拉取我的做种视图失败：{e}")
            return set()

    # ------------------------------------------------------------------ #
    # 下载器层
    # ------------------------------------------------------------------ #
    def _get_manage_services(self) -> dict:
        """获取配置的下载器服务。"""
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
    # 核心巡检
    # ------------------------------------------------------------------ #
    def run(self):
        """巡检入口：拉列表 → 与下载器比对 → 到期处置/预警/推送。"""
        if self._running:
            logger.info("彩虹岛复活区管家正在运行中，跳过本次触发")
            return
        self._running = True
        logs: List[str] = []
        try:
            self._do_run(logs)
        except Exception as e:
            logger.error(f"彩虹岛复活区管家运行异常：{e}", exc_info=True)
            logs.append(f"运行异常：{e}")
            self._last_result = f"运行失败：{e}"
            if self._notify:
                try:
                    self.post_message(title="【彩虹岛复活区管家】运行异常", text=str(e))
                except Exception:
                    pass
        finally:
            self._running = False

    def _do_run(self, logs: List[str]):
        """巡检主逻辑。"""
        t0 = time.time()
        base = self._get_site_url()
        cookie = self._get_site_cookie()
        if not base or not cookie:
            raise Exception("未找到彩虹岛站点配置或 Cookie（请在站点管理中配置）")
        session = self._session(cookie)

        seeds, pages = self._fetch_revive_list(session, base)
        if not seeds:
            raise Exception("复活区列表为空（Cookie 失效或页面结构变化）")
        my_ids = self._fetch_my_seeding_ids(session, base)
        by_norm: Dict[str, dict] = {}
        for s in seeds:
            by_norm.setdefault(s["norm"], s)
        logs.append(f"复活区 {len(seeds)} 个种子（{pages} 页），我在做种 {len(my_ids)} 个")

        now = time.time()
        warn_sec = self._warn_hours * 3600
        dedupe = self.get_data("notify_dedupe") or {}
        # 清理 30 天前的去重键
        cutoff = now - 30 * 86400
        dedupe = {k: v for k, v in dedupe.items() if isinstance(v, (int, float)) and v > cutoff}

        deleted: List[dict] = []
        hr_skipped: List[dict] = []
        warned: List[dict] = []
        early_deleted: List[dict] = []
        seeding_expiring: List[dict] = []
        deal_seeds: List[dict] = []
        no_downloaders = 0

        # --- 下载器比对与处置 --- #
        services = self._get_manage_services()
        if not services:
            no_downloaders = 1
            logs.append("未配置有效下载器，跳过到期处置")
        for dl_name, sv in services.items():
            try:
                torrents, error = sv.instance.get_torrents()
            except Exception as e:
                logs.append(f"下载器 {dl_name} 查询失败：{e}")
                continue
            if error or torrents is None:
                logs.append(f"下载器 {dl_name} 返回错误，本轮跳过")
                continue
            for t in torrents:
                try:
                    tname = str(self._tf(t, "name", default="") or "")
                    norm = self._norm_title(tname)
                    seed = by_norm.get(norm)
                    if not seed:
                        continue
                    progress = float(self._tf(t, "progress", "percentDone", default=0) or 0)
                    thash = self._thash(t)
                    # 已完成在做种的任务不碰（不吃下载量、纯收益）
                    if progress >= 1.0:
                        continue
                    free_until = seed.get("free_until")
                    if not free_until:
                        continue
                    remain_sec = free_until - now
                    # 到期处置
                    if remain_sec <= -self._GRACE_SEC:
                        if seed.get("hr"):
                            key = f"hr:{seed['tid']}:{int(free_until)}"
                            if key not in dedupe:
                                dedupe[key] = now
                                hr_skipped.append(seed)
                            continue
                        # 删除前复核仍未完成（防最后一刻下完被误删）
                        if not self._recheck_incomplete(sv, thash):
                            logs.append(f"{seed['title'][:40]} 删除前复核已完成/不可判定，跳过")
                            continue
                        ok = self._delete_task(sv, thash, dl_name)
                        entry = {"time": int(now), "tid": seed["tid"], "title": seed["title"],
                                 "size_gb": seed["size_gb"], "progress": round(progress * 100, 1),
                                 "downloader": dl_name, "action": "expired_delete", "ok": ok}
                        self._append_disposal(entry)
                        (deleted if ok else warned).append(seed)
                        logs.append(f"到期删除：{seed['title'][:50]}（进度 {progress*100:.0f}%，{dl_name}）成功={ok}")
                        continue
                    # 到期前预警
                    if 0 < remain_sec <= warn_sec:
                        key = f"warn:{seed['tid']}:{int(free_until)}"
                        if key not in dedupe:
                            dedupe[key] = now
                            warned.append({**seed, "progress_pct": round(progress * 100, 1),
                                           "remain_min": round(remain_sec / 60)})
                        # 预计完不成提前删
                        if self._early_delete and remain_sec <= warn_sec:
                            speed = float(self._tf(t, "dlspeed", "rateDownload", "downloadSpeed", default=0) or 0)
                            size_b = float(self._tf(t, "size", "totalSize", default=0) or 0)
                            if speed > 0 and size_b > 0:
                                eta = size_b * (1 - progress) / speed
                                if eta > remain_sec:
                                    if seed.get("hr"):
                                        continue
                                    if not self._recheck_incomplete(sv, thash):
                                        continue
                                    ok = self._delete_task(sv, thash, dl_name)
                                    entry = {"time": int(now), "tid": seed["tid"], "title": seed["title"],
                                             "size_gb": seed["size_gb"], "progress": round(progress * 100, 1),
                                             "downloader": dl_name, "action": "early_delete", "ok": ok}
                                    self._append_disposal(entry)
                                    (early_deleted if ok else warned).append(seed)
                                    logs.append(f"预计完不成提前删：{seed['title'][:50]}（ETA {eta/60:.0f}min > 剩余 {remain_sec/60:.0f}min）成功={ok}")
                except Exception as e:
                    logger.warning(f"彩虹岛复活区管家：处理任务 {self._tf(t, 'name', default='?')} 出错：{e}")
                    continue

        # --- 做种到期提醒（站点侧信息，不依赖下载器）--- #
        for s in seeds:
            if s["tid"] in my_ids and s.get("free_until"):
                remain = s["free_until"] - now
                if 0 < remain <= warn_sec:
                    key = f"seedexp:{s['tid']}:{int(s['free_until'])}"
                    if key not in dedupe:
                        dedupe[key] = now
                        seeding_expiring.append({**s, "remain_min": round(remain / 60)})

        # --- 捡漏推送：免费中 + 做种人数低 --- #
        if self._low_seeder_max > 0:
            for s in seeds:
                if s["is_free"] and s["seeders"] <= self._low_seeder_max and s.get("free_until"):
                    key = f"deal:{s['tid']}:{int(s['free_until'])}"
                    if key not in dedupe:
                        dedupe[key] = now
                        deal_seeds.append(s)
                    if len(deal_seeds) >= 20:
                        break

        self.save_data("notify_dedupe", dedupe)

        # --- 通知 --- #
        if self._notify:
            self._send_notifications(deleted, hr_skipped, warned, early_deleted,
                                     seeding_expiring, deal_seeds, no_downloaders)

        # --- 概览缓存与结果 --- #
        self._last_overview = {
            "ts": now, "total": len(seeds), "pages": pages,
            "free_active": sum(1 for s in seeds if s["is_free"] and s.get("free_until") and s["free_until"] > now),
            "expired_free": sum(1 for s in seeds if s.get("free_until") and s["free_until"] <= now),
            "my_seeding": len(my_ids), "deleted": len(deleted), "early": len(early_deleted),
            "warned": len(warned), "seeds": seeds, "my_ids": sorted(my_ids),
        }
        self._last_overview_ts = now
        self._last_result = (
            f"{datetime.now().strftime('%m-%d %H:%M')} 巡检完成：复活区 {len(seeds)} 种"
            f"（免费中 {self._last_overview['free_active']}），我的做种 {len(my_ids)}，"
            f"到期删 {len(deleted)}，提前删 {len(early_deleted)}，预警 {len(warned)}，耗时 {time.time()-t0:.1f}s")
        logger.info(f"彩虹岛复活区管家：{self._last_result}")

    def _recheck_incomplete(self, sv, thash: str) -> bool:
        """删除前复核：任务仍存在且未完成才返回 True；查询失败返回 False（本轮不删）。"""
        try:
            ts, err = sv.instance.get_torrents(ids=thash)
            if err or not ts:
                return False
            t = ts[0]
            progress = float(self._tf(t, "progress", "percentDone", default=0) or 0)
            return progress < 1.0
        except Exception:
            return False

    def _delete_task(self, sv, thash: str, dl_name: str) -> bool:
        """删除下载器任务（可选含数据文件），复核不存在才算成功。"""
        try:
            sv.instance.delete_torrents(delete_file=self._delete_files, ids=[thash])
            ts, err = sv.instance.get_torrents(ids=thash)
            if ts and not err:
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

    def _send_notifications(self, deleted, hr_skipped, warned, early_deleted,
                            seeding_expiring, deal_seeds, no_downloaders):
        """汇总通知（逐类一条，不刷屏）。"""
        try:
            if deleted or early_deleted:
                lines = []
                for s in (deleted + early_deleted)[:15]:
                    lines.append(f"  {s['title'][:60]}（{s['size_gb']:.1f}GB）")
                more = len(deleted) + len(early_deleted) - 15
                if more > 0:
                    lines.append(f"  …等共 {len(deleted)+len(early_deleted)} 个")
                self.post_message(
                    title=f"【彩虹岛复活区管家】免费到期已删除 {len(deleted)+len(early_deleted)} 个任务",
                    text=("删除含数据文件" if self._delete_files else "仅删任务")
                    + "，避免转为计费下载：\n" + "\n".join(lines))
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
            if seeding_expiring:
                lines = [f"  {s['title'][:50]}（剩 {s.get('remain_min', '?')} 分钟）"
                         for s in seeding_expiring[:10]]
                self.post_message(
                    title=f"【彩虹岛复活区管家】{len(seeding_expiring)} 个我在做种的复活种免费将结束",
                    text="免费结束后新下载骤减、上传收益下降，去留请自行判断：\n" + "\n".join(lines))
            if deal_seeds:
                lines = [f"  {s['title'][:60]}（{s['size_gb']:.1f}GB，做种 {s['seeders']}）"
                         for s in deal_seeds[:10]]
                more = len(deal_seeds) - 10
                if more > 0:
                    lines.append(f"  …等共 {len(deal_seeds)} 个")
                self.post_message(
                    title=f"【彩虹岛复活区管家】捡漏：{len(deal_seeds)} 个低做种免费种子",
                    text="免费中且做种人数 ≤ {}，ratio 黄金窗口：\n{}".format(
                        self._low_seeder_max, "\n".join(lines)))
            if no_downloaders and (warned or deleted or early_deleted):
                self.post_message(
                    title="【彩虹岛复活区管家】提醒",
                    text="本轮未配置有效下载器，到期处置未执行，请检查插件配置。")
        except Exception as e:
            logger.error(f"彩虹岛复活区管家：发送通知失败：{e}")

    # ------------------------------------------------------------------ #
    # 概况后台刷新（daemon 线程，6h 一次；与巡检相互独立）
    # ------------------------------------------------------------------ #
    def _start_overview_thread(self):
        """启动概况预取线程。"""
        try:
            self._overview_stop = threading.Event()
            self._overview_thread = threading.Thread(
                target=self._overview_loop, daemon=True, name="RainbowReviveButler-Overview")
            self._overview_thread.start()
        except Exception as e:
            logger.error(f"彩虹岛复活区管家：启动概况线程失败：{e}")

    def _overview_loop(self):
        """概况线程主循环：唤醒即刷，最多睡 300s 以便响应停止信号。"""
        while True:
            try:
                if self._overview_stop is None or self._overview_stop.is_set():
                    return
                hours = 6.0
                wait = hours * 3600 - (time.time() - self._last_overview_ts)
                if wait <= 0 and self._enabled:
                    self._refresh_overview()
                    wait = hours * 3600
                self._overview_stop.wait(min(wait, 300.0) if wait > 0 else 300)
            except Exception:
                try:
                    if self._overview_stop is not None:
                        self._overview_stop.wait(300)
                except Exception:
                    return

    def _refresh_overview(self, manual: bool = False):
        """只刷站点列表快照（只读、无处置动作）。"""
        try:
            base = self._get_site_url()
            cookie = self._get_site_cookie()
            if not base or not cookie:
                return
            session = self._session(cookie)
            seeds, pages = self._fetch_revive_list(session, base)
            my_ids = self._fetch_my_seeding_ids(session, base)
            now = time.time()
            self._last_overview = {
                "ts": now, "total": len(seeds), "pages": pages,
                "free_active": sum(1 for s in seeds if s["is_free"] and s.get("free_until") and s["free_until"] > now),
                "expired_free": sum(1 for s in seeds if s.get("free_until") and s["free_until"] <= now),
                "my_seeding": len(my_ids), "deleted": 0, "early": 0, "warned": 0,
                "seeds": seeds, "my_ids": sorted(my_ids),
            }
            self._last_overview_ts = now
            logger.info(f"彩虹岛复活区管家：概况已刷新（复活区 {len(seeds)} 种，我的做种 {len(my_ids)}）")
        except Exception as e:
            logger.warning(f"彩虹岛复活区管家：概况刷新失败（{'手动' if manual else '自动'}）：{e}")

    def _get_overview_cached_or_refresh(self) -> dict:
        """取缓存概况，超过 6h 或缺失时同步刷新。"""
        if self._last_overview and time.time() - self._last_overview_ts < 6 * 3600:
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
            "auth": "apikey", "summary": "立即巡检（含到期处置）",
        }, {
            "path": "/refresh", "endpoint": self.api_refresh, "methods": ["GET"],
            "auth": "apikey", "summary": "只刷概况（无处置动作）",
        }, {
            "path": "/data", "endpoint": self.api_data, "methods": ["GET"],
            "auth": "apikey", "summary": "复活区数据与处置日志",
        }, {
            "path": "/panel", "endpoint": self.api_panel, "methods": ["GET"],
            "auth": "apikey", "summary": "复活区工作台",
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
        """返回概况 + 处置日志（工作台数据源）。"""
        ov = self._get_overview_cached_or_refresh()
        seeds = ov.get("seeds", [])
        my_ids = set(ov.get("my_ids", []))
        now = time.time()
        out = []
        for s in seeds:
            out.append({
                "tid": s["tid"], "title": s["title"], "size_gb": s["size_gb"],
                "seeders": s["seeders"], "leechers": s["leechers"],
                "is_free": s["is_free"],
                "free_until": s.get("free_until"),
                "remain_sec": int((s.get("free_until") or 0) - now),
                "expired": bool(s.get("free_until") and s["free_until"] <= now),
                "hr": s.get("hr", False), "mine": s["tid"] in my_ids,
            })
        out.sort(key=lambda x: (not x["is_free"], x["remain_sec"]))
        return {
            "success": True,
            "overview": {k: ov.get(k) for k in
                         ("ts", "total", "pages", "free_active", "expired_free",
                          "my_seeding", "deleted", "early", "warned")},
            "last_result": self._last_result,
            "seeds": out,
            "disposal_log": list(reversed(self.get_data("disposal_log") or []))[:50],
        }

    def api_panel(self):
        """工作台单文件 HTML 页。"""
        apikey = getattr(settings, "API_TOKEN", "") or ""
        return HTMLResponse(content=self._build_panel_html(apikey))

    # ------------------------------------------------------------------ #
    # 页面（Vuetify JSON）
    # ------------------------------------------------------------------ #
    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """配置表单 + 顶部数据面板。"""
        downloaders = []
        try:
            for config in DownloaderHelper().get_configs().values():
                downloaders.append({"title": config.name, "value": config.name})
        except Exception as e:
            logger.warning(f"彩虹岛复活区管家：获取下载器列表失败：{e}")
        ov = self._last_overview or {}
        if ov:
            stat_line = (f"复活区 {ov.get('total', 0)} 种（免费中 {ov.get('free_active', 0)}"
                         f"/已过期 {ov.get('expired_free', 0)}） · 我在做种 {ov.get('my_seeding', 0)} 个"
                         f" · {datetime.fromtimestamp(ov.get('ts', time.time())).strftime('%H:%M')} 刷新")
        else:
            stat_line = "尚未拉取数据（启用后自动刷新，或点下方立即刷新）"
        panel = {
            "component": "VAlert",
            "props": {"type": "info", "variant": "tonal", "density": "compact",
                      "text": stat_line, "class": "mb-2"},
        }
        form = [
            {
                "component": "VRow",
                "props": {"dense": True},
                "content": [
                    {"component": "VCol", "props": {"cols": 12},
                     "content": [panel]},
                ],
            },
            {
                "component": "VForm",
                "content": [
                    {"component": "VRow", "props": {"dense": True}, "content": [
                        {"component": "VCol", "props": {"cols": 4}, "content": [
                            {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件", "color": "primary"}},
                        ]},
                        {"component": "VCol", "props": {"cols": 4}, "content": [
                            {"component": "VSwitch", "props": {"model": "notify", "label": "发送通知"}},
                        ]},
                        {"component": "VCol", "props": {"cols": 4}, "content": [
                            {"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即运行一次"}},
                        ]},
                    ]},
                    {"component": "VRow", "props": {"dense": True}, "content": [
                        {"component": "VCol", "props": {"cols": 4}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "cron", "label": "巡检周期",
                                "placeholder": "*/15 * * * *（每15分钟）",
                                "hint": "到期处置频率；到期即删要求周期尽量短", "persistent-hint": True}},
                        ]},
                        {"component": "VCol", "props": {"cols": 4}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "site_name", "label": "站点名",
                                "placeholder": "彩虹岛", "hint": "自动匹配站点管理中的彩虹岛（ptchdbits）",
                                "persistent-hint": True}},
                        ]},
                        {"component": "VCol", "props": {"cols": 4}, "content": [
                            {"component": "VSelect", "props": {
                                "model": "downloaders", "label": "管理的下载器",
                                "items": downloaders, "multiple": True, "chips": True,
                                "hint": "巡检这些下载器中的未完成任务", "persistent-hint": True}},
                        ]},
                    ]},
                    {"component": "VRow", "props": {"dense": True}, "content": [
                        {"component": "VCol", "props": {"cols": 3}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "warn_hours", "label": "到期前预警(小时)",
                                "type": "number"}},
                        ]},
                        {"component": "VCol", "props": {"cols": 3}, "content": [
                            {"component": "VSwitch", "props": {
                                "model": "delete_files", "label": "删除时含数据文件",
                                "hint": "关闭=仅删任务保留文件", "persistent-hint": True}},
                        ]},
                        {"component": "VCol", "props": {"cols": 3}, "content": [
                            {"component": "VSwitch", "props": {
                                "model": "early_delete", "label": "预计完不成提前删",
                                "hint": "按当前速率 ETA 超过剩余免费时长即提前删", "persistent-hint": True}},
                        ]},
                        {"component": "VCol", "props": {"cols": 3}, "content": [
                            {"component": "VTextField", "props": {
                                "model": "low_seeder_max", "label": "捡漏做种阈值(人)",
                                "type": "number", "hint": "免费且做种≤N 推送；0=关闭", "persistent-hint": True}},
                        ]},
                    ]},
                ],
            },
        ]
        defaults = {
            "enabled": False, "onlyonce": False, "notify": True,
            "cron": "*/15 * * * *", "site_name": "彩虹岛", "downloaders": [],
            "warn_hours": 2, "delete_files": True, "early_delete": True,
            "low_seeder_max": 3,
        }
        return form, defaults

    def _overview_html(self) -> str:
        """概况 HTML（get_page 与 dashboard 复用）。"""
        ov = self._last_overview or {}
        ts = ov.get("ts")
        ts_s = datetime.fromtimestamp(ts).strftime("%m-%d %H:%M") if ts else "-"
        rows = "".join(
            f"<tr><td>{s['seeders']}</td><td>{s['leechers']}</td>"
            f"<td>{s['size_gb']:.1f}</td>"
            f"<td>{'免费中' if (s['is_free'] and s.get('free_until') and s['free_until'] > time.time()) else ('已过期' if s.get('free_until') else '-')}</td>"
            f"<td style='text-align:left;padding-left:8px;'>{'🟢' if s['tid'] in set(ov.get('my_ids', [])) else ''} {s['title'][:70]}</td></tr>"
            for s in (ov.get("seeds") or [])[:30])
        empty = "<tr><td colspan='5' style='text-align:center;color:#888;'>暂无数据，启用插件后自动拉取</td></tr>" if not rows else ""
        return (
            '<div style="padding:8px 4px;font-size:13px;">'
            f'<div style="margin-bottom:8px;">复活区 <b>{ov.get("total", 0)}</b> 种'
            f'（免费中 <b style="color:#10b981">{ov.get("free_active", 0)}</b>'
            f' / 已过期 <b style="color:#f59e0b">{ov.get("expired_free", 0)}</b>） · '
            f'我在做种 <b style="color:#6366f1">{ov.get("my_seeding", 0)}</b> 个 · '
            f'最后刷新 {ts_s}</div>'
            '<table style="width:100%;border-collapse:collapse;font-size:12px;">'
            '<tr style="border-bottom:1px solid rgba(var(--v-theme-on-surface),.15);">'
            '<th style="width:50px;">做种</th><th style="width:50px;">下载</th>'
            '<th style="width:70px;">大小(GB)</th><th style="width:70px;">免费</th>'
            '<th style="text-align:left;">种子名称</th></tr>'
            f'{rows}{empty}</table>'
            '<div style="margin-top:8px;color:#888;font-size:11px;">'
            f'{self._last_result}</div></div>')

    def get_page(self) -> Optional[List[dict]]:
        """插件详情页：概况 + 工作台入口。"""
        apikey = getattr(settings, "API_TOKEN", "") or ""
        panel_url = f"/api/v1/plugin/RainbowReviveButler/panel?apikey={apikey}"
        html = (
            self._overview_html()
            + '<div style="padding:10px 0;">'
            f'<a href="{panel_url}" target="_blank" style="display:inline-block;padding:7px 18px;'
            'background:rgba(99,102,241,.12);color:#6366f1;text-decoration:none;border-radius:4px;'
            'font-size:13px;border:1px solid rgba(99,102,241,.3);">🌈 打开复活区工作台</a></div>')
        return [{"component": "div", "props": {"html": html}}]

    def get_dashboard_meta(self) -> Optional[List[Dict[str, str]]]:
        """仪表盘元数据。"""
        return [{"key": "revive", "name": "彩虹岛复活区管家"}]

    def get_dashboard(self, key: str, **kwargs):
        """首页仪表盘：概况摘要。"""
        if key and key != "revive":
            return None
        try:
            ov = self._get_overview_cached_or_refresh()
        except Exception as e:
            logger.error(f"彩虹岛复活区管家：仪表盘概况获取失败：{e}")
            ov = {}
        return [{
            "component": "div",
            "props": {"html": (
                '<div style="font-size:12px;line-height:1.8;">'
                f'复活区 <b>{ov.get("total", 0)}</b> 种 · 免费中 <b style="color:#10b981">{ov.get("free_active", 0)}</b>'
                f' · 已过期 <b style="color:#f59e0b">{ov.get("expired_free", 0)}</b>'
                f'<br/>我在做种 <b style="color:#6366f1">{ov.get("my_seeding", 0)}</b> 个'
                f'<br/><span style="color:#888;">{self._last_result}</span></div>')},
        }]

    # ------------------------------------------------------------------ #
    # 工作台（单文件 HTML）
    # ------------------------------------------------------------------ #
    def _build_panel_html(self, apikey: str) -> str:
        """工作台页面：列表 + 筛选 + 处置日志 + 刷新。"""
        ak = json.dumps(apikey)
        return r"""<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>彩虹岛复活区管家</title><style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:"Segoe UI","PingFang SC",Arial,sans-serif;background:#0f1115;color:#e6e8ee;font-size:12px;padding:14px}
h1{font-size:15px;margin-bottom:10px}
h1 .sub{font-size:11px;color:#8b90a0;font-weight:400;margin-left:8px}
.stats{display:flex;gap:10px;margin-bottom:10px;flex-wrap:wrap}
.stat{background:#171a21;border:1px solid #262a35;border-radius:6px;padding:8px 14px;min-width:90px}
.stat b{display:block;font-size:17px;margin-top:2px}
.g{color:#10b981}.o{color:#f59e0b}.p{color:#6366f1}.r{color:#ef4444}
.toolbar{display:flex;gap:6px;margin-bottom:8px;align-items:center;flex-wrap:wrap}
.chip{padding:3px 12px;border-radius:12px;background:#1d212b;cursor:pointer;border:1px solid #2a2f3d;font-size:11px}
.chip.on{background:#6366f1;border-color:#6366f1;color:#fff}
button{padding:4px 14px;border-radius:5px;border:1px solid #2a2f3d;background:#1d212b;color:#e6e8ee;cursor:pointer;font-size:12px}
button:hover{background:#262a35}
table{width:100%;border-collapse:collapse;font-size:12px}
th{position:sticky;top:0;background:#171a21;padding:6px 8px;text-align:left;border-bottom:1px solid #2a2f3d;color:#8b90a0;font-weight:500}
td{padding:5px 8px;border-bottom:1px solid #1d212b;vertical-align:top}
tr.mine td{background:rgba(99,102,241,.07)}
.title{word-break:break-all;max-width:520px}
.pill{display:inline-block;padding:1px 8px;border-radius:10px;font-size:10px}
.free{background:rgba(16,185,129,.15);color:#10b981}
.expired{background:rgba(245,158,11,.15);color:#f59e0b}
.dead{background:rgba(139,144,160,.15);color:#8b90a0}
.hrp{background:rgba(239,68,68,.15);color:#ef4444}
.minep{background:rgba(99,102,241,.15);color:#6366f1}
#wrap{max-height:62vh;overflow:auto;border:1px solid #262a35;border-radius:6px}
#logwrap{margin-top:12px;max-height:22vh;overflow:auto;border:1px solid #262a35;border-radius:6px}
#status{color:#8b90a0;font-size:11px;margin:6px 0}
</style></head><body>
<h1>🌈 彩虹岛复活区管家<span class="sub">基于憨憨保种区管家 HHClubButler 架构 · 致谢 SixOrg</span></h1>
<div class="stats" id="stats">加载中…</div>
<div class="toolbar">
  <span class="chip on" data-f="all">全部</span>
  <span class="chip" data-f="free">免费中</span>
  <span class="chip" data-f="soon">即将到期</span>
  <span class="chip" data-f="mine">我做种</span>
  <span class="chip" data-f="expired">已过期</span>
  <span style="flex:1"></span>
  <button onclick="refresh()">刷新数据</button>
</div>
<div id="status"></div>
<div id="wrap"><table><thead><tr>
<th>做种</th><th>下载</th><th>大小(GB)</th><th>免费状态</th><th>种子名称</th>
</tr></thead><tbody id="tbody"><tr><td colspan="5" style="text-align:center;color:#888">加载中…</td></tr></tbody></table></div>
<h1 style="margin-top:12px;font-size:13px;">处置日志</h1>
<div id="logwrap"><table><thead><tr><th>时间</th><th>动作</th><th>进度%</th><th>下载器</th><th>种子</th></tr></thead>
<tbody id="logbody"><tr><td colspan="5" style="text-align:center;color:#888">无</td></tr></tbody></table></div>
<script>
var AK=__AK__;var DATA=null;var FILTER="all";
function fmtT(sec){if(sec<=0)return"已过期";var d=Math.floor(sec/86400),h=Math.floor(sec%86400/3600),m=Math.floor(sec%3600/60);return(d?d+"天":"")+h+"小时"+m+"分"}
function fmtTs(ts){if(!ts)return"-";var d=new Date(ts*1000);return(d.getMonth()+1)+"-"+d.getDate()+" "+String(d.getHours()).padStart(2,"0")+":"+String(d.getMinutes()).padStart(2,"0")}
function esc(s){var d=document.createElement("div");d.textContent=s;return d.innerHTML}
function render(){
 if(!DATA)return;
 var ov=DATA.overview||{};
 document.getElementById("stats").innerHTML=
  '<div class="stat">复活区总数<b>'+ov.total+'</b></div>'+
  '<div class="stat">免费中<b class="g">'+ov.free_active+'</b></div>'+
  '<div class="stat">已过期<b class="o">'+ov.expired_free+'</b></div>'+
  '<div class="stat">我做种<b class="p">'+ov.my_seeding+'</b></div>'+
  '<div class="stat">今日删除<b class="r">'+((ov.deleted||0)+(ov.early||0))+'</b></div>';
 document.getElementById("status").textContent=DATA.last_result||"";
 var h="";
 (DATA.seeds||[]).forEach(function(s){
  if(FILTER==="free"&&!(s.is_free&&!s.expired))return;
  if(FILTER==="soon"&&!(s.is_free&&!s.expired&&s.remain_sec<=7200))return;
  if(FILTER==="mine"&&!s.mine)return;
  if(FILTER==="expired"&&!s.expired)return;
  var pill=s.expired?'<span class="pill expired">已过期</span>':
    (s.is_free?'<span class="pill free">免费 '+fmtT(s.remain_sec)+'</span>':'<span class="pill dead">-</span>');
  h+='<tr class="'+(s.mine?"mine":"")+'"><td>'+s.seeders+'</td><td>'+s.leechers+'</td><td>'+s.size_gb+'</td>'+
     '<td>'+pill+(s.hr?' <span class="pill hrp">HR</span>':'')+(s.mine?' <span class="pill minep">做种中</span>':'')+'</td>'+
     '<td class="title">'+esc(s.title)+'</td></tr>';
 });
 document.getElementById("tbody").innerHTML=h||'<tr><td colspan="5" style="text-align:center;color:#888">无匹配</td></tr>';
 var lh="";
 (DATA.disposal_log||[]).forEach(function(l){
  var act=l.action==="early_delete"?"提前删":"到期删";
  lh+='<tr><td>'+new Date(l.time*1000).toLocaleString()+'</td><td>'+(l.ok?'<span class="pill free">'+act+'</span>':'<span class="pill hrp">'+act+'失败</span>')+
      '</td><td>'+l.progress+'</td><td>'+esc(l.downloader||"")+'</td><td class="title">'+esc((l.title||"").slice(0,80))+'</td></tr>';
 });
 document.getElementById("logbody").innerHTML=lh||'<tr><td colspan="5" style="text-align:center;color:#888">无</td></tr>';
}
function refresh(){
 document.getElementById("status").textContent="加载中…";
 fetch("/api/v1/plugin/RainbowReviveButler/data?apikey="+encodeURIComponent(AK))
  .then(function(r){return r.json()}).then(function(d){DATA=d;render()}).catch(function(e){document.getElementById("status").textContent="加载失败:"+e});
}
document.querySelectorAll(".chip").forEach(function(c){c.onclick=function(){
 document.querySelectorAll(".chip").forEach(function(x){x.classList.remove("on")});
 c.classList.add("on");FILTER=c.dataset.f;render()}});
refresh();setInterval(refresh,120000);
</script></body></html>""".replace("__AK__", ak)
