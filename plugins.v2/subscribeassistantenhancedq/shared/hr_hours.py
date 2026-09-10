"""站点 H&R 时长自动抓取。

用站点域名 + 已保存 Cookie 请求常见 H&R 页面（myhr.php / hr.php / rules.php），
只在包含 H&R / Hit and Run 关键词的文本片段中提取要求做种小时数；
抓不到返回 None，由调用方回退到配置值或默认值。
"""

import re
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

import requests

from app.sdk.config import settings
from app.sdk.logging import logger
from app.sdk.network import SitesHelper

requests.packages.urllib3.disable_warnings()

# H&R 关键词与小时数匹配（仅在这些片段里取值，避免误读规则页其它时长）
HR_MARK = re.compile(r"H&?R|Hit\s*and\s*Run|H\s*&\s*R", re.IGNORECASE)
HOURS = re.compile(r"(\d+(?:\.\d+)?)\s*(?:小时|hours?)", re.IGNORECASE)
PATHS = ("/myhr.php", "/hr.php", "/rules.php")


class SiteHrHoursScanner:
    """抓取各站点要求的 H&R 做种时长（小时）。"""

    def scan(self, site_names: Optional[List[str]] = None) -> Dict[str, float]:
        """扫描启用站点，返回 {站点名: 小时}；未抓到的站点不出现在结果里。"""
        result: Dict[str, float] = {}
        indexers = self._load_sites()
        logger.info(f"站点 H&R 时长抓取：待扫描站点 {len(indexers)} 个")
        skipped = 0
        targets: List[dict] = []
        for site in indexers:
            name = str(site.get("name") or "").strip()
            if not name or (site_names and name not in site_names):
                continue
            if site.get("public") or not site.get("is_active", True):
                continue
            if not (str(site.get("domain") or "").strip() and str(site.get("cookie") or "").strip()):
                skipped += 1
                continue
            targets.append(site)
        if skipped:
            logger.info(f"站点 H&R 时长抓取：{skipped} 个站点缺少域名或 Cookie，已跳过")
        if not targets:
            return result
        # 并发抓取，避免逐站串行导致一轮十几分钟
        with ThreadPoolExecutor(max_workers=5) as executor:
            for site, hours in zip(targets, executor.map(self.fetch, targets)):
                name = str(site.get("name") or "").strip()
                if hours and name:
                    result[name] = hours
        return result

    @staticmethod
    def _load_sites() -> List[dict]:
        """读取站点配置：优先直接读库（字段最完整），失败时回退 SitesHelper。"""
        sites: List[dict] = []
        try:
            from app.db.oper.site import SiteOper

            for row in SiteOper().list() or []:
                sites.append({
                    "name": getattr(row, "name", None),
                    "domain": getattr(row, "domain", None),
                    "cookie": getattr(row, "cookie", None),
                    "ua": getattr(row, "ua", None),
                    "public": getattr(row, "public", None),
                    "is_active": getattr(row, "is_active", None),
                    "proxy": getattr(row, "proxy", None),
                })
        except Exception as err:
            logger.warning(f"站点 H&R 时长抓取：直接读库失败（{err}），改为读取站点索引")
        if sites:
            return sites
        try:
            sites = SitesHelper().get_indexers() or []
        except Exception as err:
            logger.error(f"站点 H&R 时长抓取：读取站点列表失败：{err}")
        return sites

    def fetch(self, site: dict) -> Optional[float]:
        """抓取单个站点的 H&R 时长；抓不到返回 None。"""
        domain = str(site.get("domain") or "").strip()
        cookie = str(site.get("cookie") or "").strip()
        if not domain or not cookie:
            return None
        headers = {"Cookie": cookie}
        ua = site.get("ua")
        if ua:
            headers["User-Agent"] = str(ua)
        # 站点标记使用代理且系统配置了代理时才走代理，其余直连（直连更稳，避免代理侧 403）
        proxy_host = str(getattr(settings, "PROXY_HOST", "") or "").strip()
        proxies = {"http": proxy_host, "https": proxy_host} if (site.get("proxy") and proxy_host) else None
        for path in PATHS:
            for scheme in ("https", "http"):
                url = f"{scheme}://{domain}{path}"
                try:
                    resp = requests.get(url, headers=headers, timeout=12, verify=False,
                                        proxies=proxies, allow_redirects=True)
                except Exception:
                    continue
                if not resp or resp.status_code != 200 or not resp.text:
                    continue
                hours = self.parse(resp.text)
                if hours:
                    logger.info(f"站点 H&R 时长抓取：{site.get('name')} → {hours:g} 小时（{url}）")
                    return hours
        return None

    @staticmethod
    def parse(content: str) -> Optional[float]:
        """从页面内容提取 H&R 小时数（仅取含 H&R 关键词的片段，返回其中最大值）。"""
        text = re.sub(r"<script.*?</script>", " ", content, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r"<style.*?</style>", " ", text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r"<[^>]+>", "\n", text)
        text = text.replace("&nbsp;", " ").replace("&#160;", " ")
        values = []
        for segment in re.split(r"[\n\r。;；]+", text):
            if not HR_MARK.search(segment):
                continue
            for match in HOURS.finditer(segment):
                try:
                    value = float(match.group(1))
                except Exception:
                    continue
                if 1 <= value <= 720:
                    values.append(value)
        return max(values) if values else None
