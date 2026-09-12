"""种子删除后的统一善后编排。"""
import time
from typing import Callable, Optional

from app.chain.subscribe import SubscribeChain
from app.schemas.types import MediaType

from ..engine.types import PriorityManagerProtocol
from ..shared.log import detail
from ..shared.subscribe import format_subscribe, resolve_subscribe_media_type
from ..shared.update import update_subscribe

# 收容回调的返回值：relocated=已收容；restored=已从收容目录搬回影视目录；
# kept_final=已达收容往返次数上限，保留在影视目录并停手；
# retained=H&R 种子收容失败（保留不删）；skip=未收容，走原删除逻辑
RELOCATE_RELOCATED = "relocated"
RELOCATE_RESTORED = "restored"
RELOCATE_KEPT_FINAL = "kept_final"
RELOCATE_FINAL_KEEP = "final_keep"
RELOCATE_RETAINED = "retained"
RELOCATE_SKIP = "skip"


class TorrentCleanup:
    """种子删除统一编排：归档删除指纹 → 删种 → 回滚优先级 → 清任务 → 补搜。

    外部副作用通过注入回调执行，避免本模块直接绑定下载器、搜索或文件系统实现。
    """

    # 收容失败通知的限频窗口：同一 hash 在该秒数内只提醒一次
    RETAIN_NOTIFY_INTERVAL_SECONDS = 6 * 3600
    # 收容失败通知记录的保留窗口：写入时顺带清理更早的旧键，避免数据无限增长
    RETAIN_NOTIFY_RETENTION_SECONDS = 7 * 24 * 3600

    def __init__(self, priority_manager: PriorityManagerProtocol,
                 clear_download_pending_fn: Callable,
                 task_data_update: Callable,
                 task_data_read: Optional[Callable] = None,
                 deletes_store=None,
                 delete_torrent_fn: Optional[Callable] = None,
                 relocate_torrent_fn: Optional[Callable] = None,
                 search_fn: Optional[Callable] = None,
                 notify_fn: Optional[Callable] = None,
                 get_subscribe_image_fn: Optional[Callable] = None,
                 subscribe_oper=None):
        """注入删种、任务清理、补搜和通知依赖。"""
        self._priority = priority_manager
        self._clear_pending = clear_download_pending_fn
        self._update = task_data_update
        self._read = task_data_read
        self._deletes = deletes_store
        self._delete_torrent = delete_torrent_fn
        self._relocate_torrent = relocate_torrent_fn
        self._search = search_fn
        self._notify = notify_fn
        self._get_subscribe_image = get_subscribe_image_fn
        self._subscribe_oper = subscribe_oper

    def handle_torrent_deleted(self, subscribe, torrent_hash: str,
                                reason: str = "download_timeout",
                                reason_detail: Optional[str] = None,
                                downloader: Optional[str] = None,
                                delete_from_downloader: bool = True,
                                search_enabled: bool = True):
        """种子删除后的统一处理，步骤顺序固定，避免中途失败留下不一致状态。

        delete_from_downloader：仅下载器主动删种（timeout/tracker）为 True；手动删除时种子已不在，
        传 False 跳过删种。删除指纹负责防止同一坏种被立即重选，订阅继续保持可搜索状态。
        收容回调返回 retained 时只保留种子（H&R 收容失败），不执行删除，等下一轮重试。
        """
        sid = subscribe.id
        # 收容、搬回影视目录、保留停手与删除要给出不同通知文案，避免用户误以为种子被删除
        relocated = False
        retained = False
        restored = False
        kept_final = False
        detail(
            f"种子删除处理：{format_subscribe(subscribe)} 开始处理 hash={torrent_hash}"
            f"（reason={reason}, delete_from_downloader={delete_from_downloader}）"
        )

        # 1. 读取删除前任务记录，并先尝试收容再决定删除：
        #    H&R 种子收容失败（retained）时直接保留返回，不写删除指纹、不回滚订阅缺失状态，
        #    避免种子仍在下载做种时订阅误判为缺集而重复补搜。
        torrent_task = self._read_torrent_task(torrent_hash)
        outcome = ""
        if (delete_from_downloader and downloader and torrent_hash
                and reason == "timeout" and self._relocate_torrent):
            outcome = self._relocate_torrent(downloader, torrent_hash, subscribe, torrent_task) or ""
        if outcome == RELOCATE_RELOCATED:
            relocated = True
        elif outcome == RELOCATE_RESTORED:
            restored = True
        elif outcome == RELOCATE_KEPT_FINAL:
            kept_final = True
        elif outcome == RELOCATE_FINAL_KEEP:
            # 已达收容往返上限且已保留在影视目录：静默保留，不再写指纹、不回滚、不补搜、不通知
            detail(f"种子删除处理：{torrent_hash} 已达收容往返上限，保留在影视目录，本轮静默跳过")
            return
        elif outcome == RELOCATE_RETAINED:
            retained = True

        if retained:
            detail(f"种子删除处理：{torrent_hash} 为 H&R 种子但收容未成功，"
                   f"本轮保留种子并等待下一轮重试")
            if self._should_notify_retained(torrent_hash):
                self._notify(
                    f"{format_subscribe(subscribe)} H&R 种子收容未成功，本轮已保留种子",
                    "收容目录或下载器暂不可用，种子保留继续做种，下一轮巡检会再次尝试收容",
                    image=self._subscribe_image(subscribe),
                    diagnostic=True,
                )
            return

        # 2. 清 torrents 任务前归档删除指纹，供 ResourceSelection 防止坏种立即重选。
        if self._deletes and torrent_task:
            detail(f"种子删除处理：已记录种子 {torrent_hash}，避免后续被重新选中")
            self._deletes.save(torrent_task, reason=reason)

        # 删种后先恢复下载事实，再交给主程序按当前合同刷新订阅进度。
        self._restore_subscribe_missing_state(subscribe, torrent_task)

        # 3. 下载器主动删除场景处理种子；用户手动删除场景种子已不存在。
        #    Q 版改造：超时（timeout）不再直接删除，优先收容到独立目录保留做种，
        #    已收容的种子留在收容目录继续做种，只有未收容的普通种子才按原逻辑删除。
        #    收容往返（restored / kept_final）同样不删除：种子同一份，只改保存目录，
        #    保证它有机会下完并被正常整理入库，同时 H&R 做种不中断。
        if (delete_from_downloader and downloader and torrent_hash
                and not relocated and not restored and not kept_final and self._delete_torrent):
            self._delete_torrent(downloader, torrent_hash)

        # 4. 洗版按 enclosure 归属回滚，隔离并行洗版；旧数据无归属时退回整体基线。
        if subscribe.best_version:
            enclosure = (torrent_task or {}).get("enclosure")
            if enclosure:
                detail(f"种子删除处理：{format_subscribe(subscribe)} 恢复本次洗版下载对应集数的优先级")
                self._priority.rollback_torrent(subscribe, enclosure)
            else:
                detail(f"种子删除处理：{format_subscribe(subscribe)} 无法确认对应集数，恢复整体洗版优先级")
                self._priority.rollback(subscribe, baseline=None)

        # 5. 清理种子任务与下载待定，避免订阅长期保持下载中。
        self._clean_torrent_task(torrent_hash)
        self._clean_subscribe_torrent_task(sid, torrent_hash)
        self._clear_pending(sid, torrent_hash)

        # 6. 按配置触发补搜，避免删种后长期缺集。
        search_delay_seconds = None
        if search_enabled and self._search and subscribe:
            search_delay_seconds = self._search(subscribe)
            if not isinstance(search_delay_seconds, (int, float)):
                search_delay_seconds = None
        self._notify_deleted(
            subscribe, torrent_task, reason,
            reason_detail=reason_detail,
            search_delay_seconds=search_delay_seconds,
            relocated=relocated,
            retained=retained,
            restored=restored,
            kept_final=kept_final,
        )

    def handle_timeout_manual_review(self, subscribe, torrent_hash: str,
                                     reason_detail: str, ignore_hours: int = 48):
        """连续低进度达到保护上限时保留种子，并通知用户人工判断。"""
        self._notify_manual_review(subscribe, torrent_hash, reason_detail, ignore_hours)

    def handle_timeout_limit(self, subscribe, torrent_hash: str, reason_detail: str,
                             ignore_hours: int = 24, downloader: Optional[str] = None):
        """连续低进度达到上限时的处置入口：H&R 种子交给收容往返链路，其余沿用原有「保留 + 通知」。

        收容往返由回调决定本次动作（收容 / 搬回影视目录 / 达上限保留停手），
        因此配置的次数对 H&R 种子按「往返轮次」精确生效，不再受低进度计数窗口影响；
        非 H&R 种子或收容不可用时回退到原有行为，保证普通种子的处置不变。
        """
        torrent_task = self._read_torrent_task(torrent_hash) or {}
        outcome = ""
        if self._relocate_torrent and downloader and torrent_hash:
            outcome = self._relocate_torrent(downloader, torrent_hash, subscribe, torrent_task) or ""
        if outcome == RELOCATE_FINAL_KEEP:
            detail(f"种子删除处理：{torrent_hash} 已达收容往返上限并保留停手，本轮静默跳过")
            return
        if outcome in (RELOCATE_RELOCATED, RELOCATE_RESTORED, RELOCATE_KEPT_FINAL):
            # 收容往返链路已完成搬动：按本次动作发送通知，不重复走「保留」文案
            self._notify_deleted(
                subscribe, torrent_task, "timeout",
                reason_detail=reason_detail,
                relocated=outcome == RELOCATE_RELOCATED,
                restored=outcome == RELOCATE_RESTORED,
                kept_final=outcome == RELOCATE_KEPT_FINAL,
            )
            return
        if outcome == RELOCATE_RETAINED:
            if self._should_notify_retained(torrent_hash):
                self._notify(
                    f"{format_subscribe(subscribe)} H&R 种子收容未成功，本轮已保留种子",
                    "收容目录或下载器暂不可用，种子保留继续做种，下一轮巡检会再次尝试收容",
                    image=self._subscribe_image(subscribe),
                    diagnostic=True,
                )
            return
        # 非 H&R 种子或收容未启用：维持原有的「保留种子 + 通知 + 保护期」
        self._notify_manual_review(subscribe, torrent_hash, reason_detail, ignore_hours)

    def _notify_manual_review(self, subscribe, torrent_hash: str,
                              reason_detail: str, ignore_hours: int = 48):
        """原有「保留种子并通知人工判断」文案。"""
        if not self._notify:
            return
        torrent_task = self._read_torrent_task(torrent_hash) or {}
        detail_parts = []
        if torrent_task.get("title"):
            detail_parts.append(f"标题：{torrent_task.get('title')}")
        if torrent_task.get("description"):
            detail_parts.append(f"内容：{torrent_task.get('description')}")
        action = f"已保留当前种子，{ignore_hours} 小时内不再自动删除"
        detail(f"种子删除处理：{format_subscribe(subscribe)} 原因={reason_detail}，处理={action}，后续=请手动判断")
        self._notify(
            f"{format_subscribe(subscribe)} {self._manual_review_title_reason(reason_detail)}，{action}",
            "\n".join(detail_parts) if detail_parts else None,
            image=self._subscribe_image(subscribe),
            follow_up="请手动判断",
            diagnostic=True,
        )

    @staticmethod
    def _manual_review_title_reason(reason_detail: str) -> str:
        """将低进度诊断改写为通知标题语序，保留下载时长与进度判断信息。"""
        prefix = "订阅种子，"
        if reason_detail.startswith(prefix):
            return f"{prefix}下载连续超时，{reason_detail[len(prefix):]}"
        return f"下载连续超时，{reason_detail}"

    def _should_notify_retained(self, torrent_hash: str) -> bool:
        """收容失败通知限频：同一 hash 6 小时内只提醒一次，避免每轮巡检重复推送。"""
        if not self._read or not self._update or not torrent_hash:
            return True
        try:
            last = float((self._read("relocate_retain_notify") or {}).get(torrent_hash) or 0)
        except Exception:
            last = 0
        now = time.time()
        if last and now - last < self.RETAIN_NOTIFY_INTERVAL_SECONDS:
            return False

        def updater(data: dict) -> dict:
            data = dict(data or {})
            # 顺带清掉超过保留窗口的旧键，避免此数据随种子数量长期膨胀
            for key, value in list(data.items()):
                try:
                    expired = now - float(value or 0) > self.RETAIN_NOTIFY_RETENTION_SECONDS
                except Exception:
                    expired = True
                if expired:
                    data.pop(key, None)
            data[torrent_hash] = now
            return data

        self._update("relocate_retain_notify", updater)
        return True

    def _read_torrent_task(self, torrent_hash: str) -> Optional[dict]:
        """删除前读取种子任务，供删除指纹归档与按集基线回滚。"""
        if not self._read or not torrent_hash:
            return None
        return (self._read("torrents") or {}).get(torrent_hash)

    def _restore_subscribe_missing_state(self, subscribe, torrent_task: Optional[dict]):
        """删种善后恢复下载事实，确保后续补搜能覆盖被删集。"""
        if not self._subscribe_oper or not subscribe or not torrent_task:
            return
        media_type = resolve_subscribe_media_type(subscribe)
        payload = {}
        if media_type == MediaType.TV:
            note = list(subscribe.note or [])
            episodes = torrent_task.get("episodes") or []
            episode_set = set(episodes if isinstance(episodes, list) else [episodes])
            kept_note = [episode for episode in note if episode not in episode_set]
            payload["note"] = kept_note
        elif media_type == MediaType.MOVIE:
            payload["note"] = []
        if payload:
            update_subscribe(self._subscribe_oper, subscribe.id, payload)
            for key, value in payload.items():
                setattr(subscribe, key, value)
            if media_type == MediaType.TV:
                SubscribeChain().refresh_subscribe_progress(subscribe, scene="plugin_delete_rollback")

    def _clean_torrent_task(self, torrent_hash: str):
        """清理种子任务数据。"""
        def updater(data: dict) -> dict:
            data.pop(torrent_hash, None)
            return data
        self._update("torrents", updater)

    def _clean_subscribe_torrent_task(self, subscribe_id: int, torrent_hash: str):
        """同步清理订阅内 torrent_tasks，兼容订阅级种子记录。"""
        sid = str(subscribe_id)

        def updater(data: dict) -> dict:
            task = data.get(sid, {})
            torrent_tasks = task.get("torrent_tasks")
            if torrent_tasks:
                task["torrent_tasks"] = [
                    item for item in torrent_tasks
                    if item.get("hash") != torrent_hash
                ]
            data[sid] = task
            return data

        self._update("subscribes", updater)

    def _notify_deleted(self, subscribe, torrent_task: Optional[dict], reason: str,
                        reason_detail: Optional[str] = None,
                        search_delay_seconds: Optional[float] = None,
                        relocated: bool = False, retained: bool = False,
                        restored: bool = False, kept_final: bool = False):
        """发送种子处理通知，标题包含订阅、原因和最终动作（收容 / 保留 / 删除）。"""
        if not self._notify:
            return
        reason_text = {
            "timeout": "超时无进度",
            "delete_tracker": "Tracker 返回内容包含删除关键字",
            "manual": "订阅种子手动删除",
            "download_timeout": "超时无进度",
        }.get(reason, reason)
        detail_parts = []
        if torrent_task:
            if torrent_task.get("title"):
                detail_parts.append(f"标题：{torrent_task.get('title')}")
            if torrent_task.get("description"):
                detail_parts.append(f"内容：{torrent_task.get('description')}")
        follow_up = None
        if search_delay_seconds is not None:
            follow_up = f"将在 {search_delay_seconds / 60:.2f} 分钟后触发搜索补全"
        if restored:
            action_text = "已搬回影视目录（同一份种子继续下载，完成后自动整理入库）"
        elif kept_final:
            action_text = "已保留在影视目录（已达配置的收容往返次数，不再自动搬动或删除）"
        elif relocated:
            action_text = "已收容（移入收容目录继续做种，到期后自动删除）"
        elif retained:
            action_text = "已保留（收容未成功，本轮不删除，下一轮继续尝试）"
        else:
            action_text = "已删除"
        detail(
            f"种子删除处理：{format_subscribe(subscribe)} 原因={reason_detail or reason_text}，"
            f"处理={action_text}，后续={follow_up or '无'}"
        )
        self._notify(
            f"{format_subscribe(subscribe)} {reason_detail or reason_text}，{action_text}",
            "\n".join(detail_parts) if detail_parts else None,
            image=self._subscribe_image(subscribe),
            follow_up=follow_up,
            diagnostic=True,
        )

    def _subscribe_image(self, subscribe):
        """读取订阅通知图片；未注入图片解析器时保持兼容。"""
        return self._get_subscribe_image(subscribe) if self._get_subscribe_image else None
