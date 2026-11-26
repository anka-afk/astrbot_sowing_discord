"""Command handler for /烂史 [原因]: blinded report/negative feedback on quoted bot memes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Optional

try:
    from ..feedback.collector import FeedbackCollector, normalize_report_reason
    from ..models.meme import REPORT_REASONS
    from ..sources.hub_source import HubSource
except ImportError:
    from feedback.collector import FeedbackCollector, normalize_report_reason
    from models.meme import REPORT_REASONS
    from sources.hub_source import HubSource


@dataclass
class ReportResult:
    ok: bool
    message: str
    reason: str = ""
    outbox_id: Optional[int] = None


class ReportHandler:
    """Handles /烂史 [原因] when a user quotes a meme previously sent by the bot."""

    def __init__(
        self,
        feedback_collector: FeedbackCollector,
        hub_sources: Mapping[str, HubSource],
    ):
        self.feedback_collector = feedback_collector
        self.hub_sources = hub_sources

    def handle_report(
        self,
        *,
        group_umo: str,
        user_id: str,
        quoted_bot_msg_id: str,
        raw_reason: str = "",
    ) -> ReportResult:
        if not quoted_bot_msg_id:
            reasons_help = " / ".join(REPORT_REASONS)
            return ReportResult(
                ok=False,
                message=f"请引用 Bot 搬运的那条消息再发送 `/烂史 [原因]`。\n可选原因：{reasons_help}",
            )

        canonical = normalize_report_reason(raw_reason)
        outbox_id = self.feedback_collector.collect_report(
            group_umo=group_umo,
            quoted_bot_msg_id=quoted_bot_msg_id,
            user_id=user_id,
            reason=canonical,
            hub_sources=self.hub_sources,
        )
        if outbox_id is None:
            return ReportResult(
                ok=False,
                reason=canonical,
                message="未找到该引用消息对应的 Hub 史库记录（可能是本地源/RSS 条目或非本插件发送）。",
            )
        if outbox_id == 0:
            return ReportResult(
                ok=True,
                reason=canonical,
                outbox_id=0,
                message="已记录过您的匿名举报，无需重复提交，感谢维护史库质量！",
            )
        return ReportResult(
            ok=True,
            reason=canonical,
            outbox_id=outbox_id,
            message=f"已记录匿名举报（原因：{canonical}），将在下次批量同步时上报 Hub，感谢共同维护史库质量！",
        )
