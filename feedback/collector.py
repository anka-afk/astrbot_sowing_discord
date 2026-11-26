"""Anonymous crowd feedback collector & batch reporter (POST /api/v1/feedback/batch)."""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

try:
    from ..models.meme import REPORT_REASONS
    from ..sources.hub_source import HubSource
    from ..storage.db import SowingDB
except ImportError:
    from models.meme import REPORT_REASONS
    from sources.hub_source import HubSource
    from storage.db import SowingDB

logger = logging.getLogger(__name__)

# Positive & negative reply keywords when quoting a bot meme message
POSITIVE_REPLY_RE = re.compile(
    r"(笑死|绷不住|好活|哈哈+|草+|乐死|太典|典中典|绝了|神回|收藏|爱看|多发|再来|牛逼|nb|666|👍|😂|🤣)",
    re.IGNORECASE,
)
NEGATIVE_REPLY_RE = re.compile(
    r"(看过了|火星|太尬|难绷|什么玩意|烂活|不好笑|发过了|搬重了|退钱|无聊|👎|🤮)",
    re.IGNORECASE,
)

# Common QQ / Unicode emoji IDs mapped to like / dislike
NEGATIVE_EMOJI_IDS = {"128078", "77", "128169", "129326", "👎", "🤮", "💩"}

REPORT_REASON_ALIASES: dict[str, str] = {
    "nsfw": "nsfw_unmarked",
    "nsfw_unmarked": "nsfw_unmarked",
    "色情": "nsfw_unmarked",
    "擦边": "nsfw_unmarked",
    "分级": "nsfw_unmarked",
    "未标分级": "nsfw_unmarked",
    "privacy": "privacy_leak",
    "privacy_leak": "privacy_leak",
    "隐私": "privacy_leak",
    "泄露": "privacy_leak",
    "挂人": "privacy_leak",
    "copyright": "copyright",
    "版权": "copyright",
    "侵权": "copyright",
    "broken": "broken_media",
    "broken_media": "broken_media",
    "裂图": "broken_media",
    "挂了": "broken_media",
    "失效": "broken_media",
    "duplicate": "duplicate",
    "重复": "duplicate",
    "火星": "duplicate",
    "other": "other",
    "其他": "other",
}


def compute_voter_hash(instance_salt: str, group_id: str, user_id: str) -> str:
    """Blinded voter hash: sha256(instance_salt + group_id + user_id)."""
    raw = f"{instance_salt}{group_id}{user_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def compute_group_hash(instance_salt: str, group_id: str) -> str:
    """Blinded group hash: sha256(instance_salt + group_id)."""
    raw = f"{instance_salt}{group_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def normalize_report_reason(raw_reason: str = "") -> str:
    cleaned = (raw_reason or "").strip().lower()
    if not cleaned:
        return "other"
    if cleaned in REPORT_REASONS:
        return cleaned
    for keyword, canonical in REPORT_REASON_ALIASES.items():
        if keyword in cleaned:
            return canonical
    return "other"


def parse_hub_item_uid(item_uid: str) -> Optional[tuple[str, str]]:
    """Parse 'hub:<hub_id>:<item_id>' into (hub_id, item_id); returns None for non-hub items."""
    parts = item_uid.split(":", 2)
    if len(parts) == 3 and parts[0] == "hub" and parts[1] and parts[2]:
        return parts[1], parts[2]
    return None


class FeedbackCollector:
    """Collects emoji_like, quoted keyword replies, and /烂史 reports and flushes in batches."""

    def __init__(self, db: SowingDB, default_salt: str = "default-local-salt"):
        self.db = db
        self.default_salt = default_salt

    def _get_salt_for_hub(self, hub_id: str, hub_sources: Optional[Mapping[str, HubSource]] = None) -> str:
        if hub_sources and hub_id in hub_sources:
            return hub_sources[hub_id].get_instance_salt()
        # Look up in source_cursors by hub_id
        for cursor in self.db.list_source_cursors():
            if cursor.get("hub_id") == hub_id and cursor.get("instance_salt"):
                return str(cursor["instance_salt"])
        return self.default_salt

    def _record_event(
        self,
        *,
        group_umo: str,
        bot_msg_id: str,
        user_id: str,
        action: str,
        reason: Optional[str] = None,
        hub_sources: Optional[Mapping[str, HubSource]] = None,
        occurred_at: Optional[str] = None,
    ) -> Optional[int]:
        sent = self.db.find_sent_by_bot_msg_id(group_umo, bot_msg_id)
        if not sent:
            return None
        parsed = parse_hub_item_uid(sent["item_uid"])
        if parsed is None:
            return None
        hub_id, item_id = parsed

        salt = self._get_salt_for_hub(hub_id, hub_sources)
        group_id = group_umo.rsplit(":", 1)[-1] if ":" in group_umo else group_umo
        voter_hash = compute_voter_hash(salt, group_id, str(user_id))
        group_hash = compute_group_hash(salt, group_id)
        ts = occurred_at or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        return self.db.enqueue_feedback(
            hub_id=hub_id,
            item_id=item_id,
            action=action,
            reason=reason,
            voter_hash=voter_hash,
            group_hash=group_hash,
            occurred_at=ts,
        )

    def collect_emoji_like(
        self,
        *,
        group_umo: str,
        bot_msg_id: str,
        user_id: str,
        emoji_id: str,
        is_add: bool = True,
        hub_sources: Optional[Mapping[str, HubSource]] = None,
    ) -> Optional[int]:
        """Collect reaction feedback on a bot meme message."""
        if not is_add:
            return None
        action = "dislike" if str(emoji_id) in NEGATIVE_EMOJI_IDS else "like"
        return self._record_event(
            group_umo=group_umo,
            bot_msg_id=bot_msg_id,
            user_id=user_id,
            action=action,
            hub_sources=hub_sources,
        )

    def collect_reply_text(
        self,
        *,
        group_umo: str,
        quoted_bot_msg_id: str,
        user_id: str,
        reply_text: str,
        hub_sources: Optional[Mapping[str, HubSource]] = None,
    ) -> Optional[int]:
        """Match positive/negative keywords when a user quotes a bot meme message."""
        text = (reply_text or "").strip()
        if not text:
            return None
        action: Optional[str] = None
        if NEGATIVE_REPLY_RE.search(text):
            action = "dislike"
        elif POSITIVE_REPLY_RE.search(text):
            action = "like"
        if not action:
            return None
        return self._record_event(
            group_umo=group_umo,
            bot_msg_id=quoted_bot_msg_id,
            user_id=user_id,
            action=action,
            hub_sources=hub_sources,
        )

    def collect_report(
        self,
        *,
        group_umo: str,
        quoted_bot_msg_id: str,
        user_id: str,
        reason: str = "",
        hub_sources: Optional[Mapping[str, HubSource]] = None,
    ) -> Optional[int]:
        """Record a standardized report from /烂史 [原因]."""
        canonical_reason = normalize_report_reason(reason)
        return self._record_event(
            group_umo=group_umo,
            bot_msg_id=quoted_bot_msg_id,
            user_id=user_id,
            action="report",
            reason=canonical_reason,
            hub_sources=hub_sources,
        )

    async def flush(
        self,
        hub_sources: Mapping[str, HubSource],
        *,
        batch_size: int = 50,
    ) -> dict[str, int]:
        """Flush queued feedback events to their respective Hubs (<=50 per batch)."""
        batch_limit = max(1, min(int(batch_size), 50))
        flushed_counts: dict[str, int] = {}

        for hub_id, hub in hub_sources.items():
            if not hub.enabled:
                continue
            if hub.cached_meta is not None and not hub.cached_meta.capabilities.feedback:
                continue

            total_sent = 0
            while True:
                rows = self.db.peek_feedback_batch(hub_id=hub_id, limit=batch_limit)
                if not rows:
                    break
                payload_events: list[dict[str, Any]] = []
                row_ids: list[int] = []
                for row in rows:
                    row_ids.append(int(row["id"]))
                    ev: dict[str, Any] = {
                        "item_id": row["item_id"],
                        "action": row["action"],
                        "voter_hash": row["voter_hash"],
                        "group_hash": row["group_hash"],
                        "occurred_at": row["occurred_at"],
                    }
                    if row.get("reason"):
                        ev["reason"] = row["reason"]
                    payload_events.append(ev)

                try:
                    await hub.send_feedback_batch(payload_events)
                except Exception as exc:
                    logger.warning("Feedback batch flush failed for hub %s: %s", hub_id, exc)
                    break

                self.db.delete_feedback_ids(row_ids)
                total_sent += len(row_ids)
                if len(rows) < batch_limit:
                    break

            if total_sent > 0:
                flushed_counts[hub_id] = total_sent

        return flushed_counts
