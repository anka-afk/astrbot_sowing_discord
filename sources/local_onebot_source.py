"""Event-driven local OneBot source listening to notice.group_msg_emoji_like (no polling)."""

from __future__ import annotations

import hashlib
import inspect
import logging
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Iterable, Optional

try:
    from ..models.meme import MemeAttribution, MemeBlock, MemeItem
    from ..storage.db import SowingDB
except ImportError:
    from models.meme import MemeAttribution, MemeBlock, MemeItem
    from storage.db import SowingDB

from .base import BaseSource, SyncResult

logger = logging.getLogger(__name__)

GetMsgCallable = Callable[[str], Awaitable[Optional[dict[str, Any]]]]


class LocalOneBotSource(BaseSource):
    """
    Event-driven collector for private/local OneBot v11 source groups.

    Replaces v1 brute-force polling (`fetch_emoji_like`) with passive listening on
    `notice.group_msg_emoji_like` (`is_add=true/false` net counter). When a message's
    net reactions cross `emoji_threshold`, invokes `get_msg` exactly once.
    """

    source_type = "local"

    def __init__(
        self,
        db: SowingDB,
        source_groups: Iterable[str] = (),
        *,
        source_id: str = "local:onebot",
        emoji_threshold: int = 3,
        enabled: bool = True,
        max_tracked_messages: int = 5000,
    ):
        super().__init__(source_id=source_id, enabled=enabled)
        self.db = db
        self.source_groups: set[str] = {str(g).strip() for g in source_groups if str(g).strip()}
        self.emoji_threshold = max(1, int(emoji_threshold))
        self.max_tracked_messages = max_tracked_messages

        # In-memory bounded state: (group_id, message_id) -> net reaction count
        self._reaction_counts: OrderedDict[tuple[str, str], int] = OrderedDict()
        # Messages already fetched via get_msg so we never call get_msg twice
        self._captured: OrderedDict[tuple[str, str], bool] = OrderedDict()

    def _evict_if_needed(self) -> None:
        while len(self._reaction_counts) > self.max_tracked_messages:
            self._reaction_counts.popitem(last=False)
        while len(self._captured) > self.max_tracked_messages:
            self._captured.popitem(last=False)

    def get_net_count(self, group_id: str, message_id: str) -> int:
        return self._reaction_counts.get((str(group_id), str(message_id)), 0)

    def is_captured(self, group_id: str, message_id: str) -> bool:
        return (str(group_id), str(message_id)) in self._captured

    async def sync(self) -> SyncResult:
        # Purely event-driven: no active polling!
        return SyncResult(source_id=self.source_id, not_modified=True)

    async def handle_emoji_like_event(
        self,
        raw_event: dict[str, Any],
        get_msg: Optional[GetMsgCallable] = None,
    ) -> Optional[MemeItem]:
        """
        Process a OneBot v11 `notice.group_msg_emoji_like` payload.

        Expected fields:
        - `notice_type`: "group_msg_emoji_like"
        - `group_id`: int | str
        - `message_id`: int | str
        - `likes`: [{"emoji_id": "...", "count": 1}, ...]
        - `is_add`: bool (default True)
        """
        if not self.enabled:
            return None
        if raw_event.get("notice_type") != "group_msg_emoji_like":
            return None

        group_id = str(raw_event.get("group_id", "")).strip()
        message_id = str(raw_event.get("message_id", "")).strip()
        if not group_id or not message_id or group_id not in self.source_groups:
            return None

        likes = raw_event.get("likes") or []
        delta = 0
        for entry in likes:
            if isinstance(entry, dict):
                delta += max(1, int(entry.get("count", 1) or 1))
        if delta == 0:
            delta = 1

        is_add = bool(raw_event.get("is_add", True))
        key = (group_id, message_id)
        prev = self._reaction_counts.get(key, 0)
        new_count = prev + delta if is_add else max(0, prev - delta)
        self._reaction_counts[key] = new_count
        self._reaction_counts.move_to_end(key)
        self._evict_if_needed()

        if new_count < self.emoji_threshold:
            return None
        if key in self._captured:
            return None

        # Mark captured BEFORE awaiting get_msg to guarantee single fetch
        self._captured[key] = True
        self._captured.move_to_end(key)
        self._evict_if_needed()

        if get_msg is None:
            return None

        try:
            msg_payload = get_msg(message_id)
            if inspect.isawaitable(msg_payload):
                msg_payload = await msg_payload
        except Exception as exc:
            logger.warning("get_msg(%s) failed in LocalOneBotSource: %s", message_id, exc)
            # Allow retry if RPC transiently failed
            self._captured.pop(key, None)
            return None

        if not isinstance(msg_payload, dict):
            return None

        item = self._parse_onebot_message(group_id, message_id, msg_payload)
        if item is None:
            return None

        self.db.upsert_item(item, source_id=self.source_id)
        return item

    def _parse_onebot_message(
        self, group_id: str, message_id: str, msg_payload: dict[str, Any]
    ) -> Optional[MemeItem]:
        """Convert OneBot `get_msg` response into a privacy-scrubbed MemeItem."""
        segments = msg_payload.get("message")
        if isinstance(segments, str):
            segments = [{"type": "text", "data": {"text": segments}}]
        if not isinstance(segments, list):
            return None

        blocks: list[MemeBlock] = []
        text_parts: list[str] = []
        for seg in segments:
            if not isinstance(seg, dict):
                continue
            stype = str(seg.get("type", ""))
            sdata = seg.get("data") or {}
            if stype == "text":
                txt = str(sdata.get("text", "")).strip()
                if txt:
                    text_parts.append(txt)
            elif stype == "image":
                url = str(sdata.get("url") or sdata.get("file") or "").strip()
                if not url:
                    continue
                is_gif = url.lower().split("?", 1)[0].endswith(".gif") or str(
                    sdata.get("sub_type", "")
                ) == "1"
                if is_gif:
                    blocks.append(MemeBlock(type="gif", url=url, mime="image/gif"))
                else:
                    blocks.append(MemeBlock(type="image", url=url, mime="image/jpeg"))
            elif stype == "video":
                # MVP explicitly disallows raw local video ingestion
                continue

        if text_parts:
            blocks.insert(0, MemeBlock(type="text", text="\n".join(text_parts)))

        if not blocks:
            return None

        digest = hashlib.sha256(f"{group_id}:{message_id}".encode("utf-8")).hexdigest()[:24]
        summary = " ".join(text_parts)[:120]
        return MemeItem(
            id=digest,
            source_type="local",
            hub_id="local",
            title="",
            summary=summary,
            tags=["本地源群"],
            nsfw_level=0,
            attribution=MemeAttribution(submitter_name="匿名群友", origin_label="本地源群"),
            published_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            blocks=blocks,
        )
