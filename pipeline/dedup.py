"""Global uid/hash deduplication and strict per-group sent deduplication."""

from __future__ import annotations

from typing import Iterable

try:
    from ..models.meme import MemeItem
    from ..storage.db import SowingDB
except ImportError:
    from models.meme import MemeItem
    from storage.db import SowingDB


class Deduplicator:
    """Enforces both global cache deduplication and per-group never-repeat delivery."""

    def __init__(self, db: SowingDB):
        self.db = db

    def is_globally_known(self, item: MemeItem) -> bool:
        """Check whether item uid or identical content_hash already exists in cache."""
        if self.db.has_uid(item.uid):
            return True
        return self.db.has_content_hash(item.content_hash())

    def is_sent_to_group(self, group_umo: str, item: MemeItem) -> bool:
        """Return True if item uid or its content_hash was already sent to group_umo."""
        if self.db.is_sent(group_umo, item.uid):
            return True
        return self.db.is_content_hash_sent(group_umo, item.content_hash())

    def filter_unsent_for_group(
        self, group_umo: str, items: Iterable[MemeItem]
    ) -> list[MemeItem]:
        """Remove items already sent to group_umo and deduplicate within the batch."""
        seen_uids: set[str] = set()
        seen_hashes: set[str] = set()
        result: list[MemeItem] = []

        for item in items:
            uid = item.uid
            chash = item.content_hash()
            if uid in seen_uids or chash in seen_hashes:
                continue
            if self.is_sent_to_group(group_umo, item):
                continue
            seen_uids.add(uid)
            seen_hashes.add(chash)
            result.append(item)
        return result

    def mark_sent(
        self,
        group_umo: str,
        item: MemeItem,
        bot_msg_id: str = "",
        trigger_type: str = "ambient",
    ) -> None:
        self.db.mark_sent(
            group_umo=group_umo,
            item_uid=item.uid,
            bot_msg_id=bot_msg_id,
            trigger_type=trigger_type,
        )
