"""Per-group content filter: max_nsfw, tag allow/block lists, and block type filtering."""

from __future__ import annotations

from typing import Iterable, Optional

try:
    from ..models.meme import MemeItem
except ImportError:
    from models.meme import MemeItem


def _norm_tags(tags: Iterable[str]) -> set[str]:
    return {str(t).strip().lower() for t in tags if str(t).strip()}


class MemeFilter:
    """Stateless filter enforcing per-group NSFW cap, tag lists, and block types."""

    def matches(
        self,
        item: MemeItem,
        *,
        max_nsfw: int = 1,
        blocked_tags: Iterable[str] = (),
        allowed_tags: Iterable[str] = (),
        required_tag: Optional[str] = None,
        allowed_block_types: Optional[set[str]] = None,
        blocked_block_types: Optional[set[str]] = None,
        allow_video_file: bool = False,
    ) -> bool:
        if item.nsfw_level > int(max_nsfw):
            return False

        item_types = item.content_types()
        if not item_types:
            return False
        if not allow_video_file and "video_file" in item_types:
            return False
        if blocked_block_types and (item_types & blocked_block_types):
            return False
        if allowed_block_types is not None and not item_types.issubset(allowed_block_types):
            return False

        item_tags = _norm_tags(item.tags)
        blocked = _norm_tags(blocked_tags)
        if blocked and (item_tags & blocked):
            return False

        allowed = _norm_tags(allowed_tags)
        if allowed and not (item_tags & allowed):
            return False

        if required_tag and required_tag.strip():
            req = required_tag.strip().lower()
            if req not in item_tags:
                return False

        return True

    def filter_items(
        self,
        items: Iterable[MemeItem],
        *,
        max_nsfw: int = 1,
        blocked_tags: Iterable[str] = (),
        allowed_tags: Iterable[str] = (),
        required_tag: Optional[str] = None,
        allowed_block_types: Optional[set[str]] = None,
        blocked_block_types: Optional[set[str]] = None,
        allow_video_file: bool = False,
    ) -> list[MemeItem]:
        return [
            item
            for item in items
            if self.matches(
                item,
                max_nsfw=max_nsfw,
                blocked_tags=blocked_tags,
                allowed_tags=allowed_tags,
                required_tag=required_tag,
                allowed_block_types=allowed_block_types,
                blocked_block_types=blocked_block_types,
                allow_video_file=allow_video_file,
            )
        ]
