"""Command handler for /来坨史 [tag] (alias /搬史) and ambient pull delivery."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional, Sequence

try:
    from ..ambient.air_reader import AirReader
    from ..models.meme import MemeItem
    from ..pipeline.commentary import CommentaryGenerator
    from ..pipeline.dedup import Deduplicator
    from ..pipeline.filter import MemeFilter
    from ..sinks.astrbot_sink import AstrBotSink, DeliveryResult
    from ..sources.base import BaseSource
    from ..sources.hub_source import HubSource
    from ..storage.db import SowingDB
except ImportError:
    from ambient.air_reader import AirReader
    from models.meme import MemeItem
    from pipeline.commentary import CommentaryGenerator
    from pipeline.dedup import Deduplicator
    from pipeline.filter import MemeFilter
    from sinks.astrbot_sink import AstrBotSink, DeliveryResult
    from sources.base import BaseSource
    from sources.hub_source import HubSource
    from storage.db import SowingDB

logger = logging.getLogger(__name__)


@dataclass
class PullResult:
    ok: bool
    message: str = ""
    item: Optional[MemeItem] = None
    delivery: Optional[DeliveryResult] = None
    commentary: Optional[str] = None


class ManualPullHandler:
    """Selects an unsent, filtered meme from local cache (or replenishes via /random) and delivers it."""

    def __init__(
        self,
        db: SowingDB,
        dedup: Deduplicator,
        meme_filter: MemeFilter,
        sink: AstrBotSink,
        commentary: CommentaryGenerator,
        sources: Sequence[BaseSource],
        *,
        default_group_settings: Optional[dict[str, Any]] = None,
        air_reader: Optional[AirReader] = None,
    ):
        self.db = db
        self.dedup = dedup
        self.meme_filter = meme_filter
        self.sink = sink
        self.commentary = commentary
        self.sources = list(sources)
        self.default_group_settings = dict(default_group_settings or {})
        self.air_reader = air_reader

    def _select_candidate(
        self,
        group_umo: str,
        settings: dict[str, Any],
        tag: Optional[str] = None,
    ) -> Optional[MemeItem]:
        max_nsfw = int(settings.get("max_nsfw", 1))
        blocked_tags = settings.get("blocked_tags") or []

        candidates = self.db.get_unsent_candidates(
            group_umo, max_nsfw=max_nsfw, limit=80
        )
        candidates = self.dedup.filter_unsent_for_group(group_umo, candidates)
        filtered = self.meme_filter.filter_items(
            candidates,
            max_nsfw=max_nsfw,
            blocked_tags=blocked_tags,
            required_tag=tag,
        )
        return filtered[0] if filtered else None

    async def _replenish(
        self, settings: dict[str, Any], tag: Optional[str] = None
    ) -> int:
        max_nsfw = int(settings.get("max_nsfw", 1))
        blocked_tags = list(settings.get("blocked_tags") or [])
        added = 0
        for src in self.sources:
            if not src.enabled:
                continue
            if isinstance(src, HubSource):
                fetched = await src.fetch_random(
                    limit=20,
                    tags=[tag] if tag else None,
                    exclude_tags=blocked_tags or None,
                    max_nsfw=max_nsfw,
                )
                added += len(fetched)
                if not fetched:
                    res = await src.sync()
                    added += len(res.new_items)
            else:
                res = await src.sync()
                added += len(res.new_items)
        return added

    async def pull_and_deliver(
        self,
        group_umo: str,
        *,
        tag: Optional[str] = None,
        trigger_type: str = "manual",
    ) -> PullResult:
        settings = self.db.get_group_settings(
            group_umo, defaults=self.default_group_settings
        )
        if not settings.get("enabled", True):
            return PullResult(ok=False, message="本群已关闭搬史功能，可使用 /史库 开关 开 重新启用。")

        clean_tag = tag.strip() if tag and tag.strip() else None
        item = self._select_candidate(group_umo, settings, tag=clean_tag)
        if item is None:
            await self._replenish(settings, tag=clean_tag)
            item = self._select_candidate(group_umo, settings, tag=clean_tag)

        if item is None:
            hint = f"（标签：{clean_tag}）" if clean_tag else ""
            return PullResult(ok=False, message=f"史库暂时掏空了{hint}，稍后再试或使用 /投稿 投喂新史吧~")

        comment = await self.commentary.generate(
            item, probability=float(settings.get("llm_comment_prob", 0.3))
        )
        delivery = await self.sink.deliver(item, group_umo, commentary=comment)
        if not delivery.ok:
            return PullResult(
                ok=False,
                message=f"发送失败：{delivery.error or '未知错误'}",
                item=item,
                delivery=delivery,
                commentary=comment,
            )

        self.dedup.mark_sent(
            group_umo=group_umo,
            item=item,
            bot_msg_id=delivery.bot_msg_id,
            trigger_type=trigger_type,
        )
        if self.air_reader is not None:
            self.air_reader.record_bot_message(group_umo, is_meme=True)

        return PullResult(
            ok=True,
            message="",
            item=item,
            delivery=delivery,
            commentary=comment,
        )
