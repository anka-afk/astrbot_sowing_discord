"""Tests for event-driven LocalOneBotSource (notice.group_msg_emoji_like, no polling)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from sources.local_onebot_source import LocalOneBotSource
from storage.db import SowingDB


@pytest.mark.asyncio
async def test_local_onebot_source_emoji_like_threshold_and_single_get_msg(tmp_path: Path) -> None:
    db = SowingDB(tmp_path / "local.db")
    src = LocalOneBotSource(db=db, source_groups=["10001"], emoji_threshold=3)

    get_msg_calls: list[str] = []

    async def fake_get_msg(message_id: str) -> dict[str, Any]:
        get_msg_calls.append(message_id)
        return {
            "message_id": message_id,
            "message": [
                {"type": "text", "data": {"text": "典中典语录"}},
                {"type": "image", "data": {"url": "https://qpic.example.org/meme.jpg"}},
                {"type": "video", "data": {"url": "https://qpic.example.org/skip.mp4"}},
            ],
        }

    try:
        # Event from non-subscribed group is ignored
        res_ignored = await src.handle_emoji_like_event(
            {
                "notice_type": "group_msg_emoji_like",
                "group_id": 99999,
                "message_id": "m1",
                "likes": [{"emoji_id": "128077", "count": 5}],
                "is_add": True,
            },
            get_msg=fake_get_msg,
        )
        assert res_ignored is None
        assert get_msg_calls == []

        # +2 likes (below threshold 3)
        await src.handle_emoji_like_event(
            {
                "notice_type": "group_msg_emoji_like",
                "group_id": 10001,
                "message_id": "m2",
                "likes": [{"emoji_id": "128077", "count": 2}],
                "is_add": True,
            },
            get_msg=fake_get_msg,
        )
        assert src.get_net_count("10001", "m2") == 2
        assert get_msg_calls == []

        # -1 unlike -> net count drops to 1
        await src.handle_emoji_like_event(
            {
                "notice_type": "group_msg_emoji_like",
                "group_id": 10001,
                "message_id": "m2",
                "likes": [{"emoji_id": "128077", "count": 1}],
                "is_add": False,
            },
            get_msg=fake_get_msg,
        )
        assert src.get_net_count("10001", "m2") == 1
        assert get_msg_calls == []

        # +2 likes -> net count hits 3 -> triggers single get_msg!
        captured = await src.handle_emoji_like_event(
            {
                "notice_type": "group_msg_emoji_like",
                "group_id": 10001,
                "message_id": "m2",
                "likes": [{"emoji_id": "128077", "count": 2}],
                "is_add": True,
            },
            get_msg=fake_get_msg,
        )
        assert captured is not None
        assert captured.uid.startswith("local:")
        assert get_msg_calls == ["m2"]
        # Raw video segment must be filtered out
        assert all(b.type != "video" and b.type != "video_file" for b in captured.blocks)
        assert db.get_item(captured.uid) is not None

        # Further likes on same message MUST NOT trigger get_msg again
        second = await src.handle_emoji_like_event(
            {
                "notice_type": "group_msg_emoji_like",
                "group_id": 10001,
                "message_id": "m2",
                "likes": [{"emoji_id": "128514", "count": 1}],
                "is_add": True,
            },
            get_msg=fake_get_msg,
        )
        assert second is None
        assert get_msg_calls == ["m2"]
    finally:
        db.close()
