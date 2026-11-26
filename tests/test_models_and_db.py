"""Tests for Schema v1.0 models, SQLite 5-table storage, and plugin metadata."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from models.meme import (
    ChatContentPart,
    ChatMessageNode,
    FeedbackEvent,
    MemeBlock,
    MemeItem,
    RenderedImage,
)
from storage.db import SowingDB


def _make_item(
    item_id: str = "01JTEST0000000000000000001",
    *,
    hub_id: str = "official-main",
    text: str = "绷不住了",
    tags: list[str] | None = None,
    nsfw_level: int = 0,
    score: float = 10.0,
) -> MemeItem:
    return MemeItem(
        id=item_id,
        seq=101,
        hub_id=hub_id,
        title="今日典中典",
        summary="一句文字梗概",
        tags=tags if tags is not None else ["沙雕", "群聊日常"],
        nsfw_level=nsfw_level,
        score=score,
        blocks=[
            MemeBlock(type="text", text=text),
            MemeBlock(type="image", url="https://cdn.example.org/a.webp", mime="image/webp", w=720, h=960),
        ],
    )


def test_metadata_and_conf_schema_valid() -> None:
    root = Path(__file__).resolve().parent.parent
    meta = yaml.safe_load((root / "metadata.yaml").read_text(encoding="utf-8"))
    assert meta["version"] == "2.0.0"
    assert meta["name"] == "astrbot_sowing_discord"

    schema = json.loads((root / "_conf_schema.json").read_text(encoding="utf-8"))
    for key in (
        "official_hub_enabled",
        "official_hub_url",
        "user_token",
        "extra_hubs",
        "rss_sources",
        "local_onebot_groups",
        "target_groups",
        "default_max_nsfw",
        "quiet_hours",
        "cooldown_sec",
        "llm_comment_prob",
    ):
        assert key in schema


def test_meme_item_roundtrip_and_validation() -> None:
    item = MemeItem(
        id="01JABCDEF1234567890ABCDEFG",
        seq=1042,
        hub_id="official-main",
        title="聊天记录切片",
        summary="",
        tags=["群聊日常"],
        nsfw_level=1,
        score=12.5,
        blocks=[
            MemeBlock(
                type="chat_record",
                rendered_images=[RenderedImage(url="https://cdn.example.org/c_1.webp", w=720, h=1650)],
                messages=[
                    ChatMessageNode(
                        sender_alias="群友A",
                        avatar_color_idx=0,
                        time_label="14:20",
                        content=[ChatContentPart(type="text", text="绷不住了")],
                    )
                ],
            ),
            MemeBlock(
                type="video_link",
                url="https://www.bilibili.com/video/BV1xxxxxxxxx",
                platform="bilibili",
                title="视频标题",
                cover_url="https://cdn.example.org/cover.webp",
                duration_sec=45,
            ),
        ],
    )
    item.validate()
    assert item.uid == "hub:official-main:01JABCDEF1234567890ABCDEFG"
    assert "群友A: 绷不住了" in item.plain_text_preview()

    restored = MemeItem.from_json(item.to_json())
    assert restored.uid == item.uid
    assert restored.content_hash() == item.content_hash()
    assert restored.blocks[0].rendered_images[0].w == 720
    assert restored.blocks[1].platform == "bilibili"

    bad = MemeItem(id="x", nsfw_level=5, blocks=[MemeBlock(type="text", text="hi")])
    with pytest.raises(ValueError):
        bad.validate()

    fb = FeedbackEvent(
        item_id="01J1",
        action="report",
        reason="privacy_leak",
        voter_hash="vhash",
        group_hash="ghash",
        occurred_at="2026-11-26T09:00:00Z",
    )
    fb.validate()
    assert fb.to_api_dict()["reason"] == "privacy_leak"


def test_sqlite_five_tables_and_per_group_dedup(tmp_path: Path) -> None:
    db = SowingDB(tmp_path / "sowing_v2.db")
    try:
        # Insert two distinct items and one duplicate-content item from a second hub
        item1 = _make_item("id-1", hub_id="hub-a", text="同一坨史", score=20.0)
        item1_dup = _make_item("id-99", hub_id="hub-b", text="同一坨史", score=15.0)
        item2 = _make_item("id-2", hub_id="hub-a", text="另一坨史", nsfw_level=2, score=5.0)

        assert db.upsert_item(item1, source_id="hub:hub-a") is True
        assert db.upsert_item(item1, source_id="hub:hub-a") is False  # update
        assert db.upsert_item(item1_dup, source_id="hub:hub-b") is True
        assert db.upsert_item(item2, source_id="hub:hub-a") is True

        # Group 1 with max_nsfw=1 gets item1 & item1_dup initially
        cands = db.get_unsent_candidates("aiocqhttp:GroupMessage:100", max_nsfw=1)
        assert {c.uid for c in cands} == {item1.uid, item1_dup.uid}

        # Mark item1 sent to Group 1 -> BOTH item1 and item1_dup (same content_hash!) are excluded for Group 1
        db.mark_sent("aiocqhttp:GroupMessage:100", item1.uid, bot_msg_id="m_1001", trigger_type="manual")
        assert db.is_sent("aiocqhttp:GroupMessage:100", item1.uid) is True
        assert db.is_content_hash_sent("aiocqhttp:GroupMessage:100", item1_dup.content_hash()) is True
        assert db.get_unsent_candidates("aiocqhttp:GroupMessage:100", max_nsfw=1) == []

        # Group 2 still sees item1 as unsent!
        cands_g2 = db.get_unsent_candidates("aiocqhttp:GroupMessage:200", max_nsfw=1)
        assert len(cands_g2) == 2

        # Lookup by bot_msg_id works
        found = db.find_sent_by_bot_msg_id("aiocqhttp:GroupMessage:100", "m_1001")
        assert found is not None
        assert found["item_uid"] == item1.uid

        # Tombstone eviction only deletes from matching hub
        removed = db.delete_items_by_hub_item_ids("hub-a", ["id-1", "id-2"])
        assert removed == 2
        assert db.get_item(item1.uid) is None
        assert db.get_item(item1_dup.uid) is not None

        # Group settings & cursors & outbox
        gs = db.upsert_group_settings("g1", max_nsfw=2, blocked_tags=["地狱"])
        assert gs["max_nsfw"] == 2
        assert gs["blocked_tags"] == ["地狱"]

        cur = db.upsert_source_cursor("hub:official-main", hub_id="official-main", next_seq=42)
        assert cur["next_seq"] == 42

        fb_id = db.enqueue_feedback(
            hub_id="official-main",
            item_id="id-1",
            action="like",
            voter_hash="vh",
            group_hash="gh",
            occurred_at="2026-11-26T09:00:00Z",
        )
        assert fb_id > 0
        batch = db.peek_feedback_batch("official-main", limit=50)
        assert len(batch) == 1
        assert db.delete_feedback_ids([fb_id]) == 1
        assert db.get_stats()["pending_feedback"] == 0
    finally:
        db.close()
