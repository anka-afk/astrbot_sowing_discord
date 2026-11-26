"""Tests for pipeline (filter, dedup, LLM commentary 5s timeout) and sinks (MediaCache LRU, AstrBotSink)."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any

import pytest

from models.meme import ChatContentPart, ChatMessageNode, MemeBlock, MemeItem, RenderedImage
from pipeline.commentary import CommentaryGenerator
from pipeline.filter import MemeFilter
from sinks.astrbot_sink import AstrBotSink, Image, Plain
from sinks.media_cache import MediaCache


def test_meme_filter_nsfw_tags_and_video_file() -> None:
    flt = MemeFilter()
    items = [
        MemeItem(id="1", tags=["沙雕"], nsfw_level=0, blocks=[MemeBlock(type="text", text="a")]),
        MemeItem(id="2", tags=["地狱"], nsfw_level=1, blocks=[MemeBlock(type="text", text="b")]),
        MemeItem(id="3", tags=["二次元"], nsfw_level=2, blocks=[MemeBlock(type="text", text="c")]),
        MemeItem(
            id="4",
            tags=["沙雕"],
            nsfw_level=0,
            blocks=[MemeBlock(type="video_file", url="https://cdn.example.org/v.mp4")],
        ),
    ]

    kept = flt.filter_items(items, max_nsfw=1, blocked_tags=["地狱"])
    assert [i.id for i in kept] == ["1"]

    kept_high = flt.filter_items(items, max_nsfw=2, required_tag="二次元")
    assert [i.id for i in kept_high] == ["3"]

    kept_video = flt.filter_items(items, max_nsfw=1, allow_video_file=True)
    assert "4" in [i.id for i in kept_video]


@pytest.mark.asyncio
async def test_commentary_feeds_text_only_and_degrades_on_timeout() -> None:
    seen_prompts: list[str] = []

    async def fast_llm(prompt: str) -> str:
        seen_prompts.append(prompt)
        return "“这波属实典中典了”"

    item = MemeItem(
        id="c1",
        title="群友抽象发言",
        summary="某群友把闹钟定成了明年",
        tags=["抽象", "群聊"],
        blocks=[
            MemeBlock(type="image", url="https://secret-cdn.example.org/img.webp"),
        ],
    )
    gen = CommentaryGenerator(timeout_sec=5.0, llm_invoker=fast_llm)
    comment = await gen.generate(item, probability=1.0)
    assert comment == "这波属实典中典了"
    assert len(seen_prompts) == 1
    assert "群友抽象发言" in seen_prompts[0]
    assert "某群友把闹钟定成了明年" in seen_prompts[0]
    assert "secret-cdn.example.org" not in seen_prompts[0]

    # Timeout degrades silently to None
    async def slow_llm(_prompt: str) -> str:
        await asyncio.sleep(1.0)
        return "too late"

    slow_gen = CommentaryGenerator(timeout_sec=0.1, llm_invoker=slow_llm)
    assert await slow_gen.generate(item, probability=1.0) is None


def test_media_cache_lru_eviction(tmp_path: Path) -> None:
    cache = MediaCache(tmp_path / "cache", max_bytes=1024 * 1024)  # 1MB floor
    # Override max_bytes for small deterministic LRU test
    cache.max_bytes = 250

    p1 = cache.put_bytes("k1", b"a" * 100, ext=".webp")
    os.utime(p1, (1000, 1000))
    p2 = cache.put_bytes("k2", b"b" * 100, ext=".webp")
    os.utime(p2, (2000, 2000))
    p3 = cache.put_bytes("k3", b"c" * 100, ext=".webp")  # total 300 > 250 -> evicts k1

    assert not p1.exists()
    assert p2.exists()
    assert p3.exists()
    assert cache.total_size_bytes() <= 250


@pytest.mark.asyncio
async def test_astrbot_sink_chat_record_native_forward_and_fallback() -> None:
    sent_chains: list[tuple[str, Any]] = []

    async def fake_send(umo: str, chain: Any) -> dict[str, str]:
        sent_chains.append((umo, chain))
        return {"message_id": "bot-msg-42"}

    chat_item = MemeItem(
        id="chat1",
        blocks=[
            MemeBlock(
                type="chat_record",
                rendered_images=[
                    RenderedImage(url="https://cdn.example.org/slice_1.webp"),
                    RenderedImage(url="https://cdn.example.org/slice_2.webp"),
                ],
                messages=[
                    ChatMessageNode(
                        sender_alias="群友A",
                        content=[ChatContentPart(type="text", text="笑死")],
                    )
                ],
            )
        ],
    )

    # 1. Default sends rendered_images slices on any platform (e.g., Telegram)
    sink_default = AstrBotSink(send_fn=fake_send, qq_native_forward=False)
    res1 = await sink_default.deliver(chat_item, "telegram:GroupMessage:999")
    assert res1.ok is True
    assert res1.used_native_forward is False
    imgs = [c for c in sent_chains[0][1].chain if isinstance(c, Image)]
    assert [i.url for i in imgs] == [
        "https://cdn.example.org/slice_1.webp",
        "https://cdn.example.org/slice_2.webp",
    ]

    # 2. QQ native forward fails -> seamlessly falls back to rendered_images slices!
    sent_chains.clear()

    async def failing_forward(_gid: str, _nodes: list[dict[str, Any]]) -> Any:
        raise RuntimeError("OneBot forward blocked")

    sink_fallback = AstrBotSink(
        send_fn=fake_send,
        qq_native_forward=True,
        qq_forward_fn=failing_forward,
    )
    res2 = await sink_fallback.deliver(chat_item, "aiocqhttp:GroupMessage:12345")
    assert res2.ok is True
    assert res2.used_native_forward is False
    assert len(sent_chains) == 1
    assert len([c for c in sent_chains[0][1].chain if isinstance(c, Image)]) == 2

    # 3. QQ native forward succeeds
    sent_chains.clear()
    forwarded_nodes: list[dict[str, Any]] = []

    async def ok_forward(_gid: str, nodes: list[dict[str, Any]]) -> dict[str, str]:
        forwarded_nodes.extend(nodes)
        return {"message_id": "fwd-99"}

    sink_native = AstrBotSink(
        send_fn=fake_send,
        qq_native_forward=True,
        qq_forward_fn=ok_forward,
    )
    res3 = await sink_native.deliver(chat_item, "aiocqhttp:GroupMessage:12345")
    assert res3.ok is True
    assert res3.used_native_forward is True
    assert res3.bot_msg_id == "fwd-99"
    assert forwarded_nodes[0]["data"]["name"] == "群友A"

    # 4. video_link renders cover + title + link
    sent_chains.clear()
    vlink_item = MemeItem(
        id="vid1",
        blocks=[
            MemeBlock(
                type="video_link",
                url="https://www.bilibili.com/video/BV1xx",
                platform="bilibili",
                title="神级整活",
                cover_url="https://cdn.example.org/cover.webp",
            )
        ],
    )
    res4 = await sink_default.deliver(vlink_item, "discord:GroupMessage:555")
    assert res4.ok is True
    chain_elems = sent_chains[0][1].chain
    assert any(isinstance(e, Image) and e.url == "https://cdn.example.org/cover.webp" for e in chain_elems)
    assert any(isinstance(e, Plain) and "https://www.bilibili.com/video/BV1xx" in e.text for e in chain_elems)
