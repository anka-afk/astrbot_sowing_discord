"""Additional edge-case tests: RSS HTTP/304, WeChat MediaCache sink, >50 feedback batching, ambient tick."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from aiohttp import web
import pytest

from feedback.collector import FeedbackCollector
from main import SowingDiscordPlugin
from models.meme import MemeBlock, MemeItem
from sinks.astrbot_sink import AstrBotSink, Image
from sinks.media_cache import MediaCache
from sources.hub_source import HubSource
from sources.rss_source import RssSource
from storage.db import SowingDB


@pytest.mark.asyncio
async def test_rss_source_http_sync_and_304(tmp_path: Path) -> None:
    app = web.Application()
    etag = '"rss-v1"'
    xml_body = """<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0"><channel><title>Test RSS</title>
      <item><title>条目1</title><guid>g1</guid><description>好活&lt;img src="https://img.example.org/1.png"/&gt;</description></item>
    </channel></rss>"""

    async def handle_rss(req: web.Request) -> web.Response:
        if req.headers.get("If-None-Match") == etag:
            return web.Response(status=304)
        return web.Response(text=xml_body, content_type="application/xml", headers={"ETag": etag})

    app.router.add_get("/feed.xml", handle_rss)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]

    db = SowingDB(tmp_path / "rss.db")
    origin = f"http://127.0.0.1:{port}"
    src = RssSource(
        source_id="rss:test",
        feed_url=f"{origin}/feed.xml",
        db=db,
        trusted_origins=[origin],
    )
    try:
        r1 = await src.sync()
        assert len(r1.new_items) == 1
        r2 = await src.sync()
        assert r2.not_modified is True
    finally:
        await src.close()
        db.close()
        await runner.cleanup()


@pytest.mark.asyncio
async def test_wechat_sink_downloads_via_media_cache(tmp_path: Path) -> None:
    app = web.Application()

    async def handle_img(_req: web.Request) -> web.Response:
        return web.Response(body=b"RIFF\x00\x00\x00\x00WEBPVP8 ", content_type="image/webp")

    app.router.add_get("/pic.webp", handle_img)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]

    origin = f"http://127.0.0.1:{port}"
    cache = MediaCache(tmp_path / "mcache", trusted_origins=[origin])
    delivered: list[Any] = []

    async def fake_send(umo: str, chain: Any) -> dict[str, str]:
        delivered.append((umo, chain))
        return {"message_id": "wx-1"}

    sink = AstrBotSink(media_cache=cache, send_fn=fake_send, trusted_origins=[origin])
    try:
        item = MemeItem(
            id="wx-meme",
            blocks=[
                MemeBlock(
                    type="image",
                    url=f"http://127.0.0.1:{port}/pic.webp",
                    mime="image/webp",
                )
            ],
        )
        res = await sink.deliver(item, "gewechat:GroupMessage:room1")
        assert res.ok is True
        img_comp = [c for c in delivered[0][1].chain if isinstance(c, Image)][0]
        # WeChat platform must resolve to a local filesystem path via MediaCache
        assert img_comp.file != ""
        assert Path(img_comp.file).exists()
    finally:
        await cache.close()
        await runner.cleanup()


@pytest.mark.asyncio
async def test_feedback_collector_chunks_over_50_events(tmp_path: Path) -> None:
    db = SowingDB(tmp_path / "chunk.db")
    batches: list[int] = []

    class DummyHub(HubSource):
        async def send_feedback_batch(self, events: list[dict[str, Any]]) -> dict[str, Any]:
            assert len(events) <= 50
            batches.append(len(events))
            return {"accepted": len(events)}

    hub = DummyHub(source_id="hub:official-main", base_url="http://127.0.0.1:9", db=db, hub_id="official-main")
    collector = FeedbackCollector(db)
    try:
        for i in range(65):
            db.enqueue_feedback(
                hub_id="official-main",
                item_id=f"item-{i}",
                action="like",
                voter_hash=f"vh-{i}",
                group_hash="gh-1",
                occurred_at="2026-11-26T09:00:00Z",
            )
        flushed = await collector.flush({"official-main": hub})
        assert flushed == {"official-main": 65}
        assert batches == [50, 15]
        assert db.get_stats()["pending_feedback"] == 0
    finally:
        await hub.close()
        db.close()


@pytest.mark.asyncio
async def test_plugin_ambient_tick_and_onebot_notice(tmp_path: Path) -> None:
    plugin = SowingDiscordPlugin(
        context=None,
        config={
            "official_hub_enabled": False,
            "local_onebot_groups": ["50001"],
            "local_emoji_threshold": 2,
            "target_groups": ["aiocqhttp:GroupMessage:60001"],
            "quiet_hours": "00:00-00:00",  # disable quiet hours for deterministic test
            "llm_comment_prob": 0.0,
        },
        data_dir=tmp_path / "p_ambient",
        start_background_tasks=False,
    )
    sent: list[str] = []

    async def fake_send(umo: str, _chain: Any) -> dict[str, str]:
        sent.append(umo)
        return {"message_id": "amb-1"}

    plugin.sink._send_fn = fake_send

    async def fake_get_msg(mid: str) -> dict[str, Any]:
        return {"message_id": mid, "message": [{"type": "text", "data": {"text": "本地热梗"}}]}

    try:
        # Trigger local onebot collection via notice.group_msg_emoji_like
        await plugin.handle_onebot_notice(
            {
                "notice_type": "group_msg_emoji_like",
                "group_id": 50001,
                "message_id": "src-9",
                "user_id": 111,
                "likes": [{"emoji_id": "128077", "count": 2}],
                "is_add": True,
            },
            get_msg=fake_get_msg,
        )
        assert plugin.db.get_stats()["cached_items"] == 1

        # Simulate human message 20 minutes ago so target group is in LULL_READY
        twenty_min_ago = (datetime.now() - timedelta(minutes=20)).timestamp()
        plugin.air_reader.record_human_message("aiocqhttp:GroupMessage:60001", now_ts=twenty_min_ago)

        delivered = await plugin.run_ambient_tick()
        assert delivered == ["aiocqhttp:GroupMessage:60001"]
        assert sent == ["aiocqhttp:GroupMessage:60001"]

        # Running ambient tick again immediately must NOT send (now in COOLDOWN & last_sender_is_bot)
        delivered2 = await plugin.run_ambient_tick()
        assert delivered2 == []
    finally:
        await plugin.terminate()
