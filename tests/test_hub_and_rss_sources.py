"""Tests for 16-bit PoW, HubSource (/meta, /feed, ETag/304, tombstones, /random, backoff, federation), and RssSource."""

from __future__ import annotations

import hashlib
import time
from pathlib import Path
from typing import Any

import aiohttp
from aiohttp import web
import pytest

from sources.hub_source import HubSource, solve_pow, verify_pow
from sources.rss_source import RssSource, parse_feed_xml
from storage.db import SowingDB


def test_pow_solver_16_bits_matches_contract() -> None:
    nonce = "test-nonce-2026"
    t0 = time.perf_counter()
    solution = solve_pow(nonce, difficulty_bits=16)
    elapsed_ms = (time.perf_counter() - t0) * 1000.0

    digest = hashlib.sha256(f"{nonce}:{solution}".encode("utf-8")).digest()
    assert digest[0] == 0 and digest[1] == 0
    assert verify_pow(nonce, solution, difficulty_bits=16) is True
    assert verify_pow(nonce, "wrong-solution-xyz", difficulty_bits=16) is False or (
        hashlib.sha256(f"{nonce}:wrong-solution-xyz".encode("utf-8")).digest()[:2] == b"\x00\x00"
    )
    assert elapsed_ms < 5000.0


async def _start_mock_hub(state: dict[str, Any]) -> tuple[web.AppRunner, str]:
    app = web.Application()

    async def handle_meta(_req: web.Request) -> web.Response:
        return web.json_response(
            {
                "hub_id": state["hub_id"],
                "hub_name": state.get("hub_name", "Mock Hub"),
                "schema_version": "1.0",
                "capabilities": {"video_file": False, "feedback": True},
                "limits": {
                    "max_image_bytes": 10_485_760,
                    "max_gif_bytes": 5_242_880,
                    "max_images_per_submission": 9,
                    "max_chat_nodes": 20,
                    "max_chat_depth": 1,
                },
                "tags": ["沙雕", "群聊日常"],
            }
        )

    async def handle_challenge(_req: web.Request) -> web.Response:
        state["challenge_count"] = state.get("challenge_count", 0) + 1
        return web.json_response(
            {
                "challenge_id": "ch-001",
                "nonce_prefix": "prefix-abc",
                "difficulty_bits": 12,  # fast for unit test; 16-bit tested above
                "expires_at": "2099-01-01T00:00:00Z",
            }
        )

    async def handle_register(req: web.Request) -> web.Response:
        body = await req.json()
        assert body["challenge_id"] == "ch-001"
        assert verify_pow("prefix-abc", body["solution"], difficulty_bits=12)
        state["registered"] = True
        return web.json_response(
            {
                "instance_id": f"inst-{state['hub_id']}",
                "instance_token": f"tok-{state['hub_id']}",
            }
        )

    async def handle_feed(req: web.Request) -> web.Response:
        if state.get("fail_feed"):
            return web.Response(status=503, text="service unavailable")
        assert req.headers.get("Authorization") == f"Bearer tok-{state['hub_id']}"
        etag = state.get("etag", '"v1"')
        if req.headers.get("If-None-Match") == etag:
            return web.Response(status=304)

        after_seq = int(req.query.get("after_seq", "0"))
        items = [it for it in state.get("items", []) if it["seq"] > after_seq]
        tombstones = list(state.get("tombstones", []))
        next_seq = max([after_seq] + [it["seq"] for it in items] + [state.get("tombstone_seq", 0)])
        return web.json_response(
            {
                "items": items,
                "tombstones": tombstones,
                "next_seq": next_seq,
                "has_more": False,
            },
            headers={"ETag": etag},
        )

    async def handle_random(req: web.Request) -> web.Response:
        assert req.headers.get("Authorization") == f"Bearer tok-{state['hub_id']}"
        return web.json_response({"items": state.get("random_items", [])})

    async def handle_item(req: web.Request) -> web.Response:
        item_id = req.match_info["id"]
        if item_id in state.get("gone_ids", set()):
            return web.Response(status=410, text="Gone")
        return web.Response(status=404)

    app.router.add_get("/api/v1/meta", handle_meta)
    app.router.add_get("/api/v1/instances/challenge", handle_challenge)
    app.router.add_post("/api/v1/instances/register", handle_register)
    app.router.add_get("/api/v1/feed", handle_feed)
    app.router.add_get("/api/v1/random", handle_random)
    app.router.add_get("/api/v1/items/{id}", handle_item)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    return runner, f"http://127.0.0.1:{port}"


@pytest.mark.asyncio
async def test_hub_source_full_lifecycle_and_tombstones(tmp_path: Path) -> None:
    state: dict[str, Any] = {
        "hub_id": "official-main",
        "etag": '"etag-1"',
        "items": [
            {
                "schema_version": "1.0",
                "id": "01JITEM001",
                "seq": 10,
                "hub_id": "official-main",
                "title": "第一条",
                "summary": "梗概1",
                "tags": ["沙雕"],
                "nsfw_level": 0,
                "score": 8.0,
                "blocks": [{"type": "text", "text": "笑死我了"}],
            },
            {
                "schema_version": "1.0",
                "id": "01JITEM002",
                "seq": 11,
                "hub_id": "official-main",
                "title": "第二条",
                "summary": "梗概2",
                "tags": ["群聊日常"],
                "nsfw_level": 1,
                "score": 9.0,
                "blocks": [{"type": "text", "text": "绷不住了"}],
            },
        ],
        "random_items": [
            {
                "schema_version": "1.0",
                "id": "01JRAND001",
                "seq": 5,
                "hub_id": "official-main",
                "title": "老库补仓",
                "summary": "经典老史",
                "tags": ["沙雕"],
                "nsfw_level": 0,
                "score": 15.0,
                "blocks": [{"type": "text", "text": "老史一条"}],
            }
        ],
        "gone_ids": {"01JITEM002"},
    }
    runner, base_url = await _start_mock_hub(state)
    db = SowingDB(tmp_path / "test.db")
    hub = HubSource(source_id="hub:official-main", base_url=base_url, db=db, hub_id="official-main")
    try:
        # 1. Sync triggers /meta + PoW registration + /feed
        r1 = await hub.sync()
        assert r1.error is None
        assert len(r1.new_items) == 2
        assert state["registered"] is True
        cursor = db.get_source_cursor("hub:official-main")
        assert cursor is not None
        assert cursor["instance_id"] == "inst-official-main"
        assert cursor["instance_token"] == "tok-official-main"
        assert len(cursor["instance_salt"]) == 64
        assert cursor["next_seq"] == 11

        # 2. Second sync with same ETag returns 304 Not Modified without re-registering
        r2 = await hub.sync()
        assert r2.not_modified is True
        assert state["challenge_count"] == 1

        # 3. Hub emits tombstone for 01JITEM001 -> immediately purged from local items_cache
        state["etag"] = '"etag-2"'
        state["tombstones"] = ["01JITEM001"]
        state["tombstone_seq"] = 12
        r3 = await hub.sync()
        assert r3.tombstones_removed == 1
        assert db.get_item("hub:official-main:01JITEM001") is None
        assert db.get_item("hub:official-main:01JITEM002") is not None

        # 4. /random batch replenishment caches items locally
        rand_items = await hub.fetch_random(limit=20, tags=["沙雕"])
        assert len(rand_items) == 1
        assert db.get_item("hub:official-main:01JRAND001") is not None

        # 5. /items/{id} returning 410 Gone evicts cached item
        assert await hub.get_item("01JITEM002") is None
        assert db.get_item("hub:official-main:01JITEM002") is None

        # 6. Failure triggers exponential backoff circuit breaker
        state["etag"] = '"etag-3"'
        state["fail_feed"] = True
        r_fail = await hub.sync()
        assert r_fail.error is not None
        assert hub.is_in_backoff() is True
        r_skip = await hub.sync()
        assert r_skip.skipped_backoff is True
    finally:
        await hub.close()
        db.close()
        await runner.cleanup()


@pytest.mark.asyncio
async def test_multi_hub_federation_isolation(tmp_path: Path) -> None:
    state_a: dict[str, Any] = {
        "hub_id": "hub-alpha",
        "etag": '"a1"',
        "items": [
            {
                "schema_version": "1.0",
                "id": "SAME_ID",
                "seq": 1,
                "hub_id": "hub-alpha",
                "title": "Alpha",
                "tags": ["A"],
                "nsfw_level": 0,
                "blocks": [{"type": "text", "text": "from alpha"}],
            }
        ],
    }
    state_b: dict[str, Any] = {
        "hub_id": "hub-beta",
        "etag": '"b1"',
        "items": [
            {
                "schema_version": "1.0",
                "id": "SAME_ID",
                "seq": 1,
                "hub_id": "hub-beta",
                "title": "Beta",
                "tags": ["B"],
                "nsfw_level": 0,
                "blocks": [{"type": "text", "text": "from beta"}],
            }
        ],
    }
    runner_a, url_a = await _start_mock_hub(state_a)
    runner_b, url_b = await _start_mock_hub(state_b)
    db = SowingDB(tmp_path / "fed.db")
    hub_a = HubSource(source_id="hub:hub-alpha", base_url=url_a, db=db, hub_id="hub-alpha")
    hub_b = HubSource(source_id="hub:hub-beta", base_url=url_b, db=db, hub_id="hub-beta")
    try:
        await hub_a.sync()
        await hub_b.sync()
        # Both exist independently even with identical item id!
        assert db.get_item("hub:hub-alpha:SAME_ID") is not None
        assert db.get_item("hub:hub-beta:SAME_ID") is not None
        # Tombstone from hub-alpha MUST NOT touch hub-beta!
        db.delete_items_by_hub_item_ids("hub-alpha", ["SAME_ID"])
        assert db.get_item("hub:hub-alpha:SAME_ID") is None
        assert db.get_item("hub:hub-beta:SAME_ID") is not None
    finally:
        await hub_a.close()
        await hub_b.close()
        db.close()
        await runner_a.cleanup()
        await runner_b.cleanup()


def test_rss_and_atom_pure_stdlib_parsing() -> None:
    rss_xml = """<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0">
      <channel>
        <title>沙雕日报</title>
        <item>
          <title>今日份乐子</title>
          <link>https://example.org/post/1</link>
          <guid>post-1</guid>
          <category>沙雕</category>
          <description><![CDATA[<p>笑死我了</p><img src="https://img.example.org/pic.jpg"/>]]></description>
          <enclosure url="https://img.example.org/anim.gif" type="image/gif" length="12345"/>
        </item>
      </channel>
    </rss>"""
    rss_items = parse_feed_xml(rss_xml, source_id="rss:daily", default_tags=["RSS"])
    assert len(rss_items) == 1
    item = rss_items[0]
    assert item.uid.startswith("rss:")
    assert item.title == "今日份乐子"
    assert "沙雕" in item.tags and "RSS" in item.tags
    types = [b.type for b in item.blocks]
    assert "text" in types and "gif" in types and "image" in types

    atom_xml = """<?xml version="1.0" encoding="utf-8"?>
    <feed xmlns="http://www.w3.org/2005/Atom">
      <title>Atom Meme Feed</title>
      <entry>
        <title>Atom Entry</title>
        <id>urn:uuid:1234</id>
        <link href="https://example.org/atom/1" rel="alternate"/>
        <category term="科技"/>
        <content type="html">&lt;div&gt;很有趣&lt;img src="https://img.example.org/atom.png"/&gt;&lt;/div&gt;</content>
      </entry>
    </feed>"""
    atom_items = parse_feed_xml(atom_xml, source_id="rss:atom")
    assert len(atom_items) == 1
    assert atom_items[0].title == "Atom Entry"
    assert any(b.type == "image" and b.url == "https://img.example.org/atom.png" for b in atom_items[0].blocks)
