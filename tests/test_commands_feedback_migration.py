"""Tests for /投稿 (anonymization, video block, duplicate early-stop), feedback blinding & batch flush, /史库, and v1->v2 migration."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from aiohttp import web
import pytest

from ambient.air_reader import AirReader
from commands.admin import AdminHandler
from commands.manual_pull import ManualPullHandler
from commands.report import ReportHandler
from commands.submit import SubmitHandler, anonymize_chat_nodes
from feedback.collector import FeedbackCollector, compute_voter_hash
from main import SowingDiscordPlugin
from models.meme import MemeBlock, MemeItem
from migration import detect_v1_config, migrate_v1_to_v2
from pipeline.commentary import CommentaryGenerator
from pipeline.dedup import Deduplicator
from pipeline.filter import MemeFilter
from sinks.astrbot_sink import AstrBotSink
from sources.hub_source import HubSource, verify_pow
from storage.db import SowingDB


def test_anonymize_chat_nodes_depth_limit_and_node_cap() -> None:
    raw_nodes: list[dict[str, Any]] = []
    for i in range(25):
        raw_nodes.append(
            {
                "user_id": 100000 + (i % 3),
                "nickname": f"RealUser_{i}",
                "group_id": 888888,
                "avatar": "https://q.qlogo.cn/real.jpg",
                "time_label": f"12:{i:02d}",
                "content": [{"type": "text", "text": f"msg-{i}"}],
                "nested_nodes": (
                    [
                        {
                            "user_id": 999999,
                            "content": [{"type": "text", "text": "level-1"}],
                            "nested_nodes": [
                                {
                                    "user_id": 777777,
                                    "content": [{"type": "text", "text": "level-2-should-fold"}],
                                }
                            ],
                        }
                    ]
                    if i == 0
                    else None
                ),
            }
        )

    clean = anonymize_chat_nodes(raw_nodes, max_depth=1, max_nodes=20)
    assert len(clean) == 20
    serialized = json.dumps(clean, ensure_ascii=False)
    # Real QQ numbers, group IDs, nicknames, avatars must be completely absent
    assert "100000" not in serialized
    assert "888888" not in serialized
    assert "RealUser_" not in serialized
    assert "qlogo.cn" not in serialized
    # Senders mapped in order of appearance
    assert clean[0]["sender_alias"] == "群友A"
    assert clean[0]["avatar_color_idx"] == 0
    assert clean[1]["sender_alias"] == "群友B"
    # Level-2 nested forward folded
    assert "level-2-should-fold" not in serialized
    assert "[嵌套聊天记录已折叠]" in serialized


async def _start_submit_and_feedback_hub(state: dict[str, Any]) -> tuple[web.AppRunner, str]:
    app = web.Application()

    async def handle_meta(_req: web.Request) -> web.Response:
        return web.json_response(
            {
                "hub_id": "official-main",
                "hub_name": "Test Hub",
                "schema_version": "1.0",
                "capabilities": {"video_file": False, "feedback": True},
                "limits": {
                    "max_image_bytes": 10_485_760,
                    "max_gif_bytes": 5_242_880,
                    "max_images_per_submission": 9,
                    "max_chat_nodes": 20,
                    "max_chat_depth": 1,
                },
                "tags": ["沙雕"],
            }
        )

    async def handle_challenge(_req: web.Request) -> web.Response:
        return web.json_response(
            {
                "challenge_id": "ch-sub",
                "nonce_prefix": "sub-prefix",
                "difficulty_bits": 8,
                "expires_at": "2099-01-01T00:00:00Z",
            }
        )

    async def handle_register(req: web.Request) -> web.Response:
        body = await req.json()
        assert verify_pow("sub-prefix", body["solution"], difficulty_bits=8)
        return web.json_response({"instance_id": "inst-1", "instance_token": "tok-1"})

    async def handle_upload(req: web.Request) -> web.Response:
        assert req.headers.get("Authorization") == "Bearer tok-1"
        state["upload_headers"].append(dict(req.headers))
        reader = await req.multipart()
        field = await reader.next()
        assert field is not None and field.name == "file"
        content = await field.read()
        state["uploads"].append(content)
        if state.get("dup_mode") == "approved":
            return web.json_response(
                {
                    "media_id": "m-dup",
                    "duplicate": True,
                    "match_type": "exact",
                    "existing_status": "approved",
                    "existing_item_id": "01JEXISTING",
                }
            )
        if state.get("dup_mode") == "pending":
            return web.json_response(
                {
                    "media_id": "m-dup-p",
                    "duplicate": True,
                    "match_type": "similar",
                    "existing_status": "pending",
                    "existing_item_id": None,
                }
            )
        idx = len(state["uploads"])
        return web.json_response(
            {
                "media_id": f"media-{idx}",
                "duplicate": False,
                "existing_status": None,
                "existing_item_id": None,
            }
        )

    async def handle_submit(req: web.Request) -> web.Response:
        assert req.headers.get("Authorization") == "Bearer tok-1"
        state["submit_headers"].append(dict(req.headers))
        body = await req.json()
        state["submissions"].append(body)
        return web.json_response({"submission_id": "sub-001", "status": "pending"})

    async def handle_feedback(req: web.Request) -> web.Response:
        assert req.headers.get("Authorization") == "Bearer tok-1"
        body = await req.json()
        assert len(body["events"]) <= 50
        state["feedback_batches"].append(body["events"])
        return web.json_response({"accepted": len(body["events"])})

    app.router.add_get("/api/v1/meta", handle_meta)
    app.router.add_get("/api/v1/instances/challenge", handle_challenge)
    app.router.add_post("/api/v1/instances/register", handle_register)
    app.router.add_post("/api/v1/submissions/media", handle_upload)
    app.router.add_post("/api/v1/submissions", handle_submit)
    app.router.add_post("/api/v1/feedback/batch", handle_feedback)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    return runner, f"http://127.0.0.1:{port}"


@pytest.mark.asyncio
async def test_submit_two_step_video_block_and_duplicate_early_stop(tmp_path: Path) -> None:
    state: dict[str, Any] = {
        "uploads": [],
        "upload_headers": [],
        "submissions": [],
        "submit_headers": [],
        "feedback_batches": [],
    }
    runner, base_url = await _start_submit_and_feedback_hub(state)
    db = SowingDB(tmp_path / "sub.db")
    hub = HubSource(
        source_id="hub:official-main",
        base_url=base_url,
        db=db,
        hub_id="official-main",
        user_token="utok-trusted-999",
    )
    handler = SubmitHandler(hub)
    try:
        # 1. Raw local video is rejected immediately with zero uploads
        vid_res = await handler.submit(videos=[{"file": "a.mp4"}])
        assert vid_res.ok is False
        assert "暂不支持投稿原始视频" in vid_res.message
        assert state["uploads"] == []

        # 2. Normal two-step submission with anonymized chat_record + image + X-User-Token
        ok_res = await handler.submit(
            title="绝世好活",
            tags=["沙雕"],
            images=[{"data_bytes": b"\x89PNG\r\n\x1a\nfake", "filename": "a.png", "mime": "image/png"}],
            chat_nodes=[
                {
                    "user_id": 123456789,
                    "content": [
                        {"type": "text", "text": "看图"},
                        {"type": "image", "data_bytes": b"GIF89afake", "filename": "b.gif", "mime": "image/gif"},
                    ],
                }
            ],
        )
        assert ok_res.ok is True
        assert ok_res.submission_id == "sub-001"
        assert len(state["uploads"]) == 2
        assert state["submit_headers"][0].get("X-User-Token") == "utok-trusted-999"
        chat_blk = [b for b in state["submissions"][0]["blocks"] if b["type"] == "chat_record"][0]
        assert "rendered_images" not in chat_blk
        assert chat_blk["messages"][0]["sender_alias"] == "群友A"
        assert chat_blk["messages"][0]["content"][1] == {"type": "image", "media_id": "media-2"}

        # 3. Duplicate early-stop on approved -> stops after 1st image, does NOT call /submissions
        state["uploads"].clear()
        state["submissions"].clear()
        state["dup_mode"] = "approved"
        dup_approved = await handler.submit(
            images=[
                {"data_bytes": b"img1", "filename": "1.png", "mime": "image/png"},
                {"data_bytes": b"img2", "filename": "2.png", "mime": "image/png"},
            ]
        )
        assert dup_approved.ok is False
        assert dup_approved.duplicate is True
        assert dup_approved.existing_item_id == "01JEXISTING"
        assert len(state["uploads"]) == 1  # early stop!
        assert state["submissions"] == []

        # 4. Duplicate early-stop on pending -> existing_item_id is None
        state["uploads"].clear()
        state["dup_mode"] = "pending"
        dup_pending = await handler.submit(
            images=[{"data_bytes": b"img3", "filename": "3.png", "mime": "image/png"}]
        )
        assert dup_pending.ok is False
        assert dup_pending.duplicate is True
        assert dup_pending.existing_item_id is None
        assert state["submissions"] == []
    finally:
        await hub.close()
        db.close()
        await runner.cleanup()


@pytest.mark.asyncio
async def test_feedback_blinding_signals_and_batch_flush(tmp_path: Path) -> None:
    state: dict[str, Any] = {
        "uploads": [],
        "upload_headers": [],
        "submissions": [],
        "submit_headers": [],
        "feedback_batches": [],
    }
    runner, base_url = await _start_submit_and_feedback_hub(state)
    db = SowingDB(tmp_path / "fb.db")
    hub = HubSource(source_id="hub:official-main", base_url=base_url, db=db, hub_id="official-main")
    hub_map = {"official-main": hub}
    collector = FeedbackCollector(db)
    report_handler = ReportHandler(collector, hub_map)

    try:
        # Ensure salt exists
        await hub.ensure_registered()
        salt = hub.get_instance_salt()

        # Record a sent hub meme
        group_umo = "aiocqhttp:GroupMessage:555000"
        db.mark_sent(group_umo, "hub:official-main:01JMEME1", bot_msg_id="msg-888")

        # 1. Emoji like -> like
        id1 = collector.collect_emoji_like(
            group_umo=group_umo,
            bot_msg_id="msg-888",
            user_id="u100",
            emoji_id="128077",
            is_add=True,
            hub_sources=hub_map,
        )
        assert id1 is not None

        # 2. Quoted keyword reply -> dislike
        id2 = collector.collect_reply_text(
            group_umo=group_umo,
            quoted_bot_msg_id="msg-888",
            user_id="u200",
            reply_text="太火星了看过了",
            hub_sources=hub_map,
        )
        assert id2 is not None

        # 3. /烂史 隐私泄露 -> report (privacy_leak)
        rep = report_handler.handle_report(
            group_umo=group_umo,
            user_id="u300",
            quoted_bot_msg_id="msg-888",
            raw_reason="隐私泄露",
        )
        assert rep.ok is True
        assert rep.reason == "privacy_leak"

        # Verify exact blinding hash formula: sha256(instance_salt + group_id + user_id)
        expected_voter_hash = hashlib.sha256(f"{salt}555000u300".encode("utf-8")).hexdigest()
        assert compute_voter_hash(salt, "555000", "u300") == expected_voter_hash

        # Flush batch to Hub
        flushed = await collector.flush(hub_map)
        assert flushed == {"official-main": 3}
        assert len(state["feedback_batches"]) == 1
        events = state["feedback_batches"][0]
        assert [e["action"] for e in events] == ["like", "dislike", "report"]
        assert events[2]["voter_hash"] == expected_voter_hash
        assert events[2]["reason"] == "privacy_leak"
        assert db.get_stats()["pending_feedback"] == 0
    finally:
        await hub.close()
        db.close()
        await runner.cleanup()


@pytest.mark.asyncio
async def test_v1_to_v2_migration_admin_and_plugin_boot(tmp_path: Path) -> None:
    v1_cfg = {
        "banshi_group_list": ["11111", "22222"],
        "banshi_target_list": ["aiocqhttp:GroupMessage:99999"],
        "banshi_cooldown_minutes": 45,
        "banshi_emoji_threshold": 4,
        "banshi_quiet_hours": "00:00-07:00",
    }
    assert detect_v1_config(v1_cfg) is True

    migrated, report = migrate_v1_to_v2(v1_cfg, tmp_path)
    assert report.migrated is True
    assert report.backup_path is not None
    backup_file = Path(report.backup_path)
    assert backup_file.name == "config.v1.bak.json"
    assert json.loads(backup_file.read_text(encoding="utf-8"))["banshi_group_list"] == ["11111", "22222"]

    assert migrated["target_groups"] == ["aiocqhttp:GroupMessage:99999"]
    assert migrated["local_onebot_groups"] == ["11111", "22222"]
    assert migrated["cooldown_sec"] == 2700
    assert migrated["local_emoji_threshold"] == 4
    assert migrated["quiet_hours"] == "00:00-07:00"
    assert migrated["official_hub_enabled"] is True
    assert not any(k.startswith("banshi_") for k in migrated)

    # Boot full plugin without real AstrBot installed using v1 config
    plugin = SowingDiscordPlugin(
        context=None,
        config=v1_cfg,
        data_dir=tmp_path / "plugin_data",
        start_background_tasks=False,
    )
    try:
        assert plugin.migration_report.migrated is True
        assert plugin.local_onebot_source is not None
        assert plugin.local_onebot_source.source_groups == {"11111", "22222"}

        # Admin commands
        g = "aiocqhttp:GroupMessage:99999"
        status_text = plugin.admin_handler.handle(g, "状态")
        assert "搬史 v2 史库状态" in status_text
        assert "迁移提示" in status_text

        assert "关闭" in plugin.admin_handler.handle(g, "开关 关")
        assert "开启" in plugin.admin_handler.handle(g, "开关 开")
        assert "地狱" in plugin.admin_handler.handle(g, "屏蔽 添加 地狱")
        assert "2（高能预警）" in plugin.admin_handler.handle(g, "分级 2")
        assert "120 分钟" in plugin.admin_handler.handle(g, "读空气 冷却 120")
        assert "official-main" in plugin.admin_handler.handle(g, "订阅")

        # ManualPullHandler delivers cached item cleanly
        delivered: list[Any] = []

        async def fake_send(umo: str, chain: Any) -> dict[str, str]:
            delivered.append((umo, chain))
            return {"message_id": "m-pull-1"}

        plugin.sink._send_fn = fake_send
        plugin.db.upsert_item(
            MemeItem(
                id="m-local-1",
                hub_id="official-main",
                title="手拉测试",
                tags=["沙雕"],
                nsfw_level=0,
                blocks=[MemeBlock(type="text", text="手动拉取成功")],
            ),
            source_id="hub:official-main",
        )
        pull_res = await plugin.manual_pull_handler.pull_and_deliver(g, tag="沙雕")
        assert pull_res.ok is True
        assert len(delivered) == 1
        assert plugin.db.is_sent(g, "hub:official-main:m-local-1") is True
    finally:
        await plugin.terminate()
