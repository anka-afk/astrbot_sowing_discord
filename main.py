"""AstrBot Star entry point for astrbot_sowing_discord v2."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
import inspect
import json
import logging
import math
import time
from pathlib import Path
from typing import Any, Callable, Optional

# Graceful compatibility when AstrBot is not installed (unit/integration test environments)
try:
    from astrbot.api import AstrBotConfig, logger as astr_logger  # type: ignore
    from astrbot.api.event import AstrMessageEvent, filter  # type: ignore
    from astrbot.api.star import Context, Star, StarTools, register  # type: ignore
except ImportError:
    astr_logger = logging.getLogger("astrbot_sowing_discord")
    AstrBotConfig = dict  # type: ignore
    AstrMessageEvent = Any  # type: ignore
    Context = Any  # type: ignore

    class Star:  # type: ignore[no-redef]
        def __init__(self, context: Any = None):
            self.context = context

    class StarTools:  # type: ignore[no-redef]
        @staticmethod
        def get_data_dir(plugin_name: str = "astrbot_sowing_discord") -> Path:
            p = Path.cwd() / ".data" / plugin_name
            p.mkdir(parents=True, exist_ok=True)
            return p

    def register(*_args: Any, **_kwargs: Any):  # type: ignore[no-redef]
        def _decorator(cls: Any) -> Any:
            return cls
        return _decorator

    class _DummyFilter:
        class EventMessageType:
            GROUP_MESSAGE = "group"

        @staticmethod
        def command(*_args: Any, **_kwargs: Any):
            def _dec(fn: Any) -> Any:
                return fn
            return _dec

        @staticmethod
        def event_message_type(*_args: Any, **_kwargs: Any):
            def _dec(fn: Any) -> Any:
                return fn
            return _dec

        @staticmethod
        def platform_adapter_type(*_args: Any, **_kwargs: Any):
            def _dec(fn: Any) -> Any:
                return fn
            return _dec

    filter = _DummyFilter()  # type: ignore[no-redef]

try:
    from .ambient.air_reader import AirReader
    from .commands.admin import AdminHandler
    from .commands.manual_pull import ManualPullHandler
    from .commands.report import ReportHandler
    from .commands.submit import SubmitHandler
    from .feedback.collector import FeedbackCollector
    from .migration import MigrationReport, migrate_v1_to_v2
    from .pipeline.commentary import CommentaryGenerator
    from .pipeline.dedup import Deduplicator
    from .pipeline.filter import MemeFilter
    from .sinks.astrbot_sink import AstrBotSink
    from .sinks.media_cache import MediaCache
    from .sources.base import BaseSource
    from .sources.hub_source import HubSource
    from .sources.local_onebot_source import LocalOneBotSource
    from .sources.rss_source import RssSource
    from .storage.db import SowingDB
except ImportError:
    from ambient.air_reader import AirReader
    from commands.admin import AdminHandler
    from commands.manual_pull import ManualPullHandler
    from commands.report import ReportHandler
    from commands.submit import SubmitHandler
    from feedback.collector import FeedbackCollector
    from migration import MigrationReport, migrate_v1_to_v2
    from pipeline.commentary import CommentaryGenerator
    from pipeline.dedup import Deduplicator
    from pipeline.filter import MemeFilter
    from sinks.astrbot_sink import AstrBotSink
    from sinks.media_cache import MediaCache
    from sources.base import BaseSource
    from sources.hub_source import HubSource
    from sources.local_onebot_source import LocalOneBotSource
    from sources.rss_source import RssSource
    from storage.db import SowingDB


def _parse_extra_hub_entry(entry: Any, idx: int) -> Optional[dict[str, Any]]:
    if isinstance(entry, dict):
        url = str(entry.get("url", "")).strip()
        if not url:
            return None
        return {
            "id": str(entry.get("id") or f"custom-hub-{idx}"),
            "url": url,
            "enabled": bool(entry.get("enabled", True)),
            "user_token": str(entry.get("user_token", "") or ""),
            "tags": list(entry.get("tags") or []),
            "exclude_tags": list(entry.get("exclude_tags") or []),
            "max_nsfw": int(entry.get("max_nsfw", 1)),
        }
    if isinstance(entry, str):
        raw = entry.strip()
        if not raw:
            return None
        if raw.startswith("{"):
            try:
                return _parse_extra_hub_entry(json.loads(raw), idx)
            except json.JSONDecodeError:
                return None
        return {
            "id": f"custom-hub-{idx}",
            "url": raw,
            "enabled": True,
            "user_token": "",
            "tags": [],
            "exclude_tags": [],
            "max_nsfw": 1,
        }
    return None


def _is_bot_admin(event: Any) -> bool:
    """Return True strictly if sender is an AstrBot bot administrator."""
    is_admin_attr = getattr(event, "is_admin", None)
    if callable(is_admin_attr):
        try:
            res = is_admin_attr()
        except Exception:
            return False
        if inspect.iscoroutine(res):
            res.close()
            return False
        return res is True
    if isinstance(is_admin_attr, bool):
        return is_admin_attr
    return str(getattr(event, "role", "") or "").strip().lower() in ("admin", "superadmin")


def _is_group_or_bot_admin(event: Any) -> bool:
    """Return True if sender is an AstrBot bot admin OR verified group owner/admin."""
    if _is_bot_admin(event):
        return True

    event_role = str(getattr(event, "role", "") or "").strip().lower()
    if event_role in ("owner", "admin"):
        return True

    msg_obj = getattr(event, "message_obj", None)
    if msg_obj is not None:
        sender = getattr(msg_obj, "sender", None)
        if sender is not None:
            sender_role = (
                sender.get("role", "")
                if isinstance(sender, dict)
                else getattr(sender, "role", "")
            )
            if str(sender_role or "").strip().lower() in ("owner", "admin"):
                return True

        raw = getattr(msg_obj, "raw_message", None)
        if isinstance(raw, dict):
            raw_sender = raw.get("sender")
            if isinstance(raw_sender, dict):
                raw_role = str(raw_sender.get("role", "") or "").strip().lower()
                if raw_role in ("owner", "admin"):
                    return True

    return False


class CommandRateLimiter:
    """Lightweight bounded LRU in-memory per-user & per-group cooldown tracker."""

    def __init__(
        self,
        *,
        user_cooldown_sec: float = 10.0,
        group_cooldown_sec: float = 5.0,
        max_entries: int = 2048,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.user_cooldown_sec = max(0.0, float(user_cooldown_sec))
        self.group_cooldown_sec = max(0.0, float(group_cooldown_sec))
        self.max_entries = max(64, int(max_entries))
        self._clock = clock
        self._user_last: OrderedDict[tuple[str, str], float] = OrderedDict()
        self._group_last: OrderedDict[tuple[str, str], float] = OrderedDict()

    def _prune(self, store: OrderedDict[tuple[str, str], float], now: float, ttl: float) -> None:
        if len(store) <= self.max_entries:
            return
        cutoff = now - max(ttl, 60.0)
        stale = [k for k, ts in store.items() if ts < cutoff]
        for k in stale:
            store.pop(k, None)
        while len(store) > self.max_entries:
            store.popitem(last=False)

    def check_and_record(
        self,
        command: str,
        *,
        group_umo: str = "",
        user_id: str = "",
        bypass: bool = False,
    ) -> float:
        """
        Check cooldowns and record invocation if allowed.

        Returns remaining wait seconds (> 0.0 if rate-limited, 0.0 if allowed).
        """
        if bypass:
            return 0.0
        now = self._clock()
        rem_user = 0.0
        rem_group = 0.0

        if user_id and self.user_cooldown_sec > 0:
            ukey = (command, user_id)
            last_u = self._user_last.get(ukey)
            if last_u is not None:
                rem_user = max(0.0, self.user_cooldown_sec - (now - last_u))

        if group_umo and self.group_cooldown_sec > 0:
            gkey = (command, group_umo)
            last_g = self._group_last.get(gkey)
            if last_g is not None:
                rem_group = max(0.0, self.group_cooldown_sec - (now - last_g))

        remaining = max(rem_user, rem_group)
        if remaining > 0.0:
            return remaining

        if user_id and self.user_cooldown_sec > 0:
            ukey = (command, user_id)
            self._user_last[ukey] = now
            self._user_last.move_to_end(ukey)
            self._prune(self._user_last, now, self.user_cooldown_sec)
        if group_umo and self.group_cooldown_sec > 0:
            gkey = (command, group_umo)
            self._group_last[gkey] = now
            self._group_last.move_to_end(gkey)
            self._prune(self._group_last, now, self.group_cooldown_sec)
        return 0.0


def _extract_quoted_msg_id(event: Any) -> str:
    """Best-effort extraction of quoted/replied message ID from an AstrBot event."""
    msg_obj = getattr(event, "message_obj", None)
    if msg_obj is not None:
        raw = getattr(msg_obj, "raw_message", None)
        if isinstance(raw, dict):
            for seg in raw.get("message") or []:
                if isinstance(seg, dict) and seg.get("type") == "reply":
                    mid = (seg.get("data") or {}).get("id")
                    if mid:
                        return str(mid)
        for comp in getattr(msg_obj, "message", None) or []:
            if type(comp).__name__.lower() == "reply":
                mid = getattr(comp, "id", None) or getattr(comp, "message_id", None)
                if mid:
                    return str(mid)
    return ""


def _extract_submission_parts(event: Any) -> dict[str, Any]:
    """Extract text, images, videos, and chat_nodes from an AstrBot message event."""
    images: list[dict[str, Any]] = []
    videos: list[dict[str, Any]] = []
    chat_nodes: list[dict[str, Any]] = []
    text_parts: list[str] = []

    msg_obj = getattr(event, "message_obj", None)
    components = getattr(msg_obj, "message", None) or []
    for comp in components:
        cname = type(comp).__name__.lower()
        if cname == "plain":
            txt = str(getattr(comp, "text", "") or "").strip()
            if txt:
                text_parts.append(txt)
        elif cname == "image":
            url = str(getattr(comp, "url", "") or getattr(comp, "file", "") or "").strip()
            if url:
                images.append({"url": url})
        elif cname == "video":
            videos.append({"file": str(getattr(comp, "file", "") or "")})
        elif cname in ("nodes", "node", "forward"):
            nodes = getattr(comp, "nodes", None) or getattr(comp, "content", None) or []
            if isinstance(nodes, list):
                for n in nodes:
                    if isinstance(n, dict):
                        chat_nodes.append(n)

    return {
        "text": "\n".join(text_parts).strip(),
        "images": images,
        "videos": videos,
        "chat_nodes": chat_nodes,
    }


@register(
    "astrbot_plugin_sowing_discord",
    "Anka",
    "搬史自动化 2.0：多源可插拔跨平台群聊搬史插件",
    "2.0.1",
    "https://github.com/anka/astrbot_plugin_sowing_discord",
)
class SowingDiscordPlugin(Star):
    """AstrBot v2 plugin implementing Source -> Pipeline -> Sink & 5-state AirReader."""

    def __init__(
        self,
        context: Context = None,
        config: Optional[dict[str, Any]] = None,
        *,
        data_dir: Optional[str | Path] = None,
        start_background_tasks: bool = True,
    ):
        super().__init__(context)
        self.data_dir = (
            Path(data_dir)
            if data_dir is not None
            else Path(StarTools.get_data_dir("astrbot_sowing_discord"))
        )
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # 1. Detect & migrate legacy v1 config if present
        raw_cfg = dict(config or {})
        migrated_cfg, report = migrate_v1_to_v2(raw_cfg, self.data_dir)
        self.config: dict[str, Any] = migrated_cfg
        self.migration_report: MigrationReport = report
        if report.migrated:
            astr_logger.info(report.summary)
            # Persist migrated keys back if AstrBotConfig supports save_config()
            if config is not None and hasattr(config, "save_config"):
                try:
                    config.clear()  # type: ignore[attr-defined]
                    config.update(migrated_cfg)  # type: ignore[attr-defined]
                    config.save_config()  # type: ignore[attr-defined]
                except Exception as exc:
                    astr_logger.warning("Failed to save migrated AstrBotConfig: %s", exc)

        # 2. Storage & Cache (allow admin-configured Hub base URLs as trusted origins for LAN/self-hosted Hubs)
        self.db = SowingDB(self.data_dir / "sowing_v2.db")
        self.trusted_origins: tuple[str, ...] = self._collect_trusted_hub_origins()
        cache_max_bytes = int(self.config.get("media_cache_max_mb", 200)) * 1024 * 1024
        self.media_cache = MediaCache(
            self.data_dir / "media_cache",
            max_bytes=cache_max_bytes,
            trusted_origins=self.trusted_origins,
        )

        # 3. Default group settings
        self.default_group_settings: dict[str, Any] = {
            "enabled": True,
            "ambient_enabled": bool(self.config.get("ambient_enabled", True)),
            "quiet_hours": str(self.config.get("quiet_hours", "01:00-08:00")),
            "cooldown_sec": int(self.config.get("cooldown_sec", 5400)),
            "max_nsfw": int(self.config.get("default_max_nsfw", 1)),
            "blocked_tags": list(self.config.get("default_blocked_tags") or []),
            "llm_comment_prob": float(self.config.get("llm_comment_prob", 0.3)),
        }

        # 4. Sources (Official Hub + Extra Federated Hubs + RSS + Local OneBot)
        self.sources: list[BaseSource] = []
        self.hub_sources: dict[str, HubSource] = {}
        self.local_onebot_source: Optional[LocalOneBotSource] = None
        self._init_sources()

        # 5. Pipeline, Sink, Ambient AirReader, Feedback
        self.dedup = Deduplicator(self.db)
        self.meme_filter = MemeFilter()
        self.commentary = CommentaryGenerator(
            context=context,
            timeout_sec=float(self.config.get("llm_timeout_sec", 5.0)),
        )
        self.sink = AstrBotSink(
            context=context,
            media_cache=self.media_cache,
            qq_native_forward=bool(self.config.get("qq_native_forward", False)),
            trusted_origins=self.trusted_origins,
        )
        self.air_reader = AirReader(
            default_quiet_hours=self.default_group_settings["quiet_hours"],
            default_cooldown_sec=self.default_group_settings["cooldown_sec"],
            active_chat_threshold_5m=int(self.config.get("active_chat_threshold_5m", 6)),
            lull_min_sec=int(self.config.get("lull_min_sec", 900)),
            lull_max_sec=int(self.config.get("lull_max_sec", 2700)),
            dead_group_sec=int(self.config.get("dead_group_sec", 86400)),
        )
        self.feedback_collector = FeedbackCollector(self.db)

        # 6. Command handlers
        self.manual_pull_handler = ManualPullHandler(
            db=self.db,
            dedup=self.dedup,
            meme_filter=self.meme_filter,
            sink=self.sink,
            commentary=self.commentary,
            sources=self.sources,
            default_group_settings=self.default_group_settings,
            air_reader=self.air_reader,
        )
        primary_hub = self._get_submit_hub()
        allowed_local_dirs = self.config.get("submit_allowed_local_dirs") or []
        self.submit_handler: Optional[SubmitHandler] = (
            SubmitHandler(primary_hub, allowed_local_dirs=allowed_local_dirs)
            if primary_hub is not None
            else None
        )
        self.report_handler = ReportHandler(self.feedback_collector, self.hub_sources)
        self.admin_handler = AdminHandler(
            db=self.db,
            air_reader=self.air_reader,
            sources=self.sources,
            default_group_settings=self.default_group_settings,
            migration_report=self.migration_report,
        )
        self.pull_rate_limiter = CommandRateLimiter(
            user_cooldown_sec=float(self.config.get("manual_pull_user_cooldown_sec", 10.0)),
            group_cooldown_sec=float(self.config.get("manual_pull_group_cooldown_sec", 5.0)),
        )
        self.submit_rate_limiter = CommandRateLimiter(
            user_cooldown_sec=float(self.config.get("submit_user_cooldown_sec", 10.0)),
            group_cooldown_sec=float(self.config.get("submit_group_cooldown_sec", 5.0)),
        )

        # 7. Background asyncio tasks
        self._bg_tasks: list[asyncio.Task[Any]] = []
        if start_background_tasks:
            self._maybe_start_background_tasks()

    def _collect_trusted_hub_origins(self) -> tuple[str, ...]:
        """Collect exact admin-configured Hub base URLs for LAN/self-hosted media fetches."""
        origins: list[str] = []
        if bool(self.config.get("official_hub_enabled", True)):
            official_url = str(
                self.config.get("official_hub_url", "https://banshi-api.ankaanka.org")
            ).strip()
            if official_url:
                origins.append(official_url)
        for idx, raw_hub in enumerate(self.config.get("extra_hubs") or [], start=1):
            parsed = _parse_extra_hub_entry(raw_hub, idx)
            if parsed and parsed.get("url"):
                origins.append(parsed["url"])
        return tuple(dict.fromkeys(origins))

    def _init_sources(self) -> None:
        if bool(self.config.get("official_hub_enabled", True)):
            official_url = str(
                self.config.get("official_hub_url", "https://banshi-api.ankaanka.org")
            ).strip()
            if official_url:
                official = HubSource(
                    source_id="hub:official-main",
                    base_url=official_url,
                    db=self.db,
                    hub_id="official-main",
                    enabled=True,
                    user_token=str(self.config.get("user_token", "") or ""),
                    max_nsfw=int(self.config.get("default_max_nsfw", 1)),
                    media_cache=self.media_cache,
                )
                self.sources.append(official)
                self.hub_sources[official.hub_id] = official

        for idx, raw_hub in enumerate(self.config.get("extra_hubs") or [], start=1):
            parsed = _parse_extra_hub_entry(raw_hub, idx)
            if not parsed:
                continue
            hub = HubSource(
                source_id=f"hub:{parsed['id']}",
                base_url=parsed["url"],
                db=self.db,
                hub_id=parsed["id"],
                enabled=parsed["enabled"],
                user_token=parsed["user_token"],
                tags=parsed["tags"],
                exclude_tags=parsed["exclude_tags"],
                max_nsfw=parsed["max_nsfw"],
                media_cache=self.media_cache,
            )
            self.sources.append(hub)
            self.hub_sources[hub.hub_id] = hub

        for idx, raw_rss in enumerate(self.config.get("rss_sources") or [], start=1):
            if isinstance(raw_rss, dict):
                url = str(raw_rss.get("url", "")).strip()
                sid = str(raw_rss.get("id") or f"rss:{idx}")
                tags = list(raw_rss.get("tags") or ["RSS"])
            else:
                url = str(raw_rss or "").strip()
                sid = f"rss:{idx}"
                tags = ["RSS"]
            if url:
                self.sources.append(RssSource(source_id=sid, feed_url=url, db=self.db, default_tags=tags))

        local_groups = [str(g).strip() for g in (self.config.get("local_onebot_groups") or []) if str(g).strip()]
        if local_groups:
            self.local_onebot_source = LocalOneBotSource(
                db=self.db,
                source_groups=local_groups,
                emoji_threshold=int(self.config.get("local_emoji_threshold", 3)),
            )
            self.sources.append(self.local_onebot_source)

    def _get_submit_hub(self) -> Optional[HubSource]:
        target_id = str(self.config.get("submit_hub_id", "official-main") or "official-main")
        if target_id in self.hub_sources:
            return self.hub_sources[target_id]
        for hub in self.hub_sources.values():
            if hub.enabled:
                return hub
        return None

    def _maybe_start_background_tasks(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._bg_tasks.append(loop.create_task(self._sync_loop(), name="sowing_v2_sync"))
        self._bg_tasks.append(loop.create_task(self._ambient_loop(), name="sowing_v2_ambient"))
        self._bg_tasks.append(loop.create_task(self._feedback_loop(), name="sowing_v2_feedback"))

    async def sync_all_sources(self) -> list[Any]:
        results = []
        for src in self.sources:
            if not src.enabled:
                continue
            results.append(await src.sync())
        return results

    async def run_ambient_tick(self) -> list[str]:
        """Evaluate all target groups with AirReader and deliver to LULL_READY groups."""
        delivered_groups: list[str] = []
        target_groups = [
            str(g).strip() for g in (self.config.get("target_groups") or []) if str(g).strip()
        ]
        for group_umo in target_groups:
            cfg = self.db.get_group_settings(group_umo, defaults=self.default_group_settings)
            if not cfg.get("enabled", True) or not cfg.get("ambient_enabled", True):
                continue
            air = self.air_reader.evaluate(
                group_umo,
                quiet_hours=cfg["quiet_hours"],
                cooldown_sec=int(cfg["cooldown_sec"]),
            )
            if not air.can_send:
                continue
            res = await self.manual_pull_handler.pull_and_deliver(
                group_umo, trigger_type="ambient"
            )
            if res.ok:
                delivered_groups.append(group_umo)
        return delivered_groups

    async def _sync_loop(self) -> None:
        interval = max(30, int(self.config.get("sync_interval_sec", 300)))
        while True:
            try:
                await self.sync_all_sources()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                astr_logger.warning("Background sync error: %s", exc)
            await asyncio.sleep(interval)

    async def _ambient_loop(self) -> None:
        interval = max(15, int(self.config.get("ambient_check_interval_sec", 60)))
        while True:
            try:
                await self.run_ambient_tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                astr_logger.warning("Ambient tick error: %s", exc)
            await asyncio.sleep(interval)

    async def _feedback_loop(self) -> None:
        interval = max(30, int(self.config.get("feedback_flush_interval_sec", 120)))
        while True:
            await asyncio.sleep(interval)
            try:
                await self.feedback_collector.flush(self.hub_sources)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                astr_logger.warning("Feedback flush error: %s", exc)

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------
    @filter.command("来坨史", alias={"搬史"})
    async def cmd_manual_pull(self, event: AstrMessageEvent, tag: str = ""):
        group_umo = str(getattr(event, "unified_msg_origin", ""))
        user_id = str(event.get_sender_id()) if hasattr(event, "get_sender_id") else ""
        is_admin = _is_bot_admin(event)
        wait_sec = self.pull_rate_limiter.check_and_record(
            "manual_pull", group_umo=group_umo, user_id=user_id, bypass=is_admin
        )
        if wait_sec > 0.0:
            yield event.plain_result(f"手速太快啦，请 {math.ceil(wait_sec)} 秒后再来坨史~")
            return
        res = await self.manual_pull_handler.pull_and_deliver(
            group_umo, tag=tag, trigger_type="manual"
        )
        if not res.ok and res.message:
            yield event.plain_result(res.message)

    @filter.command("投稿")
    async def cmd_submit(self, event: AstrMessageEvent, tags_or_note: str = ""):
        if self.submit_handler is None:
            yield event.plain_result("当前未启用任何 Hub，无法使用 /投稿。")
            return
        group_umo = str(getattr(event, "unified_msg_origin", ""))
        user_id = str(event.get_sender_id()) if hasattr(event, "get_sender_id") else ""
        is_admin = _is_bot_admin(event)
        wait_sec = self.submit_rate_limiter.check_and_record(
            "submit", group_umo=group_umo, user_id=user_id, bypass=is_admin
        )
        if wait_sec > 0.0:
            yield event.plain_result(f"投稿太频繁啦，请 {math.ceil(wait_sec)} 秒后再试~")
            return
        parts = _extract_submission_parts(event)
        tags = [t.lstrip("#") for t in tags_or_note.split() if t.startswith("#")]
        note = " ".join(t for t in tags_or_note.split() if not t.startswith("#")).strip()
        text = parts["text"] or note
        res = await self.submit_handler.submit(
            text=text,
            title=note[:60] if note else "",
            tags=tags,
            images=parts["images"],
            chat_nodes=parts["chat_nodes"],
            videos=parts["videos"],
            is_bot_admin=is_admin,
        )
        yield event.plain_result(res.message)

    @filter.command("烂史")
    async def cmd_report(self, event: AstrMessageEvent, reason: str = ""):
        group_umo = str(getattr(event, "unified_msg_origin", ""))
        user_id = str(event.get_sender_id()) if hasattr(event, "get_sender_id") else ""
        quoted_id = _extract_quoted_msg_id(event)
        res = self.report_handler.handle_report(
            group_umo=group_umo,
            user_id=user_id,
            quoted_bot_msg_id=quoted_id,
            raw_reason=reason,
        )
        yield event.plain_result(res.message)

    @filter.command("史库")
    async def cmd_admin(self, event: AstrMessageEvent, args: str = ""):
        group_umo = str(getattr(event, "unified_msg_origin", ""))
        authorized = _is_group_or_bot_admin(event)
        reply = self.admin_handler.handle(group_umo, args, is_authorized=authorized)
        yield event.plain_result(reply)

    # ------------------------------------------------------------------
    # Passive group message & OneBot notice hooks
    # ------------------------------------------------------------------
    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def on_group_message(self, event: AstrMessageEvent):
        group_umo = str(getattr(event, "unified_msg_origin", ""))
        if not group_umo:
            return
        self.air_reader.record_human_message(group_umo)

        quoted_id = _extract_quoted_msg_id(event)
        if quoted_id:
            user_id = str(event.get_sender_id()) if hasattr(event, "get_sender_id") else ""
            msg_text = str(event.get_message_str()) if hasattr(event, "get_message_str") else ""
            if user_id and msg_text:
                self.feedback_collector.collect_reply_text(
                    group_umo=group_umo,
                    quoted_bot_msg_id=quoted_id,
                    user_id=user_id,
                    reply_text=msg_text,
                    hub_sources=self.hub_sources,
                )

    async def handle_onebot_notice(
        self,
        raw_event: dict[str, Any],
        *,
        group_umo: str = "",
        get_msg: Optional[Any] = None,
    ) -> None:
        """Process OneBot v11 notice.group_msg_emoji_like for local collection & crowd feedback."""
        if raw_event.get("notice_type") != "group_msg_emoji_like":
            return

        if self.local_onebot_source is not None:
            await self.local_onebot_source.handle_emoji_like_event(raw_event, get_msg=get_msg)

        # Also check if group members reacted to a bot-sent meme for anonymous feedback
        gid = str(raw_event.get("group_id", "")).strip()
        umo = group_umo or (f"aiocqhttp:GroupMessage:{gid}" if gid else "")
        mid = str(raw_event.get("message_id", "")).strip()
        uid = str(raw_event.get("user_id", "")).strip()
        is_add = bool(raw_event.get("is_add", True))
        if umo and mid and uid:
            for like_item in raw_event.get("likes") or []:
                if isinstance(like_item, dict):
                    emoji_id = str(like_item.get("emoji_id", ""))
                    self.feedback_collector.collect_emoji_like(
                        group_umo=umo,
                        bot_msg_id=mid,
                        user_id=uid,
                        emoji_id=emoji_id,
                        is_add=is_add,
                        hub_sources=self.hub_sources,
                    )

    async def terminate(self) -> None:
        """Gracefully cancel background loops and release sockets/DB."""
        for task in self._bg_tasks:
            task.cancel()
        if self._bg_tasks:
            await asyncio.gather(*self._bg_tasks, return_exceptions=True)
        self._bg_tasks.clear()

        for src in self.sources:
            await src.close()
        await self.media_cache.close()
        self.db.close()
