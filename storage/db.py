"""SQLite storage layer (sowing_v2.db) for astrbot_sowing_discord v2."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Optional

try:
    from ..models.meme import MemeItem
except ImportError:
    from models.meme import MemeItem


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS items_cache (
    uid TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    tags TEXT NOT NULL DEFAULT '[]',
    nsfw_level INTEGER NOT NULL DEFAULT 0,
    score REAL NOT NULL DEFAULT 0.0,
    fetched_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_items_cache_source_id ON items_cache(source_id);
CREATE INDEX IF NOT EXISTS idx_items_cache_content_hash ON items_cache(content_hash);
CREATE INDEX IF NOT EXISTS idx_items_cache_score ON items_cache(score DESC);
CREATE INDEX IF NOT EXISTS idx_items_cache_nsfw ON items_cache(nsfw_level);

CREATE TABLE IF NOT EXISTS group_sent (
    group_umo TEXT NOT NULL,
    item_uid TEXT NOT NULL,
    sent_at INTEGER NOT NULL,
    bot_msg_id TEXT NOT NULL DEFAULT '',
    trigger_type TEXT NOT NULL DEFAULT 'ambient',
    PRIMARY KEY (group_umo, item_uid)
);
CREATE INDEX IF NOT EXISTS idx_group_sent_msg_id ON group_sent(group_umo, bot_msg_id);
CREATE INDEX IF NOT EXISTS idx_group_sent_sent_at ON group_sent(group_umo, sent_at DESC);

CREATE TABLE IF NOT EXISTS group_settings (
    group_umo TEXT PRIMARY KEY,
    enabled INTEGER NOT NULL DEFAULT 1,
    ambient_enabled INTEGER NOT NULL DEFAULT 1,
    quiet_hours TEXT NOT NULL DEFAULT '01:00-08:00',
    cooldown_sec INTEGER NOT NULL DEFAULT 5400,
    max_nsfw INTEGER NOT NULL DEFAULT 1,
    blocked_tags TEXT NOT NULL DEFAULT '[]',
    llm_comment_prob REAL NOT NULL DEFAULT 0.3
);

CREATE TABLE IF NOT EXISTS source_cursors (
    source_id TEXT PRIMARY KEY,
    hub_id TEXT NOT NULL DEFAULT '',
    instance_id TEXT NOT NULL DEFAULT '',
    instance_token TEXT NOT NULL DEFAULT '',
    instance_salt TEXT NOT NULL DEFAULT '',
    next_seq INTEGER NOT NULL DEFAULT 0,
    etag TEXT NOT NULL DEFAULT '',
    last_sync_at INTEGER NOT NULL DEFAULT 0,
    fail_count INTEGER NOT NULL DEFAULT 0,
    backoff_until INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS feedback_outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hub_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    action TEXT NOT NULL,
    reason TEXT DEFAULT NULL,
    voter_hash TEXT NOT NULL,
    group_hash TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_feedback_outbox_hub ON feedback_outbox(hub_id, id);
"""


class SowingDB:
    """Thread-safe SQLite repository wrapping the 5 plugin tables."""

    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        if str(db_path) != ":memory:":
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL;")
            self._conn.execute("PRAGMA foreign_keys=ON;")
            self._conn.executescript(SCHEMA_SQL)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------
    # items_cache
    # ------------------------------------------------------------------
    def upsert_item(
        self,
        item: MemeItem,
        source_id: str,
        fetched_at: Optional[int] = None,
        expires_at: int = 0,
    ) -> bool:
        """Insert or update a cached MemeItem. Returns True if newly inserted."""
        now = int(fetched_at if fetched_at is not None else time.time())
        uid = item.uid
        chash = item.content_hash()
        payload_json = item.to_json(include_internal=True)
        tags_json = json.dumps(item.tags, ensure_ascii=False)
        with self._lock:
            existing = self._conn.execute(
                "SELECT 1 FROM items_cache WHERE uid = ?", (uid,)
            ).fetchone()
            self._conn.execute(
                """
                INSERT INTO items_cache
                    (uid, source_id, content_hash, payload_json, tags, nsfw_level, score, fetched_at, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(uid) DO UPDATE SET
                    source_id = excluded.source_id,
                    content_hash = excluded.content_hash,
                    payload_json = excluded.payload_json,
                    tags = excluded.tags,
                    nsfw_level = excluded.nsfw_level,
                    score = excluded.score,
                    fetched_at = excluded.fetched_at,
                    expires_at = excluded.expires_at
                """,
                (
                    uid,
                    source_id,
                    chash,
                    payload_json,
                    tags_json,
                    item.nsfw_level,
                    item.score,
                    now,
                    expires_at,
                ),
            )
            self._conn.commit()
            return existing is None

    def get_item(self, uid: str) -> Optional[MemeItem]:
        with self._lock:
            row = self._conn.execute(
                "SELECT payload_json FROM items_cache WHERE uid = ?", (uid,)
            ).fetchone()
            if not row:
                return None
            return MemeItem.from_json(row["payload_json"])

    def has_uid(self, uid: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM items_cache WHERE uid = ?", (uid,)
            ).fetchone()
            return row is not None

    def has_content_hash(self, content_hash: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM items_cache WHERE content_hash = ? LIMIT 1",
                (content_hash,),
            ).fetchone()
            return row is not None

    def delete_item(self, uid: str) -> bool:
        with self._lock:
            cur = self._conn.execute("DELETE FROM items_cache WHERE uid = ?", (uid,))
            self._conn.commit()
            return cur.rowcount > 0

    def delete_items_by_hub_item_ids(self, hub_id: str, item_ids: list[str]) -> int:
        """Remove tombstoned items belonging to a specific Hub from local cache."""
        if not item_ids:
            return 0
        uids = [
            iid if iid.startswith("hub:") else f"hub:{hub_id}:{iid}"
            for iid in item_ids
        ]
        placeholders = ",".join("?" for _ in uids)
        with self._lock:
            cur = self._conn.execute(
                f"DELETE FROM items_cache WHERE uid IN ({placeholders})",
                uids,
            )
            self._conn.commit()
            return cur.rowcount

    def purge_expired_items(self, now: Optional[int] = None) -> int:
        ts = int(now if now is not None else time.time())
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM items_cache WHERE expires_at > 0 AND expires_at <= ?",
                (ts,),
            )
            self._conn.commit()
            return cur.rowcount

    def get_unsent_candidates(
        self,
        group_umo: str,
        max_nsfw: int = 1,
        limit: int = 50,
        source_id: Optional[str] = None,
        now: Optional[int] = None,
    ) -> list[MemeItem]:
        """Fetch cached items not yet sent to group_umo (by uid or content_hash)."""
        ts = int(now if now is not None else time.time())
        params: list[Any] = [group_umo, group_umo, max_nsfw, ts]
        source_clause = ""
        if source_id:
            source_clause = "AND ic.source_id = ?"
            params.append(source_id)
        params.append(limit)
        sql = f"""
            SELECT ic.payload_json
            FROM items_cache ic
            WHERE NOT EXISTS (
                SELECT 1 FROM group_sent gs
                WHERE gs.group_umo = ? AND gs.item_uid = ic.uid
            )
            AND NOT EXISTS (
                SELECT 1
                FROM group_sent gs2
                JOIN items_cache ic2 ON ic2.uid = gs2.item_uid
                WHERE gs2.group_umo = ? AND ic2.content_hash = ic.content_hash
            )
            AND ic.nsfw_level <= ?
            AND (ic.expires_at = 0 OR ic.expires_at > ?)
            {source_clause}
            ORDER BY ic.score DESC, ic.fetched_at DESC
            LIMIT ?
        """
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [MemeItem.from_json(r["payload_json"]) for r in rows]

    def count_unsent(self, group_umo: str, max_nsfw: int = 1) -> int:
        ts = int(time.time())
        with self._lock:
            row = self._conn.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM items_cache ic
                WHERE NOT EXISTS (
                    SELECT 1 FROM group_sent gs
                    WHERE gs.group_umo = ? AND gs.item_uid = ic.uid
                )
                AND ic.nsfw_level <= ?
                AND (ic.expires_at = 0 OR ic.expires_at > ?)
                """,
                (group_umo, max_nsfw, ts),
            ).fetchone()
            return int(row["cnt"]) if row else 0

    # ------------------------------------------------------------------
    # group_sent
    # ------------------------------------------------------------------
    def mark_sent(
        self,
        group_umo: str,
        item_uid: str,
        bot_msg_id: str = "",
        trigger_type: str = "ambient",
        sent_at: Optional[int] = None,
    ) -> None:
        ts = int(sent_at if sent_at is not None else time.time())
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO group_sent (group_umo, item_uid, sent_at, bot_msg_id, trigger_type)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(group_umo, item_uid) DO UPDATE SET
                    sent_at = excluded.sent_at,
                    bot_msg_id = excluded.bot_msg_id,
                    trigger_type = excluded.trigger_type
                """,
                (group_umo, item_uid, ts, str(bot_msg_id or ""), trigger_type),
            )
            self._conn.commit()

    def is_sent(self, group_umo: str, item_uid: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM group_sent WHERE group_umo = ? AND item_uid = ?",
                (group_umo, item_uid),
            ).fetchone()
            return row is not None

    def is_content_hash_sent(self, group_umo: str, content_hash: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT 1
                FROM group_sent gs
                JOIN items_cache ic ON ic.uid = gs.item_uid
                WHERE gs.group_umo = ? AND ic.content_hash = ?
                LIMIT 1
                """,
                (group_umo, content_hash),
            ).fetchone()
            return row is not None

    def find_sent_by_bot_msg_id(self, group_umo: str, bot_msg_id: str) -> Optional[dict[str, Any]]:
        if not bot_msg_id:
            return None
        with self._lock:
            row = self._conn.execute(
                """
                SELECT group_umo, item_uid, sent_at, bot_msg_id, trigger_type
                FROM group_sent
                WHERE group_umo = ? AND bot_msg_id = ?
                ORDER BY sent_at DESC
                LIMIT 1
                """,
                (group_umo, str(bot_msg_id)),
            ).fetchone()
            return dict(row) if row else None

    def get_last_sent_time(self, group_umo: str) -> Optional[int]:
        with self._lock:
            row = self._conn.execute(
                "SELECT MAX(sent_at) AS last_sent FROM group_sent WHERE group_umo = ?",
                (group_umo,),
            ).fetchone()
            if not row or row["last_sent"] is None:
                return None
            return int(row["last_sent"])

    # ------------------------------------------------------------------
    # group_settings
    # ------------------------------------------------------------------
    def get_group_settings(
        self, group_umo: str, defaults: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        base: dict[str, Any] = {
            "group_umo": group_umo,
            "enabled": True,
            "ambient_enabled": True,
            "quiet_hours": "01:00-08:00",
            "cooldown_sec": 5400,
            "max_nsfw": 1,
            "blocked_tags": [],
            "llm_comment_prob": 0.3,
        }
        if defaults:
            for k in (
                "enabled",
                "ambient_enabled",
                "quiet_hours",
                "cooldown_sec",
                "max_nsfw",
                "blocked_tags",
                "llm_comment_prob",
            ):
                if k in defaults and defaults[k] is not None:
                    base[k] = defaults[k]

        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM group_settings WHERE group_umo = ?", (group_umo,)
            ).fetchone()
        if not row:
            return base
        return {
            "group_umo": row["group_umo"],
            "enabled": bool(row["enabled"]),
            "ambient_enabled": bool(row["ambient_enabled"]),
            "quiet_hours": row["quiet_hours"],
            "cooldown_sec": int(row["cooldown_sec"]),
            "max_nsfw": int(row["max_nsfw"]),
            "blocked_tags": json.loads(row["blocked_tags"] or "[]"),
            "llm_comment_prob": float(row["llm_comment_prob"]),
        }

    def upsert_group_settings(
        self,
        group_umo: str,
        defaults: Optional[dict[str, Any]] = None,
        **updates: Any,
    ) -> dict[str, Any]:
        current = self.get_group_settings(group_umo, defaults=defaults)
        for k, v in updates.items():
            if k in current and v is not None:
                current[k] = v
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO group_settings
                    (group_umo, enabled, ambient_enabled, quiet_hours, cooldown_sec, max_nsfw, blocked_tags, llm_comment_prob)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(group_umo) DO UPDATE SET
                    enabled = excluded.enabled,
                    ambient_enabled = excluded.ambient_enabled,
                    quiet_hours = excluded.quiet_hours,
                    cooldown_sec = excluded.cooldown_sec,
                    max_nsfw = excluded.max_nsfw,
                    blocked_tags = excluded.blocked_tags,
                    llm_comment_prob = excluded.llm_comment_prob
                """,
                (
                    group_umo,
                    1 if current["enabled"] else 0,
                    1 if current["ambient_enabled"] else 0,
                    str(current["quiet_hours"]),
                    int(current["cooldown_sec"]),
                    int(current["max_nsfw"]),
                    json.dumps(list(current["blocked_tags"]), ensure_ascii=False),
                    float(current["llm_comment_prob"]),
                ),
            )
            self._conn.commit()
        return current

    def list_group_settings(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM group_settings").fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            result.append(
                {
                    "group_umo": row["group_umo"],
                    "enabled": bool(row["enabled"]),
                    "ambient_enabled": bool(row["ambient_enabled"]),
                    "quiet_hours": row["quiet_hours"],
                    "cooldown_sec": int(row["cooldown_sec"]),
                    "max_nsfw": int(row["max_nsfw"]),
                    "blocked_tags": json.loads(row["blocked_tags"] or "[]"),
                    "llm_comment_prob": float(row["llm_comment_prob"]),
                }
            )
        return result

    # ------------------------------------------------------------------
    # source_cursors
    # ------------------------------------------------------------------
    def get_source_cursor(self, source_id: str) -> Optional[dict[str, Any]]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM source_cursors WHERE source_id = ?", (source_id,)
            ).fetchone()
            return dict(row) if row else None

    def upsert_source_cursor(self, source_id: str, **updates: Any) -> dict[str, Any]:
        current = self.get_source_cursor(source_id) or {
            "source_id": source_id,
            "hub_id": "",
            "instance_id": "",
            "instance_token": "",
            "instance_salt": "",
            "next_seq": 0,
            "etag": "",
            "last_sync_at": 0,
            "fail_count": 0,
            "backoff_until": 0,
        }
        for k, v in updates.items():
            if k in current and v is not None:
                current[k] = v
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO source_cursors
                    (source_id, hub_id, instance_id, instance_token, instance_salt,
                     next_seq, etag, last_sync_at, fail_count, backoff_until)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_id) DO UPDATE SET
                    hub_id = excluded.hub_id,
                    instance_id = excluded.instance_id,
                    instance_token = excluded.instance_token,
                    instance_salt = excluded.instance_salt,
                    next_seq = excluded.next_seq,
                    etag = excluded.etag,
                    last_sync_at = excluded.last_sync_at,
                    fail_count = excluded.fail_count,
                    backoff_until = excluded.backoff_until
                """,
                (
                    source_id,
                    str(current["hub_id"]),
                    str(current["instance_id"]),
                    str(current["instance_token"]),
                    str(current["instance_salt"]),
                    int(current["next_seq"]),
                    str(current["etag"]),
                    int(current["last_sync_at"]),
                    int(current["fail_count"]),
                    int(current["backoff_until"]),
                ),
            )
            self._conn.commit()
        return current

    def list_source_cursors(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM source_cursors").fetchall()
            return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # feedback_outbox
    # ------------------------------------------------------------------
    def enqueue_feedback(
        self,
        hub_id: str,
        item_id: str,
        action: str,
        voter_hash: str,
        group_hash: str,
        occurred_at: str,
        reason: Optional[str] = None,
    ) -> int:
        with self._lock:
            cur = self._conn.execute(
                """
                INSERT INTO feedback_outbox
                    (hub_id, item_id, action, reason, voter_hash, group_hash, occurred_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (hub_id, item_id, action, reason, voter_hash, group_hash, occurred_at),
            )
            self._conn.commit()
            return int(cur.lastrowid)

    def peek_feedback_batch(
        self, hub_id: Optional[str] = None, limit: int = 50
    ) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 50))
        with self._lock:
            if hub_id:
                rows = self._conn.execute(
                    """
                    SELECT * FROM feedback_outbox
                    WHERE hub_id = ?
                    ORDER BY id ASC
                    LIMIT ?
                    """,
                    (hub_id, limit),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM feedback_outbox ORDER BY id ASC LIMIT ?",
                    (limit,),
                ).fetchall()
            return [dict(r) for r in rows]

    def delete_feedback_ids(self, ids: list[int]) -> int:
        if not ids:
            return 0
        placeholders = ",".join("?" for _ in ids)
        with self._lock:
            cur = self._conn.execute(
                f"DELETE FROM feedback_outbox WHERE id IN ({placeholders})", ids
            )
            self._conn.commit()
            return cur.rowcount

    # ------------------------------------------------------------------
    # stats
    # ------------------------------------------------------------------
    def get_stats(self) -> dict[str, int]:
        with self._lock:
            cached = self._conn.execute("SELECT COUNT(*) FROM items_cache").fetchone()[0]
            sent = self._conn.execute("SELECT COUNT(*) FROM group_sent").fetchone()[0]
            groups = self._conn.execute("SELECT COUNT(*) FROM group_settings").fetchone()[0]
            sources = self._conn.execute("SELECT COUNT(*) FROM source_cursors").fetchone()[0]
            outbox = self._conn.execute("SELECT COUNT(*) FROM feedback_outbox").fetchone()[0]
        return {
            "cached_items": int(cached),
            "sent_records": int(sent),
            "configured_groups": int(groups),
            "tracked_sources": int(sources),
            "pending_feedback": int(outbox),
        }
