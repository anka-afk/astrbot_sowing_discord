"""Hub API source client: /meta, 16-bit SHA-256 PoW, /feed + ETag + tombstones, /random, federation."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import secrets
import time
from typing import Any, Optional

import aiohttp

try:
    from ..models.meme import ChatMessageNode, HubMeta, MemeItem
    from ..sinks.media_cache import MediaCache
    from ..storage.db import SowingDB
except ImportError:
    from models.meme import ChatMessageNode, HubMeta, MemeItem
    from sinks.media_cache import MediaCache
    from storage.db import SowingDB

from .base import BaseSource, SyncResult

logger = logging.getLogger(__name__)

MIN_POW_DIFFICULTY_BITS = 1
MAX_POW_DIFFICULTY_BITS = 22
MAX_POW_ATTEMPTS = 5_000_000


def _raise_for_status_no_redirect(resp: aiohttp.ClientResponse) -> None:
    """Reject 3xx redirect responses (which aiohttp's raise_for_status ignores) as well as 4xx/5xx."""
    if 300 <= resp.status < 400:
        raise aiohttp.ClientResponseError(
            resp.request_info,
            resp.history,
            status=resp.status,
            message=f"Unexpected redirect status {resp.status}",
            headers=resp.headers,
        )
    resp.raise_for_status()


def extract_media_urls_from_item(item: MemeItem) -> list[str]:
    """Recursively collect all media URLs referenced by a cached MemeItem."""
    urls: list[str] = []

    def _add(url: str) -> None:
        cleaned = (url or "").strip()
        if cleaned and cleaned not in urls:
            urls.append(cleaned)

    def _walk_messages(messages: list[ChatMessageNode], depth: int = 0) -> None:
        if depth > 8:
            return
        for node in messages:
            for part in node.content:
                if part.type in ("image", "gif") and part.url:
                    _add(part.url)
                elif part.type == "forward_quote" and part.messages:
                    _walk_messages(part.messages, depth=depth + 1)

    for block in item.blocks:
        if block.type in ("image", "gif", "video_file") and block.url:
            _add(block.url)
        if block.cover_url:
            _add(block.cover_url)
        if block.type == "chat_record":
            for rendered in block.rendered_images:
                if rendered.url:
                    _add(rendered.url)
            if block.messages:
                _walk_messages(block.messages, depth=0)

    return urls


def _has_leading_zero_bits(digest: bytes, difficulty_bits: int) -> bool:
    """Return True if the first `difficulty_bits` bits of `digest` are zero."""
    if difficulty_bits <= 0:
        return True
    full_bytes, rem_bits = divmod(difficulty_bits, 8)
    for idx in range(full_bytes):
        if digest[idx] != 0:
            return False
    if rem_bits > 0:
        mask = 0xFF << (8 - rem_bits) & 0xFF
        if (digest[full_bytes] & mask) != 0:
            return False
    return True


def verify_pow(nonce_prefix: str, solution: str, difficulty_bits: int = 16) -> bool:
    """Verify SHA-256(f"{nonce_prefix}:{solution}") has >= difficulty_bits leading zero bits."""
    digest = hashlib.sha256(f"{nonce_prefix}:{solution}".encode("utf-8")).digest()
    return _has_leading_zero_bits(digest, difficulty_bits)


def solve_pow(nonce_prefix: str, difficulty_bits: int = 16, max_attempts: int = MAX_POW_ATTEMPTS) -> str:
    """Solve lightweight SHA-256 PoW: sha256(f"{nonce_prefix}:{solution}") leading zero bits."""
    bits = int(difficulty_bits)
    if bits < MIN_POW_DIFFICULTY_BITS or bits > MAX_POW_DIFFICULTY_BITS:
        raise ValueError(
            f"Unsupported PoW difficulty_bits={bits}; allowed range is "
            f"[{MIN_POW_DIFFICULTY_BITS}, {MAX_POW_DIFFICULTY_BITS}]"
        )
    attempts = max(1, min(int(max_attempts), MAX_POW_ATTEMPTS))
    prefix_bytes = f"{nonce_prefix}:".encode("utf-8")
    for counter in range(attempts):
        sol = f"{counter:x}"
        digest = hashlib.sha256(prefix_bytes + sol.encode("ascii")).digest()
        if _has_leading_zero_bits(digest, bits):
            return sol
    raise RuntimeError(f"Failed to solve {bits}-bit PoW within {attempts} attempts")


class HubSource(BaseSource):
    """Multi-Hub federated client implementing the Sowing Discord v1.0 REST contract."""

    source_type = "hub"

    def __init__(
        self,
        source_id: str,
        base_url: str,
        db: SowingDB,
        *,
        hub_id: str = "",
        enabled: bool = True,
        user_token: str = "",
        tags: Optional[list[str]] = None,
        exclude_tags: Optional[list[str]] = None,
        max_nsfw: int = 1,
        client_version: str = "2.0.0",
        timeout_sec: float = 15.0,
        session: Optional[aiohttp.ClientSession] = None,
        base_backoff_sec: int = 30,
        max_backoff_sec: int = 3600,
        media_cache: Optional[MediaCache] = None,
    ):
        super().__init__(source_id=source_id, enabled=enabled)
        self.base_url = base_url.rstrip("/")
        self.db = db
        self.hub_id = hub_id or source_id.removeprefix("hub:")
        self.user_token = user_token.strip()
        self.tags = list(tags or [])
        self.exclude_tags = list(exclude_tags or [])
        self.max_nsfw = max_nsfw
        self.client_version = client_version
        self.timeout_sec = timeout_sec
        self._external_session = session is not None
        self._session = session
        self.base_backoff_sec = base_backoff_sec
        self.max_backoff_sec = max_backoff_sec
        self.media_cache = media_cache

        self._meta: Optional[HubMeta] = None
        self._meta_fetched_at: float = 0.0
        self._meta_ttl_sec: float = 300.0
        self._reg_lock = asyncio.Lock()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout_sec)
            self._session = aiohttp.ClientSession(timeout=timeout)
            self._external_session = False
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._external_session and not self._session.closed:
            await self._session.close()

    def _api_url(self, path: str) -> str:
        return f"{self.base_url}/api/v1{path}"

    def _get_cursor(self) -> dict[str, Any]:
        cursor = self.db.get_source_cursor(self.source_id)
        if cursor is None:
            cursor = self.db.upsert_source_cursor(self.source_id, hub_id=self.hub_id)
        elif not cursor.get("hub_id") and self.hub_id:
            cursor = self.db.upsert_source_cursor(self.source_id, hub_id=self.hub_id)
        return cursor

    def get_instance_salt(self) -> str:
        cursor = self._get_cursor()
        salt = cursor.get("instance_salt") or ""
        if not salt:
            salt = secrets.token_hex(32)
            self.db.upsert_source_cursor(self.source_id, instance_salt=salt)
        return salt

    def is_in_backoff(self, now: Optional[int] = None) -> bool:
        ts = int(now if now is not None else time.time())
        cursor = self._get_cursor()
        return int(cursor.get("backoff_until", 0)) > ts

    def _record_failure(self, now: Optional[int] = None) -> int:
        ts = int(now if now is not None else time.time())
        cursor = self._get_cursor()
        fail_count = int(cursor.get("fail_count", 0)) + 1
        backoff_sec = min(self.max_backoff_sec, self.base_backoff_sec * (2 ** (fail_count - 1)))
        backoff_until = ts + backoff_sec
        self.db.upsert_source_cursor(
            self.source_id,
            fail_count=fail_count,
            backoff_until=backoff_until,
        )
        return backoff_until

    def _record_success(self, **extra: Any) -> None:
        self.db.upsert_source_cursor(
            self.source_id,
            fail_count=0,
            backoff_until=0,
            last_sync_at=int(time.time()),
            **extra,
        )

    # ------------------------------------------------------------------
    # /meta discovery
    # ------------------------------------------------------------------
    async def fetch_meta(self, force: bool = False) -> HubMeta:
        now = time.monotonic()
        if not force and self._meta is not None and (now - self._meta_fetched_at) < self._meta_ttl_sec:
            return self._meta

        session = await self._get_session()
        async with session.get(self._api_url("/meta"), allow_redirects=False) as resp:
            _raise_for_status_no_redirect(resp)
            data = await resp.json()

        meta = HubMeta.from_dict(data)
        self._meta = meta
        self._meta_fetched_at = now
        if meta.hub_id:
            self.hub_id = meta.hub_id
            self.db.upsert_source_cursor(self.source_id, hub_id=meta.hub_id)
        return meta

    @property
    def cached_meta(self) -> Optional[HubMeta]:
        return self._meta

    # ------------------------------------------------------------------
    # Anonymous 16-bit PoW instance registration
    # ------------------------------------------------------------------
    async def ensure_registered(self, force: bool = False) -> dict[str, str]:
        async with self._reg_lock:
            cursor = self._get_cursor()
            if not force and cursor.get("instance_id") and cursor.get("instance_token"):
                salt = cursor.get("instance_salt") or self.get_instance_salt()
                return {
                    "instance_id": cursor["instance_id"],
                    "instance_token": cursor["instance_token"],
                    "instance_salt": salt,
                }

            session = await self._get_session()
            async with session.get(
                self._api_url("/instances/challenge"), allow_redirects=False
            ) as resp:
                _raise_for_status_no_redirect(resp)
                challenge = await resp.json()

            challenge_id = str(challenge["challenge_id"])
            nonce_prefix = str(challenge["nonce_prefix"])
            difficulty_bits = int(challenge.get("difficulty_bits", 16))
            if (
                difficulty_bits < MIN_POW_DIFFICULTY_BITS
                or difficulty_bits > MAX_POW_DIFFICULTY_BITS
            ):
                raise ValueError(
                    f"Hub requested unsupported PoW difficulty_bits={difficulty_bits} "
                    f"(allowed {MIN_POW_DIFFICULTY_BITS}..{MAX_POW_DIFFICULTY_BITS})"
                )

            solution = await asyncio.to_thread(
                solve_pow, nonce_prefix, difficulty_bits, MAX_POW_ATTEMPTS
            )

            payload = {
                "challenge_id": challenge_id,
                "solution": solution,
                "client_version": self.client_version,
            }
            async with session.post(
                self._api_url("/instances/register"),
                json=payload,
                allow_redirects=False,
            ) as resp:
                _raise_for_status_no_redirect(resp)
                reg = await resp.json()

            instance_id = str(reg["instance_id"])
            instance_token = str(reg["instance_token"])
            salt = cursor.get("instance_salt") or secrets.token_hex(32)

            self.db.upsert_source_cursor(
                self.source_id,
                hub_id=self.hub_id,
                instance_id=instance_id,
                instance_token=instance_token,
                instance_salt=salt,
            )
            logger.info("Registered Hub instance %s for source %s", instance_id, self.source_id)
            return {
                "instance_id": instance_id,
                "instance_token": instance_token,
                "instance_salt": salt,
            }

    async def _auth_headers(self, include_user_token: bool = False) -> dict[str, str]:
        creds = await self.ensure_registered()
        headers = {"Authorization": f"Bearer {creds['instance_token']}"}
        if include_user_token and self.user_token:
            headers["X-User-Token"] = self.user_token
        return headers

    def _evict_tombstoned_items(self, item_ids: list[str]) -> int:
        """Read cached items to evict on-disk media before deleting them from SQLite."""
        if not item_ids:
            return 0
        if self.media_cache is not None:
            cached_items = self.db.get_items_by_hub_item_ids(self.hub_id, item_ids)
            for item in cached_items:
                for url in extract_media_urls_from_item(item):
                    self.media_cache.evict_url(url)
        return self.db.delete_items_by_hub_item_ids(self.hub_id, item_ids)

    # ------------------------------------------------------------------
    # Incremental /feed sync with ETag/304 & tombstone eviction
    # ------------------------------------------------------------------
    async def sync(self, limit: int = 20, max_pages: int = 5) -> SyncResult:
        if not self.enabled:
            return SyncResult(source_id=self.source_id)
        if self.is_in_backoff():
            return SyncResult(source_id=self.source_id, skipped_backoff=True, error="circuit_open")

        try:
            if self._meta is None:
                await self.fetch_meta()
            headers = await self._auth_headers()
            cursor = self._get_cursor()
            after_seq = int(cursor.get("next_seq", 0))
            etag = str(cursor.get("etag", "") or "")

            new_items: list[MemeItem] = []
            total_tombstones = 0
            not_modified = False
            reauth_attempts = 0
            pages_fetched = 0
            session = await self._get_session()

            while pages_fetched < max_pages:
                params: dict[str, Any] = {
                    "after_seq": after_seq,
                    "limit": max(1, min(int(limit), 50)),
                    "max_nsfw": self.max_nsfw,
                }
                if self.tags:
                    params["tags"] = ",".join(self.tags)
                if self.exclude_tags:
                    params["exclude_tags"] = ",".join(self.exclude_tags)

                req_headers = dict(headers)
                if pages_fetched == 0 and etag:
                    req_headers["If-None-Match"] = etag

                async with session.get(
                    self._api_url("/feed"),
                    params=params,
                    headers=req_headers,
                    allow_redirects=False,
                ) as resp:
                    if resp.status == 304:
                        not_modified = True
                        self._record_success()
                        break
                    if resp.status == 401:
                        if reauth_attempts >= 1:
                            raise RuntimeError(
                                "Hub authentication failed after re-registration (HTTP 401)"
                            )
                        reauth_attempts += 1
                        await self.ensure_registered(force=True)
                        headers = await self._auth_headers()
                        continue
                    _raise_for_status_no_redirect(resp)
                    new_etag = resp.headers.get("ETag") or etag
                    data = await resp.json()

                raw_items = data.get("items") or []
                for raw in raw_items:
                    item = MemeItem.from_dict(raw, source_type="hub")
                    if not item.hub_id:
                        item.hub_id = self.hub_id
                    if self.db.upsert_item(item, source_id=self.source_id):
                        new_items.append(item)

                tombstones = [str(t) for t in (data.get("tombstones") or []) if t]
                if tombstones:
                    total_tombstones += self._evict_tombstoned_items(tombstones)

                next_seq = int(data.get("next_seq", after_seq))
                has_more = bool(data.get("has_more", False))
                after_seq = max(after_seq, next_seq)
                etag = new_etag
                pages_fetched += 1

                self._record_success(next_seq=after_seq, etag=etag)
                if not has_more:
                    break

            return SyncResult(
                source_id=self.source_id,
                new_items=new_items,
                tombstones_removed=total_tombstones,
                not_modified=not_modified,
            )
        except Exception as exc:
            self._record_failure()
            logger.warning("Hub sync failed for %s: %s", self.source_id, exc)
            return SyncResult(source_id=self.source_id, error=str(exc))

    # ------------------------------------------------------------------
    # /random batch replenishment
    # ------------------------------------------------------------------
    async def fetch_random(
        self,
        limit: int = 20,
        *,
        tags: Optional[list[str]] = None,
        exclude_tags: Optional[list[str]] = None,
        max_nsfw: Optional[int] = None,
        older_than_days: Optional[int] = None,
        within_days: Optional[int] = None,
        min_score: Optional[float] = None,
    ) -> list[MemeItem]:
        if not self.enabled or self.is_in_backoff():
            return []
        try:
            if self._meta is None:
                await self.fetch_meta()
            headers = await self._auth_headers()
            params: dict[str, Any] = {
                "limit": max(1, min(int(limit), 50)),
                "max_nsfw": self.max_nsfw if max_nsfw is None else max_nsfw,
            }
            use_tags = tags if tags is not None else self.tags
            use_exclude = exclude_tags if exclude_tags is not None else self.exclude_tags
            if use_tags:
                params["tags"] = ",".join(use_tags)
            if use_exclude:
                params["exclude_tags"] = ",".join(use_exclude)
            if older_than_days is not None:
                params["older_than_days"] = int(older_than_days)
            if within_days is not None:
                params["within_days"] = int(within_days)
            if min_score is not None:
                params["min_score"] = float(min_score)

            session = await self._get_session()
            async with session.get(
                self._api_url("/random"),
                params=params,
                headers=headers,
                allow_redirects=False,
            ) as resp:
                _raise_for_status_no_redirect(resp)
                data = await resp.json()

            items: list[MemeItem] = []
            for raw in data.get("items") or []:
                item = MemeItem.from_dict(raw, source_type="hub")
                if not item.hub_id:
                    item.hub_id = self.hub_id
                self.db.upsert_item(item, source_id=self.source_id)
                items.append(item)
            self._record_success()
            return items
        except Exception as exc:
            self._record_failure()
            logger.warning("Hub /random failed for %s: %s", self.source_id, exc)
            return []

    # ------------------------------------------------------------------
    # /items/{id} liveness check
    # ------------------------------------------------------------------
    async def get_item(self, item_id: str) -> Optional[MemeItem]:
        headers = await self._auth_headers()
        session = await self._get_session()
        async with session.get(
            self._api_url(f"/items/{item_id}"),
            headers=headers,
            allow_redirects=False,
        ) as resp:
            if resp.status in (404, 410):
                self._evict_tombstoned_items([item_id])
                return None
            _raise_for_status_no_redirect(resp)
            data = await resp.json()
        item = MemeItem.from_dict(data, source_type="hub")
        if not item.hub_id:
            item.hub_id = self.hub_id
        self.db.upsert_item(item, source_id=self.source_id)
        return item

    # ------------------------------------------------------------------
    # Two-step submission & feedback batch endpoints
    # ------------------------------------------------------------------
    async def upload_media(
        self,
        data: bytes,
        filename: str = "upload.bin",
        content_type: str = "application/octet-stream",
        *,
        include_user_token: bool = False,
    ) -> dict[str, Any]:
        headers = await self._auth_headers(include_user_token=include_user_token)
        form = aiohttp.FormData()
        form.add_field("file", data, filename=filename, content_type=content_type)
        session = await self._get_session()
        async with session.post(
            self._api_url("/submissions/media"),
            data=form,
            headers=headers,
            allow_redirects=False,
        ) as resp:
            _raise_for_status_no_redirect(resp)
            return await resp.json()

    async def submit_item(
        self,
        blocks: list[dict[str, Any]],
        *,
        title: str = "",
        tags: Optional[list[str]] = None,
        include_user_token: bool = False,
    ) -> dict[str, Any]:
        headers = await self._auth_headers(include_user_token=include_user_token)
        payload: dict[str, Any] = {"blocks": blocks}
        if title:
            payload["title"] = title
        if tags:
            payload["tags"] = list(tags)
        session = await self._get_session()
        async with session.post(
            self._api_url("/submissions"),
            json=payload,
            headers=headers,
            allow_redirects=False,
        ) as resp:
            _raise_for_status_no_redirect(resp)
            return await resp.json()

    async def send_feedback_batch(self, events: list[dict[str, Any]]) -> dict[str, Any]:
        if not events:
            return {"accepted": 0}
        if len(events) > 50:
            raise ValueError("feedback/batch accepts at most 50 events per request")
        headers = await self._auth_headers()
        session = await self._get_session()
        async with session.post(
            self._api_url("/feedback/batch"),
            json={"events": events},
            headers=headers,
            allow_redirects=False,
        ) as resp:
            _raise_for_status_no_redirect(resp)
            return await resp.json()
