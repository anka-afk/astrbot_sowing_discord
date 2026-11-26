"""Local LRU media cache (default 200MB) with optional WebP -> JPEG soft degradation."""

from __future__ import annotations

import hashlib
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Optional, Sequence
from urllib.parse import urlparse

import aiohttp

try:
    from ..utils.net_safety import (
        create_safe_http_session,
        is_public_http_url_literal,
        safe_fetch_bytes,
    )
except ImportError:
    from utils.net_safety import (
        create_safe_http_session,
        is_public_http_url_literal,
        safe_fetch_bytes,
    )

logger = logging.getLogger(__name__)

DEFAULT_MAX_DOWNLOAD_BYTES = 12 * 1024 * 1024  # 12 MB
MAX_IMAGE_PIXELS_CAP = 40_000_000

_MIME_TO_EXT = {
    "image/webp": ".webp",
    "image/gif": ".gif",
    "image/jpeg": ".jpg",
    "image/jpg": ".jpg",
    "image/png": ".png",
    "video/mp4": ".mp4",
}


class MediaCache:
    """Bounded LRU on-disk cache for cross-platform media delivery."""

    def __init__(
        self,
        cache_dir: str | Path,
        *,
        max_bytes: int = 200 * 1024 * 1024,
        max_download_bytes: int = DEFAULT_MAX_DOWNLOAD_BYTES,
        timeout_sec: float = 20.0,
        session: Optional[aiohttp.ClientSession] = None,
        trusted_origins: Sequence[str] = (),
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_bytes = max(1024 * 1024, int(max_bytes))
        self.max_download_bytes = max(1024, int(max_download_bytes))
        self.timeout_sec = timeout_sec
        self.trusted_origins: tuple[str, ...] = tuple(str(o).strip() for o in (trusted_origins or ()) if str(o).strip())
        self._external_session = session is not None
        self._session = session

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = create_safe_http_session(
                timeout=self.timeout_sec,
                trusted_origins=self.trusted_origins,
            )
            self._external_session = False
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._external_session and not self._session.closed:
            await self._session.close()

    @staticmethod
    def _guess_ext(url: str, mime_hint: str = "") -> str:
        if mime_hint:
            clean_mime = mime_hint.split(";", 1)[0].strip().lower()
            if clean_mime in _MIME_TO_EXT:
                return _MIME_TO_EXT[clean_mime]
        path = urlparse(url).path.lower()
        for ext in (".webp", ".gif", ".jpg", ".jpeg", ".png", ".mp4"):
            if path.endswith(ext):
                return ".jpg" if ext == ".jpeg" else ext
        return ".bin"

    def _cache_key(self, url: str, suffix: str = "") -> str:
        digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:32]
        return f"{digest}{suffix}"

    def _touch(self, path: Path) -> None:
        now = time.time()
        try:
            os.utime(path, (now, now))
        except OSError:
            pass

    def total_size_bytes(self) -> int:
        total = 0
        for p in self.cache_dir.iterdir():
            if p.is_file():
                try:
                    total += p.stat().st_size
                except OSError:
                    continue
        return total

    def evict_lru(self) -> int:
        """Delete least-recently-used files until total size <= max_bytes."""
        files: list[tuple[float, int, Path]] = []
        total = 0
        for p in self.cache_dir.iterdir():
            if not p.is_file():
                continue
            try:
                st = p.stat()
                files.append((st.st_mtime, st.st_size, p))
                total += st.st_size
            except OSError:
                continue

        if total <= self.max_bytes:
            return 0

        # Oldest mtime first
        files.sort(key=lambda entry: entry[0])
        evicted = 0
        for _, size, path in files:
            if total <= self.max_bytes:
                break
            try:
                path.unlink(missing_ok=True)
                total -= size
                evicted += 1
            except OSError:
                continue
        return evicted

    def put_bytes(self, key: str, data: bytes, ext: str = ".bin") -> Path:
        if not ext.startswith("."):
            ext = f".{ext}"
        target = self.cache_dir / f"{key}{ext}"
        tmp = self.cache_dir / f".{key}.{uuid.uuid4().hex}.tmp"
        try:
            tmp.write_bytes(data)
            tmp.replace(target)
        except Exception:
            tmp.unlink(missing_ok=True)
            raise
        self._touch(target)
        self.evict_lru()
        return target

    def evict_url(self, url: str) -> int:
        """Delete all cached files (including converted .jpg siblings) for `url`."""
        cleaned = (url or "").strip()
        if not cleaned:
            return 0
        key = self._cache_key(cleaned)
        removed = 0
        for p in self.cache_dir.glob(f"{key}.*"):
            if not p.is_file():
                continue
            try:
                p.unlink(missing_ok=True)
                removed += 1
            except OSError:
                continue
        return removed

    def _maybe_convert_webp_to_jpeg(self, webp_path: Path) -> Path:
        """Soft-degrade static WebP to JPEG if Pillow is available; fallback silently otherwise."""
        if webp_path.suffix.lower() != ".webp":
            return webp_path
        jpg_path = webp_path.with_suffix(".jpg")
        if jpg_path.exists():
            self._touch(jpg_path)
            return jpg_path

        try:
            from PIL import Image  # type: ignore
        except ImportError:
            logger.debug("Pillow not installed; skipping WebP -> JPEG soft degradation")
            return webp_path

        tmp_jpg = self.cache_dir / f".{webp_path.stem}.{uuid.uuid4().hex}.jpg.tmp"
        try:
            Image.MAX_IMAGE_PIXELS = MAX_IMAGE_PIXELS_CAP
            with Image.open(webp_path) as img:
                if getattr(img, "is_animated", False):
                    return webp_path
                rgb = img.convert("RGB")
                rgb.save(tmp_jpg, format="JPEG", quality=88)
            tmp_jpg.replace(jpg_path)
            self._touch(jpg_path)
            self.evict_lru()
            return jpg_path
        except Exception as exc:
            tmp_jpg.unlink(missing_ok=True)
            logger.debug("WebP -> JPEG conversion failed (%s); keeping WebP", exc)
            return webp_path

    async def get_or_download(
        self,
        url: str,
        *,
        mime_hint: str = "",
        convert_webp_to_jpeg: bool = False,
    ) -> Path:
        url = (url or "").strip()
        if not is_public_http_url_literal(url, trusted_origins=self.trusted_origins):
            raise ValueError("Unsupported or unsafe media URL")

        ext = self._guess_ext(url, mime_hint)
        key = self._cache_key(url)
        candidate = self.cache_dir / f"{key}{ext}"
        if candidate.exists():
            self._touch(candidate)
            if convert_webp_to_jpeg and candidate.suffix.lower() == ".webp":
                return self._maybe_convert_webp_to_jpeg(candidate)
            return candidate

        session = await self._get_session()
        data, resp_mime, final_url = await safe_fetch_bytes(
            session,
            url,
            max_bytes=self.max_download_bytes,
            timeout=self.timeout_sec,
            trusted_origins=self.trusted_origins,
        )

        final_ext = self._guess_ext(final_url, resp_mime or mime_hint)
        saved = self.put_bytes(key, data, ext=final_ext)
        if convert_webp_to_jpeg and saved.suffix.lower() == ".webp":
            return self._maybe_convert_webp_to_jpeg(saved)
        return saved
