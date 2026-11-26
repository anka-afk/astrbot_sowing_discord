"""Local LRU media cache (default 200MB) with optional WebP -> JPEG soft degradation."""

from __future__ import annotations

import hashlib
import logging
import os
import time
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import aiohttp

logger = logging.getLogger(__name__)

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
        timeout_sec: float = 20.0,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.max_bytes = max(1024 * 1024, int(max_bytes))
        self.timeout_sec = timeout_sec
        self._external_session = session is not None
        self._session = session

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout_sec)
            self._session = aiohttp.ClientSession(timeout=timeout)
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
        tmp = self.cache_dir / f".{key}.{os.getpid()}.tmp"
        tmp.write_bytes(data)
        tmp.replace(target)
        self._touch(target)
        self.evict_lru()
        return target

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

        try:
            with Image.open(webp_path) as img:
                if getattr(img, "is_animated", False):
                    return webp_path
                rgb = img.convert("RGB")
                rgb.save(jpg_path, format="JPEG", quality=88)
            self._touch(jpg_path)
            self.evict_lru()
            return jpg_path
        except Exception as exc:
            logger.debug("WebP -> JPEG conversion failed (%s); keeping WebP", exc)
            return webp_path

    async def get_or_download(
        self,
        url: str,
        *,
        mime_hint: str = "",
        convert_webp_to_jpeg: bool = False,
    ) -> Path:
        ext = self._guess_ext(url, mime_hint)
        key = self._cache_key(url)
        candidate = self.cache_dir / f"{key}{ext}"
        if candidate.exists():
            self._touch(candidate)
            if convert_webp_to_jpeg and candidate.suffix.lower() == ".webp":
                return self._maybe_convert_webp_to_jpeg(candidate)
            return candidate

        session = await self._get_session()
        async with session.get(url) as resp:
            resp.raise_for_status()
            resp_mime = resp.headers.get("Content-Type", mime_hint)
            data = await resp.read()

        final_ext = self._guess_ext(url, resp_mime)
        saved = self.put_bytes(key, data, ext=final_ext)
        if convert_webp_to_jpeg and saved.suffix.lower() == ".webp":
            return self._maybe_convert_webp_to_jpeg(saved)
        return saved
