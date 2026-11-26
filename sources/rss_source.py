"""Pure-stdlib RSS/Atom/RSSHub image & text source using xml.etree.ElementTree."""

from __future__ import annotations

import hashlib
import html
import logging
import re
import time
import xml.etree.ElementTree as ET
from typing import Optional

import aiohttp

try:
    from ..models.meme import MemeAttribution, MemeBlock, MemeItem
    from ..storage.db import SowingDB
except ImportError:
    from models.meme import MemeAttribution, MemeBlock, MemeItem
    from storage.db import SowingDB

from .base import BaseSource, SyncResult

logger = logging.getLogger(__name__)

_IMG_SRC_RE = re.compile(r'<img\b[^>]*?\bsrc=["\']([^"\']+)["\']', re.IGNORECASE)
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def _strip_html(raw: str) -> str:
    if not raw:
        return ""
    text = _HTML_TAG_RE.sub(" ", raw)
    text = html.unescape(text)
    return _WS_RE.sub(" ", text).strip()


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1].lower()
    return tag.lower()


def _extract_images_from_html(raw_html: str) -> list[str]:
    if not raw_html:
        return []
    urls: list[str] = []
    for match in _IMG_SRC_RE.findall(raw_html):
        url = html.unescape(match).strip()
        if url.startswith(("http://", "https://")) and url not in urls:
            urls.append(url)
    return urls


def _media_block_for_url(url: str, mime_hint: str = "") -> MemeBlock:
    lower_url = url.lower().split("?", 1)[0]
    if "gif" in mime_hint.lower() or lower_url.endswith(".gif"):
        return MemeBlock(type="gif", url=url, mime="image/gif")
    return MemeBlock(type="image", url=url, mime=mime_hint or "image/jpeg")


def parse_feed_xml(
    xml_text: str,
    source_id: str,
    default_tags: Optional[list[str]] = None,
    max_items: int = 30,
) -> list[MemeItem]:
    """Parse RSS 2.0 or Atom XML into normalized MemeItem objects."""
    root = ET.fromstring(xml_text)
    root_name = _local_name(root.tag)
    items: list[MemeItem] = []
    base_tags = list(default_tags or [])

    if root_name == "rss" or any(_local_name(c.tag) == "channel" for c in root):
        channel = None
        for child in root:
            if _local_name(child.tag) == "channel":
                channel = child
                break
        if channel is None:
            return []
        feed_title = ""
        raw_entries: list[ET.Element] = []
        for child in channel:
            lname = _local_name(child.tag)
            if lname == "title" and child.text:
                feed_title = child.text.strip()
            elif lname == "item":
                raw_entries.append(child)

        for entry in raw_entries[:max_items]:
            title = ""
            link = ""
            guid = ""
            pub_date = ""
            desc_html = ""
            content_html = ""
            tags = list(base_tags)
            media_urls: list[tuple[str, str]] = []

            for child in entry:
                lname = _local_name(child.tag)
                text = (child.text or "").strip()
                if lname == "title":
                    title = html.unescape(text)
                elif lname == "link":
                    link = text
                elif lname == "guid":
                    guid = text
                elif lname in ("pubdate", "date"):
                    pub_date = text
                elif lname == "description":
                    desc_html = child.text or ""
                elif lname in ("encoded", "content"):
                    content_html = child.text or ""
                elif lname == "category" and text:
                    if text not in tags:
                        tags.append(text)
                elif lname == "enclosure":
                    url = (child.attrib.get("url") or "").strip()
                    mtype = (child.attrib.get("type") or "").strip()
                    if url.startswith(("http://", "https://")) and (
                        mtype.startswith("image/") or url.lower().endswith((".jpg", ".jpeg", ".png", ".gif", ".webp"))
                    ):
                        media_urls.append((url, mtype))
                elif lname in ("content", "thumbnail") and "media" in child.tag.lower():
                    url = (child.attrib.get("url") or "").strip()
                    mtype = (child.attrib.get("type") or "").strip()
                    if url.startswith(("http://", "https://")):
                        media_urls.append((url, mtype))

            html_body = content_html or desc_html
            for img_url in _extract_images_from_html(html_body):
                if all(existing != img_url for existing, _ in media_urls):
                    media_urls.append((img_url, ""))

            clean_text = _strip_html(html_body)
            blocks: list[MemeBlock] = []
            if clean_text:
                blocks.append(MemeBlock(type="text", text=clean_text[:500]))
            elif title:
                blocks.append(MemeBlock(type="text", text=title))
            for url, mtype in media_urls[:9]:
                blocks.append(_media_block_for_url(url, mtype))
            if not blocks:
                continue

            stable_key = guid or link or f"{source_id}:{title}:{clean_text[:80]}"
            item_hash = hashlib.sha256(stable_key.encode("utf-8")).hexdigest()[:24]
            items.append(
                MemeItem(
                    id=item_hash,
                    source_type="rss",
                    hub_id=source_id,
                    title=title,
                    summary=(clean_text or title)[:120],
                    tags=tags,
                    nsfw_level=0,
                    attribution=MemeAttribution(
                        submitter_name=feed_title or "RSS 订阅",
                        origin_label=source_id,
                    ),
                    web_url=link,
                    published_at=pub_date,
                    blocks=blocks,
                )
            )
        return items

    if root_name == "feed":
        feed_title = ""
        raw_entries = []
        for child in root:
            lname = _local_name(child.tag)
            if lname == "title" and child.text:
                feed_title = child.text.strip()
            elif lname == "entry":
                raw_entries.append(child)

        for entry in raw_entries[:max_items]:
            title = ""
            link = ""
            entry_id = ""
            updated = ""
            content_html = ""
            summary_html = ""
            tags = list(base_tags)
            media_urls: list[tuple[str, str]] = []

            for child in entry:
                lname = _local_name(child.tag)
                text = (child.text or "").strip()
                if lname == "title":
                    title = html.unescape(text)
                elif lname == "id":
                    entry_id = text
                elif lname in ("updated", "published"):
                    updated = text
                elif lname == "content":
                    content_html = child.text or ""
                elif lname == "summary":
                    summary_html = child.text or ""
                elif lname == "category":
                    term = (child.attrib.get("term") or child.attrib.get("label") or text).strip()
                    if term and term not in tags:
                        tags.append(term)
                elif lname == "link":
                    href = (child.attrib.get("href") or "").strip()
                    rel = (child.attrib.get("rel") or "alternate").strip()
                    ltype = (child.attrib.get("type") or "").strip()
                    if rel == "enclosure" and href.startswith(("http://", "https://")) and ltype.startswith("image/"):
                        media_urls.append((href, ltype))
                    elif rel == "alternate" and href and not link:
                        link = href

            html_body = content_html or summary_html
            for img_url in _extract_images_from_html(html_body):
                if all(existing != img_url for existing, _ in media_urls):
                    media_urls.append((img_url, ""))

            clean_text = _strip_html(html_body)
            blocks = []
            if clean_text:
                blocks.append(MemeBlock(type="text", text=clean_text[:500]))
            elif title:
                blocks.append(MemeBlock(type="text", text=title))
            for url, mtype in media_urls[:9]:
                blocks.append(_media_block_for_url(url, mtype))
            if not blocks:
                continue

            stable_key = entry_id or link or f"{source_id}:{title}:{clean_text[:80]}"
            item_hash = hashlib.sha256(stable_key.encode("utf-8")).hexdigest()[:24]
            items.append(
                MemeItem(
                    id=item_hash,
                    source_type="rss",
                    hub_id=source_id,
                    title=title,
                    summary=(clean_text or title)[:120],
                    tags=tags,
                    nsfw_level=0,
                    attribution=MemeAttribution(
                        submitter_name=feed_title or "Atom 订阅",
                        origin_label=source_id,
                    ),
                    web_url=link,
                    published_at=updated,
                    blocks=blocks,
                )
            )
        return items

    return []


class RssSource(BaseSource):
    """RSS/Atom/RSSHub content source using stdlib XML parsing."""

    source_type = "rss"

    def __init__(
        self,
        source_id: str,
        feed_url: str,
        db: SowingDB,
        *,
        enabled: bool = True,
        default_tags: Optional[list[str]] = None,
        timeout_sec: float = 15.0,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        super().__init__(source_id=source_id, enabled=enabled)
        self.feed_url = feed_url
        self.db = db
        self.default_tags = list(default_tags or ["RSS"])
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

    async def sync(self) -> SyncResult:
        if not self.enabled:
            return SyncResult(source_id=self.source_id)

        cursor = self.db.get_source_cursor(self.source_id) or {}
        now = int(time.time())
        if int(cursor.get("backoff_until", 0)) > now:
            return SyncResult(source_id=self.source_id, skipped_backoff=True, error="circuit_open")

        headers: dict[str, str] = {"User-Agent": "astrbot_sowing_discord/2.0"}
        etag = str(cursor.get("etag", "") or "")
        if etag:
            headers["If-None-Match"] = etag

        try:
            session = await self._get_session()
            async with session.get(self.feed_url, headers=headers) as resp:
                if resp.status == 304:
                    self.db.upsert_source_cursor(
                        self.source_id, fail_count=0, backoff_until=0, last_sync_at=now
                    )
                    return SyncResult(source_id=self.source_id, not_modified=True)
                resp.raise_for_status()
                new_etag = resp.headers.get("ETag") or etag
                xml_text = await resp.text()

            parsed = parse_feed_xml(xml_text, self.source_id, default_tags=self.default_tags)
            new_items: list[MemeItem] = []
            for item in parsed:
                if self.db.upsert_item(item, source_id=self.source_id):
                    new_items.append(item)

            self.db.upsert_source_cursor(
                self.source_id,
                etag=new_etag,
                fail_count=0,
                backoff_until=0,
                last_sync_at=now,
            )
            return SyncResult(source_id=self.source_id, new_items=new_items)
        except Exception as exc:
            fail_count = int(cursor.get("fail_count", 0)) + 1
            backoff_until = now + min(3600, 60 * (2 ** (fail_count - 1)))
            self.db.upsert_source_cursor(
                self.source_id, fail_count=fail_count, backoff_until=backoff_until
            )
            logger.warning("RSS sync failed for %s: %s", self.source_id, exc)
            return SyncResult(source_id=self.source_id, error=str(exc))
