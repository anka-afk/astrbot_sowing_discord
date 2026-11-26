"""Command handler for /投稿: local anonymization, video blocking, two-step upload & duplicate early-stop."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional
from urllib.parse import urlparse

import aiohttp

try:
    from ..models.meme import DEFAULT_LIMITS
    from ..sources.hub_source import HubSource
except ImportError:
    from models.meme import DEFAULT_LIMITS
    from sources.hub_source import HubSource

logger = logging.getLogger(__name__)

MediaFetcher = Callable[[str], Awaitable[tuple[bytes, str, str]]]

_VIDEO_LINK_HOSTS: dict[str, str] = {
    "bilibili.com": "bilibili",
    "www.bilibili.com": "bilibili",
    "m.bilibili.com": "bilibili",
    "b23.tv": "bilibili",
    "youtube.com": "youtube",
    "www.youtube.com": "youtube",
    "youtu.be": "youtube",
    "douyin.com": "douyin",
    "www.douyin.com": "douyin",
    "v.douyin.com": "douyin",
}

_URL_RE = re.compile(r"https?://[^\s<>\"']+", re.IGNORECASE)
_VIDEO_EXTS = (".mp4", ".mov", ".mkv", ".avi", ".webm", ".flv", ".m4v")


def _alias_for_index(idx: int) -> str:
    """Generate '群友A', '群友B', ..., '群友Z', '群友AA', ..."""
    letters: list[str] = []
    n = idx
    while True:
        letters.append(chr(ord("A") + (n % 26)))
        n = n // 26 - 1
        if n < 0:
            break
    return "群友" + "".join(reversed(letters))


def detect_video_link(text: str) -> Optional[dict[str, str]]:
    """Detect a whitelisted external video URL inside text."""
    for match in _URL_RE.findall(text or ""):
        parsed = urlparse(match)
        host = (parsed.hostname or "").lower()
        if host in _VIDEO_LINK_HOSTS:
            return {"url": match, "platform": _VIDEO_LINK_HOSTS[host]}
    return None


def extract_raw_video_present(payload: dict[str, Any]) -> bool:
    """Return True if submission input contains raw local video attachments."""
    if payload.get("videos"):
        return True
    for seg in payload.get("segments") or []:
        if not isinstance(seg, dict):
            continue
        stype = str(seg.get("type", "")).lower()
        if stype in ("video", "short_video", "video_file"):
            return True
        url = str(seg.get("url") or (seg.get("data") or {}).get("url") or (seg.get("data") or {}).get("file") or "")
        if url.lower().split("?", 1)[0].endswith(_VIDEO_EXTS):
            return True
    for img in payload.get("images") or []:
        if isinstance(img, dict):
            mime = str(img.get("mime", "")).lower()
            filename = str(img.get("filename", "")).lower()
            url = str(img.get("url", "")).lower()
            if mime.startswith("video/") or filename.endswith(_VIDEO_EXTS) or url.split("?", 1)[0].endswith(_VIDEO_EXTS):
                return True
    for node in payload.get("chat_nodes") or []:
        if isinstance(node, dict):
            for part in node.get("content") or []:
                if isinstance(part, dict) and str(part.get("type", "")).lower() in ("video", "video_file"):
                    return True
    return False


def _flatten_chat_nodes(
    raw_nodes: list[dict[str, Any]],
    *,
    max_depth: int = 1,
    current_depth: int = 0,
) -> list[dict[str, Any]]:
    """Flatten nested forward nodes strictly respecting max_chat_depth."""
    flattened: list[dict[str, Any]] = []
    for node in raw_nodes:
        if not isinstance(node, dict):
            continue
        nested = node.get("nested_nodes") or node.get("forward_nodes")
        raw_content = node.get("content") or []
        if isinstance(raw_content, str):
            raw_content = [{"type": "text", "text": raw_content}]

        kept_parts: list[dict[str, Any]] = []
        inline_nested: list[dict[str, Any]] = []
        for part in raw_content:
            if not isinstance(part, dict):
                kept_parts.append({"type": "text", "text": str(part)})
                continue
            ptype = str(part.get("type", "text")).lower()
            if ptype == "forward":
                sub = part.get("messages") or part.get("nodes") or []
                if current_depth < max_depth and isinstance(sub, list):
                    inline_nested.extend(sub)
                else:
                    kept_parts.append({"type": "text", "text": "[嵌套聊天记录已折叠]"})
            elif ptype in ("text", "image", "gif"):
                kept_parts.append(part)

        if kept_parts:
            flattened.append(
                {
                    "sender_key": str(
                        node.get("sender_id")
                        or node.get("user_id")
                        or node.get("uin")
                        or node.get("sender_name")
                        or node.get("nickname")
                        or node.get("sender_alias")
                        or "unknown"
                    ),
                    "time_label": str(node.get("time_label", "") or ""),
                    "content": kept_parts,
                }
            )

        if nested and isinstance(nested, list):
            if current_depth < max_depth:
                flattened.extend(
                    _flatten_chat_nodes(nested, max_depth=max_depth, current_depth=current_depth + 1)
                )
            else:
                flattened.append(
                    {
                        "sender_key": str(node.get("sender_id") or node.get("user_id") or "system"),
                        "time_label": str(node.get("time_label", "") or ""),
                        "content": [{"type": "text", "text": "[嵌套聊天记录已折叠]"}],
                    }
                )

        if inline_nested:
            flattened.extend(
                _flatten_chat_nodes(inline_nested, max_depth=max_depth, current_depth=current_depth + 1)
            )
    return flattened


def anonymize_chat_nodes(
    raw_nodes: list[dict[str, Any]],
    *,
    max_depth: int = DEFAULT_LIMITS["max_chat_depth"],
    max_nodes: int = DEFAULT_LIMITS["max_chat_nodes"],
) -> list[dict[str, Any]]:
    """
    Scrub real QQ/group IDs/nicknames/avatars, map senders in appearance order to
    `群友A`, `群友B`, ... with `avatar_color_idx`, and enforce `max_depth=1, max_nodes=20`.
    """
    flattened = _flatten_chat_nodes(raw_nodes, max_depth=max_depth)[:max_nodes]
    alias_map: dict[str, tuple[str, int]] = {}
    sanitized: list[dict[str, Any]] = []

    for node in flattened:
        key = node["sender_key"]
        if key not in alias_map:
            idx = len(alias_map)
            alias_map[key] = (_alias_for_index(idx), idx % 8)
        alias, color_idx = alias_map[key]

        clean_parts: list[dict[str, Any]] = []
        for part in node["content"]:
            ptype = str(part.get("type", "text")).lower()
            if ptype == "text":
                txt = str(part.get("text", "") or part.get("data", {}).get("text", "")).strip()
                if txt:
                    clean_parts.append({"type": "text", "text": txt})
            elif ptype in ("image", "gif"):
                img_entry: dict[str, Any] = {"type": "image"}
                if part.get("data_bytes") is not None:
                    img_entry["data_bytes"] = part["data_bytes"]
                if part.get("filename"):
                    img_entry["filename"] = str(part["filename"])
                if part.get("mime"):
                    img_entry["mime"] = str(part["mime"])
                url = str(part.get("url") or part.get("data", {}).get("url") or part.get("data", {}).get("file") or "")
                if url:
                    img_entry["url"] = url
                if part.get("media_id"):
                    img_entry["media_id"] = str(part["media_id"])
                clean_parts.append(img_entry)

        if clean_parts:
            sanitized.append(
                {
                    "sender_alias": alias,
                    "avatar_color_idx": color_idx,
                    "time_label": node["time_label"],
                    "content": clean_parts,
                }
            )
    return sanitized


@dataclass
class SubmitResult:
    ok: bool
    message: str
    submission_id: str = ""
    status: str = ""
    duplicate: bool = False
    match_type: Optional[str] = None
    existing_status: Optional[str] = None
    existing_item_id: Optional[str] = None
    blocks_submitted: list[dict[str, Any]] = field(default_factory=list)


class SubmitHandler:
    """
    Handles /投稿 with:
    1. Raw video blocking (unless Hub `/meta` enables `video_file`)
    2. Local anonymization (`群友A/B`, `max_chat_depth=1`, `max_chat_nodes=20`)
    3. Local media fetch + Step 1 upload to `POST /api/v1/submissions/media` with duplicate early-stop
    4. Step 2 structured submission to `POST /api/v1/submissions`
    """

    def __init__(
        self,
        hub_source: HubSource,
        *,
        media_fetcher: Optional[MediaFetcher] = None,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        self.hub_source = hub_source
        self._media_fetcher = media_fetcher
        self._session = session

    def _limits(self) -> dict[str, int]:
        meta = self.hub_source.cached_meta
        if meta is None:
            return dict(DEFAULT_LIMITS)
        return {
            "max_image_bytes": meta.limits.max_image_bytes,
            "max_gif_bytes": meta.limits.max_gif_bytes,
            "max_images_per_submission": meta.limits.max_images_per_submission,
            "max_chat_nodes": meta.limits.max_chat_nodes,
            "max_chat_depth": meta.limits.max_chat_depth,
        }

    def _video_file_allowed(self) -> bool:
        meta = self.hub_source.cached_meta
        return bool(meta is not None and meta.capabilities.video_file)

    async def _fetch_media_bytes(self, item: dict[str, Any]) -> tuple[bytes, str, str]:
        if isinstance(item.get("data_bytes"), (bytes, bytearray)):
            data = bytes(item["data_bytes"])
            filename = str(item.get("filename") or "image.png")
            mime = str(item.get("mime") or ("image/gif" if filename.lower().endswith(".gif") else "image/png"))
            return data, filename, mime

        url = str(item.get("url") or item.get("file") or "").strip()
        if not url:
            raise ValueError("Missing media bytes or url")

        if self._media_fetcher is not None:
            return await self._media_fetcher(url)

        if not url.startswith(("http://", "https://")):
            path = Path(url)
            data = path.read_bytes()
            ext = path.suffix.lower()
            mime = "image/gif" if ext == ".gif" else ("image/webp" if ext == ".webp" else "image/jpeg")
            return data, path.name, mime

        own_session = self._session is None or self._session.closed
        session = self._session if not own_session else aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20))
        try:
            async with session.get(url) as resp:
                resp.raise_for_status()
                mime = (resp.headers.get("Content-Type") or "application/octet-stream").split(";", 1)[0].strip()
                data = await resp.read()
                filename = Path(urlparse(url).path).name or "upload.bin"
                return data, filename, mime
        finally:
            if own_session:
                await session.close()

    def _validate_media_size(self, data: bytes, mime: str, filename: str, limits: dict[str, int]) -> None:
        is_gif = "gif" in mime.lower() or filename.lower().endswith(".gif") or data[:6] in (b"GIF87a", b"GIF89a")
        if is_gif and len(data) > limits["max_gif_bytes"]:
            raise ValueError(f"GIF 文件过大（上限 {limits['max_gif_bytes'] // (1024 * 1024)}MB）")
        if not is_gif and len(data) > limits["max_image_bytes"]:
            raise ValueError(f"图片文件过大（上限 {limits['max_image_bytes'] // (1024 * 1024)}MB）")

    @staticmethod
    def _duplicate_result(upload_resp: dict[str, Any]) -> SubmitResult:
        existing_status = upload_resp.get("existing_status")
        existing_id = upload_resp.get("existing_item_id")
        match_type = upload_resp.get("match_type")
        if existing_status == "approved" and existing_id:
            msg = f"该内容已收录于史库（ID: {existing_id}），已自动跳过重复投稿~"
        else:
            msg = "该内容已存在或正在待审队列中，已自动跳过重复投稿~"
        return SubmitResult(
            ok=False,
            message=msg,
            duplicate=True,
            match_type=match_type,
            existing_status=existing_status,
            existing_item_id=existing_id,
        )

    async def submit(
        self,
        *,
        text: str = "",
        title: str = "",
        tags: Optional[list[str]] = None,
        images: Optional[list[dict[str, Any]]] = None,
        chat_nodes: Optional[list[dict[str, Any]]] = None,
        videos: Optional[list[Any]] = None,
        segments: Optional[list[dict[str, Any]]] = None,
    ) -> SubmitResult:
        """Execute full local validation, scrubbing, media upload (with early-stop), and submission."""
        raw_payload = {
            "images": images or [],
            "chat_nodes": chat_nodes or [],
            "videos": videos or [],
            "segments": segments or [],
        }

        # 1. Block local raw videos when Hub doesn't advertise video_file capability
        if extract_raw_video_present(raw_payload) and not self._video_file_allowed():
            return SubmitResult(
                ok=False,
                message="暂不支持投稿原始视频文件（当前仅支持图文、GIF、聊天记录及白名单视频外链）。",
            )

        limits = self._limits()
        blocks: list[dict[str, Any]] = []
        uploaded_count = 0
        max_images = limits["max_images_per_submission"]

        # 2. Text & whitelisted video_link extraction
        clean_text = (text or "").strip()
        if clean_text:
            vlink = detect_video_link(clean_text)
            if vlink is not None:
                blocks.append(
                    {
                        "type": "video_link",
                        "url": vlink["url"],
                        "platform": vlink["platform"],
                        "title": title.strip() or clean_text[:80],
                    }
                )
            else:
                blocks.append({"type": "text", "text": clean_text})

        # 3. Upload standalone images / GIFs with duplicate early-stop
        for img in images or []:
            if uploaded_count >= max_images:
                break
            try:
                data, filename, mime = await self._fetch_media_bytes(img)
                self._validate_media_size(data, mime, filename, limits)
            except Exception as exc:
                return SubmitResult(ok=False, message=f"媒体预处理失败：{exc}")

            is_gif = (
                "gif" in mime.lower()
                or filename.lower().endswith(".gif")
                or data[:6] in (b"GIF87a", b"GIF89a")
            )
            upload_resp = await self.hub_source.upload_media(
                data, filename=filename, content_type=mime
            )
            if upload_resp.get("duplicate"):
                return self._duplicate_result(upload_resp)

            media_id = str(upload_resp["media_id"])
            blocks.append(
                {
                    "type": "gif" if is_gif else "image",
                    "media_id": media_id,
                }
            )
            uploaded_count += 1

        # 4. Anonymize & flatten chat_record (max_chat_depth=1, max_chat_nodes=20), upload inline images
        if chat_nodes:
            sanitized_nodes = anonymize_chat_nodes(
                chat_nodes,
                max_depth=limits["max_chat_depth"],
                max_nodes=limits["max_chat_nodes"],
            )
            final_messages: list[dict[str, Any]] = []
            for node in sanitized_nodes:
                final_parts: list[dict[str, Any]] = []
                for part in node["content"]:
                    if part["type"] == "text":
                        final_parts.append({"type": "text", "text": part["text"]})
                    elif part["type"] == "image":
                        if part.get("media_id"):
                            final_parts.append({"type": "image", "media_id": part["media_id"]})
                            continue
                        if uploaded_count >= max_images:
                            continue
                        try:
                            data, filename, mime = await self._fetch_media_bytes(part)
                            self._validate_media_size(data, mime, filename, limits)
                        except Exception as exc:
                            return SubmitResult(ok=False, message=f"聊天记录图片预处理失败：{exc}")

                        upload_resp = await self.hub_source.upload_media(
                            data, filename=filename, content_type=mime
                        )
                        if upload_resp.get("duplicate"):
                            return self._duplicate_result(upload_resp)

                        final_parts.append(
                            {"type": "image", "media_id": str(upload_resp["media_id"])}
                        )
                        uploaded_count += 1
                if final_parts:
                    final_messages.append(
                        {
                            "sender_alias": node["sender_alias"],
                            "avatar_color_idx": node["avatar_color_idx"],
                            "time_label": node["time_label"],
                            "content": final_parts,
                        }
                    )
            if final_messages:
                # Note: submission only sends anonymized `messages` (no rendered_images)
                blocks.append({"type": "chat_record", "messages": final_messages})

        if not blocks:
            return SubmitResult(
                ok=False,
                message="请引用一条包含文字/图片/聊天记录的消息，或在 /投稿 后附带内容再提交。",
            )

        # 5. Step 2: submit structured item
        resp = await self.hub_source.submit_item(
            blocks=blocks,
            title=title.strip(),
            tags=[t.strip() for t in (tags or []) if t.strip()],
        )
        sub_id = str(resp.get("submission_id", ""))
        status = str(resp.get("status", "pending"))
        return SubmitResult(
            ok=True,
            submission_id=sub_id,
            status=status,
            blocks_submitted=blocks,
            message=f"投稿成功（编号：{sub_id}，状态：{status}），已自动脱敏并进入人工审核队列，感谢投喂！",
        )
