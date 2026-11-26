"""Unified Meme Protocol JSON Schema v1.0 data models and validation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Optional

SCHEMA_VERSION = "1.0"

NSFW_SAFE = 0
NSFW_MILD = 1
NSFW_HIGH = 2
VALID_NSFW_LEVELS = (NSFW_SAFE, NSFW_MILD, NSFW_HIGH)

DEFAULT_LIMITS: dict[str, int] = {
    "max_image_bytes": 10_485_760,  # 10MB
    "max_gif_bytes": 5_242_880,  # 5MB
    "max_images_per_submission": 20,
    "max_chat_nodes": 50,
    "max_chat_depth": 3,
}

REPORT_REASONS = (
    "nsfw_unmarked",
    "privacy_leak",
    "copyright",
    "broken_media",
    "duplicate",
    "other",
)

VALID_BLOCK_TYPES = (
    "text",
    "image",
    "gif",
    "chat_record",
    "video_link",
    "video_file",
)

FEEDBACK_ACTIONS = ("like", "dislike", "report")


@dataclass
class RenderedImage:
    url: str
    mime: str = "image/webp"
    w: int = 0
    h: int = 0
    size_bytes: int = 0

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RenderedImage":
        return cls(
            url=str(data.get("url", "")),
            mime=str(data.get("mime", "image/webp")),
            w=int(data.get("w", 0) or 0),
            h=int(data.get("h", 0) or 0),
            size_bytes=int(data.get("size_bytes", 0) or 0),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "mime": self.mime,
            "w": self.w,
            "h": self.h,
            "size_bytes": self.size_bytes,
        }


@dataclass
class ChatContentPart:
    type: str  # "text" | "image" | "gif" | "forward_quote"
    text: str = ""
    url: str = ""
    media_id: str = ""
    w: int = 0
    h: int = 0
    title: str = ""
    messages: list["ChatMessageNode"] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ChatContentPart":
        raw_nested = data.get("messages") or []
        nested_msgs = [
            ChatMessageNode.from_dict(m)
            for m in raw_nested
            if isinstance(m, dict)
        ]
        return cls(
            type=str(data.get("type", "text")),
            text=str(data.get("text", "")),
            url=str(data.get("url", "")),
            media_id=str(data.get("media_id", "")),
            w=int(data.get("w", 0) or 0),
            h=int(data.get("h", 0) or 0),
            title=str(data.get("title", "")),
            messages=nested_msgs,
        )

    def to_dict(self) -> dict[str, Any]:
        if self.type == "text":
            return {"type": "text", "text": self.text}
        if self.type == "forward_quote":
            return {
                "type": "forward_quote",
                "title": self.title or "嵌套聊天记录",
                "messages": [m.to_dict() for m in self.messages],
            }
        result: dict[str, Any] = {"type": self.type}
        if self.media_id:
            result["media_id"] = self.media_id
        if self.url:
            result["url"] = self.url
        if self.w:
            result["w"] = self.w
        if self.h:
            result["h"] = self.h
        return result


@dataclass
class ChatMessageNode:
    sender_alias: str
    avatar_color_idx: int = 0
    time_label: str = ""
    content: list[ChatContentPart] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ChatMessageNode":
        raw_content = data.get("content") or []
        parts = [
            ChatContentPart.from_dict(p) if isinstance(p, dict) else ChatContentPart(type="text", text=str(p))
            for p in raw_content
        ]
        return cls(
            sender_alias=str(data.get("sender_alias", "群友A")),
            avatar_color_idx=int(data.get("avatar_color_idx", 0) or 0),
            time_label=str(data.get("time_label", "")),
            content=parts,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "sender_alias": self.sender_alias,
            "avatar_color_idx": self.avatar_color_idx,
            "time_label": self.time_label,
            "content": [p.to_dict() for p in self.content],
        }


@dataclass
class MemeBlock:
    type: str
    # text
    text: str = ""
    # image / gif / video_link / video_file
    url: str = ""
    media_id: str = ""
    mime: str = ""
    w: int = 0
    h: int = 0
    size_bytes: int = 0
    # chat_record
    rendered_images: list[RenderedImage] = field(default_factory=list)
    messages: list[ChatMessageNode] = field(default_factory=list)
    # video_link / video_file
    platform: str = ""
    title: str = ""
    cover_url: str = ""
    play_page_url: str = ""
    duration_sec: int = 0

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MemeBlock":
        btype = str(data.get("type", ""))
        rendered = [
            RenderedImage.from_dict(r)
            for r in (data.get("rendered_images") or [])
            if isinstance(r, dict)
        ]
        messages = [
            ChatMessageNode.from_dict(m)
            for m in (data.get("messages") or [])
            if isinstance(m, dict)
        ]
        return cls(
            type=btype,
            text=str(data.get("text", "")),
            url=str(data.get("url", "")),
            media_id=str(data.get("media_id", "")),
            mime=str(data.get("mime", "")),
            w=int(data.get("w", 0) or 0),
            h=int(data.get("h", 0) or 0),
            size_bytes=int(data.get("size_bytes", 0) or 0),
            rendered_images=rendered,
            messages=messages,
            platform=str(data.get("platform", "")),
            title=str(data.get("title", "")),
            cover_url=str(data.get("cover_url", "")),
            play_page_url=str(data.get("play_page_url", "")),
            duration_sec=int(data.get("duration_sec", 0) or 0),
        )

    def to_dict(self) -> dict[str, Any]:
        if self.type == "text":
            return {"type": "text", "text": self.text}
        if self.type in ("image", "gif"):
            res: dict[str, Any] = {"type": self.type}
            if self.url:
                res["url"] = self.url
            if self.media_id:
                res["media_id"] = self.media_id
            if self.mime:
                res["mime"] = self.mime
            if self.w:
                res["w"] = self.w
            if self.h:
                res["h"] = self.h
            if self.size_bytes:
                res["size_bytes"] = self.size_bytes
            return res
        if self.type == "chat_record":
            res = {"type": "chat_record"}
            if self.rendered_images:
                res["rendered_images"] = [r.to_dict() for r in self.rendered_images]
            res["messages"] = [m.to_dict() for m in self.messages]
            return res
        if self.type == "video_link":
            res = {
                "type": "video_link",
                "url": self.url,
                "platform": self.platform,
                "title": self.title,
                "cover_url": self.cover_url,
                "duration_sec": self.duration_sec,
            }
            return res
        if self.type == "video_file":
            return {
                "type": "video_file",
                "url": self.url,
                "cover_url": self.cover_url,
                "play_page_url": self.play_page_url,
                "mime": self.mime or "video/mp4",
                "w": self.w,
                "h": self.h,
                "duration_sec": self.duration_sec,
                "size_bytes": self.size_bytes,
            }
        raise ValueError(f"Unsupported block type: {self.type!r}")

    def validate(self) -> None:
        if self.type not in VALID_BLOCK_TYPES:
            raise ValueError(f"Invalid block type: {self.type!r}")
        if self.type == "text" and not self.text.strip():
            raise ValueError("text block must contain non-empty text")
        if self.type in ("image", "gif") and not (self.url or self.media_id):
            raise ValueError(f"{self.type} block must contain url or media_id")
        if self.type == "chat_record" and not (self.rendered_images or self.messages):
            raise ValueError("chat_record block must contain rendered_images or messages")
        if self.type == "video_link" and not self.url:
            raise ValueError("video_link block must contain url")


@dataclass
class MemeStats:
    likes: int = 0
    dislikes: int = 0

    @classmethod
    def from_dict(cls, data: Optional[dict[str, Any]]) -> "MemeStats":
        if not isinstance(data, dict):
            return cls()
        return cls(
            likes=int(data.get("likes", 0) or 0),
            dislikes=int(data.get("dislikes", 0) or 0),
        )

    def to_dict(self) -> dict[str, int]:
        return {"likes": self.likes, "dislikes": self.dislikes}


@dataclass
class MemeAttribution:
    submitter_name: str = "匿名网友"
    origin_label: str = ""

    @classmethod
    def from_dict(cls, data: Optional[dict[str, Any]]) -> "MemeAttribution":
        if not isinstance(data, dict):
            return cls()
        return cls(
            submitter_name=str(data.get("submitter_name", "匿名网友") or "匿名网友"),
            origin_label=str(data.get("origin_label", "") or ""),
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "submitter_name": self.submitter_name,
            "origin_label": self.origin_label,
        }


@dataclass
class MemeItem:
    id: str
    blocks: list[MemeBlock]
    schema_version: str = SCHEMA_VERSION
    seq: int = 0
    hub_id: str = ""
    title: str = ""
    summary: str = ""
    tags: list[str] = field(default_factory=list)
    nsfw_level: int = NSFW_SAFE
    score: float = 0.0
    stats: MemeStats = field(default_factory=MemeStats)
    attribution: MemeAttribution = field(default_factory=MemeAttribution)
    web_url: str = ""
    published_at: str = ""
    source_type: str = "hub"  # "hub" | "rss" | "local"

    @property
    def uid(self) -> str:
        if self.id.startswith(("hub:", "rss:", "local:")):
            return self.id
        if self.source_type == "hub":
            hid = self.hub_id or "default"
            return f"hub:{hid}:{self.id}"
        return f"{self.source_type}:{self.id}"

    def content_types(self) -> set[str]:
        return {b.type for b in self.blocks}

    def plain_text_preview(self, max_len: int = 120) -> str:
        if self.summary.strip():
            return self.summary.strip()[:max_len]
        parts: list[str] = []
        if self.title.strip():
            parts.append(self.title.strip())
        for block in self.blocks:
            if block.type == "text" and block.text.strip():
                parts.append(block.text.strip())
            elif block.type == "video_link" and block.title.strip():
                parts.append(block.title.strip())
            elif block.type == "chat_record":
                for node in block.messages:
                    for cp in node.content:
                        if cp.type == "text" and cp.text.strip():
                            parts.append(f"{node.sender_alias}: {cp.text.strip()}")
        joined = " ".join(parts).strip()
        return joined[:max_len]

    def content_hash(self) -> str:
        """Compute deterministic content hash across normalized blocks and title."""
        canonical = {
            "title": self.title.strip(),
            "blocks": [b.to_dict() for b in self.blocks],
        }
        raw = json.dumps(canonical, sort_keys=True, ensure_ascii=False).encode("utf-8")
        return hashlib.sha256(raw).hexdigest()

    def validate(self) -> None:
        if not self.id:
            raise ValueError("MemeItem.id must not be empty")
        if self.schema_version != SCHEMA_VERSION:
            raise ValueError(f"Unsupported schema_version: {self.schema_version!r}")
        if self.nsfw_level not in VALID_NSFW_LEVELS:
            raise ValueError(f"Invalid nsfw_level: {self.nsfw_level!r}")
        if not self.blocks:
            raise ValueError("MemeItem.blocks must not be empty")
        for block in self.blocks:
            block.validate()

    @classmethod
    def from_dict(cls, data: dict[str, Any], source_type: str = "hub") -> "MemeItem":
        raw_blocks = data.get("blocks") or []
        blocks = [MemeBlock.from_dict(b) for b in raw_blocks if isinstance(b, dict)]
        tags = [str(t) for t in (data.get("tags") or []) if str(t).strip()]
        item = cls(
            schema_version=str(data.get("schema_version", SCHEMA_VERSION)),
            id=str(data.get("id", "")),
            seq=int(data.get("seq", 0) or 0),
            hub_id=str(data.get("hub_id", "")),
            title=str(data.get("title", "") or ""),
            summary=str(data.get("summary", "") or ""),
            tags=tags,
            nsfw_level=int(data.get("nsfw_level", NSFW_SAFE) or 0),
            score=float(data.get("score", 0.0) or 0.0),
            stats=MemeStats.from_dict(data.get("stats")),
            attribution=MemeAttribution.from_dict(data.get("attribution")),
            web_url=str(data.get("web_url", "") or ""),
            published_at=str(data.get("published_at", "") or ""),
            blocks=blocks,
            source_type=str(data.get("_source_type", source_type)),
        )
        return item

    def to_dict(self, include_internal: bool = False) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": self.schema_version,
            "id": self.id,
            "seq": self.seq,
            "hub_id": self.hub_id,
            "title": self.title,
            "summary": self.summary,
            "tags": list(self.tags),
            "nsfw_level": self.nsfw_level,
            "score": self.score,
            "stats": self.stats.to_dict(),
            "attribution": self.attribution.to_dict(),
            "web_url": self.web_url,
            "published_at": self.published_at,
            "blocks": [b.to_dict() for b in self.blocks],
        }
        if include_internal:
            payload["_source_type"] = self.source_type
        return payload

    def to_json(self, include_internal: bool = True) -> str:
        return json.dumps(self.to_dict(include_internal=include_internal), ensure_ascii=False)

    @classmethod
    def from_json(cls, raw: str, source_type: str = "hub") -> "MemeItem":
        return cls.from_dict(json.loads(raw), source_type=source_type)


@dataclass
class FeedbackEvent:
    item_id: str
    action: str  # "like" | "dislike" | "report"
    voter_hash: str
    group_hash: str
    occurred_at: str
    reason: Optional[str] = None
    hub_id: str = ""

    def validate(self) -> None:
        if self.action not in FEEDBACK_ACTIONS:
            raise ValueError(f"Invalid feedback action: {self.action!r}")
        if self.action == "report" and self.reason and self.reason not in REPORT_REASONS:
            raise ValueError(f"Invalid report reason: {self.reason!r}")
        if not self.item_id or not self.voter_hash or not self.group_hash:
            raise ValueError("FeedbackEvent missing required identifiers")

    def to_api_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "item_id": self.item_id,
            "action": self.action,
            "voter_hash": self.voter_hash,
            "group_hash": self.group_hash,
            "occurred_at": self.occurred_at,
        }
        if self.reason is not None:
            d["reason"] = self.reason
        return d


@dataclass
class HubCapabilities:
    video_file: bool = False
    feedback: bool = True

    @classmethod
    def from_dict(cls, data: Optional[dict[str, Any]]) -> "HubCapabilities":
        if not isinstance(data, dict):
            return cls()
        return cls(
            video_file=bool(data.get("video_file", False)),
            feedback=bool(data.get("feedback", True)),
        )


@dataclass
class HubLimits:
    max_image_bytes: int = DEFAULT_LIMITS["max_image_bytes"]
    max_gif_bytes: int = DEFAULT_LIMITS["max_gif_bytes"]
    max_images_per_submission: int = DEFAULT_LIMITS["max_images_per_submission"]
    max_chat_nodes: int = DEFAULT_LIMITS["max_chat_nodes"]
    max_chat_depth: int = DEFAULT_LIMITS["max_chat_depth"]

    @classmethod
    def from_dict(cls, data: Optional[dict[str, Any]]) -> "HubLimits":
        if not isinstance(data, dict):
            return cls()
        return cls(
            max_image_bytes=int(data.get("max_image_bytes", DEFAULT_LIMITS["max_image_bytes"])),
            max_gif_bytes=int(data.get("max_gif_bytes", DEFAULT_LIMITS["max_gif_bytes"])),
            max_images_per_submission=int(
                data.get("max_images_per_submission", DEFAULT_LIMITS["max_images_per_submission"])
            ),
            max_chat_nodes=int(data.get("max_chat_nodes", DEFAULT_LIMITS["max_chat_nodes"])),
            max_chat_depth=int(data.get("max_chat_depth", DEFAULT_LIMITS["max_chat_depth"])),
        )


@dataclass
class HubMeta:
    hub_id: str
    hub_name: str = ""
    schema_version: str = SCHEMA_VERSION
    capabilities: HubCapabilities = field(default_factory=HubCapabilities)
    limits: HubLimits = field(default_factory=HubLimits)
    tags: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "HubMeta":
        return cls(
            hub_id=str(data.get("hub_id", "official-main")),
            hub_name=str(data.get("hub_name", "")),
            schema_version=str(data.get("schema_version", SCHEMA_VERSION)),
            capabilities=HubCapabilities.from_dict(data.get("capabilities")),
            limits=HubLimits.from_dict(data.get("limits")),
            tags=[str(t) for t in (data.get("tags") or [])],
        )
