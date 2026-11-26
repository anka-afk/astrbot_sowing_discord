"""Cross-platform AstrBot sink using unified_msg_origin + MessageChain."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

try:
    from astrbot.api.event import MessageChain  # type: ignore
    from astrbot.api.message_components import Image, Plain  # type: ignore
except ImportError:
    # Graceful lightweight shims when AstrBot is not installed (e.g., unit tests)
    @dataclass
    class Plain:  # type: ignore[no-redef]
        text: str

    @dataclass
    class Image:  # type: ignore[no-redef]
        url: str = ""
        file: str = ""

        @classmethod
        def fromURL(cls, url: str) -> "Image":
            return cls(url=url)

        @classmethod
        def fromFileSystem(cls, path: str) -> "Image":
            return cls(file=str(path))

    @dataclass
    class MessageChain:  # type: ignore[no-redef]
        chain: list[Any] = field(default_factory=list)

        def message(self, text: str) -> "MessageChain":
            self.chain.append(Plain(text))
            return self

        def url_image(self, url: str) -> "MessageChain":
            self.chain.append(Image.fromURL(url))
            return self

        def file_image(self, path: str) -> "MessageChain":
            self.chain.append(Image.fromFileSystem(path))
            return self


try:
    from ..models.meme import MemeBlock, MemeItem
except ImportError:
    from models.meme import MemeBlock, MemeItem

from .media_cache import MediaCache

logger = logging.getLogger(__name__)

# Platforms where external URL images are unreliable or WebP needs JPEG fallback
_LOCAL_DOWNLOAD_PLATFORMS = ("wechat", "gewechat", "wechatpad", "wecom", "wx")
_WEBP_DEGRADE_PLATFORMS = ("wechat", "gewechat", "wechatpad", "wecom", "wx")


@dataclass
class DeliveryResult:
    ok: bool
    bot_msg_id: str = ""
    used_native_forward: bool = False
    chains_sent: int = 0
    error: Optional[str] = None


SendCallable = Callable[[str, Any], Awaitable[Any]]
ForwardCallable = Callable[[str, list[dict[str, Any]]], Awaitable[Any]]


def detect_platform(group_umo: str) -> str:
    """Extract lowercase platform identifier from AstrBot unified_msg_origin."""
    if ":" in group_umo:
        return group_umo.split(":", 1)[0].strip().lower()
    return "unknown"


def extract_group_id(group_umo: str) -> str:
    if ":" in group_umo:
        return group_umo.rsplit(":", 1)[-1].strip()
    return group_umo.strip()


def _extract_msg_id(send_result: Any) -> str:
    if send_result is None:
        return ""
    if isinstance(send_result, dict):
        mid = send_result.get("message_id") or send_result.get("msg_id") or ""
        return str(mid)
    for attr in ("message_id", "msg_id", "id"):
        val = getattr(send_result, attr, None)
        if val:
            return str(val)
    return ""


class AstrBotSink:
    """Delivers MemeItem objects across QQ/Telegram/Discord/WeChat/WeCom/Lark via AstrBot."""

    def __init__(
        self,
        context: Any = None,
        *,
        media_cache: Optional[MediaCache] = None,
        qq_native_forward: bool = False,
        send_fn: Optional[SendCallable] = None,
        qq_forward_fn: Optional[ForwardCallable] = None,
    ):
        self.context = context
        self.media_cache = media_cache
        self.qq_native_forward = qq_native_forward
        self._send_fn = send_fn
        self._qq_forward_fn = qq_forward_fn

    def _needs_local_file(self, platform: str) -> bool:
        return any(p in platform for p in _LOCAL_DOWNLOAD_PLATFORMS)

    def _needs_webp_degrade(self, platform: str) -> bool:
        return any(p in platform for p in _WEBP_DEGRADE_PLATFORMS)

    async def _build_image_component(
        self, url: str, platform: str, mime_hint: str = ""
    ) -> Any:
        if self.media_cache is not None and self._needs_local_file(platform):
            try:
                local_path = await self.media_cache.get_or_download(
                    url,
                    mime_hint=mime_hint,
                    convert_webp_to_jpeg=self._needs_webp_degrade(platform),
                )
                return Image.fromFileSystem(str(local_path))
            except Exception as exc:
                logger.warning("Local media cache download failed for %s: %s", url, exc)
        return Image.fromURL(url)

    def _build_onebot_forward_nodes_from_msgs(
        self, messages: list[Any], depth: int = 0
    ) -> list[dict[str, Any]]:
        nodes: list[dict[str, Any]] = []
        for idx, msg in enumerate(messages):
            segments: list[dict[str, Any]] = []
            for part in msg.content:
                if part.type == "text" and part.text:
                    segments.append({"type": "text", "data": {"text": part.text}})
                elif part.type in ("image", "gif") and part.url:
                    segments.append({"type": "image", "data": {"file": part.url}})
                elif part.type == "forward_quote" and part.messages and depth < 3:
                    nested_nodes = self._build_onebot_forward_nodes_from_msgs(
                        part.messages, depth=depth + 1
                    )
                    if nested_nodes:
                        segments.extend(nested_nodes)
            if not segments:
                continue
            nodes.append(
                {
                    "type": "node",
                    "data": {
                        "name": msg.sender_alias,
                        "uin": str(10000 + depth * 100 + idx),
                        "content": segments,
                    },
                }
            )
        return nodes

    def _build_onebot_forward_nodes(self, block: MemeBlock) -> list[dict[str, Any]]:
        return self._build_onebot_forward_nodes_from_msgs(block.messages, depth=0)

    async def _try_send_qq_forward(
        self, group_umo: str, block: MemeBlock
    ) -> Optional[str]:
        if not block.messages:
            return None
        nodes = self._build_onebot_forward_nodes(block)
        if not nodes:
            return None
        group_id = extract_group_id(group_umo)

        if self._qq_forward_fn is not None:
            res = await self._qq_forward_fn(group_id, nodes)
            return _extract_msg_id(res) or "forward_sent"

        # Try AstrBot aiocqhttp platform adapter if available
        if self.context is not None:
            get_platform = getattr(self.context, "get_platform", None)
            if callable(get_platform):
                plat = get_platform("aiocqhttp")
                client = getattr(plat, "get_client", lambda: None)() if plat else None
                if client is not None and hasattr(client, "api"):
                    res = await client.api.call_action(
                        "send_group_forward_msg",
                        group_id=int(group_id) if group_id.isdigit() else group_id,
                        messages=nodes,
                    )
                    return _extract_msg_id(res) or "forward_sent"
        return None

    async def build_chains(
        self,
        item: MemeItem,
        group_umo: str,
        *,
        commentary: Optional[str] = None,
        skip_chat_blocks: bool = False,
    ) -> list[MessageChain]:
        """Convert MemeItem blocks into one or more AstrBot MessageChains."""
        platform = detect_platform(group_umo)
        components: list[Any] = []

        if commentary:
            components.append(Plain(f"{commentary}\n"))
        if item.title and not any(b.type == "video_link" and b.title == item.title for b in item.blocks):
            components.append(Plain(f"【{item.title}】\n"))

        for block in item.blocks:
            if block.type == "text" and block.text:
                components.append(Plain(block.text))
            elif block.type in ("image", "gif") and block.url:
                components.append(
                    await self._build_image_component(block.url, platform, block.mime)
                )
            elif block.type == "chat_record":
                if skip_chat_blocks:
                    continue
                if block.rendered_images:
                    for slice_img in block.rendered_images:
                        if slice_img.url:
                            components.append(
                                await self._build_image_component(
                                    slice_img.url, platform, slice_img.mime
                                )
                            )
                elif block.messages:
                    # Fallback plain-text dialogue when rendered_images absent (e.g., local source)
                    lines: list[str] = []
                    for node in block.messages:
                        body = " ".join(
                            p.text for p in node.content if p.type == "text" and p.text
                        )
                        if body:
                            lines.append(f"{node.sender_alias}: {body}")
                    if lines:
                        components.append(Plain("\n".join(lines)))
            elif block.type == "video_link":
                if block.cover_url:
                    components.append(
                        await self._build_image_component(block.cover_url, platform, "image/webp")
                    )
                label = f"[{block.platform}] " if block.platform else ""
                title = block.title or "外链视频"
                components.append(Plain(f"{label}{title}\n{block.url}"))
            elif block.type == "video_file":
                if block.cover_url:
                    components.append(
                        await self._build_image_component(block.cover_url, platform, "image/webp")
                    )
                link = block.play_page_url or block.url
                if link:
                    components.append(Plain(f"[视频] {link}"))

        if not components:
            return []
        return [MessageChain(chain=components)]

    async def _send_chain(self, group_umo: str, chain: MessageChain) -> Any:
        if self._send_fn is not None:
            return await self._send_fn(group_umo, chain)
        if self.context is not None and hasattr(self.context, "send_message"):
            return await self.context.send_message(group_umo, chain)
        raise RuntimeError("No AstrBot context or send_fn configured for AstrBotSink")

    async def deliver(
        self,
        item: MemeItem,
        group_umo: str,
        *,
        commentary: Optional[str] = None,
    ) -> DeliveryResult:
        """Deliver MemeItem to group_umo with QQ native-forward optional path + image fallback."""
        platform = detect_platform(group_umo)
        used_native_forward = False
        first_msg_id = ""
        chains_sent = 0

        chat_blocks = [b for b in item.blocks if b.type == "chat_record"]
        skip_chat_in_chain = False

        if self.qq_native_forward and platform == "aiocqhttp" and chat_blocks:
            all_forwarded = True
            for cb in chat_blocks:
                try:
                    fwd_id = await self._try_send_qq_forward(group_umo, cb)
                    if not fwd_id:
                        all_forwarded = False
                        break
                    if not first_msg_id:
                        first_msg_id = fwd_id
                except Exception as exc:
                    logger.warning(
                        "QQ native forward failed for %s (%s); falling back to rendered_images",
                        item.uid,
                        exc,
                    )
                    all_forwarded = False
                    break
            if all_forwarded:
                used_native_forward = True
                skip_chat_in_chain = True
                chains_sent += len(chat_blocks)

        chains = await self.build_chains(
            item,
            group_umo,
            commentary=commentary,
            skip_chat_blocks=skip_chat_in_chain,
        )

        try:
            for chain in chains:
                res = await self._send_chain(group_umo, chain)
                chains_sent += 1
                if not first_msg_id:
                    first_msg_id = _extract_msg_id(res)
            if chains_sent == 0:
                return DeliveryResult(ok=False, error="empty_meme_content")
            return DeliveryResult(
                ok=True,
                bot_msg_id=first_msg_id,
                used_native_forward=used_native_forward,
                chains_sent=chains_sent,
            )
        except Exception as exc:
            logger.warning("Delivery failed for %s -> %s: %s", item.uid, group_umo, exc)
            return DeliveryResult(
                ok=False,
                bot_msg_id=first_msg_id,
                used_native_forward=used_native_forward,
                chains_sent=chains_sent,
                error=str(exc),
            )
