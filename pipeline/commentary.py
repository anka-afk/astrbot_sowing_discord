"""Optional text-only LLM commentary generator with 5s hard timeout silent degradation."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any, Awaitable, Callable, Optional

try:
    from ..models.meme import MemeItem
except ImportError:
    from models.meme import MemeItem

logger = logging.getLogger(__name__)

LLMInvoker = Callable[[str], Awaitable[Optional[str]]]

_PROMPT_TEMPLATE = (
    "你是一个幽默克制的群聊捧哏助手。请根据下面这条即将分享给群友的梗的文字元信息，"
    "写一句简短自然的中文吐槽或暖场开场白（不超过 25 个字，不要带引号，不要剧透，不要说教）。\n"
    "标题：{title}\n"
    "梗概：{summary}\n"
    "标签：{tags}\n"
    "只输出一句话："
)


class CommentaryGenerator:
    """
    Reuses AstrBot's configured LLM provider to generate a brief commentary.

    Only feeds `title + summary + tags` (never images), enforces a strict 5s hard
    timeout, and silently returns `None` on timeout, error, or probability skip.
    """

    def __init__(
        self,
        context: Any = None,
        *,
        timeout_sec: float = 5.0,
        max_chars: int = 80,
        llm_invoker: Optional[LLMInvoker] = None,
        rng: Optional[random.Random] = None,
    ):
        self.context = context
        self.timeout_sec = max(0.1, float(timeout_sec))
        self.max_chars = max_chars
        self._llm_invoker = llm_invoker
        self._rng = rng or random.Random()

    def build_prompt(self, item: MemeItem) -> Optional[str]:
        """Build a strictly text-only prompt from title + summary + tags."""
        title = item.title.strip()
        summary = item.summary.strip() or item.plain_text_preview(120)
        tags = ", ".join(item.tags[:6])
        if not (title or summary or tags):
            return None
        return _PROMPT_TEMPLATE.format(
            title=title or "无",
            summary=summary or "无",
            tags=tags or "无",
        )

    async def _call_astrbot_provider(self, prompt: str) -> Optional[str]:
        if self._llm_invoker is not None:
            return await self._llm_invoker(prompt)
        if self.context is None:
            return None

        get_provider = getattr(self.context, "get_using_provider", None)
        if not callable(get_provider):
            return None
        provider = get_provider()
        if provider is None:
            return None

        text_chat = getattr(provider, "text_chat", None)
        if not callable(text_chat):
            return None

        resp = await text_chat(prompt=prompt)
        if resp is None:
            return None
        text = getattr(resp, "completion_text", None)
        if text is None and isinstance(resp, str):
            text = resp
        return str(text) if text else None

    async def generate(
        self, item: MemeItem, *, probability: float = 0.3, force: bool = False
    ) -> Optional[str]:
        """Return a short commentary string or None (on skip, timeout, or error)."""
        if not force:
            if probability <= 0.0:
                return None
            if probability < 1.0 and self._rng.random() > probability:
                return None

        prompt = self.build_prompt(item)
        if not prompt:
            return None

        try:
            raw = await asyncio.wait_for(
                self._call_astrbot_provider(prompt), timeout=self.timeout_sec
            )
        except asyncio.TimeoutError:
            logger.debug("LLM commentary timed out after %.1fs; degrading silently", self.timeout_sec)
            return None
        except Exception as exc:
            logger.debug("LLM commentary failed (%s); degrading silently", exc)
            return None

        if not raw:
            return None
        cleaned = raw.strip().strip('"“”\'').replace("\n", " ").strip()
        if not cleaned:
            return None
        return cleaned[: self.max_chars]
