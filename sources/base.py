"""Abstract base class for pluggable meme sources."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Optional

try:
    from ..models.meme import MemeItem
except ImportError:
    from models.meme import MemeItem


@dataclass
class SyncResult:
    source_id: str
    new_items: list[MemeItem] = field(default_factory=list)
    tombstones_removed: int = 0
    not_modified: bool = False
    skipped_backoff: bool = False
    error: Optional[str] = None


class BaseSource(ABC):
    """Pluggable content source interface."""

    source_type: str = "generic"

    def __init__(self, source_id: str, enabled: bool = True):
        self.source_id = source_id
        self.enabled = enabled

    @abstractmethod
    async def sync(self) -> SyncResult:
        """Synchronize new items (and tombstones if supported) into local storage."""

    async def fetch_random(self, limit: int = 20, **kwargs: Any) -> list[MemeItem]:
        """Optional replenishment API for random candidates."""
        return []

    async def close(self) -> None:
        """Release any open network resources."""
        return None
