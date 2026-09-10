"""Bounded in-memory dedup cache for MQTT event_ids (contract section 8).

QoS 1 is at-least-once delivery, and the backend guarantees a stable
event_id per detection even across its own retries -- so the same
event_id can legitimately arrive more than once. This cache is the
integration's side of that contract: a bounded, in-memory, non-persisted
set of recently seen ids. Not required to survive a Home Assistant
restart (contract section 8 is explicit about this) and MUST NOT be
backed by a database just for this.
"""

from __future__ import annotations

from collections import OrderedDict

from .const import DEDUP_CACHE_SIZE


class EventDedupCache:
    """LRU-bounded membership cache of recently seen event_ids."""

    def __init__(self, max_size: int = DEDUP_CACHE_SIZE) -> None:
        self._max_size = max_size
        self._seen: OrderedDict[str, None] = OrderedDict()

    def seen(self, event_id: str) -> bool:
        """Return True if event_id was already recorded; record it either way.

        Recently-touched ids are moved to the end (LRU-ish), so the
        entries most likely to be re-delivered by a broker retry are the
        ones least likely to be evicted first.
        """
        if event_id in self._seen:
            self._seen.move_to_end(event_id)
            return True

        self._seen[event_id] = None
        if len(self._seen) > self._max_size:
            self._seen.popitem(last=False)
        return False

    def __len__(self) -> int:
        return len(self._seen)
