"""Unit tests for the bounded event_id dedup cache (contract section 8)."""

from __future__ import annotations

from custom_components.kameraposti.dedup import EventDedupCache


def test_first_sighting_is_not_a_duplicate() -> None:
    cache = EventDedupCache()

    assert cache.seen("01A") is False


def test_second_sighting_of_the_same_id_is_a_duplicate() -> None:
    """B. Dedup: same event_id twice -- first processed, second ignored."""
    cache = EventDedupCache()

    assert cache.seen("01A") is False
    assert cache.seen("01A") is True


def test_different_event_ids_are_never_deduplicated_against_each_other() -> None:
    """C. Same content, different event_id -- both must be treated as new."""
    cache = EventDedupCache()

    assert cache.seen("01A") is False
    assert cache.seen("01B") is False
    assert cache.seen("01A") is True
    assert cache.seen("01B") is True


def test_cache_is_bounded_and_evicts_the_oldest_entry() -> None:
    cache = EventDedupCache(max_size=3)

    cache.seen("1")
    cache.seen("2")
    cache.seen("3")
    assert len(cache) == 3

    cache.seen("4")
    assert len(cache) == 3
    # "1" was the oldest and least recently touched -- evicted, so it now
    # looks unseen again if it somehow arrived a second time.
    assert cache.seen("1") is False
    assert cache.seen("4") is True


def test_re_seeing_an_id_refreshes_its_recency() -> None:
    cache = EventDedupCache(max_size=2)

    cache.seen("1")
    cache.seen("2")
    cache.seen("1")  # touch "1" again -- "2" is now the least recently used
    cache.seen("3")  # should evict "2", not "1"

    assert cache.seen("1") is True
    assert cache.seen("2") is False
