"""The snapshots' message budget (1.14.2, bug hunt 19.10 H-L4).

Kameraposti counts every message of an account -- every Home Assistant and device on it -- against a
budget of 300 a minute (1000 on an account without sensor limits) before reading it, and drops the rest
unread: a description, the selection list, an alarm. The broker acknowledges them all the same. An
unlimited account's full snapshot (200 sensors and their states, 50 cameras, the list) is about 455
messages, and a reconnect, a reload or the 15-minute resend could follow another one within seconds.

The snapshots (sensor_export.py, camera_export.py) of one account on one service share one budget, kept
across reloads and shared by the account's entries: at most SNAPSHOT_BUDGET_MESSAGES in any
SNAPSHOT_BUDGET_SECONDS; what does not fit follows as it frees. A state change never waits for it (nor
counts in it); what a break left unsent does not wait either, but counts, so the snapshot leaves it room.
"""

from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta

from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util

from .const import DOMAIN, SNAPSHOT_BUDGET_MESSAGES, SNAPSHOT_BUDGET_SECONDS

_BUDGETS = f"{DOMAIN}_snapshot_budgets"


class SnapshotBudget:
    """How many snapshot messages may go out now: at most ``messages`` in any ``seconds``."""

    def __init__(
        self, messages: int = SNAPSHOT_BUDGET_MESSAGES, seconds: float = SNAPSHOT_BUDGET_SECONDS
    ) -> None:
        self._messages = messages
        self._window = timedelta(seconds=seconds)
        # When each counted message went out, oldest first.
        self._sent: deque[datetime] = deque()

    def _expire(self, now: datetime) -> None:
        while self._sent and now - self._sent[0] >= self._window:
            self._sent.popleft()

    def has_room(self, count: int) -> bool:
        """Whether ``count`` more messages fit now (more than the whole budget fits an empty one)."""
        self._expire(dt_util.utcnow())
        return len(self._sent) + min(count, self._messages) <= self._messages

    def record(self, count: int = 1) -> None:
        """``count`` messages went out now."""
        now = dt_util.utcnow()
        self._expire(now)
        self._sent.extend([now] * count)

    def seconds_until_room(self, count: int) -> float:
        """Until ``count`` more messages fit (0: now)."""
        now = dt_util.utcnow()
        self._expire(now)
        over = len(self._sent) + min(count, self._messages) - self._messages
        if over <= 0:
            return 0.0
        return max(0.0, (self._sent[over - 1] + self._window - now).total_seconds())


def snapshot_budget(hass: HomeAssistant, host: str, customer_id: int) -> SnapshotBudget:
    """The account's budget on that service: Kameraposti counts the account, not the login or the entry."""
    budgets: dict[tuple[str, int], SnapshotBudget] = hass.data.setdefault(_BUDGETS, {})
    return budgets.setdefault((host, customer_id), SnapshotBudget())
