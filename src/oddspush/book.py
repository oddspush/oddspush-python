"""A local price book: the current open markets of every event, kept up to date from the
stream.

The rules (the same as in the API documentation):

- markets are ordered by ``version``: an update older than the one held is ignored;
- a market whose ``status`` is not ``open`` is removed;
- when a period turns ``closed`` or ``settled``, every market of that period is removed;
- ``op: "del"`` removes the whole event, whatever channel it comes on;
- a snapshot is merged by version, never used to clear the book.

Specials and player props (``rec.type == "special"``) have no market ``status``: a special
market is kept while its period is not ``settled``. A ``closed`` period suspends it (see
:meth:`Book.is_suspended`) instead of removing it, because it usually reopens within
seconds. Their market updates often come without ``version``: those repeat the price
held, so a held market is kept. ``rec.version`` of a special goes up when its periods change,
so a special update with a lower ``rec.version`` than one already applied is ignored.

:class:`~oddspush.Stream` keeps one up to date for you (``stream.book``); you can also feed
one yourself with :meth:`Book.apply_message`.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from ._constants import PRICE_CHANNELS

Market = Dict[str, Any]
_CLOSED_PERIOD = ("closed", "settled")
_REMOVED_MAX = 20000      # removed event ids remembered, so a late snapshot cannot revive them


def channel_of(topic: Optional[str]) -> str:
    """The channel of an ``odds`` update, from the last segment of its ``topic``."""
    last = (topic or "").rsplit("/", 1)[-1]
    return last if last in ("ld", "both", "dz", "pre") else "other"


class _Event:
    __slots__ = ("meta", "markets", "versions", "fair", "updated", "live", "rec_version")

    def __init__(self) -> None:
        self.updated = time.monotonic()
        self.live = False           # got a live update: snapshots no longer rewind it
        self.rec_version: Optional[int] = None   # specials: highest rec.version applied
        self.meta: Dict[str, Any] = {}
        self.markets: Dict[str, Market] = {}
        # The highest version seen per key, closed markets included: a late update of a
        # market that has since closed must not bring it back.
        self.versions: Dict[str, int] = {}
        # key -> {"version", "margin", "prices"} as sent in `fair` messages.
        self.fair: Dict[str, Dict[str, Any]] = {}


class Book:
    """Open markets per event, merged by version.

    ``channels`` are the channels whose live updates go into the book: ``ld`` and ``both``
    by default, which are the prices. Keep ``dz`` (volatile moments) out of a price book.
    Add ``pre`` to follow pre-match prices.
    """

    def __init__(self, channels: Iterable[str] = PRICE_CHANNELS) -> None:
        self.channels = frozenset(channels)
        self._events: Dict[int, _Event] = {}
        self._removed: "OrderedDict[Any, None]" = OrderedDict()

    # ------------------------------------------------------------------ feeding it
    def apply_message(self, msg: Dict[str, Any]) -> bool:
        """Apply one message from the stream. Returns ``True`` if it touched the book.

        Handles ``odds`` (live updates, removals and the state resent by the source),
        ``snapshot`` parts and ``fair``. Any other message is ignored.
        """
        kind = msg.get("type")
        if kind == "odds":
            data = msg.get("data") or {}
            dtype = data.get("type")
            if dtype == "snapshot":
                for rec in data.get("events") or ():
                    self.apply_rec(rec, snapshot=True)
                return True
            if dtype != "live":
                return False
            rec = data.get("rec") or {}
            if data.get("op") == "del":
                return self.remove(rec.get("id"))
            if channel_of(data.get("topic")) not in self.channels:
                return False
            self.apply_rec(rec)
            return True
        if kind == "snapshot":
            for rec in msg.get("events") or ():
                self.apply_rec(rec, snapshot=True)
            return True
        if kind == "fair":
            return self.apply_fair(msg.get("data") or {})
        return False

    def apply_rec(self, rec: Dict[str, Any], snapshot: bool = False) -> None:
        """Merge one event (a ``rec``) into the book.

        ``snapshot=True`` for a rec from a snapshot. A snapshot is taken when it is asked
        for and can arrive after newer live updates, so it never brings back a removed
        event, and for an event already updated live it only adds what is newer: its
        details and closed periods do not overwrite the live ones.
        """
        event_id = rec.get("id")
        if event_id is None:
            return
        if snapshot and event_id in self._removed:
            return
        ev = self._events.get(event_id)
        if ev is None:
            ev = self._events[event_id] = _Event()
            if not snapshot:
                self._removed.pop(event_id, None)
        special = rec.get("type") == "special"
        newer = False
        if special:
            rv = rec.get("version")
            if isinstance(rv, int) and not isinstance(rv, bool):
                if ev.rec_version is not None and rv < ev.rec_version:
                    return                      # an older special update arriving late
                newer = ev.rec_version is not None and rv > ev.rec_version
                ev.rec_version = rv
        ev.updated = time.monotonic()
        # A snapshot of a special newer than what is held (by rec.version) is not a rewind:
        # its periods are the current ones.
        rewind = snapshot and ev.live and not newer
        if rewind:
            for k, v in rec.items():
                if k != "markets":
                    ev.meta.setdefault(k, v)
        else:
            ev.meta.update((k, v) for k, v in rec.items() if k != "markets")
            if not snapshot:
                ev.live = True
        newest = None
        for m in rec.get("markets") or ():
            key = m.get("key")
            if key is None:
                continue
            version = m.get("version")
            if version is not None and (newest is None or version > newest):
                newest = version
            seen = ev.versions.get(key)
            if (special and version is None and key in ev.markets
                    and m.get("status", "open") == "open"):
                continue                        # no version: it repeats the price held
            if version is not None:
                if seen is not None and version < seen:
                    continue                    # an older update arriving late
                ev.versions[key] = version
            if m.get("status") == "open" or (special and "status" not in m):
                fair = m.get("fair")
                if fair is not None:            # snapshot parts carry it inline
                    m = {k: v for k, v in m.items() if k != "fair"}
                    ev.fair[key] = {"version": version, **fair}
                ev.markets[key] = m
            else:
                ev.markets.pop(key, None)
                ev.fair.pop(key, None)
        if rewind:
            # An older picture of the periods: close only markets no newer than it.
            if newest is not None:
                self._close_periods(ev, rec.get("periods"), up_to=newest, special=special)
        else:
            self._close_periods(ev, rec.get("periods"), special=special)

    @staticmethod
    def _period_status(periods: Any) -> Dict[str, Any]:
        """``{"<period>": status}`` from either form of ``periods``."""
        out: Dict[str, Any] = {}
        if isinstance(periods, dict):           # {"0": {"status": ...}} or {"0": "open"}
            for num, p in periods.items():
                out[str(num)] = p.get("status") if isinstance(p, dict) else p
        elif isinstance(periods, list):
            for p in periods:
                if isinstance(p, dict):
                    out[str(p.get("period"))] = p.get("status")
        return out

    @classmethod
    def _close_periods(cls, ev: _Event, periods: Any, up_to: Optional[int] = None,
                       special: bool = False) -> None:
        # A special is only gone when its period is settled: `closed` is a suspension.
        removing = ("settled",) if special else _CLOSED_PERIOD
        closed = {num for num, status in cls._period_status(periods).items()
                  if status in removing}
        if not closed:
            return
        for key in [k for k, m in ev.markets.items() if str(m.get("period")) in closed
                    and (up_to is None or (m.get("version") or 0) <= up_to)]:
            del ev.markets[key]
            ev.fair.pop(key, None)

    def apply_fair(self, data: Dict[str, Any]) -> bool:
        """Store the no-vig and fair prices of a ``fair`` message: only for the book's
        channels, and only for the very market version the book holds."""
        ev = self._events.get(data.get("event_id"))
        if ev is None:
            return False
        channel = data.get("channel")
        if channel not in ("ld", "both", "dz", "pre"):
            channel = "other"
        if channel not in self.channels:
            return False
        stored = False
        for m in data.get("markets") or ():
            key = m.get("key")
            held = ev.markets.get(key) if key is not None else None
            if held is None or held.get("version") != m.get("version"):
                continue
            ev.fair[key] = {"version": m.get("version"), "margin": m.get("margin"),
                            "prices": m.get("prices") or []}
            stored = True
        return stored

    def remove(self, event_id: Any) -> bool:
        """Remove a whole event. Returns ``True`` if it was in the book. A snapshot that
        arrives later will not bring it back; a live update will."""
        self._removed[event_id] = None
        self._removed.move_to_end(event_id)
        while len(self._removed) > _REMOVED_MAX:
            self._removed.popitem(last=False)
        return self._events.pop(event_id, None) is not None

    def prune(self, max_idle_s: float) -> List[int]:
        """Remove the events with no update for ``max_idle_s`` seconds and no open market
        left, and return their ids. Useful in a long-running bot: an event whose removal you
        missed (while disconnected, say) otherwise stays in the book."""
        limit = time.monotonic() - max_idle_s
        gone = [eid for eid, ev in self._events.items()
                if ev.updated < limit and not ev.markets]
        for eid in gone:
            del self._events[eid]
        return gone

    def clear(self) -> None:
        self._events.clear()
        self._removed.clear()

    # ------------------------------------------------------------------ reading it
    def __len__(self) -> int:
        return len(self._events)

    def __contains__(self, event_id: object) -> bool:
        return event_id in self._events

    def __iter__(self) -> Iterator[int]:
        return iter(list(self._events))

    def event_ids(self) -> List[int]:
        return list(self._events)

    def event(self, event_id: int) -> Optional[Dict[str, Any]]:
        """The event's details (``league``, ``participants``, ``state``, ``startTime``...),
        without its markets, or ``None``."""
        ev = self._events.get(event_id)
        return ev.meta if ev is not None else None

    def markets(self, event_id: int) -> Dict[str, Market]:
        """The open markets of an event, by ``key`` (empty if unknown)."""
        ev = self._events.get(event_id)
        return dict(ev.markets) if ev is not None else {}

    def market(self, event_id: int, key: str) -> Optional[Market]:
        ev = self._events.get(event_id)
        return ev.markets.get(key) if ev is not None else None

    def is_suspended(self, event_id: int, key: str) -> bool:
        """``True`` if the market is held but its period is ``closed`` right now.

        Only specials stay in the book while suspended (their period turns ``closed`` and
        back to ``open``); other markets are removed when their period closes.
        """
        ev = self._events.get(event_id)
        m = ev.markets.get(key) if ev is not None else None
        if m is None:
            return False
        status = self._period_status(ev.meta.get("periods")).get(str(m.get("period")))
        return status == "closed"

    def fair(self, event_id: int, key: str) -> Optional[Dict[str, Any]]:
        """``{"version", "margin", "prices"}`` for the market held, or ``None``.

        Only returned when it belongs to the very ``version`` of the market in the book: an
        older or newer one would be another price.
        """
        ev = self._events.get(event_id)
        if ev is None:
            return None
        m = ev.markets.get(key)
        f = ev.fair.get(key)
        if m is None or f is None or f.get("version") != m.get("version"):
            return None
        return f

    def items(self) -> Iterator[Tuple[int, Dict[str, Market]]]:
        """``(event_id, markets)`` for every event."""
        for event_id, ev in list(self._events.items()):
            yield event_id, dict(ev.markets)
