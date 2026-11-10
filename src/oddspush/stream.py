"""The real-time stream: one WebSocket that logs in, subscribes, and yields every message.

>>> async with Stream("opk_...", sports=[1], fair=True) as stream:
...     async for msg in stream:
...         if msg["type"] == "odds":
...             ...

What it does for you:

- logs in and subscribes; after a disconnect it reconnects with exponential backoff
  (1 s, 2 s, 4 s... up to 30 s), subscribes again to the filter the server last confirmed
  and asks for a snapshot, so your book catches up;
- asks for a snapshot after ``missed`` (you fell behind) and after ``feed`` ``up`` (the
  source came back), and asks again when the snapshot rate limit says so;
- keeps ``stream.book`` (a :class:`~oddspush.Book`) up to date with every update;
- never lets a snapshot put ``dz`` (or any channel outside the book) into the book: with such
  a channel in the filter it asks for snapshots of the book's channels only;
- stops, raising the error, on what retrying cannot fix: an unknown token, an expired plan,
  a plan without the stream, or the token taken over from another machine (``replaced``:
  reconnecting would knock that machine off, and it would do the same to you).

Besides the API's own messages, the iterator yields one of its own: ``{"type":
"disconnected", "reason": ..., "retry_in_s": ...}`` each time the connection drops.

Read fast: process the messages in another task if your work is slow. If you fall more than
15 seconds behind, the server closes the connection (``too_slow``) and the stream
reconnects.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from typing import Any, Dict, FrozenSet, Iterable, List, Optional, Tuple

import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from ._constants import PRICE_CHANNELS, STREAM_URL
from .book import Book
from .errors import (CLOSE_CODES, FatalError, OddsPushError, RateLimited, StreamClosed,
                     error_for)

log = logging.getLogger("oddspush")

_END = object()
_MAX_MESSAGE = 16 * 1024 * 1024
_STABLE_S = 60.0           # connected this long: the next drop starts the backoff again
_MAINTENANCE_WAIT_S = 3.0  # `feed: maintenance` means we are restarting
_RETRY_MARGIN_S = 0.5      # `retry_after_s` is rounded to 0.1 s: ask a little later
_RETRY_DEFAULT_S = 60.0    # a refusal without `retry_after_s`
_PENDING_MAX = 100         # messages kept while disconnected
_FILTER_ERRORS = ("too_many_events", "filter_too_large", "empty_filter")


def _fields(sports: Optional[Iterable[int]] = None, events: Optional[Iterable[int]] = None,
            channels: Optional[Iterable[str]] = None, streams: Optional[Iterable[str]] = None,
            everything: bool = False) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if sports:
        out["sports"] = [int(s) for s in sports]
    if events:
        out["events"] = [int(e) for e in events]
    if channels:
        out["channels"] = list(channels)
    if streams:
        out["streams"] = list(streams)
    if everything:
        out["all"] = True
    return out


def _asks_snapshot(msg: Dict[str, Any]) -> bool:
    kind = msg.get("type")
    return kind == "snapshot" or (kind == "subscribe" and msg.get("snapshot", True) is not False)


def _close_info(exc: BaseException) -> Tuple[Optional[int], str]:
    rcvd = getattr(exc, "rcvd", None)
    if rcvd is not None:
        return rcvd.code, rcvd.reason or ""
    return None, ""


class Stream:
    """The OddsPush WebSocket stream, with reconnection and a local price book.

    :param token: your API key (``opk_...``).
    :param sports: sport IDs to follow (see :data:`oddspush.SPORTS`).
    :param events: specific event IDs to follow.
    :param channels: ``ld`` and ``both`` are the prices; ``pre`` adds pre-match prices,
        ``dz`` volatile moments. Without it, live updates come on every channel and the
        snapshot covers ``ld`` and ``both``.
    :param streams: ``["live"]``; leave it out.
    :param everything: ``True`` for everything your plan includes (the API's ``all``).
    :param fair: also receive the no-vig and fair prices of every market (``fair``
        messages, and inline in snapshots).
    :param closing: receive the closing line of each match you follow when it kicks off.
    :param snapshot: ask for the current state when first subscribing (default ``True``).
    :param resync: ask for a snapshot by itself after a reconnect, ``missed`` and ``feed``
        ``up`` (default ``True``). Snapshots count against ``snapshots_per_hour``.
    :param book: keep ``stream.book`` up to date (default ``True``).
    :param book_channels: channels whose prices go into the book. Default: the
        ``channels`` you subscribe to without ``dz``, or ``ld`` and ``both``.
    :param reconnect: reconnect after a drop (default ``True``). With ``False`` the
        iterator raises :class:`~oddspush.errors.StreamClosed` instead.
    :param resync_jitter: after ``feed`` ``up``, wait a random 0 to this many seconds before
        asking for a snapshot, so that several bots do not ask at once.
    :param queue_size: messages read ahead of your loop before the stream stops reading.
    """

    def __init__(self, token: str, *, sports: Optional[Iterable[int]] = None,
                 events: Optional[Iterable[int]] = None,
                 channels: Optional[Iterable[str]] = None,
                 streams: Optional[Iterable[str]] = None, everything: bool = False,
                 fair: bool = False, closing: bool = False, snapshot: bool = True,
                 resync: bool = True, book: bool = True,
                 book_channels: Optional[Iterable[str]] = None, url: str = STREAM_URL,
                 reconnect: bool = True, max_backoff: float = 30.0,
                 resync_jitter: float = 5.0, queue_size: int = 1000,
                 login_timeout: float = 15.0) -> None:
        if not token:
            raise ValueError("an API key is required (it starts with opk_)")
        self.url = url
        self._token = token
        channels = list(channels) if channels else None
        self._filter = _fields(sports, events, channels, streams, everything)
        self._fair = bool(fair)
        self._closing = bool(closing)
        self._snapshot = bool(snapshot)
        self.resync = bool(resync)
        if book_channels is None:
            chans = [c for c in (channels or ()) if c != "dz"]
            book_channels = chans or PRICE_CHANNELS
        self._price_channels: FrozenSet[str] = frozenset(book_channels)
        self.book: Optional[Book] = Book(self._price_channels) if book else None
        self.reconnect = reconnect
        self.max_backoff = max_backoff
        self.resync_jitter = resync_jitter
        self.login_timeout = login_timeout
        self._queue_size = queue_size

        #: The ``welcome`` of the current connection (plan, limits, account).
        self.welcome: Optional[Dict[str, Any]] = None
        #: The last ``subscription`` the server confirmed.
        self.subscription: Optional[Dict[str, Any]] = None
        #: Times the connection was opened again after a drop.
        self.reconnects = 0

        self._queue: Optional[asyncio.Queue] = None
        self._task: Optional[asyncio.Task] = None
        self._ws: Any = None
        self._exc: Optional[BaseException] = None
        self._closed = False
        self._pending: List[Dict[str, Any]] = []       # to send after the next login
        self._unconfirmed: List[Dict[str, Any]] = []   # (un)subscribes not echoed yet
        self._owed = 0            # snapshots asked for and neither served nor refused
        self._last_error: Optional[Dict[str, Any]] = None
        self._snapshot_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------ lifecycle
    @property
    def account(self) -> Optional[Dict[str, Any]]:
        """``welcome["account"]``: ``id``, ``plan``, ``expires_at`` and ``limits``."""
        return (self.welcome or {}).get("account")

    @property
    def connected(self) -> bool:
        return self._ws is not None

    async def start(self) -> "Stream":
        """Start connecting in the background. Called for you by ``async with`` and by
        the first ``async for``."""
        if self._task is None:
            self._queue = asyncio.Queue(self._queue_size)
            self._task = asyncio.get_running_loop().create_task(self._run(),
                                                                name="oddspush-stream")
        return self

    async def close(self) -> None:
        """Close the connection and stop the iterator."""
        self._closed = True
        if self._snapshot_task is not None:
            self._snapshot_task.cancel()
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except BaseException:
                pass
        self._put_end()

    def _put_end(self) -> None:
        """Wake the reader with the end marker, without ever waiting."""
        q = self._queue
        if q is None:
            return
        while q.full():
            q.get_nowait()
        q.put_nowait(_END)

    async def __aenter__(self) -> "Stream":
        return await self.start()

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    def __aiter__(self) -> "Stream":
        return self

    async def __anext__(self) -> Dict[str, Any]:
        if self._task is None:
            await self.start()
        assert self._queue is not None
        if self._closed and self._queue.empty():
            raise StopAsyncIteration
        msg = await self._queue.get()
        if msg is _END:
            self._put_end()                     # every later call ends too
            if self._exc is not None:
                raise self._exc
            raise StopAsyncIteration
        return msg

    # ------------------------------------------------------------------ requests
    async def subscribe(self, *, sports: Optional[Iterable[int]] = None,
                        events: Optional[Iterable[int]] = None,
                        channels: Optional[Iterable[str]] = None,
                        streams: Optional[Iterable[str]] = None, everything: bool = False,
                        fair: Optional[bool] = None, closing: Optional[bool] = None,
                        snapshot: Optional[bool] = None) -> None:
        """Add to the filter (subscriptions add up). ``fair=True`` / ``closing=True`` turn
        those on; use :meth:`unsubscribe` to turn them off.

        ``snapshot`` defaults to ``True`` when you add to the filter (the state of what you
        added) and to ``False`` when you only turn ``fair`` or ``closing`` on.
        """
        fields = _fields(sports, events, channels, streams, everything)
        msg: Dict[str, Any] = {"type": "subscribe", **fields}
        if fair:
            msg["fair"] = True
        if closing:
            msg["closing"] = True
        if snapshot is None:
            snapshot = bool(fields)
        msg["snapshot"] = bool(snapshot)
        await self.send(msg)

    async def unsubscribe(self, *, sports: Optional[Iterable[int]] = None,
                          events: Optional[Iterable[int]] = None,
                          channels: Optional[Iterable[str]] = None,
                          streams: Optional[Iterable[str]] = None, everything: bool = False,
                          fair: bool = False, closing: bool = False) -> None:
        """Remove from the filter; ``fair=True`` / ``closing=True`` turn those off."""
        msg: Dict[str, Any] = {"type": "unsubscribe",
                               **_fields(sports, events, channels, streams, everything)}
        if fair:
            msg["fair"] = True
        if closing:
            msg["closing"] = True
        await self.send(msg)

    async def request_snapshot(self, *, sports: Optional[Iterable[int]] = None,
                               events: Optional[Iterable[int]] = None,
                               channels: Optional[Iterable[str]] = None,
                               streams: Optional[Iterable[str]] = None,
                               everything: bool = False) -> None:
        """Ask for the current state (counts against ``snapshots_per_hour``). Without
        arguments it covers everything you subscribed to, on the book's channels."""
        fields = _fields(sports, events, channels, streams, everything)
        await self.send({"type": "snapshot", **fields} if fields else self._snapshot_msg())

    async def ping(self, id: int = 0) -> None:
        await self.send({"type": "ping", "id": int(id)})

    async def status(self) -> None:
        """The answer arrives as a ``status`` message."""
        await self.send({"type": "status"})

    async def send(self, msg: Dict[str, Any]) -> None:
        """Send a raw message. While disconnected, ``subscribe``, ``unsubscribe`` and
        ``snapshot`` wait and go out after the next login; ``ping`` and ``status`` are
        dropped."""
        ws = self._ws
        if ws is not None:
            try:
                await self._send(ws, msg)
                return
            except ConnectionClosed:
                pass
        kind = msg.get("type")
        if kind in ("ping", "status"):
            return
        if kind == "snapshot" and self.resync:
            return                              # the reconnect asks for one anyway
        self._pending.append(msg)
        del self._pending[:-_PENDING_MAX]

    async def _send(self, ws: Any, msg: Dict[str, Any]) -> None:
        await ws.send(json.dumps(msg))
        kind = msg.get("type")
        if kind in ("subscribe", "unsubscribe"):
            self._unconfirmed.append(msg)
        if _asks_snapshot(msg):
            self._owed += 1

    # ------------------------------------------------------------------ filter state
    def _confirmed(self) -> Tuple[Dict[str, Any], bool, bool]:
        """The filter, ``fair`` and ``closing`` the server last confirmed, or the
        constructor's before any confirmation."""
        if self.subscription is None:
            return dict(self._filter), self._fair, self._closing
        f = self.subscription.get("filter") or {}
        fields = _fields(f.get("sports"), f.get("events"), f.get("channels"),
                         f.get("streams"), bool(f.get("all")))
        return (fields, self.subscription.get("fair") is True,
                self.subscription.get("closing") is True)

    def _snapshot_msg(self) -> Dict[str, Any]:
        """A snapshot of everything subscribed, but only on the book's channels: a bare
        snapshot would merge every subscribed channel (``dz`` included) into the book."""
        fields, _, _ = self._confirmed()
        chans = fields.get("channels")
        if not chans or set(chans) <= self._price_channels:
            return {"type": "snapshot"}
        safe = sorted(c for c in chans if c in self._price_channels) \
            or sorted(self._price_channels)
        return {"type": "snapshot", **fields, "channels": safe}

    # ------------------------------------------------------------------ the loop
    async def _deliver(self, msg: Dict[str, Any]) -> None:
        assert self._queue is not None
        await self._queue.put(msg)

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        backoff = 1.0
        first = True
        try:
            while not self._closed:
                started = loop.time()
                wait: Optional[float] = None
                reason = "closed"
                self._last_error = None
                try:
                    await self._session(first)
                except FatalError as e:
                    self._exc = e
                    return
                except RateLimited as e:
                    wait, reason = e.retry_after, e.code
                except OddsPushError as e:
                    reason = e.code
                except ConnectionClosed as e:
                    code, text = _close_info(e)
                    name = CLOSE_CODES.get(code or 0)
                    if name is None and self._last_error is not None:
                        name = self._last_error.get("code")
                    err = error_for(name, f"stream closed: {name} ({code})", close_code=code,
                                    retry_after=(self._last_error or {}).get("retry_after_s")) \
                        if name else None
                    if isinstance(err, FatalError):
                        self._exc = err
                        return
                    if isinstance(err, RateLimited):
                        wait = err.retry_after or self.max_backoff
                    reason = name or f"closed ({code} {text})".strip()
                except _Maintenance:
                    wait, reason = _MAINTENANCE_WAIT_S, "maintenance"
                except (OSError, asyncio.TimeoutError, WebSocketException) as e:
                    reason = f"{type(e).__name__}: {e}"
                finally:
                    self._ws = None
                    self._owed = 0
                    if self._snapshot_task is not None:
                        self._snapshot_task.cancel()
                        self._snapshot_task = None
                if self.welcome is not None:
                    first = False
                if self._closed:
                    return
                if not self.reconnect:
                    self._exc = StreamClosed("closed", f"stream closed: {reason}")
                    return
                if loop.time() - started > _STABLE_S:
                    backoff = 1.0
                if wait is None:
                    wait = backoff * random.uniform(0.75, 1.25)
                    backoff = min(backoff * 2.0, self.max_backoff)
                log.info("oddspush: disconnected (%s), reconnecting in %.1f s", reason, wait)
                await self._deliver({"type": "disconnected", "reason": reason,
                                     "retry_in_s": round(wait, 1)})
                await asyncio.sleep(wait)
                self.reconnects += 1
        except asyncio.CancelledError:
            raise
        except Exception as e:          # a bug: hand it to the reader rather than hang
            self._exc = e
        finally:
            if not self._closed:
                self._put_end()

    async def _session(self, first: bool) -> None:
        async with websockets.connect(self.url, compression=None, max_size=_MAX_MESSAGE,
                                      open_timeout=self.login_timeout, close_timeout=5,
                                      ping_interval=20, ping_timeout=20) as ws:
            await ws.send(json.dumps({"type": "login", "token": self._token}))
            raw = await asyncio.wait_for(ws.recv(), self.login_timeout)
            try:
                reply = json.loads(raw)
            except ValueError:
                reply = None
            if not isinstance(reply, dict):
                raise OddsPushError("unexpected", "the login was not answered with JSON")
            if reply.get("type") == "error":
                raise error_for(str(reply.get("code")), retry_after=reply.get("retry_after_s"))
            if reply.get("type") != "welcome":
                raise OddsPushError("unexpected", f"expected welcome, got {reply.get('type')!r}")
            self.welcome = reply
            self._ws = ws
            await self._deliver(reply)
            await self._restore(ws, self._snapshot if first else self.resync)
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except ValueError:
                    continue
                if not isinstance(msg, dict):
                    continue
                self._handle(msg)
                await self._deliver(msg)
                if msg.get("type") == "feed" and msg.get("status") == "maintenance":
                    raise _Maintenance()

    async def _restore(self, ws: Any, want_snapshot: bool) -> None:
        """Subscribe again to the confirmed filter, then send what was not confirmed or
        was asked for while disconnected."""
        fields, fair, closing = self._confirmed()
        unconfirmed, self._unconfirmed = self._unconfirmed, []
        pending, self._pending = self._pending, []
        if fields or fair or closing:
            msg: Dict[str, Any] = {"type": "subscribe", **fields}
            if fair:
                msg["fair"] = True
            if closing:
                msg["closing"] = True
            snap = self._snapshot_msg() if (want_snapshot and fields) else None
            msg["snapshot"] = snap is not None and len(snap) == 1   # bare: let subscribe do it
            await self._send(ws, msg)
            if snap is not None and len(snap) > 1:
                await self._send(ws, snap)
        for msg in unconfirmed + pending:
            await self._send(ws, msg)

    def _handle(self, msg: Dict[str, Any]) -> None:
        kind = msg.get("type")
        if self.book is not None and kind in ("odds", "snapshot", "fair"):
            self.book.apply_message(msg)
        if kind == "snapshot":
            if msg.get("final") and self._owed:
                self._owed -= 1
        elif kind == "subscription":
            self.subscription = msg
            if self._unconfirmed:
                self._unconfirmed.pop(0)
        elif kind == "missed":
            if self.resync:
                self._schedule_snapshot(0.0)
        elif kind == "feed" and msg.get("status") == "up":
            if self.resync:
                self._schedule_snapshot(random.uniform(0.0, max(0.0, self.resync_jitter)))
        elif kind == "error":
            code = msg.get("code")
            if code == "snapshot_rate_limited":
                if self._owed:
                    self._owed -= 1
                    retry = msg.get("retry_after_s")
                    wait = _RETRY_DEFAULT_S if retry is None else max(float(retry), 0.0)
                    self._schedule_snapshot(wait + _RETRY_MARGIN_S, force=True)
            elif code in _FILTER_ERRORS:
                if self._unconfirmed:
                    self._unconfirmed.pop(0)
            elif code in CLOSE_CODES.values():
                self._last_error = msg

    def _schedule_snapshot(self, delay: float, force: bool = False) -> None:
        """Ask for a snapshot after ``delay`` seconds. A request still waiting to go out
        covers the next ones."""
        task = self._snapshot_task
        if task is not None and not task.done():
            if not force:
                return
            task.cancel()
        self._snapshot_task = asyncio.get_running_loop().create_task(self._snapshot_later(delay))

    async def _snapshot_later(self, delay: float) -> None:
        await asyncio.sleep(delay)
        ws = self._ws
        if ws is not None:
            try:
                await self._send(ws, self._snapshot_msg())
            except ConnectionClosed:
                pass                            # the reconnect asks for one


class _Maintenance(Exception):
    """The server said it is restarting."""
