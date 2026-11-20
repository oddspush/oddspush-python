"""The REST API: look up the current state of events, line history, closing lines and CLV,
recent price drops, and pre-match events about to go live. :class:`Client` is synchronous,
:class:`AsyncClient` its asyncio twin.

The token goes in the ``x-token`` header. A request from a new IP beyond your connections
takes the token over, like a login on the stream does: run your REST calls from the same
machine as your stream. Requests are rate limited per account (120 at once, then 300 a
minute; the free plan 20 and 60); a 429 or a 503 is retried after ``retry-after`` up to
``max_retries`` times, then raised (:class:`RateLimited`, :class:`Unavailable`).

On the free plan pass ``accept_delay=True``: the odds come 30 seconds late, and without it
every request raises :class:`DelayedPlan`.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, AsyncIterator, Dict, Iterator, Optional, Union

import httpx

from ._constants import REST_URL
from ._version import __version__
from .errors import BadRequest, OddsPushError, error_for

#: A price as ``/v1/clv`` reads it: decimal odds below 100 (``2.10``), or American odds (100
#: or more, or negative: ``-110``, ``150``). A string is sent as is.
Price = Union[float, int, str]

log = logging.getLogger("oddspush")

_RETRY_STATUS = (429, 502, 503, 504)


def _retry_after(resp: httpx.Response) -> Optional[float]:
    value = resp.headers.get("retry-after")
    try:
        return max(0.0, float(value)) if value is not None else None
    except ValueError:
        return None


def _error(resp: httpx.Response) -> OddsPushError:
    code = None
    try:
        body = resp.json()
        if isinstance(body, dict) and isinstance(body.get("detail"), str):
            code = body["detail"]
    except ValueError:
        pass
    status = resp.status_code
    if code is None or code == "Not Found":
        code = {400: "bad_request", 401: "unauthorized", 404: "not_found",
                429: "rate_limited", 503: "unavailable"}.get(status, f"http_{status}")
    return error_for(code, f"{code} (HTTP {status})", status=status,
                     retry_after=_retry_after(resp))


def _event_id(event_id: Any) -> int:
    try:
        value = int(event_id)
    except (TypeError, ValueError):
        raise BadRequest("bad_request", f"event_id must be a number: {event_id!r}") from None
    if value < 0:
        raise BadRequest("bad_request", f"event_id must be positive: {event_id!r}")
    return value


def _next_since(rows: Any, since: Optional[int]) -> int:
    """``since`` for the next page of ``/v1/closing``: the last ``start_ms`` (inclusive, the
    rows of that same instant come again and are skipped). If the whole page shares one
    ``start_ms``, asking from there would return the same page: move past it."""
    last = int(rows[-1]["start_ms"])
    if last == since or all(int(r["start_ms"]) == last for r in rows):
        log.warning("oddspush: more than one page of closing lines share start_ms %s; "
                    "some of them may be skipped", last)
        return last + 1
    return last


def _next_closing(page: Dict[str, Any], rows: Any,
                  since: Optional[int]) -> "tuple[int, Optional[int]]":
    """``(since, after)`` for the next page of ``/v1/closing``: the ``next_since`` and
    ``next_after`` cursor of the page, which skips nothing; from a service without it, the
    last ``start_ms`` (:func:`_next_since`)."""
    nxt, aft = page.get("next_since"), page.get("next_after")
    if (isinstance(nxt, int) and not isinstance(nxt, bool)
            and isinstance(aft, int) and not isinstance(aft, bool)):
        return nxt, aft
    return _next_since(rows, since), None


def _next_history(page: Dict[str, Any], since: Optional[int]) -> Optional[int]:
    """``since`` for the next page of the line history, or ``None`` when it is the last."""
    rows = page.get("history") or []
    nxt = page.get("next_since")
    if not page.get("more") or not rows or not isinstance(nxt, int) or isinstance(nxt, bool):
        return None
    if since is not None and nxt <= since:    # the pages stopped moving: never loop forever
        log.warning("oddspush: line history pages stopped advancing at %s", since)
        return None
    return nxt


def _next_cursor(page: Dict[str, Any], after: Optional[int]) -> Optional[int]:
    """``after`` for the next page of drops, or ``None`` when it is the last."""
    cursor = page.get("cursor")
    if (not page.get("more") or not page.get("alerts") or not isinstance(cursor, int)
            or isinstance(cursor, bool) or cursor == after):
        return None
    return cursor


def _price(price: Price) -> str:
    if isinstance(price, bool):
        raise BadRequest("bad_request", f"price must be a number: {price!r}")
    if isinstance(price, str):
        return price.strip()
    if isinstance(price, int):
        return str(price)
    if isinstance(price, float):
        return repr(price)
    raise BadRequest("bad_request", f"price must be a number: {price!r}")


def _status_request(http: Any) -> Any:
    """``GET /v1/status`` without the token: it is public."""
    req = http.build_request("GET", "/v1/status")
    for header in ("x-token", "x-accept-delay"):
        if header in req.headers:
            del req.headers[header]
    return req


def _clean(params: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for k, v in params.items():
        if v is None:
            continue
        out[k] = ("true" if v else "false") if isinstance(v, bool) else v
    return out


class _Base:
    def __init__(self, token: str, *, base_url: str = REST_URL, timeout: float = 15.0,
                 max_retries: int = 2, max_retry_wait: float = 60.0,
                 accept_delay: bool = False) -> None:
        if not token:
            raise ValueError("an API key is required (it starts with opk_)")
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.max_retry_wait = max_retry_wait
        self.accept_delay = bool(accept_delay)
        self._headers = {"x-token": token, "accept": "application/json",
                         "user-agent": f"oddspush-python/{__version__}"}
        if self.accept_delay:
            self._headers["x-accept-delay"] = "true"
        self._timeout = timeout

    def _wait(self, resp: httpx.Response, attempt: int) -> Optional[float]:
        """Seconds to wait before retrying, or ``None`` to give up."""
        if resp.status_code not in _RETRY_STATUS or attempt >= self.max_retries:
            return None
        wait = _retry_after(resp)
        if wait is None:
            wait = min(2.0 ** attempt, 10.0)
        return wait if wait <= self.max_retry_wait else None


class Client(_Base):
    """Synchronous REST client.

    >>> from oddspush import Client
    >>> with Client("opk_...") as api:
    ...     soccer = api.events(sport_id=1)

    :param accept_delay: send ``X-Accept-Delay: true``, needed on the free plan (odds 30
        seconds late; without it every request raises :class:`DelayedPlan`). Harmless on a
        trial or paid plan; with it an expired token answers on the free plan.
    :param max_retries: how many times a 429 or a 5xx is retried after ``retry-after``.

    ``transport`` is passed to ``httpx.Client`` (for proxies or tests).
    """

    def __init__(self, token: str, *, transport: Optional[httpx.BaseTransport] = None,
                 **kw: Any) -> None:
        super().__init__(token, **kw)
        self._http = httpx.Client(base_url=self.base_url, headers=self._headers,
                                  timeout=self._timeout, transport=transport)

    def _get(self, path: str, **params: Any) -> Any:
        attempt = 0
        while True:
            resp = self._http.get(path, params=_clean(params))
            if resp.status_code == 200:
                return resp.json()
            wait = self._wait(resp, attempt)
            if wait is None:
                raise _error(resp)
            time.sleep(wait)
            attempt += 1

    def health(self) -> bool:
        """``True`` when the feed is flowing. Needs no plan."""
        try:
            return bool(self._get("/v1/health").get("ok"))
        except (OddsPushError, httpx.HTTPError):
            return False

    def events(self, sport_id: Optional[int] = None, channel: Optional[str] = None) -> Dict[str, Any]:
        """``{"n", "events": [{"event_id", "sport_id", "ld", "dz", "pre", "both", "other",
        "markets"}]}``: every event held, with its number of markets per channel."""
        return self._get("/v1/events", sport_id=sport_id, channel=channel)

    def event(self, event_id: int, channel: Optional[str] = None, fair: bool = False) -> Dict[str, Any]:
        """One event: ``meta``, ``periods``, its markets per channel and ``merged``.

        Without ``channel``, ``merged`` is ``ld`` and ``both`` merged by version. For a
        pre-match event, pass ``channel="pre"``. With ``fair=True`` the response adds
        ``fair``: the no-vig and fair prices of every market in ``merged``.
        """
        return self._get(f"/v1/events/{_event_id(event_id)}", channel=channel,
                         fair=True if fair else None)

    def closing(self, event_id: int) -> Dict[str, Any]:
        """The closing line of a match, by its pre-match or its live event id.

        Raises :class:`~oddspush.errors.NotFound` until the match starts, and 90 days after.
        Trials and paid plans.
        """
        return self._get(f"/v1/closing/{_event_id(event_id)}")

    def closings(self, sport_id: Optional[int] = None, since: Optional[int] = None,
                 after: Optional[int] = None) -> Dict[str, Any]:
        """One page of matches with a closing line, oldest first: ``{"n", "more", "events":
        [{"event_id", "sport_id", "start_ms"}]}``, and with ``more``, ``next_since`` and
        ``next_after`` for the next page (pass them as ``since`` and ``after``). ``since`` in
        ms, default the last 24 hours, at most 90 days back."""
        return self._get("/v1/closing", sport_id=sport_id, since=since, after=after)

    def iter_closings(self, sport_id: Optional[int] = None,
                      since: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """Every match with a closing line since ``since``, across pages."""
        seen: set = set()
        stale = 0
        after: Optional[int] = None
        while True:
            page = self.closings(sport_id=sport_id, since=since, after=after)
            rows = page.get("events") or []
            fresh = 0
            for r in rows:
                if r.get("event_id") not in seen:
                    seen.add(r.get("event_id"))
                    fresh += 1
                    yield r
            if not page.get("more") or not rows:
                return
            stale = 0 if fresh else stale + 1
            if stale >= 2:              # the pages stopped moving: never loop forever
                log.warning("oddspush: closing-line pages stopped advancing at %s", since)
                return
            since, after = _next_closing(page, rows, since)

    def clv(self, event_id: int, key: str, designation: str, price: Price) -> Dict[str, Any]:
        """The closing line value of a bet, against the closing line of that market and leg.

        ``event_id`` is the pre-match or the live event; ``key`` the market (``s;0;m``);
        ``designation`` the leg (``home``, ``away``, ``draw``, ``over``, ``under``); ``price``
        your odds: decimal (``2.10``, below 100) or American (``-110``, ``150``). Returns
        ``price``, ``closing`` (with ``fair_decimal``), ``clv_pct`` (against the fair closing
        price), ``clv_raw_pct`` and ``beat_close``. Raises
        :class:`~oddspush.errors.NotFound` without a closing line or that leg. Trials and
        paid plans.
        """
        return self._get("/v1/clv", event_id=_event_id(event_id), key=key,
                         designation=designation, price=_price(price))

    def history(self, event_id: int, key: Optional[str] = None, since: Optional[int] = None,
                fair: bool = False) -> Dict[str, Any]:
        """One page of the line history of an event: every change of price, line or status
        of its markets over the last 14 days, oldest first: ``{"event_id", "n", "more",
        "next_since", "history": [{"time", "channel", "key", "status", "prices", "fair"?}]}``.

        ``key`` for one market, ``since`` in ms, ``fair=True`` to add the no-vig and fair
        prices of each row. Up to 5,000 rows a page (1,000 with ``fair``); with ``more``, ask
        again with ``since=next_since``, or use :meth:`iter_history`. Trials and paid plans.
        """
        return self._get(f"/v1/events/{_event_id(event_id)}/history", key=key, since=since,
                         fair=True if fair else None)

    def iter_history(self, event_id: int, key: Optional[str] = None,
                     since: Optional[int] = None,
                     fair: bool = False) -> Iterator[Dict[str, Any]]:
        """Every row of the line history of an event since ``since``, across pages."""
        while True:
            page = self.history(event_id, key=key, since=since, fair=fair)
            yield from page.get("history") or []
            nxt = _next_history(page, since)
            if nxt is None:
                return
            since = nxt

    def drops(self, sport_id: Optional[int] = None, since: Optional[int] = None,
              min_pct: Optional[float] = None, window_s: Optional[int] = None,
              after: Optional[int] = None) -> Dict[str, Any]:
        """One page of the price drops of the last hour: ``{"n", "more", "cursor",
        "alerts": [...]}``, each alert the ``data`` of a ``drop`` message, oldest first.

        The alerts a stream with that ``min_pct`` (1-50, default 3) and ``window_s`` (30, 60,
        300 or 600; default 60) would have sent. ``since`` in ms (default an hour ago). For
        the next page, or only what is new since your last call, pass ``after`` set to the
        ``cursor`` you got; :meth:`iter_drops` follows it for you. Trials and paid plans.
        """
        return self._get("/v1/drops", sport_id=sport_id, since=since, min_pct=min_pct,
                         window_s=window_s, after=after)

    def iter_drops(self, sport_id: Optional[int] = None, since: Optional[int] = None,
                   min_pct: Optional[float] = None, window_s: Optional[int] = None,
                   after: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """Every drop alert, across pages, oldest first."""
        while True:
            page = self.drops(sport_id=sport_id, since=since, min_pct=min_pct,
                              window_s=window_s, after=after)
            yield from page.get("alerts") or []
            nxt = _next_cursor(page, after)
            if nxt is None:
                return
            after = nxt

    def membership(self, sport_id: Optional[int] = None) -> Dict[str, Any]:
        """``{"<sport>": {"n", "ids"}}``: pre-match events that are about to go live."""
        return self._get("/v1/membership", sport_id=sport_id)

    def status(self) -> Dict[str, Any]:
        """The public status of the service (no key is sent): ``{"ok", "latency_ms":
        {"p50", "p90"}, "our_ms": {"p50", "p99"}, "measured_ms"}``, measured every 5 minutes
        by a client connected like you. ``latency_ms`` and ``our_ms`` are ``None`` without a
        recent measurement."""
        attempt = 0
        while True:
            resp = self._http.send(_status_request(self._http))
            if resp.status_code == 200:
                return resp.json()
            wait = self._wait(resp, attempt)
            if wait is None:
                raise _error(resp)
            time.sleep(wait)
            attempt += 1

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


class AsyncClient(_Base):
    """Asyncio REST client, the same methods as :class:`Client`.

    >>> async with AsyncClient("opk_...") as api:
    ...     line = await api.closing(1639824805)
    """

    def __init__(self, token: str, *,
                 transport: Optional[httpx.AsyncBaseTransport] = None, **kw: Any) -> None:
        super().__init__(token, **kw)
        self._http = httpx.AsyncClient(base_url=self.base_url, headers=self._headers,
                                       timeout=self._timeout, transport=transport)

    async def _get(self, path: str, **params: Any) -> Any:
        attempt = 0
        while True:
            resp = await self._http.get(path, params=_clean(params))
            if resp.status_code == 200:
                return resp.json()
            wait = self._wait(resp, attempt)
            if wait is None:
                raise _error(resp)
            await asyncio.sleep(wait)
            attempt += 1

    async def health(self) -> bool:
        try:
            return bool((await self._get("/v1/health")).get("ok"))
        except (OddsPushError, httpx.HTTPError):
            return False

    async def events(self, sport_id: Optional[int] = None,
                     channel: Optional[str] = None) -> Dict[str, Any]:
        return await self._get("/v1/events", sport_id=sport_id, channel=channel)

    async def event(self, event_id: int, channel: Optional[str] = None,
                    fair: bool = False) -> Dict[str, Any]:
        return await self._get(f"/v1/events/{_event_id(event_id)}", channel=channel,
                               fair=True if fair else None)

    async def closing(self, event_id: int) -> Dict[str, Any]:
        """See :meth:`Client.closing`."""
        return await self._get(f"/v1/closing/{_event_id(event_id)}")

    async def closings(self, sport_id: Optional[int] = None, since: Optional[int] = None,
                       after: Optional[int] = None) -> Dict[str, Any]:
        """See :meth:`Client.closings`."""
        return await self._get("/v1/closing", sport_id=sport_id, since=since, after=after)

    async def iter_closings(self, sport_id: Optional[int] = None,
                            since: Optional[int] = None) -> AsyncIterator[Dict[str, Any]]:
        seen: set = set()
        stale = 0
        after: Optional[int] = None
        while True:
            page = await self.closings(sport_id=sport_id, since=since, after=after)
            rows = page.get("events") or []
            fresh = 0
            for r in rows:
                if r.get("event_id") not in seen:
                    seen.add(r.get("event_id"))
                    fresh += 1
                    yield r
            if not page.get("more") or not rows:
                return
            stale = 0 if fresh else stale + 1
            if stale >= 2:              # the pages stopped moving: never loop forever
                log.warning("oddspush: closing-line pages stopped advancing at %s", since)
                return
            since, after = _next_closing(page, rows, since)

    async def clv(self, event_id: int, key: str, designation: str,
                  price: Price) -> Dict[str, Any]:
        """See :meth:`Client.clv`."""
        return await self._get("/v1/clv", event_id=_event_id(event_id), key=key,
                               designation=designation, price=_price(price))

    async def history(self, event_id: int, key: Optional[str] = None,
                      since: Optional[int] = None, fair: bool = False) -> Dict[str, Any]:
        """See :meth:`Client.history`."""
        return await self._get(f"/v1/events/{_event_id(event_id)}/history", key=key,
                               since=since, fair=True if fair else None)

    async def iter_history(self, event_id: int, key: Optional[str] = None,
                           since: Optional[int] = None,
                           fair: bool = False) -> AsyncIterator[Dict[str, Any]]:
        """See :meth:`Client.iter_history`."""
        while True:
            page = await self.history(event_id, key=key, since=since, fair=fair)
            for row in page.get("history") or []:
                yield row
            nxt = _next_history(page, since)
            if nxt is None:
                return
            since = nxt

    async def drops(self, sport_id: Optional[int] = None, since: Optional[int] = None,
                    min_pct: Optional[float] = None, window_s: Optional[int] = None,
                    after: Optional[int] = None) -> Dict[str, Any]:
        """See :meth:`Client.drops`."""
        return await self._get("/v1/drops", sport_id=sport_id, since=since, min_pct=min_pct,
                               window_s=window_s, after=after)

    async def iter_drops(self, sport_id: Optional[int] = None, since: Optional[int] = None,
                         min_pct: Optional[float] = None, window_s: Optional[int] = None,
                         after: Optional[int] = None) -> AsyncIterator[Dict[str, Any]]:
        """See :meth:`Client.iter_drops`."""
        while True:
            page = await self.drops(sport_id=sport_id, since=since, min_pct=min_pct,
                                    window_s=window_s, after=after)
            for alert in page.get("alerts") or []:
                yield alert
            nxt = _next_cursor(page, after)
            if nxt is None:
                return
            after = nxt

    async def membership(self, sport_id: Optional[int] = None) -> Dict[str, Any]:
        return await self._get("/v1/membership", sport_id=sport_id)

    async def status(self) -> Dict[str, Any]:
        """See :meth:`Client.status`."""
        attempt = 0
        while True:
            resp = await self._http.send(_status_request(self._http))
            if resp.status_code == 200:
                return resp.json()
            wait = self._wait(resp, attempt)
            if wait is None:
                raise _error(resp)
            await asyncio.sleep(wait)
            attempt += 1

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "AsyncClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()


def get_status(base_url: str = REST_URL, timeout: float = 10.0,
               transport: Optional[httpx.BaseTransport] = None) -> Dict[str, Any]:
    """The public status of the service, without a key: see :meth:`Client.status`."""
    with httpx.Client(base_url=base_url.rstrip("/"), timeout=timeout, transport=transport,
                      headers={"accept": "application/json",
                               "user-agent": f"oddspush-python/{__version__}"}) as http:
        resp = http.get("/v1/status")
    if resp.status_code != 200:
        raise _error(resp)
    return resp.json()


__all__ = ["Client", "AsyncClient", "get_status"]
