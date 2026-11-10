"""The REST API: look up the current state of events, closing lines and pre-match events
about to go live. :class:`Client` is synchronous, :class:`AsyncClient` its asyncio twin.

The token goes in the ``x-token`` header. A request from a new IP takes the token over,
like a login on the stream does: run your REST calls from the same machine as your stream.
Requests are rate limited per account (120 at once, then 300 a minute); a 429 is retried
after ``retry-after`` up to ``max_retries`` times, then raised as :class:`RateLimited`.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, AsyncIterator, Dict, Iterator, Optional

import httpx

from ._constants import REST_URL
from ._version import __version__
from .errors import BadRequest, OddsPushError, error_for

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


def _clean(params: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for k, v in params.items():
        if v is None:
            continue
        out[k] = ("true" if v else "false") if isinstance(v, bool) else v
    return out


class _Base:
    def __init__(self, token: str, *, base_url: str = REST_URL, timeout: float = 15.0,
                 max_retries: int = 2, max_retry_wait: float = 60.0) -> None:
        if not token:
            raise ValueError("token is required (it starts with opk_)")
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.max_retry_wait = max_retry_wait
        self._headers = {"x-token": token, "accept": "application/json",
                         "user-agent": f"oddspush-python/{__version__}"}
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

        Raises :class:`~oddspush.errors.NotFound` until the match starts, and 7 days after.
        """
        return self._get(f"/v1/closing/{_event_id(event_id)}")

    def closings(self, sport_id: Optional[int] = None, since: Optional[int] = None) -> Dict[str, Any]:
        """One page of matches with a closing line: ``{"n", "more", "events": [{"event_id",
        "sport_id", "start_ms"}]}``. ``since`` in ms, default the last 24 hours."""
        return self._get("/v1/closing", sport_id=sport_id, since=since)

    def iter_closings(self, sport_id: Optional[int] = None,
                      since: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """Every match with a closing line since ``since``, across pages."""
        seen: set = set()
        stale = 0
        while True:
            page = self.closings(sport_id=sport_id, since=since)
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
            since = _next_since(rows, since)

    def membership(self, sport_id: Optional[int] = None) -> Dict[str, Any]:
        """``{"<sport>": {"n", "ids"}}``: pre-match events that are about to go live."""
        return self._get("/v1/membership", sport_id=sport_id)

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
        return await self._get(f"/v1/closing/{_event_id(event_id)}")

    async def closings(self, sport_id: Optional[int] = None,
                       since: Optional[int] = None) -> Dict[str, Any]:
        return await self._get("/v1/closing", sport_id=sport_id, since=since)

    async def iter_closings(self, sport_id: Optional[int] = None,
                            since: Optional[int] = None) -> AsyncIterator[Dict[str, Any]]:
        seen: set = set()
        stale = 0
        while True:
            page = await self.closings(sport_id=sport_id, since=since)
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
            since = _next_since(rows, since)

    async def membership(self, sport_id: Optional[int] = None) -> Dict[str, Any]:
        return await self._get("/v1/membership", sport_id=sport_id)

    async def aclose(self) -> None:
        await self._http.aclose()

    async def __aenter__(self) -> "AsyncClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()


__all__ = ["Client", "AsyncClient"]
