"""Exceptions raised by the OddsPush client.

Every error carries the API's ``code`` (``unauthorized``, ``rate_limited``...). Errors that
retrying with the same token cannot fix (an unknown token, an expired plan, a plan without
the stream, a free token that did not accept the delay) are :class:`FatalError`; the stream
stops reconnecting when it gets one.
"""

from __future__ import annotations

from typing import Any, Dict, Optional


class OddsPushError(Exception):
    """Base class of every error raised by this package.

    ``details`` holds the other fields of the API's error message, if any (``delay_s``,
    ``features``, ``max``...).
    """

    #: What to do about it, added to the message.
    hint: Optional[str] = None

    def __init__(self, code: str, message: Optional[str] = None, *,
                 status: Optional[int] = None, close_code: Optional[int] = None,
                 retry_after: Optional[float] = None,
                 details: Optional[Dict[str, Any]] = None) -> None:
        self.code = code
        self.status = status
        self.close_code = close_code
        self.retry_after = retry_after
        self.details: Dict[str, Any] = dict(details or {})
        text = message or code
        if self.hint:
            text = f"{text}: {self.hint}"
        super().__init__(text)


class FatalError(OddsPushError):
    """Retrying with the same token will not help."""


class Unauthorized(FatalError):
    """Unknown or revoked token (HTTP 401, close 4101)."""


class SubscriptionExpired(FatalError):
    """The plan has expired (close 4103)."""


class NotInPlan(FatalError):
    """The plan does not include this: ``stream_not_in_plan`` (4109), ``rest_not_in_plan``,
    ``sport_not_in_plan``, or ``not_in_plan`` (the free plan has odds only: no fair prices,
    closing lines, CLV, line history, drop alerts or specials)."""


class DelayedPlan(FatalError):
    """The token is on the free plan, whose odds are ``delay_s`` (30) seconds late, and the
    delay was not accepted (close 4110, HTTP 403). Pass ``accept_delay=True`` to
    :class:`~oddspush.Stream` or :class:`~oddspush.Client`."""

    hint = ("this token is on the free plan, whose odds are 30 seconds late; pass "
            "accept_delay=True to Stream / Client / AsyncClient to accept the delay")


class IpNotAllowed(FatalError):
    """Only for accounts set up with a fixed IP (close 4105)."""


class TokenReplaced(FatalError):
    """The token was used from another IP or connection, which now holds it (close 4107).

    Reconnecting would close the other machine's stream, which would close this one again,
    and so on. Use the token from no more IPs at a time than your connections; a new IP
    beyond that replaces the oldest.
    """


class RateLimited(OddsPushError):
    """Too many requests, reconnects or snapshots: wait ``retry_after`` seconds."""


class TryAgain(RateLimited):
    """Free plan: the token could not be checked right now (close 4112). It is not an
    unknown token: wait ``retry_after`` seconds and log in again (the stream does)."""


class PlanChanged(OddsPushError):
    """The account moved from the free plan to a trial or a paid plan (close 4111): the
    stream reconnects at once, and the odds arrive in real time."""


class NotFound(OddsPushError):
    """No such event, or no closing line yet (HTTP 404)."""


class BadRequest(OddsPushError):
    """A parameter is not valid (HTTP 400)."""


class Unavailable(OddsPushError):
    """The service is temporarily unavailable (HTTP 503)."""


class NotReady(Unavailable):
    """Free plan: the delayed feed is still filling up after a restart of the service (at
    most ``delay_s`` seconds). Ask again in a few seconds."""


class StreamClosed(OddsPushError):
    """The stream closed and is not going to be reopened (``reconnect=False``)."""


_BY_CODE = {
    "unauthorized": Unauthorized,
    "subscription_expired": SubscriptionExpired,
    "stream_not_in_plan": NotInPlan,
    "rest_not_in_plan": NotInPlan,
    "sport_not_in_plan": NotInPlan,
    "not_in_plan": NotInPlan,
    "delayed_plan": DelayedPlan,
    "plan_changed": PlanChanged,
    "try_again": TryAgain,
    "not_ready": NotReady,
    "ip_not_allowed": IpNotAllowed,
    "replaced": TokenReplaced,
    "rate_limited": RateLimited,
    "reconnect_rate_limited": RateLimited,
    "snapshot_rate_limited": RateLimited,
    "not_found": NotFound,
    "bad_request": BadRequest,
    "unavailable": Unavailable,
}

#: Close codes of the stream, by number.
CLOSE_CODES = {
    4101: "unauthorized",
    4102: "server_full",
    4103: "subscription_expired",
    4104: "connection_limit",
    4105: "ip_not_allowed",
    4106: "reconnect_rate_limited",
    4107: "replaced",
    4108: "too_slow",
    4109: "stream_not_in_plan",
    4110: "delayed_plan",
    4111: "plan_changed",
    4112: "try_again",
}


def error_for(code: str, message: Optional[str] = None, **kw) -> OddsPushError:
    """The exception that matches an API error ``code``."""
    return _BY_CODE.get(code, OddsPushError)(code, message, **kw)
