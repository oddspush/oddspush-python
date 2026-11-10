"""Exceptions raised by the OddsPush client.

Every error carries the API's ``code`` (``unauthorized``, ``rate_limited``...). Errors that
retrying with the same token cannot fix (an unknown token, an expired plan, a plan without
the stream) are :class:`FatalError`; the stream stops reconnecting when it gets one.
"""

from __future__ import annotations

from typing import Optional


class OddsPushError(Exception):
    """Base class of every error raised by this package."""

    def __init__(self, code: str, message: Optional[str] = None, *,
                 status: Optional[int] = None, close_code: Optional[int] = None,
                 retry_after: Optional[float] = None) -> None:
        self.code = code
        self.status = status
        self.close_code = close_code
        self.retry_after = retry_after
        super().__init__(message or code)


class FatalError(OddsPushError):
    """Retrying with the same token will not help."""


class Unauthorized(FatalError):
    """Unknown or revoked token (HTTP 401, close 4101)."""


class SubscriptionExpired(FatalError):
    """The plan has expired (close 4103)."""


class NotInPlan(FatalError):
    """The plan does not include this: ``stream_not_in_plan`` (4109), ``rest_not_in_plan``
    or ``sport_not_in_plan``."""


class IpNotAllowed(FatalError):
    """Only for accounts set up with a fixed IP (close 4105)."""


class TokenReplaced(FatalError):
    """The token was used from another IP or connection, which now holds it (close 4107).

    Reconnecting would close the other machine's stream, which would close this one again,
    and so on. Run each token from one machine only.
    """


class RateLimited(OddsPushError):
    """Too many requests, reconnects or snapshots: wait ``retry_after`` seconds."""


class NotFound(OddsPushError):
    """No such event, or no closing line yet (HTTP 404)."""


class BadRequest(OddsPushError):
    """A parameter is not valid (HTTP 400)."""


class Unavailable(OddsPushError):
    """The service is temporarily unavailable (HTTP 503)."""


class StreamClosed(OddsPushError):
    """The stream closed and is not going to be reopened (``reconnect=False``)."""


_BY_CODE = {
    "unauthorized": Unauthorized,
    "subscription_expired": SubscriptionExpired,
    "stream_not_in_plan": NotInPlan,
    "rest_not_in_plan": NotInPlan,
    "sport_not_in_plan": NotInPlan,
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
}


def error_for(code: str, message: Optional[str] = None, **kw) -> OddsPushError:
    """The exception that matches an API error ``code``."""
    return _BY_CODE.get(code, OddsPushError)(code, message, **kw)
