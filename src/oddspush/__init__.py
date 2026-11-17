"""Python client for the OddsPush API: real-time Pinnacle odds over WebSocket and REST,
with no-vig, fair and closing-line prices.

- :class:`Stream`: the WebSocket stream, with reconnection and a local price book.
- :class:`Client` / :class:`AsyncClient`: the REST API.
- :class:`Book`: the price book, if you feed it yourself.
- :mod:`oddspush.odds`: American/decimal odds, margin, no-vig and CLV.
- ``oddspush-mcp`` (:mod:`oddspush.mcp_server`): the MCP server over stdio, with
  ``pip install "oddspush[mcp]"``.

Docs: https://oddspush.com/docs
"""

from ._constants import CHANNELS, PRICE_CHANNELS, REST_URL, SPORTS, STREAM_URL
from ._version import __version__
from .book import Book, channel_of
from .errors import (BadRequest, DelayedPlan, FatalError, IpNotAllowed, NotFound, NotInPlan,
                     NotReady, OddsPushError, PlanChanged, RateLimited, StreamClosed,
                     SubscriptionExpired, TokenReplaced, TryAgain, Unauthorized, Unavailable)
from .odds import american_to_decimal, clv, decimal_to_american, implied_probability
from .rest import AsyncClient, Client, get_status
from .stream import DROP_MIN_PCT, DROP_WINDOWS, Stream

__all__ = [
    "__version__", "Stream", "Client", "AsyncClient", "Book", "channel_of", "get_status",
    "SPORTS", "CHANNELS", "PRICE_CHANNELS", "STREAM_URL", "REST_URL", "DROP_WINDOWS",
    "DROP_MIN_PCT",
    "american_to_decimal", "decimal_to_american", "implied_probability", "clv",
    "OddsPushError", "FatalError", "Unauthorized", "SubscriptionExpired", "NotInPlan",
    "IpNotAllowed", "TokenReplaced", "RateLimited", "NotFound", "BadRequest",
    "Unavailable", "StreamClosed", "DelayedPlan", "PlanChanged", "TryAgain", "NotReady",
]
