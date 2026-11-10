"""Python client for the OddsPush API: real-time Pinnacle odds over WebSocket and REST,
with no-vig, fair and closing-line prices.

- :class:`Stream`: the WebSocket stream, with reconnection and a local price book.
- :class:`Client` / :class:`AsyncClient`: the REST API.
- :class:`Book`: the price book, if you feed it yourself.
- :mod:`oddspush.odds`: American/decimal odds, margin, no-vig and CLV.

Docs: https://oddspush.com/docs
"""

from ._constants import CHANNELS, PRICE_CHANNELS, REST_URL, SPORTS, STREAM_URL
from ._version import __version__
from .book import Book, channel_of
from .errors import (BadRequest, FatalError, IpNotAllowed, NotFound, NotInPlan,
                     OddsPushError, RateLimited, StreamClosed, SubscriptionExpired,
                     TokenReplaced, Unauthorized, Unavailable)
from .odds import american_to_decimal, clv, decimal_to_american, implied_probability
from .rest import AsyncClient, Client
from .stream import Stream

__all__ = [
    "__version__", "Stream", "Client", "AsyncClient", "Book", "channel_of",
    "SPORTS", "CHANNELS", "PRICE_CHANNELS", "STREAM_URL", "REST_URL",
    "american_to_decimal", "decimal_to_american", "implied_probability", "clv",
    "OddsPushError", "FatalError", "Unauthorized", "SubscriptionExpired", "NotInPlan",
    "IpNotAllowed", "TokenReplaced", "RateLimited", "NotFound", "BadRequest",
    "Unavailable", "StreamClosed",
]
