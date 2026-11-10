"""Endpoints, sport IDs and channel names of the OddsPush API."""

from __future__ import annotations

STREAM_URL = "wss://feed.oddspush.com/v1/stream"
REST_URL = "https://feed.oddspush.com"

#: Sport IDs accepted by ``subscribe`` and the REST API.
SPORTS = {
    1: "Soccer",
    2: "Tennis",
    3: "Basketball",
    4: "Hockey",
    5: "American football",
    6: "Baseball",
    7: "Rugby",
    8: "MMA",
    9: "Boxing",
    10: "Other (volleyball and handball)",
    11: "Esports",
    12: "Golf",
    13: "Cricket",
}

#: Channels whose prices make up the main price book.
PRICE_CHANNELS = ("ld", "both")

#: Every channel name the API knows. ``dz`` carries volatile moments and ``pre`` pre-match
#: prices; anything else is reported as ``other``.
CHANNELS = ("ld", "both", "dz", "pre", "other")
