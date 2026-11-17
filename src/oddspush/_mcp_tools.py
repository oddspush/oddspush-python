"""The tools of the OddsPush MCP server, without any MCP library.

Each tool is a GET to the REST API with your key, from your own machine. :mod:`oddspush.mcp_server` serves them over stdio with the official ``mcp``
package (``pip install "oddspush[mcp]"``).

"""

from __future__ import annotations

import copy
import json
import logging
import math
import re
import time
import unicodedata
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

import httpx

from ._constants import REST_URL
from ._version import __version__

log = logging.getLogger("oddspush.mcp")

# ------------------------------------------------------------------ texts for the client

PRICING = "https://oddspush.com/#pricing"
ACCOUNT = "https://oddspush.com/account"
MSG_PLAN = f"This tool is available on the 7-day real-time trial and on paid plans: {PRICING}"
MSG_DELAY = ("Note: this key is on the free plan, so these odds are 30 seconds late. Real-time "
             f"odds come with the 7-day trial and the paid plans: {PRICING}")
MSG_FAIR_PLAN = (f"fair=true (no-vig and fair prices) needs the 7-day real-time trial or a paid "
                 f"plan: {PRICING}. These are the odds without it.")
#: The API takes American odds with 100 <= |a| <= 1,000,000 (``/v1/clv``): in decimal, from
#: 1.0001 (-1,000,000) to 10,001 (+1,000,000). Outside that it would answer 400 bad_request.
AMERICAN_MAX = 1_000_000
DECIMAL_MIN = 1.0001
DECIMAL_MAX = 10_001

# How to find the live event of a pre-match one: with names in the listing, list_events gives
# it as live_event_id; without them, only by opening the live events (their parent_id).
_LIVE_WITH_NAMES = "(list_events shows it as live_event_id)"
_LIVE_WITHOUT_NAMES = ("(list_events with live_only=true lists the live events; get_event on one "
                       "shows its parent_id)")


def _started_text(how: str) -> str:
    return ("This is the pre-match event and the match has started: these prices are frozen "
            "at kick-off, not current. The match in play is a separate live event whose "
            f"parent_id is this id {how}.")


def _time_passed_text(how: str) -> str:
    return ("The scheduled start time has passed: if the match is in play, these pre-match "
            "prices are frozen at kick-off and the current ones are on a separate live event "
            f"whose parent_id is this id {how}.")


#: The notes of a pre-match event that has started, with today's listing (no names).
MSG_STARTED = _started_text(_LIVE_WITHOUT_NAMES)
MSG_TIME_PASSED = _time_passed_text(_LIVE_WITHOUT_NAMES)
#: The same, with a listing that has names, start times and parent_id.
MSG_STARTED_NAMES = _started_text(_LIVE_WITH_NAMES)
MSG_TIME_PASSED_NAMES = _time_passed_text(_LIVE_WITH_NAMES)
MSG_NO_KEY = ("Missing OddsPush API key: set ODDSPUSH_API_KEY in the environment of the "
              f"oddspush-mcp server. Get a key at {ACCOUNT}")
MSG_BAD_KEY = ("That is not an OddsPush API key (opk_ followed by 43 characters). Copy it "
               f"again from {ACCOUNT}")

INSTRUCTIONS = (
    "OddsPush gives Pinnacle odds for 13 sports, live and pre-match. Typical flow: list_sports for "
    "the sport_id, list_events for the event ids (with their market counts, most first; it has no "
    "team names), then get_event for teams, start time, score and prices (fair=true adds the "
    "no-vig and fair prices, on the trial and paid plans). A match in play is a live event "
    "separate from its pre-match event: get_event on the live event gives the pre-match id as "
    "parent_id. Prices are American odds. For closing line value use get_closing or get_clv; for "
    "sharp moves use recent_drops. On a free key, odds are 30 seconds late and only list_sports, "
    "list_events and get_event (without fair) work.")

#: The instructions with a listing that has names (``index_with_names=True``).
INSTRUCTIONS_NAMES = (
    "OddsPush gives Pinnacle odds for 13 sports, live and pre-match. Typical flow: list_sports "
    "for the sport_id, list_events to find events (query matches team or league names; "
    "starts_after and starts_before filter by start time), then get_event for teams, score and "
    "prices (fair=true adds the no-vig and fair prices, on the trial and paid plans). A match in "
    "play is a live event separate from its pre-match event: list_events gives the pre-match "
    "event its live_event_id. Prices are American odds. For closing line value use get_closing "
    "or get_clv; for sharp moves use recent_drops. On a free key, odds are 30 seconds late and "
    "only list_sports, list_events and get_event (without fair) work.")

#: The longest text returned (about 25,000 tokens): longer is cut, with a note.
MAX_TEXT = 90_000

_CUT_EVENTS = "[truncated: narrow with sport_id, live_only or a smaller limit]"
_CUT_EVENTS_NAMES = "[truncated: narrow with sport_id, query, live_only or a smaller limit]"
_CUT_EVENT = "[truncated: narrow with period or market_type]"
_CUT_HISTORY = "[truncated: pass key for one market, and page with since=next_since]"
_CUT_CLOSING = ("[truncated: the closing line has many markets; use get_clv for one market "
                "and leg]")
_CUT_CLOSINGS = "[truncated: narrow with sport_id, since or a smaller limit]"
_CUT_CLV = "[truncated]"
_CUT_DROPS = "[truncated: narrow with sport_id, a higher min_pct or a smaller limit]"

SPORTS_LIST = [
    {"id": 1, "name": "Soccer"},
    {"id": 2, "name": "Tennis"},
    {"id": 3, "name": "Basketball"},
    {"id": 4, "name": "Hockey"},
    {"id": 5, "name": "American football"},
    {"id": 6, "name": "Baseball"},
    {"id": 7, "name": "Rugby"},
    {"id": 8, "name": "MMA"},
    {"id": 9, "name": "Boxing"},
    {"id": 10, "name": "Other (volleyball and handball)"},
    {"id": 11, "name": "Esports"},
    {"id": 12, "name": "Golf"},
    {"id": 13, "name": "Cricket"},
]

_WINDOWS = [30, 60, 300, 600]
_DESIGNATIONS = ["home", "away", "draw", "over", "under"]
_CHANNELS = ["ld", "dz", "pre", "both"]
_TYPES = ["moneyline", "spread", "total", "team_total"]

# ------------------------------------------------------------------ schemas

_SPORT_ID = {
    "type": "integer", "minimum": 1, "maximum": 13,
    "description": "Sport id from list_sports: 1 Soccer, 2 Tennis, 3 Basketball, 4 Hockey, "
                   "5 American football, 6 Baseball, 7 Rugby, 8 MMA, 9 Boxing, 10 Other "
                   "(volleyball, handball), 11 Esports, 12 Golf, 13 Cricket.",
}
_EVENT_ID = {
    "type": "integer", "minimum": 1,
    "description": "Event id, as returned by list_events (a pre-match event or a live event).",
}
_SINCE = {
    "type": "string",
    "description": "Start time: ISO 8601 (e.g. 2026-11-17T12:00:00Z) or epoch milliseconds.",
}
_MARKET_KEY = {
    "type": "string", "maxLength": 64,
    "description": "Market key, as in get_event: 's;<period>;<type>[;<points>]', e.g. 's;0;m' "
                   "(full-match moneyline), 's;0;ou;2.5' (total 2.5), 's;0;s;-1.5' (spread "
                   "-1.5), 's;1;m' (first-half moneyline).",
}
_READ_ONLY = {"readOnlyHint": True, "destructiveHint": False, "idempotentHint": True,
              "openWorldHint": True}

def _list_events_def(with_names: bool) -> Dict[str, Any]:
    """``list_events``. Its search (``query``, the start time) and ``live_event_id`` need the
    names, the start time and the ``parent_id`` in every row of the listing. Without them they
    are not offered: the model would use them and get every event unfiltered."""
    if with_names:
        description = (
            "List the events (matches) that currently have Pinnacle odds, with how many markets "
            "each has per book. Find a match with query (words of the team or league names, e.g. "
            "'arsenal chelsea') and starts_after/starts_before (start time). markets is the total "
            "over all books (ld, dz, pre, both, other). A match that is in play is a separate "
            "live event (books ld/both/dz) from its pre-match event (book pre): the live event "
            "has the pre-match id as parent_id, and the pre-match event gets live_event_id. "
            "Sorted by markets, largest first. Call get_event for the full details and prices.")
    else:
        description = (
            "List the events (matches) that currently have Pinnacle odds: their ids and how many "
            "markets each has per book, without team names or start times. Call get_event on the "
            "ids (most markets first) for the teams, start time and prices. markets is the total "
            "over all books (ld, dz, pre, both, other). A match that is in play is a separate "
            "live event (books ld/both/dz, live: true) from its pre-match event (book pre); "
            "get_event on the live event gives the pre-match id as parent_id. Sorted by markets, "
            "largest first.")
    properties: Dict[str, Any] = {"sport_id": _SPORT_ID}
    if with_names:
        properties["query"] = {"type": "string", "maxLength": 64,
                               "description": "Words that must all appear in the team or league "
                                              "names (accents and case ignored), e.g. 'arsenal "
                                              "chelsea' or 'premier league'."}
        properties["starts_after"] = {**_SINCE, "description": "Only events that start at or "
                                      "after this time. " + _SINCE["description"]}
        properties["starts_before"] = {**_SINCE, "description": "Only events that start at or "
                                       "before this time. " + _SINCE["description"]}
    properties["live_only"] = {"type": "boolean", "default": False,
                               "description": "Only events in play (with live markets)."}
    properties["limit"] = {"type": "integer", "minimum": 1, "maximum": 500, "default": 100,
                           "description": "Maximum events to return."}
    return {"name": "list_events", "title": "List events", "description": description,
            "inputSchema": {"type": "object", "properties": properties,
                            "additionalProperties": False},
            "annotations": _READ_ONLY}


TOOLS: List[Dict[str, Any]] = [
    {
        "name": "list_sports",
        "title": "List sports",
        "description": "List the 13 sports OddsPush covers with their sport_id. Use the "
                       "sport_id to filter list_events, list_closings and recent_drops. Works "
                       "offline, costs nothing.",
        "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        "annotations": {**_READ_ONLY, "openWorldHint": False},
    },
    _list_events_def(False),
    {
        "name": "get_event",
        "title": "Get event odds",
        "description": "Get one event: league, teams, start time, live state (score, minute, red "
                       "cards) and every open market with its prices (American odds; each price "
                       "has a designation such as home/away/draw/over/under, or a "
                       "participantId). With fair=true each market also gets its margin and the "
                       "no-vig and fair (power method) probabilities and decimal odds (trial "
                       "and paid plans). Without a channel, an event with no live prices is "
                       "shown from its pre-match book; if that match has already started, a "
                       "note says the prices are frozen at kick-off. Use period and market_type "
                       "to keep the answer short.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "event_id": _EVENT_ID,
                "fair": {"type": "boolean", "default": False,
                         "description": "Add the no-vig and fair price of every market (trial "
                                        "and paid plans; on a free key the odds come without "
                                        "it, with a note)."},
                "channel": {
                    "type": "string", "enum": _CHANNELS,
                    "description": "Which book: 'ld' and 'both' are the live prices (merged by "
                                   "default), 'pre' the pre-match book, 'dz' the in-play "
                                   "danger-zone book. Leave empty for the usual view.",
                },
                "period": {"type": "integer", "minimum": 0, "maximum": 20,
                           "description": "Only markets of this period: 0 full match, 1 first "
                                          "half or first period, 2 second..."},
                "market_type": {"type": "string", "enum": _TYPES,
                                "description": "Only markets of this type."},
                "include_meta": {"type": "boolean", "default": False,
                                 "description": "Also return the raw event record (long)."},
            },
            "required": ["event_id"],
            "additionalProperties": False,
        },
        "annotations": _READ_ONLY,
    },
    {
        "name": "get_line_history",
        "title": "Get line history",
        "description": "Get how the prices of an event moved, with the fair price of each "
                       "change. Without key: one entry per market with its opening and latest "
                       "price and how many times it changed. With key: every change of that "
                       "market, oldest first; if more is true, call again with "
                       "since=next_since for the next page. Use it to see how a line opened and "
                       "moved before kick-off or during the match. Trial and paid plans.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "event_id": _EVENT_ID,
                "key": _MARKET_KEY,
                "since": _SINCE,
                "fair": {"type": "boolean", "default": True,
                         "description": "Include the fair price of each row."},
            },
            "required": ["event_id"],
            "additionalProperties": False,
        },
        "annotations": _READ_ONLY,
    },
    {
        "name": "get_closing",
        "title": "Get closing line",
        "description": "Get the closing line of a match: every pre-match market as it was when "
                       "the match started, with its margin and its no-vig and fair prices. "
                       "Accepts the pre-match or the live event id. The closing line is the "
                       "benchmark for closing line value (CLV): for one bet, use get_clv. Trial "
                       "and paid plans.",
        "inputSchema": {
            "type": "object",
            "properties": {"event_id": _EVENT_ID},
            "required": ["event_id"],
            "additionalProperties": False,
        },
        "annotations": _READ_ONLY,
    },
    {
        "name": "list_closings",
        "title": "List closing lines",
        "description": "List the matches that have a closing line, by start time (event_id, "
                       "sport_id, start_ms), oldest first. Default: the last 24 hours. Then call "
                       "get_closing or get_clv. Trial and paid plans.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "sport_id": _SPORT_ID,
                "since": {**_SINCE, "description": "Only matches that started at or after this "
                                                   "time. " + _SINCE["description"]},
                "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 200,
                          "description": "Maximum matches to return."},
            },
            "additionalProperties": False,
        },
        "annotations": _READ_ONLY,
    },
    {
        "name": "get_clv",
        "title": "Get closing line value",
        "description": "Closing line value (CLV) of one bet: compares the price you took with "
                       "the closing fair price of that market and leg. Returns the closing price "
                       "and fair price, clv_pct (how much better your price was than the "
                       "closing fair price, in %) and beat_close. Give the price as American "
                       "odds (price) or as decimal odds (decimal_odds). Trial and paid plans.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "event_id": _EVENT_ID,
                "key": _MARKET_KEY,
                "designation": {"type": "string", "enum": _DESIGNATIONS,
                                "description": "The leg you bet on."},
                "price": {"type": "number",
                          "description": "Your price in American odds, e.g. -110 or 150 (100 "
                                         "or more, or -100 or less, up to 1000000)."},
                "decimal_odds": {"type": "number", "minimum": DECIMAL_MIN,
                                 "maximum": DECIMAL_MAX,
                                 "description": "Your price in decimal odds, e.g. 1.91 (from "
                                                "1.0001 to 10001). Use this or price."},
            },
            "required": ["event_id", "key", "designation"],
            "additionalProperties": False,
        },
        "annotations": _READ_ONLY,
    },
    {
        "name": "recent_drops",
        "title": "Recent price drops",
        "description": "Price drops of the last hour, newest first: legs whose fair price "
                       "shortened by at least min_pct within window_s seconds (measured without "
                       "the margin, so a margin change is never a drop). Each alert has the "
                       "match (league, teams, start, live state with score and minute), the "
                       "market, the leg, the price before and after and change_pct. Trial and "
                       "paid plans.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "sport_id": _SPORT_ID,
                "min_pct": {"type": "number", "minimum": 1, "maximum": 50, "default": 3,
                            "description": "Smallest drop to include, in % of the fair odds."},
                "window_s": {"type": "integer", "enum": _WINDOWS, "default": 60,
                             "description": "Window in which the price dropped, in seconds."},
                "since_minutes": {"type": "integer", "minimum": 1, "maximum": 60, "default": 60,
                                  "description": "How far back to look, in minutes."},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200, "default": 50,
                          "description": "Maximum alerts to return (the newest)."},
            },
            "additionalProperties": False,
        },
        "annotations": _READ_ONLY,
    },
]

#: The same tools, with a listing that has names, start times and parent_id.
TOOLS_NAMES: List[Dict[str, Any]] = [_list_events_def(True) if t["name"] == "list_events" else t
                                     for t in TOOLS]
_BY_NAME = {t["name"]: t for t in TOOLS}
_BY_NAME_NAMES = {t["name"]: t for t in TOOLS_NAMES}

#: A key of OddsPush: ``opk_`` and 43 characters of base64url (32 bytes).
_RE_KEY = re.compile(r"^opk_[A-Za-z0-9_-]{43}$")
_RE_ANY_KEY = re.compile(r"opk_[A-Za-z0-9_-]*")


def redact(text: str) -> str:
    """Anything shaped like a key (even a part of one) out of a text."""
    return _RE_ANY_KEY.sub("opk_***", text)


def key_problem(key: Optional[str]) -> Optional[str]:
    """What is wrong with the key, for the user (never repeating it), or ``None``."""
    if not key:
        return MSG_NO_KEY
    if not _RE_KEY.match(key):
        return MSG_BAD_KEY
    return None


# ------------------------------------------------------------------ JS-like helpers

class _Undef:
    """A field that is not there (``undefined``): left out of the JSON."""

    def __repr__(self) -> str:
        return "undefined"


UNDEF: Any = _Undef()


def _g(o: Any, k: str) -> Any:
    return o.get(k, UNDEF) if isinstance(o, dict) else UNDEF


def _nullish(v: Any) -> bool:
    return v is None or v is UNDEF


def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _num(v: Any) -> float:
    return v if _is_num(v) and math.isfinite(v) else 0


def _obj(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _list(v: Any) -> List[Dict[str, Any]]:
    return [_obj(x) for x in v] if isinstance(v, list) else []


def _js(v: Any) -> Any:
    """What ``JSON.stringify`` would write: no ``undefined`` fields (``null`` in a list),
    integral floats as integers, and non-finite numbers as ``null``."""
    if isinstance(v, dict):
        return {k: _js(x) for k, x in v.items() if x is not UNDEF}
    if isinstance(v, (list, tuple)):
        return [None if x is UNDEF else _js(x) for x in v]
    if isinstance(v, float):
        if not math.isfinite(v):
            return None
        if v.is_integer() and abs(v) < 1e21:
            return int(v)
    return v


def _dumps(v: Any) -> str:
    return json.dumps(_js(v), separators=(",", ":"), ensure_ascii=False)


def _str(v: Any) -> str:
    """``String(v)`` of JavaScript, for sorting."""
    if v is UNDEF:
        return "undefined"
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float) and v.is_integer() and abs(v) < 1e21:
        return str(int(v))
    return str(v)


def _param(v: Any) -> str:
    """A query parameter as the REST API reads it."""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float) and v.is_integer() and abs(v) < 1e21:
        return str(int(v))
    return str(v)


def _parse_time(text: str) -> Optional[float]:
    """An ISO 8601 time in ms since 1970, or ``None``. Without an offset, UTC."""
    t = text.strip()
    if not t:
        return None
    if t[-1] in "zZ":
        t = t[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(t)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp() * 1000.0


def _now_ms() -> int:
    return int(time.time() * 1000)


# ------------------------------------------------------------------ arguments

class ArgumentError(Exception):
    """A tool argument is not valid: the answer is a result with ``isError``."""


Args = Dict[str, Any]
_MAX_SAFE = 2 ** 53 - 1


def _integer(a: Args, n: str, lo: float, hi: float) -> Optional[int]:
    v = a.get(n)
    if v is None:
        return None
    x: Any = v
    if isinstance(v, str) and re.fullmatch(r"-?\d+", v.strip()):
        x = int(v.strip())
    if isinstance(x, float) and math.isfinite(x) and x.is_integer():
        x = int(x)
    if (not isinstance(x, int) or isinstance(x, bool) or abs(x) > _MAX_SAFE
            or x < lo or x > hi):
        raise ArgumentError(f"{n} must be an integer from {_str(lo)} to {_str(hi)}")
    return x


def _number(a: Args, n: str, lo: float, hi: float) -> Optional[float]:
    v = a.get(n)
    if v is None:
        return None
    x: Any = v
    if isinstance(v, str) and v.strip() != "":
        try:
            x = float(v)
        except ValueError:
            x = math.nan
    if not _is_num(x) or not math.isfinite(x) or x < lo or x > hi:
        raise ArgumentError(f"{n} must be a number from {_str(lo)} to {_str(hi)}")
    return x


def _boolean(a: Args, n: str, default: bool) -> bool:
    v = a.get(n)
    if v is None:
        return default
    if v is True or v == "true":
        return True
    if v is False or v == "false":
        return False
    raise ArgumentError(f"{n} must be true or false")


def _choice(a: Args, n: str, options: List[str]) -> Optional[str]:
    v = a.get(n)
    if v is None or v == "":
        return None
    if not isinstance(v, str) or v not in options:
        raise ArgumentError(f"{n} must be one of: {', '.join(options)}")
    return v


def _market_key(a: Args, n: str, required: bool) -> Optional[str]:
    v = a.get(n)
    if v is None or v == "":
        if required:
            raise ArgumentError(f"{n} is required")
        return None
    if not isinstance(v, str) or len(v) > 64 or not re.fullmatch(r"[A-Za-z0-9;._+-]+", v):
        raise ArgumentError(f"{n} must be a market key such as 's;0;m' or 's;0;ou;2.5'")
    return v


def _time(a: Args, n: str) -> Optional[int]:
    """A time as ISO 8601 or milliseconds, to milliseconds."""
    v = a.get(n)
    if v is None or v == "":
        return None
    ms: Any
    if _is_num(v):
        ms = v
    elif isinstance(v, str) and re.fullmatch(r"\d{10,16}", v.strip()):
        ms = int(v.strip())
    elif isinstance(v, str) and len(v) <= 40:
        ms = _parse_time(v)
    else:
        ms = None
    if isinstance(ms, float) and math.isfinite(ms) and ms.is_integer():
        ms = int(ms)
    if (not isinstance(ms, int) or isinstance(ms, bool) or ms < 1_000_000_000_000
            or ms > 99_999_999_999_999):
        raise ArgumentError(f"{n} must be an ISO 8601 time (e.g. 2026-11-17T12:00:00Z) or "
                            "epoch milliseconds")
    return ms


def _required(v: Any, n: str) -> Any:
    if v is None:
        raise ArgumentError(f"{n} is required")
    return v


# ------------------------------------------------------------------ results

Result = Dict[str, Any]


def _text(t: str, is_error: bool = False) -> Result:
    return {"content": [{"type": "text", "text": t}], "isError": is_error}


def _json(data: Any, delayed: bool, cut: str) -> Result:
    t = _dumps(data)
    if len(t) > MAX_TEXT:
        t = t[:MAX_TEXT] + "\n" + cut
    content = []
    if delayed:
        content.append({"type": "text", "text": MSG_DELAY})
    content.append({"type": "text", "text": t})
    return {"content": content, "isError": False}


class _Reply:
    """What the REST API answered: ``ok`` and ``data``, or ``status`` and ``code``."""

    __slots__ = ("ok", "data", "status", "code", "retry_s", "delayed")

    def __init__(self, ok: bool, *, data: Any = None, status: int = 200, code: str = "",
                 retry_s: Optional[int] = None, delayed: bool = False) -> None:
        self.ok = ok
        self.data = data
        self.status = status
        self.code = code
        self.retry_s = retry_s
        self.delayed = delayed


def _wait_text(r: _Reply) -> str:
    return f" Retry in {r.retry_s} s." if r.retry_s else " Retry shortly."


def from_error(r: _Reply, not_found: str) -> Result:
    """What a tool answers when the API says no. ``not_found``: the text for a 404
    ``not_found`` (not a failure: the event is not there, or has no closing line yet)."""
    c = r.code
    if c == "not_in_plan":
        return _text(MSG_PLAN)
    if c == "not_found":
        return _text(not_found)
    if c == "Not Found":
        return _text("This tool is not available on the OddsPush API yet. Try the other tools.",
                     True)
    if c == "unauthorized":
        return _text("OddsPush did not accept this API key: it is unknown or was revoked (a "
                     f"regenerated key replaces the old one). Get your current key at {ACCOUNT}",
                     True)
    if c == "subscription_expired":
        return _text(f"This key's plan has expired. Renew it at {ACCOUNT}", True)
    if c == "rest_not_in_plan":
        return _text("This key's plan does not include the REST API, which these tools use: "
                     f"{PRICING}", True)
    if c == "sport_not_in_plan":
        return _text(f"This key's plan does not include that sport: {PRICING}", True)
    if c == "ip_not_allowed":
        return _text("This key only works from a fixed IP address. Contact "
                     "support@oddspush.com.", True)
    if c == "bad_request":
        return _text("OddsPush rejected these arguments (bad_request). Check them against the "
                     "tool's schema.", True)
    if c == "rate_limited":
        return _text("This key has reached its request rate limit." + _wait_text(r), True)
    if c == "not_ready":
        return _text("The delayed free feed is filling up after a restart." + _wait_text(r),
                     True)
    if c == "unavailable":
        return _text("OddsPush is temporarily unavailable." + _wait_text(r), True)
    if c == "feed_unreachable":
        return _text("Could not reach the OddsPush API. Try again shortly.", True)
    clean = re.sub(r"[^A-Za-z0-9_ .:-]", "", redact(c))[:64]
    return _text(f"OddsPush API error (HTTP {r.status}{', ' + clean if clean else ''})."
                 + (_wait_text(r) if r.status >= 500 else ""), True)


# ------------------------------------------------------------------ the REST calls

#: The 403s that accepting the free plan's delay fixes.
_ACCEPT_DELAY = ("delayed_plan", "subscription_expired")
#: Once a key was seen on the free plan, its next requests accept the delay from the start
#: for this long (then it asks once without it, in case the plan changed).
DELAY_MEMORY_S = 900.0
TIMEOUT_S = 15.0


class _Feed:
    """GETs to the REST API with the key, from this machine."""

    def __init__(self, key: str, base_url: str, transport: Any, timeout: float) -> None:
        self._key = key
        self._http = httpx.AsyncClient(
            base_url=base_url.rstrip("/") + "/v1", timeout=timeout, transport=transport,
            follow_redirects=False,
            headers={"accept": "application/json",
                     "user-agent": f"oddspush-mcp/{__version__} (python)"})
        self._delay_until = 0.0
        #: The listing has names, start times and parent_id (``index_with_names``).
        self.with_names = False

    def _delay_known(self) -> bool:
        return time.monotonic() < self._delay_until

    async def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> _Reply:
        query = {k: _param(v) for k, v in (params or {}).items()
                 if v is not None and v is not UNDEF and v != ""}
        delayed = self._delay_known()
        while True:
            headers = {"x-token": self._key}
            if delayed:
                headers["x-accept-delay"] = "true"
            try:
                resp = await self._http.get(path, params=query, headers=headers)
            except httpx.HTTPError as e:
                log.error("oddspush-mcp: no answer from the API on %s (%s)",
                          "/".join(path.split("/")[:2]), type(e).__name__)
                return _Reply(False, status=0, code="feed_unreachable", delayed=delayed)
            body: Any = None
            if resp.content:
                try:
                    body = resp.json()
                except ValueError:
                    body = None
            if resp.is_success:
                if delayed:
                    self._delay_until = time.monotonic() + DELAY_MEMORY_S
                return _Reply(True, data=body, delayed=delayed)
            detail = body.get("detail") if isinstance(body, dict) else None
            code = detail if isinstance(detail, str) and detail else f"http_{resp.status_code}"
            if resp.status_code == 403 and not delayed and code in _ACCEPT_DELAY:
                delayed = True
                continue
            if delayed and code != "subscription_expired":
                self._delay_until = time.monotonic() + DELAY_MEMORY_S
            retry: Optional[int] = None
            try:
                value = float(resp.headers.get("retry-after", ""))
                if math.isfinite(value) and value > 0:
                    retry = math.ceil(value)
            except ValueError:
                pass
            return _Reply(False, status=resp.status_code, code=code, retry_s=retry,
                          delayed=delayed)

    async def aclose(self) -> None:
        await self._http.aclose()


# ------------------------------------------------------------------ the tools

_BOOKS = ["ld", "dz", "pre", "both", "other"]
#: Words of a search that do not help to find a match ("Arsenal v Chelsea").
_STOPWORDS = {"v", "vs", "x", "at", "and", "the", "de", "del", "la", "el", "y"}


def _normal(t: str) -> str:
    """Lower case and without accents."""
    return re.sub("[̀-ͯ]", "", unicodedata.normalize("NFD", t)).lower()


def _search(a: Args, n: str) -> Optional[List[str]]:
    v = a.get(n)
    if v is None or v == "":
        return None
    if not isinstance(v, str) or len(v) > 64:
        raise ArgumentError(f"{n} must be a text of up to 64 characters")
    words = [w for w in re.split(r"[\W_]+", _normal(v)) if w and w not in _STOPWORDS]
    if not words:
        raise ArgumentError(f"{n} must have a team or league name")
    return words


def _name_of(v: Any) -> Optional[str]:
    if isinstance(v, str):
        return v
    n = _obj(v).get("name")
    return n if isinstance(n, str) else None


def _names_of(e: Dict[str, Any]) -> List[str]:
    """The league and the teams of a row of the listing, if it has them."""
    out = []
    league = _name_of(e.get("league"))
    if league:
        out.append(league)
    if isinstance(e.get("participants"), list):
        for p in e["participants"]:
            n = _name_of(p)
            if n:
                out.append(n)
    for k in ("home", "away"):
        n = _name_of(e.get(k))
        if n:
            out.append(n)
    return out


def _start_of(e: Dict[str, Any]) -> Optional[float]:
    """The start time of a row of the listing, in ms, if it has one."""
    v = e.get("start_time")
    if v is None:
        v = e.get("start_ms")
    if _is_num(v) and math.isfinite(v):
        return v
    if isinstance(v, str):
        return _parse_time(v)
    return None


def _live(e: Dict[str, Any]) -> bool:
    return _num(e.get("ld")) + _num(e.get("dz")) + _num(e.get("both")) > 0


async def _list_events(f: _Feed, a: Args) -> Result:
    sport = _integer(a, "sport_id", 1, 13)
    words = _search(a, "query")
    after = _time(a, "starts_after")
    before = _time(a, "starts_before")
    live_only = _boolean(a, "live_only", False)
    limit = _integer(a, "limit", 1, 500) or 100
    r = await f.get("/events", {"sport_id": sport})
    if not r.ok:
        return from_error(r, "No events.")
    rows = [_obj(e) for e in _obj(r.data).get("events") or []] \
        if isinstance(_obj(r.data).get("events"), list) else []
    # From the pre-match event to its live one: the live event has the pre-match id as
    # parent_id.
    live_of: Dict[Any, Any] = {}
    for e in rows:
        pid = e.get("parent_id")
        if _live(e) and _is_num(pid) and pid != e.get("event_id"):
            live_of[pid] = _num(e.get("event_id"))
    events = []
    for e in rows:
        row = {**e, "markets": sum(_num(e.get(k)) for k in _BOOKS), "live": _live(e)}
        live_id = live_of.get(_num(e.get("event_id")))
        if live_id is not None:
            row["live_event_id"] = live_id
        events.append(row)
    notes = []
    if words:
        if any(_names_of(e) for e in rows):
            events = [e for e in events
                      if all(w in _normal(" ".join(_names_of(e))) for w in words)]
        else:
            notes.append("query was ignored: this listing has no team or league names yet. "
                         "Call get_event on the ids (most markets first) to see who plays.")
    if after is not None or before is not None:
        if any(_start_of(e) is not None for e in rows):
            def fits(e: Dict[str, Any]) -> bool:
                h = _start_of(e)
                return (h is not None and (after is None or h >= after)
                        and (before is None or h <= before))
            events = [e for e in events if fits(e)]
        else:
            notes.append("starts_after and starts_before were ignored: this listing has no "
                         "start times yet; get_event has them.")
    events = sorted((e for e in events if not live_only or e["live"]),
                    key=lambda e: (-_num(e["markets"]), _num(e.get("event_id"))))
    out: Dict[str, Any] = {"total": len(events), "returned": min(limit, len(events)),
                           "events": events[:limit]}
    if notes:
        out["note"] = " ".join(notes)
    return _json(out, r.delayed, _CUT_EVENTS_NAMES if f.with_names else _CUT_EVENTS)


def _summary(meta: Dict[str, Any]) -> Dict[str, Any]:
    """What matters of the event record, without the clutter."""
    league = _obj(meta.get("league"))

    def part(ps: Any) -> Any:
        if not isinstance(ps, list):
            return UNDEF
        return [{"id": _g(p, "id"), "name": _g(p, "name"), "alignment": _g(p, "alignment"),
                 "state": _g(p, "state")} for p in (_obj(x) for x in ps)]

    parent = _obj(meta.get("parent"))
    special = _obj(meta.get("special"))
    parent_id = _g(meta, "parentId")
    if _nullish(parent_id):
        parent_id = _g(parent, "id")
    return {
        "league": _g(league, "name"),
        "type": _g(meta, "type"),
        "status": _g(meta, "status"),
        "start_time": _g(meta, "startTime"),
        "is_live": _g(meta, "isLive"),
        "state": _g(meta, "state"),
        "parent_id": parent_id,
        "participants": part(meta.get("participants")),
        "parent": ({"participants": part(parent.get("participants")),
                    "state": _g(parent, "state")} if parent else UNDEF),
        "special": ({"description": special["description"],
                     "category": _g(special, "category")}
                    if special.get("description") else UNDEF),
    }


def _started(meta: Dict[str, Any], with_names: bool = False,
             now: Optional[float] = None) -> Optional[str]:
    """If the match of a pre-match book has started, the note that says so."""
    if meta.get("status") == "started" or meta.get("isLive") is True:
        return MSG_STARTED_NAMES if with_names else MSG_STARTED
    start = meta.get("startTime")
    t = _parse_time(start) if isinstance(start, str) else None
    if t is None or t > (now if now is not None else _now_ms()):
        return None
    return MSG_TIME_PASSED_NAMES if with_names else MSG_TIME_PASSED


async def _get_event(f: _Feed, a: Args) -> Result:
    eid = _required(_integer(a, "event_id", 1, _MAX_SAFE), "event_id")
    fair = _boolean(a, "fair", False)
    channel = _choice(a, "channel", _CHANNELS)
    period = _integer(a, "period", 0, 20)
    mtype = _choice(a, "market_type", _TYPES)
    with_meta = _boolean(a, "include_meta", False)
    missing = f"No event {eid} right now: it may have ended, or the id is wrong (use list_events)."
    path = f"/events/{eid}"
    # `fair` is not on the free plan (403 not_in_plan), but the odds are: then they are asked
    # for without `fair`, with a note, instead of answering as if the whole tool were not there.
    fair_denied = False

    async def read(ch: Optional[str]) -> _Reply:
        nonlocal fair_denied
        with_fair = fair and not fair_denied
        x = await f.get(path, {"channel": ch, "fair": "true" if with_fair else None})
        if not x.ok and with_fair and x.code == "not_in_plan":
            fair_denied = True
            return await f.get(path, {"channel": ch})
        return x

    r = await read(channel)
    if not r.ok:
        return from_error(r, missing)
    ev = _obj(r.data)
    # Without a channel, `merged` only has the live books: a match that has not started comes
    # back empty. Then, its pre-match book.
    if not channel and not _obj(ev.get("merged")) and _obj(ev.get("pre")):
        channel = "pre"
        r2 = await read(channel)
        if not r2.ok:
            return from_error(r2, missing)
        r = r2
        ev = _obj(r2.data)
    notes = []
    if channel == "pre":
        note = _started(_obj(ev.get("meta")), f.with_names)
        if note:
            notes.append(note)
    if fair_denied:
        notes.append(MSG_FAIR_PLAN)

    def fits(m: Dict[str, Any]) -> bool:
        return ((period is None or m.get("period") == period)
                and (mtype is None or m.get("type") == mtype))

    markets = sorted(((k, m) for k, m in _obj(ev.get("merged")).items() if fits(_obj(m))),
                     key=lambda km: km[0])
    keys = {k for k, _ in markets}
    eid_out = _g(ev, "event_id")
    out: Dict[str, Any] = {
        "event_id": eid if _nullish(eid_out) else eid_out,
        "sport_id": _g(ev, "sport_id"),
        "book": channel if channel is not None else "ld+both",
        **_summary(_obj(ev.get("meta"))),
        "periods": _g(ev, "periods"),
        "markets": [m for _, m in markets],
    }
    if fair and not fair_denied:
        out["fair"] = {k: v for k, v in _obj(ev.get("fair")).items() if k in keys}
    if with_meta:
        out["meta"] = _g(ev, "meta")
    if notes:
        out["note"] = " ".join(notes)
    return _json(out, r.delayed, _CUT_EVENT)


#: Pages of the line history read at most for the summary without `key`.
_HISTORY_PAGES = 3
#: What the rows or the markets of an answer can take: the rest is for the envelope.
_BUDGET = MAX_TEXT - 4_000


def _trim(rows: List[Dict[str, Any]], more: bool,
          nxt: Optional[float]) -> Tuple[List[Dict[str, Any]], bool, Optional[float]]:
    """The whole rows that fit in the budget, and where to go on without skipping any: the
    API pages by `time` (since included), so the cut also leaves out the rows that share the
    `time` of the first one that does not fit."""
    used = 0
    n = 0
    for row in rows:
        size = len(_dumps(row)) + 1
        if used + size > _BUDGET:
            break
        used += size
        n += 1
    if n == len(rows):
        return rows, more, nxt
    cut = _num(rows[n].get("time"))
    j = n
    while j > 0 and _num(rows[j - 1].get("time")) == cut:
        j -= 1
    if j == 0:
        return rows[:n], True, cut + 1
    return rows[:j], True, cut


async def _get_line_history(f: _Feed, a: Args) -> Result:
    eid = _required(_integer(a, "event_id", 1, _MAX_SAFE), "event_id")
    key = _market_key(a, "key", False)
    since = _time(a, "since")
    fair = _boolean(a, "fair", True)
    path = f"/events/{eid}/history"
    none = f"No line history for event {eid}{f' and market {key}' if key else ''} in that period."
    if key:
        r = await f.get(path, {"key": key, "since": since, "fair": "true" if fair else None})
        if not r.ok:
            return from_error(r, none)
        d = _obj(r.data)
        nxt = d.get("next_since")
        rows, more, nxt2 = _trim(_list(d.get("history")), bool(d.get("more")),
                                 nxt if _is_num(nxt) else None)
        return _json({"event_id": eid, "key": key, "n": len(rows), "more": more,
                      "next_since": nxt2, "history": rows}, r.delayed, _CUT_HISTORY)
    # Without key: the whole history of every market does not fit (thousands of rows, oldest
    # first: the openings would show and never the end). A summary per market.
    markets: Dict[str, Dict[str, Any]] = {}
    start = since
    more = False
    nxt: Optional[float] = None
    delayed = False
    read = 0
    for p in range(_HISTORY_PAGES):
        r = await f.get(path, {"since": start, "fair": "true" if fair else None})
        if not r.ok:
            if p == 0:
                return from_error(r, none)
            more = True
            nxt = start
            break
        delayed = delayed or r.delayed
        d = _obj(r.data)
        for row in _list(d.get("history")):
            k = _g(row, "key")
            ch = _g(row, "channel")
            rest = {x: y for x, y in row.items() if x not in ("key", "channel")}
            ident = f"{_str(ch)}|{_str(k)}"
            m = markets.get(ident)
            if m is None:
                m = {"key": k, "channel": ch, "changes": 0, "opening": rest, "latest": rest}
                markets[ident] = m
            m["changes"] += 1
            m["latest"] = rest
            read += 1
        more = bool(d.get("more"))
        n_since = d.get("next_since")
        nxt = n_since if _is_num(n_since) else None
        if not more or nxt is None:
            break
        start = nxt  # type: ignore[assignment]
    order = sorted(markets.values(), key=lambda m: (_str(m["key"]), _str(m["channel"])))
    inside: List[Dict[str, Any]] = []
    outside: List[Any] = []
    used = 0
    for m in order:
        size = len(_dumps(m)) + 1
        if not outside and used + size <= _BUDGET:
            inside.append(m)
            used += size
        else:
            outside.append(m["key"])
    out: Dict[str, Any] = {"event_id": eid, "summary": True, "changes": read, "more": more,
                           "next_since": nxt if more else None, "markets": inside}
    notes = []
    if outside:
        out["omitted_markets"] = outside
        notes.append("Some markets did not fit: pass key for one of omitted_markets.")
    if more:
        notes.append("There are more changes than were read: latest is the last change before "
                     "next_since. Call again with since=next_since for the rest, or pass key "
                     "for one market.")
    if notes:
        out["note"] = " ".join(notes)
    return _json(out, delayed, _CUT_HISTORY)


async def _get_closing(f: _Feed, a: Args) -> Result:
    eid = _required(_integer(a, "event_id", 1, _MAX_SAFE), "event_id")
    r = await f.get(f"/closing/{eid}")
    if not r.ok:
        return from_error(r, f"No closing line for event {eid}: the match has not started yet, "
                             "it started too long ago, or it had no open pre-match market at "
                             "kick-off.")
    return _json(r.data, r.delayed, _CUT_CLOSING)


async def _list_closings(f: _Feed, a: Args) -> Result:
    sport = _integer(a, "sport_id", 1, 13)
    since = _time(a, "since")
    limit = _integer(a, "limit", 1, 1000) or 200
    r = await f.get("/closing", {"sport_id": sport, "since": since})
    if not r.ok:
        return from_error(r, "No closing lines in that period.")
    d = _obj(r.data)
    rows = d.get("events") if isinstance(d.get("events"), list) else []
    return _json({"returned": min(limit, len(rows)),
                  "more": bool(d.get("more")) or len(rows) > limit,
                  "events": rows[:limit]}, r.delayed, _CUT_CLOSINGS)


def american(decimal: float) -> float:
    """Decimal to American odds, with two decimals (the API converts it back)."""
    a = (decimal - 1) * 100 if decimal >= 2 else -100 / (decimal - 1)
    return math.floor(a * 100 + 0.5) / 100     # Math.round


async def _get_clv(f: _Feed, a: Args) -> Result:
    eid = _required(_integer(a, "event_id", 1, _MAX_SAFE), "event_id")
    key = _market_key(a, "key", True)
    designation = _required(_choice(a, "designation", _DESIGNATIONS), "designation")
    am = _number(a, "price", -AMERICAN_MAX, AMERICAN_MAX)
    dec = _number(a, "decimal_odds", DECIMAL_MIN, DECIMAL_MAX)
    if (am is None) == (dec is None):
        raise ArgumentError("give exactly one of price (American odds) or decimal_odds")
    if am is not None and abs(am) < 100:
        raise ArgumentError("price is American odds: 100 or more, or -100 or less (e.g. -110, "
                            "150)")
    price = am if am is not None else american(dec)  # type: ignore[arg-type]
    r = await f.get("/clv", {"event_id": eid, "key": key, "designation": designation,
                             "price": price})
    if not r.ok:
        return from_error(r, f"No closing price for event {eid}, market {key}, {designation}: "
                             "the match has not started yet, the market or leg was not open at "
                             "kick-off, or it started too long ago.")
    return _json(r.data, r.delayed, _CUT_CLV)


_DROP_PAGES = 5


async def _recent_drops(f: _Feed, a: Args) -> Result:
    sport = _integer(a, "sport_id", 1, 13)
    min_pct = _number(a, "min_pct", 1, 50)
    if min_pct is None:
        min_pct = 3
    window = _integer(a, "window_s", 30, 600) or 60
    if window not in _WINDOWS:
        raise ArgumentError(f"window_s must be one of: {', '.join(map(str, _WINDOWS))}")
    minutes = _integer(a, "since_minutes", 1, 60) or 60
    limit = _integer(a, "limit", 1, 200) or 50
    base = {"sport_id": sport, "min_pct": min_pct, "window_s": window,
            "since": _now_ms() - minutes * 60_000}
    # The alerts come oldest first, 1,000 a page: to keep the newest, follow the cursor to the
    # end (or up to _DROP_PAGES).
    alerts: List[Any] = []
    delayed = False
    after: Any = None
    incomplete = False
    for p in range(_DROP_PAGES):
        r = await f.get("/drops", {**base, "after": after if _is_num(after) else None})
        if not r.ok:
            if p == 0:
                return from_error(r, "No drops in that period.")
            incomplete = True
            break
        delayed = delayed or r.delayed
        d = _obj(r.data)
        page = d.get("alerts") if isinstance(d.get("alerts"), list) else []
        alerts = (alerts + page)[-limit:]
        if not d.get("more") or not _is_num(d.get("cursor")) or not page:
            break
        after = d["cursor"]
        if p == _DROP_PAGES - 1:
            incomplete = True
    out: Dict[str, Any] = {"returned": len(alerts), "min_pct": min_pct, "window_s": window,
                           "alerts": list(reversed(alerts))}
    if incomplete:
        out["note"] = ("There were more drops than could be read: the newest may be missing. "
                       "Raise min_pct or set sport_id.")
    return _json(out, delayed, _CUT_DROPS)


async def _list_sports(f: Optional[_Feed], a: Args) -> Result:
    return _json({"sports": SPORTS_LIST}, False, _CUT_CLV)


_ACTIONS: Dict[str, Callable[[Any, Args], Awaitable[Result]]] = {
    "list_sports": _list_sports,
    "list_events": _list_events,
    "get_event": _get_event,
    "get_line_history": _get_line_history,
    "get_closing": _get_closing,
    "list_closings": _list_closings,
    "get_clv": _get_clv,
    "recent_drops": _recent_drops,
}


class OddsPushTools:
    """The OddsPush MCP tools, run against the REST API with ``key``.

    :param key: your API key (``opk_...``). Without one (or with one of the wrong shape)
        ``list_sports`` still works and the other tools answer what is wrong.
    :param base_url: the REST API (``https://feed.oddspush.com``).
    :param transport: passed to ``httpx.AsyncClient`` (for tests).
    :param index_with_names: the listing of ``/v1/events`` has team names, start times and
        ``parent_id``; then ``list_events`` offers ``query``, ``starts_after``/``starts_before``
        and ``live_event_id``. Off by default: today's listing has none of them.
    """

    def __init__(self, key: Optional[str], *, base_url: str = REST_URL,
                 transport: Optional[httpx.AsyncBaseTransport] = None,
                 timeout: float = TIMEOUT_S, index_with_names: bool = False) -> None:
        self._problem = key_problem(key)
        self._feed = _Feed(key or "", base_url, transport, timeout)
        self._feed.with_names = bool(index_with_names)
        self._by_name = _BY_NAME_NAMES if index_with_names else _BY_NAME

    @staticmethod
    def definitions() -> List[Dict[str, Any]]:
        """The tools, as ``tools/list`` gives them."""
        return copy.deepcopy(TOOLS)

    @staticmethod
    def exists(name: Any) -> bool:
        return isinstance(name, str) and name in _BY_NAME

    async def call(self, name: str, args: Any) -> Result:
        """Run a tool that exists (:meth:`exists`): ``{"content": [...], "isError": bool}``.
        Bad arguments are a result with ``isError`` (the model can fix them)."""
        if self._problem is not None and name != "list_sports":
            return _text(self._problem, True)
        a = args if isinstance(args, dict) else {}
        allowed = self._by_name[name]["inputSchema"].get("properties") or {}
        extra = [k for k in a if k not in allowed]
        try:
            if extra:
                raise ArgumentError("unknown argument: "
                                    + ", ".join(str(k)[:32] for k in extra))
            return await _ACTIONS[name](self._feed, a)
        except ArgumentError as e:
            return _text(f"Invalid arguments: {redact(str(e))}.", True)
        except Exception as e:  # a bug: say so, never the key
            log.error("oddspush-mcp: tool %s failed: %s", name, redact(str(e)))
            return _text("Internal error in the OddsPush MCP server. Try again.", True)

    async def aclose(self) -> None:
        await self._feed.aclose()


__all__ = ["OddsPushTools", "TOOLS", "TOOLS_NAMES", "INSTRUCTIONS", "INSTRUCTIONS_NAMES",
           "key_problem", "redact", "american"]
