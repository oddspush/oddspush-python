"""The MCP tools against a fake REST API (httpx.MockTransport): no network, no ``mcp``."""

import json
import time
from datetime import datetime, timezone

import httpx
import pytest

from oddspush import _mcp_tools as t
from oddspush._mcp_tools import OddsPushTools

KEY = "opk_" + "A" * 43


class Api:
    """A fake REST API: ``routes`` maps a path to a function ``(request) -> Response``."""

    def __init__(self, **routes):
        self.routes = routes
        self.seen = []

    def __call__(self, req):
        self.seen.append(req)
        path = req.url.path
        for prefix, fn in self.routes.items():
            if path == prefix:
                return fn(req)
        return httpx.Response(404, json={"detail": "Not Found"})


def tools(api, key=KEY, names=False):
    return OddsPushTools(key, transport=httpx.MockTransport(api), index_with_names=names)


def ok(body, **kw):
    return lambda req: httpx.Response(200, json=body, **kw)


def err(status, detail, **kw):
    return lambda req: httpx.Response(status, json={"detail": detail}, **kw)


def body(result):
    """The JSON of a result (its last text)."""
    assert result["isError"] is False, result
    return json.loads(result["content"][-1]["text"])


def text(result):
    return result["content"][-1]["text"]


def test_definitions_are_a_copy():
    defs = OddsPushTools.definitions()
    assert [d["name"] for d in defs] == ["list_sports", "list_events", "get_event",
                                         "get_line_history", "get_closing", "list_closings",
                                         "get_clv", "recent_drops"]
    defs[0]["name"] = "x"
    assert OddsPushTools.definitions()[0]["name"] == "list_sports"
    for d in defs:
        assert d["inputSchema"]["additionalProperties"] is False
        assert d["annotations"]["readOnlyHint"] is True


def test_no_internal_routes_in_the_definitions():
    blob = json.dumps(t.TOOLS + [t.INSTRUCTIONS, t.MSG_PLAN, t.MSG_DELAY]).lower()
    for word in ("/store", "/ws", "upstream"):
        assert word not in blob


async def test_list_sports_needs_no_key():
    tl = OddsPushTools(None, transport=httpx.MockTransport(Api()))
    r = await tl.call("list_sports", {})
    assert len(body(r)["sports"]) == 13 and body(r)["sports"][0] == {"id": 1, "name": "Soccer"}


@pytest.mark.parametrize("key,start", [(None, "Missing OddsPush API key"),
                                       ("opk_short", "That is not an OddsPush API key"),
                                       ("nope", "That is not an OddsPush API key")])
async def test_key_problems_never_call_the_api(key, start):
    api = Api()
    tl = tools(api, key)
    r = await tl.call("get_event", {"event_id": 1})
    assert r["isError"] is True and text(r).startswith(start)
    assert "opk_short" not in text(r)
    assert api.seen == []


async def test_headers_and_key():
    api = Api(**{"/v1/events": ok({"events": []})})
    await tools(api).call("list_events", {"sport_id": 1})
    req = api.seen[0]
    assert req.headers["x-token"] == KEY
    assert "x-accept-delay" not in req.headers
    assert req.headers["user-agent"].startswith("oddspush-mcp/")
    assert dict(req.url.params) == {"sport_id": "1"}


async def test_list_events_sums_books_links_live_and_sorts():
    rows = [
        {"event_id": 10, "sport_id": 1, "ld": 0, "dz": 0, "pre": 30, "both": 0, "other": 0,
         "markets": 0, "league": "England - Premier League",
         "participants": ["Arsenal", "Chelsea"], "start_time": "2026-11-17T12:00:00Z"},
        {"event_id": 11, "sport_id": 1, "ld": 5, "dz": 1, "pre": 0, "both": 4, "other": 0,
         "markets": 5, "parent_id": 10, "league": "England - Premier League",
         "participants": ["Arsenal", "Chelsea"], "start_time": "2026-11-17T12:00:00Z"},
        {"event_id": 3, "sport_id": 1, "ld": 0, "dz": 0, "pre": 2, "both": 0, "other": 0,
         "markets": 0, "league": "Spain - La Liga", "participants": ["Málaga", "Cádiz"],
         "start_time": "2026-11-18T12:00:00Z"},
    ]
    api = Api(**{"/v1/events": ok({"n": 3, "events": rows})})
    tl = tools(api, names=True)
    out = body(await tl.call("list_events", {}))
    assert [e["event_id"] for e in out["events"]] == [10, 11, 3]
    assert out["events"][0]["markets"] == 30 and out["events"][0]["live"] is False
    assert out["events"][0]["live_event_id"] == 11
    assert out["events"][1]["live"] is True and out["total"] == 3
    q = body(await tl.call("list_events", {"query": "malaga v cadiz"}))
    assert [e["event_id"] for e in q["events"]] == [3]
    live = body(await tl.call("list_events", {"live_only": True, "limit": "1"}))
    assert [e["event_id"] for e in live["events"]] == [11]
    when = body(await tl.call("list_events", {"starts_after": "2026-11-18T00:00:00Z"}))
    assert [e["event_id"] for e in when["events"]] == [3]
    noon = int(datetime(2026, 11, 17, 12, tzinfo=timezone.utc).timestamp() * 1000)
    ms = body(await tl.call("list_events", {"starts_before": noon}))
    assert {e["event_id"] for e in ms["events"]} == {10, 11}


async def test_list_events_today_has_no_search():
    """Today's listing has no names, start times or parent_id: list_events does not offer
    query or starts_after/starts_before (as the remote server with INDICE_CON_NOMBRES=false),
    and passing them is an unknown argument, not a silent unfiltered list."""
    api = Api(**{"/v1/events": ok({"events": [{"event_id": 1, "ld": 1}]})})
    props = {d["name"]: d for d in t.TOOLS}["list_events"]["inputSchema"]["properties"]
    assert set(props) == {"sport_id", "live_only", "limit"}
    names = {d["name"]: d for d in t.TOOLS_NAMES}["list_events"]["inputSchema"]["properties"]
    assert {"query", "starts_after", "starts_before"} <= set(names)
    r = await tools(api).call("list_events", {"query": "arsenal"})
    assert r["isError"] is True and "unknown argument: query" in text(r)
    assert "live_event_id" not in t.INSTRUCTIONS and "query" not in t.INSTRUCTIONS
    assert "live_event_id" not in t.MSG_STARTED and "live_event_id" in t.MSG_STARTED_NAMES


async def test_get_clv_decimal_odds_in_the_range_the_api_takes():
    api = Api()
    for dec in (1.00001, 10_002, 1_000_000):
        r = await tools(api).call("get_clv", {"event_id": 1, "key": "s;0;m",
                                              "designation": "home", "decimal_odds": dec})
        assert r["isError"] is True and "decimal_odds" in text(r)
    clv = {d["name"]: d for d in t.TOOLS}["get_clv"]["inputSchema"]["properties"]
    assert (clv["decimal_odds"]["minimum"], clv["decimal_odds"]["maximum"]) == (1.0001, 10_001)


async def test_list_events_without_names_says_so():
    api = Api(**{"/v1/events": ok({"events": [{"event_id": 1, "ld": 1}]})})
    out = body(await tools(api, names=True).call("list_events", {"query": "arsenal",
                                                                 "starts_after": "2026-11-17"}))
    assert out["total"] == 1
    assert "query was ignored" in out["note"] and "starts_after" in out["note"]


EVENT = {"event_id": 7, "sport_id": 1,
         "meta": {"league": {"name": "L", "id": 9}, "type": "matchup", "status": "pending",
                  "startTime": "2099-01-01T00:00:00Z", "isLive": False,
                  "participants": [{"id": 1, "name": "A", "alignment": "home", "x": 1},
                                   {"id": 2, "name": "B", "alignment": "away"}]},
         "periods": {"0": {"status": "open"}},
         "merged": {"s;0;ou;2.5": {"key": "s;0;ou;2.5", "type": "total", "period": 0},
                    "s;0;m": {"key": "s;0;m", "type": "moneyline", "period": 0},
                    "s;1;m": {"key": "s;1;m", "type": "moneyline", "period": 1}},
         "fair": {"s;0;m": {"margin": 0.03}, "s;1;m": {"margin": 0.04},
                  "s;0;ou;2.5": {"margin": 0.05}}}


async def test_get_event_summary_filters_and_fair():
    api = Api(**{"/v1/events/7": ok(EVENT)})
    out = body(await tools(api).call("get_event", {"event_id": 7, "fair": True,
                                                   "market_type": "moneyline", "period": 0}))
    assert out["book"] == "ld+both" and out["league"] == "L" and out["is_live"] is False
    assert out["participants"] == [{"id": 1, "name": "A", "alignment": "home"},
                                   {"id": 2, "name": "B", "alignment": "away"}]
    assert "parent" not in out and "special" not in out and "meta" not in out
    assert [m["key"] for m in out["markets"]] == ["s;0;m"]
    assert out["fair"] == {"s;0;m": {"margin": 0.03}}
    assert dict(api.seen[0].url.params) == {"fair": "true"}


async def test_get_event_falls_back_to_pre_and_notes_a_started_match():
    live = {"event_id": 8, "meta": {"status": "started"}, "merged": {}, "pre": {"s;0;m": {}}}
    pre = {"event_id": 8, "meta": {"status": "started", "startTime": "2020-01-01T00:00:00Z"},
           "merged": {"s;0;m": {"key": "s;0;m"}}, "pre": {"s;0;m": {}}}

    def route(req):
        return httpx.Response(200, json=pre if req.url.params.get("channel") == "pre" else live)

    api = Api(**{"/v1/events/8": route})
    out = body(await tools(api).call("get_event", {"event_id": 8}))
    assert out["book"] == "pre" and out["note"] == t.MSG_STARTED
    assert [dict(r.url.params) for r in api.seen] == [{}, {"channel": "pre"}]


async def test_get_event_fair_on_a_free_key():
    def route(req):
        if req.headers.get("x-accept-delay") != "true":
            return httpx.Response(403, json={"detail": "delayed_plan"})
        if req.url.params.get("fair"):
            return httpx.Response(403, json={"detail": "not_in_plan"})
        return httpx.Response(200, json=EVENT)

    api = Api(**{"/v1/events/7": route})
    tl = tools(api)
    r = await tl.call("get_event", {"event_id": 7, "fair": True})
    assert r["content"][0]["text"] == t.MSG_DELAY
    out = body(r)
    assert "fair" not in out and out["note"].endswith(t.MSG_FAIR_PLAN)
    # The next call accepts the delay from the start: no second 403.
    n = len(api.seen)
    await tl.call("get_event", {"event_id": 7})
    assert len(api.seen) == n + 1 and api.seen[-1].headers["x-accept-delay"] == "true"


async def test_free_key_on_a_paid_tool():
    def route(req):
        if req.headers.get("x-accept-delay") != "true":
            return httpx.Response(403, json={"detail": "delayed_plan"})
        return httpx.Response(403, json={"detail": "not_in_plan"})

    r = await tools(Api(**{"/v1/closing/5": route})).call("get_closing", {"event_id": 5})
    assert r == {"content": [{"type": "text", "text": t.MSG_PLAN}], "isError": False}


async def test_expired_key_retries_with_the_delay_once():
    api = Api(**{"/v1/events": err(403, "subscription_expired")})
    r = await tools(api).call("list_events", {})
    assert r["isError"] is True and "expired" in text(r)
    assert [q.headers.get("x-accept-delay") for q in api.seen] == [None, "true"]


async def test_delay_memory_expires(monkeypatch):
    calls = []

    def route(req):
        calls.append(req.headers.get("x-accept-delay"))
        if req.headers.get("x-accept-delay") != "true":
            return httpx.Response(403, json={"detail": "delayed_plan"})
        return httpx.Response(200, json={"events": []})

    tl = tools(Api(**{"/v1/events": route}))
    await tl.call("list_events", {})
    now = time.monotonic()
    monkeypatch.setattr(t.time, "monotonic", lambda: now + t.DELAY_MEMORY_S + 1)
    await tl.call("list_events", {})
    assert calls == [None, "true", None, "true"]


@pytest.mark.parametrize("status,detail,start,is_error", [
    (401, "unauthorized", "OddsPush did not accept this API key", True),
    (403, "rest_not_in_plan", "This key's plan does not include the REST API", True),
    (429, "rate_limited", "This key has reached its request rate limit. Retry in 3 s.", True),
    (503, "unavailable", "OddsPush is temporarily unavailable. Retry in 3 s.", True),
    (503, "not_ready", "The delayed free feed is filling up", True),
    (404, "not_found", "No closing line for event 5", False),
    (404, "Not Found", "This tool is not available on the OddsPush API yet", True),
    (500, "boom <script>", "OddsPush API error (HTTP 500, boom script). Retry in 3 s.", True),
])
async def test_errors(status, detail, start, is_error):
    api = Api(**{"/v1/closing/5": err(status, detail, headers={"retry-after": "2.2"})})
    r = await tools(api).call("get_closing", {"event_id": 5})
    assert r["isError"] is is_error and text(r).startswith(start), text(r)
    assert len(api.seen) == 1


async def test_network_error():
    def boom(req):
        raise httpx.ConnectError("down")

    r = await tools(Api(**{"/v1/closing/5": boom})).call("get_closing", {"event_id": 5})
    assert r["isError"] is True and text(r).startswith("Could not reach the OddsPush API")


@pytest.mark.parametrize("name,args,msg", [
    ("get_event", {}, "event_id is required"),
    ("get_event", {"event_id": "x"}, "event_id must be an integer from 1 to 9007199254740991"),
    ("get_event", {"event_id": True}, "event_id must be an integer"),
    ("get_event", {"event_id": 1, "channel": "zz"}, "channel must be one of: ld, dz, pre, both"),
    ("get_event", {"event_id": 1, "token": "opk_secret"}, "unknown argument: token"),
    ("list_events", {"query": "arsenal"}, "unknown argument: query"),
    ("get_clv", {"event_id": 1, "key": "s;0;m", "designation": "home"},
     "give exactly one of price (American odds) or decimal_odds"),
    ("get_clv", {"event_id": 1, "key": "s;0;m", "designation": "home", "price": 50},
     "price is American odds"),
    ("get_clv", {"event_id": 1, "key": "bad key!", "designation": "home", "price": 150},
     "key must be a market key"),
    ("recent_drops", {"window_s": 45}, "window_s must be one of: 30, 60, 300, 600"),
    ("recent_drops", {"min_pct": 0.5}, "min_pct must be a number from 1 to 50"),
])
async def test_bad_arguments(name, args, msg):
    api = Api()
    r = await tools(api).call(name, args)
    assert r["isError"] is True and text(r).startswith("Invalid arguments: " + msg), text(r)
    assert "secret" not in text(r) and api.seen == []


async def test_line_history_one_market_trims_on_a_whole_time(monkeypatch):
    monkeypatch.setattr(t, "_BUDGET", 400)
    rows = [{"time": 100 + i // 2, "channel": "pre", "key": "s;0;m", "status": "open",
             "prices": [{"designation": "home", "price": -110 - i}]} for i in range(12)]
    api = Api(**{"/v1/events/9/history": ok({"event_id": 9, "n": 12, "more": False,
                                             "next_since": None, "history": rows})})
    out = body(await tools(api).call("get_line_history", {"event_id": 9, "key": "s;0;m",
                                                          "since": "1793900000000"}))
    assert out["more"] is True and out["n"] % 2 == 0 and out["n"] < 12
    assert out["next_since"] == rows[out["n"]]["time"]
    assert dict(api.seen[0].url.params) == {"key": "s;0;m", "since": "1793900000000",
                                            "fair": "true"}


async def test_line_history_summary_follows_pages():
    pages = {
        None: {"more": True, "next_since": 200, "history": [
            {"time": 100, "channel": "pre", "key": "s;0;m", "status": "open", "prices": [1]},
            {"time": 150, "channel": "pre", "key": "s;0;ou;2.5", "status": "open",
             "prices": [2]}]},
        "200": {"more": False, "next_since": None, "history": [
            {"time": 200, "channel": "pre", "key": "s;0;m", "status": "closed", "prices": [3]}]},
    }

    def route(req):
        return httpx.Response(200, json=pages[req.url.params.get("since")])

    api = Api(**{"/v1/events/9/history": route})
    out = body(await tools(api).call("get_line_history", {"event_id": 9, "fair": False}))
    assert out["summary"] is True and out["changes"] == 3 and out["more"] is False
    m = out["markets"][0]
    assert m["key"] == "s;0;m" and m["changes"] == 2
    assert m["opening"] == {"time": 100, "status": "open", "prices": [1]}
    assert m["latest"]["status"] == "closed"
    assert "fair" not in dict(api.seen[0].url.params)


async def test_list_closings_and_clv():
    rows = [{"event_id": i, "sport_id": 1, "start_ms": i} for i in range(5)]
    clv = {"clv_pct": 7.42, "beat_close": True}
    api = Api(**{"/v1/closing": ok({"n": 5, "more": False, "events": rows}),
                 "/v1/clv": ok(clv)})
    tl = tools(api)
    out = body(await tl.call("list_closings", {"sport_id": 1, "limit": 2,
                                               "since": "2026-11-01T00:00:00Z"}))
    assert out == {"returned": 2, "more": True, "events": rows[:2]}
    first = int(datetime(2026, 11, 1, tzinfo=timezone.utc).timestamp() * 1000)
    assert api.seen[0].url.params["since"] == str(first)
    assert body(await tl.call("get_clv", {"event_id": 1, "key": "s;0;m", "designation": "home",
                                          "decimal_odds": 1.91})) == clv
    assert dict(api.seen[1].url.params) == {"event_id": "1", "key": "s;0;m",
                                            "designation": "home", "price": "-109.89"}
    await tl.call("get_clv", {"event_id": 1, "key": "s;0;m", "designation": "home",
                              "decimal_odds": 2.5})
    assert api.seen[2].url.params["price"] == "150"


async def test_recent_drops_keeps_the_newest_first():
    pages = {None: {"more": True, "cursor": 2, "alerts": [{"i": 1}, {"i": 2}]},
             "2": {"more": False, "cursor": 3, "alerts": [{"i": 3}]}}

    def route(req):
        return httpx.Response(200, json=pages[req.url.params.get("after")])

    api = Api(**{"/v1/drops": route})
    out = body(await tools(api).call("recent_drops", {"limit": 2, "min_pct": "5"}))
    assert out == {"returned": 2, "min_pct": 5, "window_s": 60, "alerts": [{"i": 3}, {"i": 2}]}
    p = dict(api.seen[0].url.params)
    assert p["min_pct"] == "5" and p["window_s"] == "60" and "after" not in p
    assert abs(int(p["since"]) - (time.time() * 1000 - 3_600_000)) < 60_000


async def test_long_answers_are_cut_with_the_tools_note():
    rows = [{"event_id": i, "ld": 1, "league": "x" * 500} for i in range(300)]
    api = Api(**{"/v1/events": ok({"events": rows})})
    r = await tools(api).call("list_events", {"limit": 500})
    assert r["isError"] is False
    assert len(text(r)) <= t.MAX_TEXT + 100
    assert text(r).endswith("[truncated: narrow with sport_id, live_only or a smaller limit]")


def test_redact():
    assert t.redact(f"x {KEY} y opk_") == "x opk_*** y opk_***"
