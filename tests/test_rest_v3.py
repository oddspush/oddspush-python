"""REST in 0.3.0: the free plan, history, CLV, drops and status."""

import httpx
import pytest

from oddspush import (AsyncClient, Client, DelayedPlan, NotInPlan, NotReady, get_status)


def make(handler, **kw):
    return Client("opk_test", transport=httpx.MockTransport(handler), max_retry_wait=0.5, **kw)


def test_accept_delay_header():
    seen = []

    def handler(req):
        seen.append(req)
        return httpx.Response(200, json={})

    with make(handler, accept_delay=True) as api:
        api.events()
    with make(handler) as api:
        api.events()
    assert seen[0].headers["x-accept-delay"] == "true"
    assert "x-accept-delay" not in seen[1].headers


def test_delayed_plan_says_what_to_do():
    with make(lambda req: httpx.Response(403, json={"detail": "delayed_plan"})) as api:
        with pytest.raises(DelayedPlan) as info:
            api.events()
    assert info.value.status == 403 and "accept_delay=True" in str(info.value)


def test_not_in_plan_and_not_ready():
    with make(lambda req: httpx.Response(403, json={"detail": "not_in_plan"})) as api:
        with pytest.raises(NotInPlan):
            api.history(1)
    calls = []

    def busy(req):
        calls.append(1)
        return httpx.Response(503, json={"detail": "not_ready"}, headers={"retry-after": "0"})

    with make(busy, max_retries=1) as api:
        with pytest.raises(NotReady):
            api.events()
    assert len(calls) == 2


def test_history_and_iter_history():
    pages = {
        None: {"event_id": 9, "n": 2, "more": True, "next_since": 200, "history": [
            {"time": 100, "key": "s;0;m"}, {"time": 150, "key": "s;0;m"}]},
        "200": {"event_id": 9, "n": 1, "more": False, "next_since": None, "history": [
            {"time": 200, "key": "s;0;m"}]},
    }
    seen = []

    def handler(req):
        seen.append(req)
        assert req.url.path == "/v1/events/9/history"
        return httpx.Response(200, json=pages[req.url.params.get("since")])

    with make(handler) as api:
        assert api.history(9, key="s;0;m", fair=True)["more"] is True
        assert [r["time"] for r in api.iter_history(9, key="s;0;m")] == [100, 150, 200]
    assert dict(seen[0].url.params) == {"key": "s;0;m", "fair": "true"}
    assert dict(seen[2].url.params) == {"key": "s;0;m", "since": "200"}


def test_iter_history_stops_if_the_pages_do_not_move():
    page = {"more": True, "next_since": 5, "history": [{"time": 5}]}
    calls = []

    def handler(req):
        calls.append(req.url.params.get("since"))
        return httpx.Response(200, json=page)

    with make(handler) as api:
        assert len(list(api.iter_history(1, since=5))) == 1
    assert calls == ["5"]


@pytest.mark.parametrize("price,sent", [(2.1, "2.1"), (-110, "-110"), (150, "150"),
                                        ("+150", "+150"), (1.909, "1.909")])
def test_clv(price, sent):
    seen = []

    def handler(req):
        seen.append(req)
        return httpx.Response(200, json={"clv_pct": 7.4, "beat_close": True})

    with make(handler) as api:
        assert api.clv("1639824805", "s;0;m", "home", price)["beat_close"] is True
    assert seen[0].url.path == "/v1/clv"
    assert dict(seen[0].url.params) == {"event_id": "1639824805", "key": "s;0;m",
                                        "designation": "home", "price": sent}


def test_drops_and_iter_drops():
    pages = {None: {"n": 2, "more": True, "cursor": 7, "alerts": [{"i": 1}, {"i": 2}]},
             "7": {"n": 1, "more": False, "cursor": 8, "alerts": [{"i": 3}]}}
    seen = []

    def handler(req):
        seen.append(req)
        assert req.url.path == "/v1/drops"
        return httpx.Response(200, json=pages[req.url.params.get("after")])

    with make(handler) as api:
        page = api.drops(sport_id=1, min_pct=5, window_s=30)
        assert page["cursor"] == 7
        assert [a["i"] for a in api.iter_drops(sport_id=1)] == [1, 2, 3]
    assert dict(seen[0].url.params) == {"sport_id": "1", "min_pct": "5", "window_s": "30"}
    assert seen[2].url.params["after"] == "7"


def test_status_sends_no_token():
    seen = []

    def handler(req):
        seen.append(req)
        return httpx.Response(200, json={"ok": True, "latency_ms": {"p50": 24.1, "p90": 36},
                                         "our_ms": None, "measured_ms": 1})

    with make(handler, accept_delay=True) as api:
        assert api.status()["ok"] is True
    assert seen[0].url.path == "/v1/status"
    assert "x-token" not in seen[0].headers and "x-accept-delay" not in seen[0].headers
    assert get_status(transport=httpx.MockTransport(handler))["latency_ms"]["p50"] == 24.1
    assert "x-token" not in seen[1].headers


async def test_async_new_methods():
    seen = []

    async def handler(req):
        seen.append(req)
        path = req.url.path
        if path == "/v1/drops":
            return httpx.Response(200, json={"more": False, "cursor": 1, "alerts": [{"i": 1}]})
        if path.endswith("/history"):
            return httpx.Response(200, json={"more": False, "history": [{"time": 1}]})
        return httpx.Response(200, json={"ok": True})

    async with AsyncClient("opk_test", transport=httpx.MockTransport(handler),
                           accept_delay=True) as api:
        assert [a async for a in api.iter_drops()] == [{"i": 1}]
        assert [r async for r in api.iter_history(3)] == [{"time": 1}]
        assert (await api.clv(3, "s;0;m", "away", 2.5)) == {"ok": True}
        assert (await api.status()) == {"ok": True}
    assert seen[0].headers["x-accept-delay"] == "true"
    assert "x-token" not in seen[-1].headers
