import httpx
import pytest

from oddspush import (AsyncClient, BadRequest, Client, NotFound, NotInPlan, RateLimited,
                      Unauthorized)


def make(handler, **kw):
    return Client("opk_test", transport=httpx.MockTransport(handler), max_retry_wait=0.5, **kw)


def test_headers_params_and_paths():
    seen = []

    def handler(req):
        seen.append(req)
        return httpx.Response(200, json={"ok": True})

    with make(handler) as api:
        api.event(123, channel="pre", fair=True)
        api.event("456")
        api.events(sport_id=1)
    r = seen[0]
    assert r.headers["x-token"] == "opk_test"
    assert r.headers["user-agent"].startswith("oddspush-python/")
    assert r.url.path == "/v1/events/123"
    assert dict(r.url.params) == {"channel": "pre", "fair": "true"}
    assert seen[1].url.path == "/v1/events/456" and not seen[1].url.params
    assert str(seen[2].url).endswith("/v1/events?sport_id=1")
    assert str(r.url).startswith("https://feed.oddspush.com/")


@pytest.mark.parametrize("status,body,exc", [
    (401, {"detail": "unauthorized"}, Unauthorized),
    (403, {"detail": "rest_not_in_plan"}, NotInPlan),
    (404, {"detail": "not_found"}, NotFound),
    (404, {"detail": "Not Found"}, NotFound),
    (400, {"detail": "bad_request"}, BadRequest),
])
def test_errors(status, body, exc):
    with make(lambda req: httpx.Response(status, json=body)) as api:
        with pytest.raises(exc) as info:
            api.closing(1)
    assert info.value.status == status


def test_429_is_retried_then_succeeds():
    calls = []

    def handler(req):
        calls.append(1)
        if len(calls) < 3:
            return httpx.Response(429, json={"detail": "rate_limited"},
                                  headers={"retry-after": "0"})
        return httpx.Response(200, json={"n": 0, "events": []})

    with make(handler) as api:
        assert api.events() == {"n": 0, "events": []}
    assert len(calls) == 3


def test_429_gives_up():
    with make(lambda req: httpx.Response(429, json={"detail": "rate_limited"},
                                         headers={"retry-after": "0"}), max_retries=1) as api:
        with pytest.raises(RateLimited) as info:
            api.events()
    assert info.value.retry_after == 0


def test_429_waits_too_long_raises_at_once():
    calls = []

    def handler(req):
        calls.append(1)
        return httpx.Response(429, json={"detail": "rate_limited"}, headers={"retry-after": "30"})

    with make(handler) as api:
        with pytest.raises(RateLimited):
            api.events()
    assert len(calls) == 1


def test_bad_event_id():
    with make(lambda req: httpx.Response(200, json={})) as api:
        with pytest.raises(BadRequest):
            api.event("abc")
        with pytest.raises(BadRequest):
            api.closing(-1)


def test_health():
    with make(lambda req: httpx.Response(200, json={"ok": True})) as api:
        assert api.health() is True
    with make(lambda req: httpx.Response(503, json={"detail": "unavailable"}),
              max_retries=0) as api:
        assert api.health() is False


def test_iter_closings_pages_and_dedupes():
    pages = {
        None: {"n": 3, "more": True, "events": [
            {"event_id": 1, "sport_id": 1, "start_ms": 10},
            {"event_id": 2, "sport_id": 1, "start_ms": 20},
            {"event_id": 3, "sport_id": 1, "start_ms": 20}]},
        "20": {"n": 3, "more": False, "events": [
            {"event_id": 2, "sport_id": 1, "start_ms": 20},
            {"event_id": 3, "sport_id": 1, "start_ms": 20},
            {"event_id": 4, "sport_id": 1, "start_ms": 30}]},
    }

    def handler(req):
        return httpx.Response(200, json=pages[req.url.params.get("since")])

    with make(handler) as api:
        assert [r["event_id"] for r in api.iter_closings()] == [1, 2, 3, 4]


def test_iter_closings_stops_without_progress():
    page = {"n": 1, "more": True, "events": [{"event_id": 1, "sport_id": 1, "start_ms": 5}]}
    calls = []

    def handler(req):
        calls.append(1)
        return httpx.Response(200, json=page)

    with make(handler) as api:
        assert [r["event_id"] for r in api.iter_closings()] == [1]
    assert len(calls) == 3


def test_token_required():
    with pytest.raises(ValueError):
        Client("")


async def test_async_client():
    async def handler(req):
        if req.url.path == "/v1/closing/9":
            return httpx.Response(404, json={"detail": "not_found"})
        return httpx.Response(200, json={"1": {"n": 1, "ids": [5]}})

    async with AsyncClient("opk_test", transport=httpx.MockTransport(handler)) as api:
        assert await api.membership(sport_id=1) == {"1": {"n": 1, "ids": [5]}}
        with pytest.raises(NotFound):
            await api.closing(9)


def test_iter_closings_moves_past_a_page_of_one_instant():
    full = {"n": 2, "more": True, "events": [{"event_id": 1, "sport_id": 1, "start_ms": 50},
                                             {"event_id": 2, "sport_id": 1, "start_ms": 50}]}
    rest = {"n": 1, "more": False, "events": [{"event_id": 3, "sport_id": 1, "start_ms": 60}]}
    asked = []

    def handler(req):
        since = req.url.params.get("since")
        asked.append(since)
        return httpx.Response(200, json=rest if since == "51" else full)

    with make(handler) as api:
        assert [r["event_id"] for r in api.iter_closings(since=50)] == [1, 2, 3]
    assert asked == ["50", "51"]
