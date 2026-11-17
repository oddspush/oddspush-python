"""The free plan (accept_delay, delay_s, 4110-4112, not_in_plan, not_ready) and drop alerts
on the stream, against the fake server."""

import asyncio
import copy
import logging

import pytest

from oddspush import DelayedPlan, Stream
from oddspush.errors import PlanChanged, TryAgain, error_for

from conftest import WELCOME

FREE = copy.deepcopy(WELCOME)
FREE["account"]["plan"] = "free"
FREE["account"]["limits"]["delay_s"] = 30

DROP = {"type": "drop", "data": {"event_id": 5, "sport_id": 1, "channel": "ld",
                                 "designation": "home", "change_pct": 11.3, "window_s": 60,
                                 "market": {"key": "s;0;m"}, "time": 1}}


async def take(stream, kind, timeout=5.0):
    async def go():
        async for msg in stream:
            if msg["type"] == kind:
                return msg
    return await asyncio.wait_for(go(), timeout)


def fast(stream):
    import oddspush.stream as s
    s.random.uniform = lambda a, b: 0.0 if b <= 1 else 0.01
    return stream


async def wait_for(cond, timeout=5.0):
    for _ in range(int(timeout / 0.02)):
        if cond():
            return
        await asyncio.sleep(0.02)
    raise AssertionError("condition not met")


# ------------------------------------------------------------------ free plan

async def test_accept_delay_login_and_delay_s(fake_server):
    async def script(c):
        msg = await c.recv()
        assert msg == {"type": "login", "token": "opk_test", "accept_delay": True}
        await c.send(FREE)
        await c.hold()

    async with fake_server(script) as srv:
        async with Stream("opk_test", url=srv.url, accept_delay=True) as stream:
            assert stream.delay_s == 0.0
            await take(stream, "welcome")
            assert stream.delay_s == 30.0 and stream.account["plan"] == "free"


async def test_no_accept_delay_by_default_and_real_time(fake_server):
    async def script(c):
        await c.login()          # asserts the login has no accept_delay
        await c.hold()

    async with fake_server(script) as srv:
        async with Stream("opk_test", url=srv.url) as stream:
            await take(stream, "welcome")
            assert stream.delay_s == 0.0


async def test_delayed_plan_is_fatal_and_says_what_to_do(fake_server):
    async def script(c):
        await c.recv()
        await c.send({"type": "error", "code": "delayed_plan", "delay_s": 30})
        await c.close(4110, "delayed_plan")

    async with fake_server(script) as srv:
        stream = fast(Stream("opk_test", url=srv.url, sports=[1]))
        with pytest.raises(DelayedPlan) as info:
            await take(stream, "never")
        await stream.close()
    assert "accept_delay=True" in str(info.value)
    assert info.value.details == {"delay_s": 30}
    assert len(srv.conns) == 1


async def test_plan_changed_reconnects_at_once(fake_server):
    async def first(c):
        await c.recv()
        await c.send(FREE)
        await c.recv()
        await c.send({"type": "error", "code": "plan_changed"})
        await c.close(4111, "plan_changed")

    async def second(c):
        await c.recv()
        await c.send(WELCOME)
        await c.hold()

    async with fake_server(first, second) as srv:
        async with Stream("opk_test", url=srv.url, sports=[1], accept_delay=True,
                          max_backoff=30) as stream:
            await take(stream, "welcome")
            assert stream.delay_s == 30
            d = await take(stream, "disconnected")
            assert d == {"type": "disconnected", "reason": "plan_changed", "retry_in_s": 0.0}
            await take(stream, "welcome")
            assert stream.delay_s == 0.0 and stream.reconnects == 1


async def test_try_again_waits_retry_after(fake_server):
    async def first(c):
        await c.recv()
        await c.send({"type": "error", "code": "try_again", "retry_after_s": 0.2})
        await c.close(4112, "try_again")

    async def second(c):
        await c.recv()
        await c.send(FREE)
        await c.hold()

    async with fake_server(first, second) as srv:
        async with Stream("opk_test", url=srv.url, accept_delay=True) as stream:
            d = await take(stream, "disconnected")
            assert d["reason"] == "try_again" and d["retry_in_s"] == 0.2
            await take(stream, "welcome")


async def test_try_again_mid_session(fake_server):
    async def first(c):
        await c.recv()
        await c.send(FREE)
        await c.send({"type": "error", "code": "try_again", "retry_after_s": 0.1})
        await c.close(4112, "")

    async def second(c):
        await c.recv()
        await c.send(FREE)
        await c.hold()

    async with fake_server(first, second) as srv:
        async with Stream("opk_test", url=srv.url, accept_delay=True) as stream:
            d = await take(stream, "disconnected")
            assert d["reason"] == "try_again" and d["retry_in_s"] == 0.1


async def test_not_in_plan_is_logged_and_the_extras_are_not_restored(fake_server, caplog):
    async def first(c):
        await c.recv()
        await c.send(FREE)
        sub = await c.recv()
        assert sub["fair"] is True and sub["drop_alerts"] is True
        await c.send({"type": "error", "code": "not_in_plan",
                      "features": ["fair", "drop_alerts"]})
        await c.send({"type": "subscription", "filter": {"all": False, "sports": [1],
                      "events": [], "streams": [], "channels": []},
                      "fair": False, "closing": False, "specials": False,
                      "drop_alerts": False})
        await c.close(1011, "")

    async def second(c):
        await c.recv()
        await c.send(FREE)
        await c.hold()

    caplog.set_level(logging.WARNING, logger="oddspush")
    async with fake_server(first, second) as srv:
        async with fast(Stream("opk_test", url=srv.url, sports=[1], fair=True,
                               drop_alerts=True, accept_delay=True)) as stream:
            err = await take(stream, "error")
            assert err["features"] == ["fair", "drop_alerts"]
            await take(stream, "disconnected")
            await take(stream, "welcome")
            await wait_for(lambda: len(srv.conns[1].received) >= 2)
    assert srv.conns[1].received[1] == {"type": "subscribe", "sports": [1], "snapshot": True}
    assert "fair, drop_alerts" in caplog.text


async def test_not_ready_asks_the_snapshot_again(fake_server, monkeypatch):
    import oddspush.stream as s
    monkeypatch.setattr(s, "_NOT_READY_WAIT_S", 0.0)
    monkeypatch.setattr(s, "_RETRY_MARGIN_S", 0.05)

    async def script(c):
        await c.recv()
        await c.send(FREE)
        await c.recv()                                    # subscribe with snapshot
        await c.send({"type": "error", "code": "not_ready"})
        again = await c.recv()
        assert again == {"type": "snapshot"}
        await c.send({"type": "snapshot", "part": 0, "final": True, "events": []})
        await c.hold()

    async with fake_server(script) as srv:
        async with Stream("opk_test", url=srv.url, sports=[1], accept_delay=True) as stream:
            await take(stream, "snapshot")


def test_error_classes():
    assert isinstance(error_for("try_again", retry_after=2), TryAgain)
    assert isinstance(error_for("plan_changed"), PlanChanged)
    e = error_for("delayed_plan", "delayed_plan (HTTP 403)")
    assert str(e).startswith("delayed_plan (HTTP 403): ") and "accept_delay=True" in str(e)


# ------------------------------------------------------------------ drop alerts

def test_drop_alerts_are_checked():
    for bad in ({"window_s": 45}, {"min_pct": 0}, {"min_pct": 51}, {"min_pct": True},
                {"window": 60}, "yes", 3):
        with pytest.raises(ValueError):
            Stream("opk_test", drop_alerts=bad)
    Stream("opk_test", drop_alerts={"min_pct": 2.5, "window_s": 600})


async def test_drop_alerts_subscribe_callback_iterator_and_restore(fake_server):
    got = []

    async def first(c):
        await c.login()
        sub = await c.recv()
        assert sub == {"type": "subscribe", "sports": [1],
                       "drop_alerts": {"min_pct": 5, "window_s": 30}, "snapshot": True}
        await c.send({"type": "subscription", "filter": {"all": False, "sports": [1],
                      "events": [], "streams": [], "channels": []},
                      "fair": False, "closing": False, "specials": False,
                      "drop_alerts": {"window_s": 30, "min_pct": 5.0}})
        await c.send(DROP)
        await c.send({"type": "keepalive", "time": 1})
        await c.close(1011, "boom")

    async def second(c):
        await c.login()
        await c.recv()
        await c.send(DROP)
        await c.hold()

    async with fake_server(first, second) as srv:
        stream = fast(Stream("opk_test", url=srv.url, sports=[1],
                             drop_alerts={"min_pct": 5, "window_s": 30},
                             on_drop=lambda d: got.append(d["event_id"])))
        async with stream:
            drops = stream.drops()
            first_drop = await asyncio.wait_for(drops.__anext__(), 5)
            assert first_drop == DROP["data"]
            second_drop = await asyncio.wait_for(drops.__anext__(), 5)
            assert second_drop["event_id"] == 5
            await drops.aclose()
        assert srv.conns[1].received[1] == {"type": "subscribe", "sports": [1],
                                            "drop_alerts": {"min_pct": 5.0, "window_s": 30},
                                            "snapshot": True}
    assert got == [5, 5]


async def test_on_drop_errors_do_not_break_the_stream(fake_server, caplog):
    async def script(c):
        await c.login()
        await c.recv()
        await c.send(DROP)
        await c.send({"type": "keepalive", "time": 1})
        await c.hold()

    def boom(data):
        raise RuntimeError("bad callback")

    async with fake_server(script) as srv:
        async with Stream("opk_test", url=srv.url, sports=[1], drop_alerts=True,
                          on_drop=boom) as stream:
            await take(stream, "keepalive")
    assert "on_drop raised" in caplog.text


async def test_async_on_drop(fake_server):
    seen = asyncio.Event()

    async def on_drop(data):
        seen.set()

    async def script(c):
        await c.login()
        await c.recv()
        await c.send(DROP)
        await c.hold()

    async with fake_server(script) as srv:
        async with Stream("opk_test", url=srv.url, sports=[1], drop_alerts=True,
                          on_drop=on_drop) as stream:
            await take(stream, "drop")
            await asyncio.wait_for(seen.wait(), 5)


async def test_subscribe_and_unsubscribe_drop_alerts(fake_server):
    async def script(c):
        await c.login()
        await c.hold()

    async with fake_server(script) as srv:
        async with Stream("opk_test", url=srv.url) as stream:
            await take(stream, "welcome")
            await stream.subscribe(drop_alerts=True)
            await stream.subscribe(drop_alerts={"window_s": 300})
            await stream.unsubscribe(drop_alerts=True)
            with pytest.raises(ValueError):
                await stream.subscribe(drop_alerts={"window_s": 7})
            await wait_for(lambda: len(srv.conns[0].received) >= 4)
    assert srv.conns[0].received[1:] == [
        {"type": "subscribe", "drop_alerts": True, "snapshot": False},
        {"type": "subscribe", "drop_alerts": {"window_s": 300}, "snapshot": False},
        {"type": "unsubscribe", "drop_alerts": True},
    ]
