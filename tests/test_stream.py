import asyncio

import pytest

from oddspush import (NotInPlan, OddsPushError, StreamClosed, Stream, TokenReplaced,
                      Unauthorized)

MARKET = {"key": "s;0;m", "version": 5, "status": "open", "period": 0,
          "prices": [{"designation": "home", "price": -110}]}


def odds(event_id, channel="ld", op="upd", markets=(MARKET,)):
    return {"type": "odds", "data": {"type": "live", "sport_id": 1, "op": op,
                                     "topic": f"matchups/reg/sp/29/live/{channel}",
                                     "rec": {"id": event_id, "markets": list(markets)},
                                     "ts": 1}}


async def take(stream, kind, timeout=5.0):
    async def go():
        async for msg in stream:
            if msg["type"] == kind:
                return msg
    return await asyncio.wait_for(go(), timeout)


def fast(stream):
    """No real waiting between reconnects in tests."""
    import oddspush.stream as s
    s.random.uniform = lambda a, b: 0.0 if b <= 1 else 0.01
    return stream


async def test_login_subscribe_snapshot_and_book(fake_server):
    async def script(c):
        await c.login()
        sub = await c.recv()
        assert sub == {"type": "subscribe", "sports": [1], "channels": ["ld", "both"],
                       "fair": True, "snapshot": True}
        await c.send({"type": "subscription", "filter": {"all": False, "sports": [1],
                      "events": [], "streams": [], "channels": ["both", "ld"]},
                      "fair": True, "closing": False})
        await c.send({"type": "snapshot", "part": 0, "final": True,
                      "events": [{"id": 1, "markets": [MARKET]}]})
        await c.send(odds(2))
        await c.send(odds(1, channel="pre", op="del"))
        await c.send({"type": "keepalive", "time": 1})
        await c.hold()

    async with fake_server(script) as srv:
        async with Stream("opk_test", url=srv.url, sports=[1], channels=["ld", "both"],
                          fair=True) as stream:
            welcome = await take(stream, "welcome")
            assert stream.account["plan"] == "trial" and welcome is stream.welcome
            await take(stream, "keepalive")
            assert stream.book.event_ids() == [2]
            assert stream.subscription["fair"] is True
            assert stream.connected


async def test_reconnect_restores_filter_and_asks_snapshot(fake_server):
    async def first(c):
        await c.login()
        await c.recv()
        await c.send({"type": "subscription", "filter": {"all": False, "sports": [1, 3],
                      "events": [], "streams": [], "channels": ["ld"]},
                      "fair": False, "closing": True})
        await c.close(1011, "boom")

    async def second(c):
        await c.login()
        await c.recv()
        await c.hold()

    async with fake_server(first, second) as srv:
        async with fast(Stream("opk_test", url=srv.url, sports=[1], closing=True,
                               snapshot=False)) as stream:
            d = await take(stream, "disconnected")
            assert d["retry_in_s"] >= 0
            await take(stream, "welcome")
            for _ in range(50):
                if len(srv.conns) == 2 and srv.conns[1].received[1:]:
                    break
                await asyncio.sleep(0.02)
            assert srv.conns[0].received[1] == {"type": "subscribe", "sports": [1],
                                                "closing": True, "snapshot": False}
            assert srv.conns[1].received[1] == {"type": "subscribe", "sports": [1, 3],
                                                "channels": ["ld"], "closing": True,
                                                "snapshot": True}
            assert stream.reconnects == 1


@pytest.mark.parametrize("code,exc", [("unauthorized", Unauthorized),
                                      ("stream_not_in_plan", NotInPlan)])
async def test_fatal_login_error_stops(fake_server, code, exc):
    async def script(c):
        await c.recv()
        await c.send({"type": "error", "code": code})
        await c.close({"unauthorized": 4101, "stream_not_in_plan": 4109}[code], code)

    async with fake_server(script) as srv:
        stream = fast(Stream("opk_test", url=srv.url, sports=[1]))
        with pytest.raises(exc):
            await asyncio.wait_for(take(stream, "never"), 5)
        await stream.close()
        assert len(srv.conns) == 1


async def test_replaced_mid_session_is_fatal(fake_server):
    async def script(c):
        await c.login()
        await c.recv()
        await c.send({"type": "error", "code": "replaced"})
        await c.close(4107, "replaced")

    async with fake_server(script) as srv:
        stream = fast(Stream("opk_test", url=srv.url, sports=[1]))
        with pytest.raises(TokenReplaced) as info:
            await asyncio.wait_for(take(stream, "never"), 5)
        assert info.value.close_code == 4107
        await asyncio.sleep(0.1)
        assert len(srv.conns) == 1
        await stream.close()


async def test_retryable_close_reconnects(fake_server):
    async def first(c):
        await c.login()
        await c.recv()
        await c.send({"type": "error", "code": "too_slow"})
        await c.close(4108, "too_slow")

    async def second(c):
        await c.login()
        await c.hold()

    async with fake_server(first, second) as srv:
        async with fast(Stream("opk_test", url=srv.url, sports=[1])) as stream:
            d = await take(stream, "disconnected")
            assert d["reason"] == "too_slow"
            await take(stream, "welcome")


async def test_reconnect_rate_limited_waits(fake_server):
    async def first(c):
        await c.recv()
        await c.send({"type": "error", "code": "reconnect_rate_limited", "retry_after_s": 0.2})
        await c.close(4106, "reconnect_rate_limited")

    async def second(c):
        await c.login()
        await c.hold()

    async with fake_server(first, second) as srv:
        async with Stream("opk_test", url=srv.url, sports=[1]) as stream:
            d = await take(stream, "disconnected")
            assert d["retry_in_s"] == 0.2
            await take(stream, "welcome")


async def test_missed_and_feed_up_ask_for_snapshot(fake_server):
    got = asyncio.Event()

    async def script(c):
        await c.login()
        await c.recv()
        await c.send({"type": "missed", "count": 3})
        await c.recv_type("snapshot")
        await c.send({"type": "feed", "status": "up"})
        await c.recv_type("snapshot")
        got.set()
        await c.hold()

    async with fake_server(script) as srv:
        async with Stream("opk_test", url=srv.url, sports=[1], resync_jitter=0) as stream:
            await stream.start()
            await asyncio.wait_for(_drain_until(stream, got), 5)


async def _drain_until(stream, event):
    """Read the stream in the background until the server saw what it waited for."""
    async def drain():
        async for _ in stream:
            pass
    reader = asyncio.ensure_future(drain())
    try:
        await event.wait()
    finally:
        reader.cancel()


async def test_snapshot_rate_limited_is_retried(fake_server):
    got = asyncio.Event()

    async def script(c):
        await c.login()
        await c.recv()
        await c.send({"type": "missed", "count": 1})
        await c.recv_type("snapshot")
        await c.send({"type": "error", "code": "snapshot_rate_limited", "retry_after_s": 0.1})
        await c.recv_type("snapshot")
        got.set()
        await c.hold()

    async with fake_server(script) as srv:
        async with Stream("opk_test", url=srv.url, sports=[1]) as stream:
            await asyncio.wait_for(_drain_until(stream, got), 5)


async def test_no_reconnect_raises_stream_closed(fake_server):
    async def script(c):
        await c.login()
        await c.close(1011, "x")

    async with fake_server(script) as srv:
        stream = Stream("opk_test", url=srv.url, sports=[1], reconnect=False)
        with pytest.raises(StreamClosed):
            await asyncio.wait_for(take(stream, "never"), 5)
        await stream.close()


async def test_pending_messages_go_out_after_login(fake_server):
    async def script(c):
        await c.login()
        await c.hold()

    async with fake_server(script) as srv:
        stream = Stream("opk_test", url=srv.url)
        await stream.subscribe(sports=[2], fair=True)       # before connecting
        await stream.start()
        await take(stream, "welcome")
        for _ in range(50):
            if len(srv.conns[0].received) >= 2:
                break
            await asyncio.sleep(0.02)
        # No filter in the constructor: only the pending subscribe goes out.
        assert srv.conns[0].received[1:] == [{"type": "subscribe", "sports": [2],
                                              "fair": True, "snapshot": True}]
        await stream.close()


async def test_close_ends_iteration(fake_server):
    async def script(c):
        await c.login()
        await c.hold()

    async with fake_server(script) as srv:
        stream = Stream("opk_test", url=srv.url, sports=[1])
        await take(stream, "welcome")
        reader = asyncio.ensure_future(take(stream, "never"))
        await asyncio.sleep(0.05)
        await stream.close()
        assert await asyncio.wait_for(reader, 2) is None


async def test_maintenance_reconnects_soon(fake_server, monkeypatch):
    import oddspush.stream as s
    monkeypatch.setattr(s, "_MAINTENANCE_WAIT_S", 0.05)

    async def first(c):
        await c.login()
        await c.send({"type": "feed", "status": "maintenance"})
        await c.hold()

    async def second(c):
        await c.login()
        await c.hold()

    async with fake_server(first, second) as srv:
        async with Stream("opk_test", url=srv.url, sports=[1]) as stream:
            d = await take(stream, "disconnected")
            assert d["reason"] == "maintenance"
            await take(stream, "welcome")


async def test_server_full_at_login_retries(fake_server):
    async def first(c):
        await c.recv()
        await c.send({"type": "error", "code": "server_full"})
        await c.close(4102, "server_full")

    async def second(c):
        await c.login()
        await c.hold()

    async with fake_server(first, second) as srv:
        async with fast(Stream("opk_test", url=srv.url, sports=[1])) as stream:
            d = await take(stream, "disconnected")
            assert d["reason"] == "server_full"
            await take(stream, "welcome")


def test_token_required():
    with pytest.raises(ValueError):
        Stream("")


def test_default_book_channels():
    assert Stream("t").book.channels == {"ld", "both"}
    assert Stream("t", channels=["pre", "dz"]).book.channels == {"pre"}
    assert Stream("t", channels=["dz"]).book.channels == {"ld", "both"}
    assert Stream("t", book=False).book is None
    assert issubclass(TokenReplaced, OddsPushError)
