"""Regression tests for the review of 0.1.0 (snapshots owed, dz, pending, shutdown)."""

import asyncio
import subprocess
import sys
import textwrap

import pytest

import oddspush.stream as s
from oddspush import Stream


@pytest.fixture(autouse=True)
def quick(monkeypatch):
    monkeypatch.setattr(s, "_RETRY_MARGIN_S", 0.0)
    monkeypatch.setattr(s.random, "uniform", lambda a, b: 0.0 if b <= 1 else 0.01)


async def drain_until(stream, event, timeout=5.0):
    async def drain():
        async for _ in stream:
            pass
    reader = asyncio.ensure_future(drain())
    try:
        await asyncio.wait_for(event.wait(), timeout)
    finally:
        reader.cancel()


async def test_refused_resubscribe_snapshot_is_asked_again(fake_server):
    got = asyncio.Event()

    async def first(c):
        await c.login()
        await c.recv()
        await c.close(1011, "x")

    async def second(c):
        await c.login()
        sub = await c.recv()
        assert sub["type"] == "subscribe" and sub["snapshot"] is True
        await c.send({"type": "error", "code": "snapshot_rate_limited", "retry_after_s": 0.0})
        await c.recv_type("snapshot")
        got.set()
        await c.hold()

    async with fake_server(first, second) as srv:
        async with Stream("opk_test", url=srv.url, sports=[1]) as stream:
            await drain_until(stream, got)


async def test_snapshot_served_clears_the_debt(fake_server):
    done = asyncio.Event()

    async def script(c):
        await c.login()
        await c.recv()                                   # subscribe, snapshot: true
        await c.send({"type": "snapshot", "part": 0, "final": True, "events": []})
        await c.send({"type": "error", "code": "snapshot_rate_limited", "retry_after_s": 0})
        await asyncio.sleep(0.3)
        done.set()
        await c.hold()

    async with fake_server(script) as srv:
        async with Stream("opk_test", url=srv.url, sports=[1]) as stream:
            await drain_until(stream, done)
            # The refusal was not ours (nothing owed): no retry.
            assert [m["type"] for m in srv.conns[0].received] == ["login", "subscribe"]


async def test_dz_never_gets_a_bare_snapshot(fake_server):
    got = asyncio.Event()

    async def script(c):
        await c.login()
        sub = await c.recv()
        assert sub == {"type": "subscribe", "sports": [1], "channels": ["ld", "both", "dz"],
                       "snapshot": False}
        snap = await c.recv()
        assert snap == {"type": "snapshot", "sports": [1], "channels": ["both", "ld"]}
        await c.send({"type": "subscription", "filter": {"all": False, "sports": [1],
                      "events": [], "streams": [], "channels": ["both", "dz", "ld"]},
                      "fair": False, "closing": False})
        await c.send({"type": "missed", "count": 1})
        again = await c.recv_type("snapshot")
        assert again["channels"] == ["both", "ld"]
        got.set()
        await c.hold()

    async with fake_server(script) as srv:
        async with Stream("opk_test", url=srv.url, sports=[1],
                          channels=["ld", "both", "dz"]) as stream:
            await drain_until(stream, got)
            assert stream.book.channels == {"ld", "both"}


async def test_subscribe_fair_only_asks_no_snapshot(fake_server):
    async def script(c):
        await c.login()
        await c.hold()

    async with fake_server(script) as srv:
        async with Stream("opk_test", url=srv.url) as stream:
            await asyncio.wait_for(_until_type(stream, "welcome"), 5)
            await stream.subscribe(fair=True)
            await stream.subscribe(sports=[3])
            for _ in range(50):
                if len(srv.conns[0].received) >= 3:
                    break
                await asyncio.sleep(0.02)
            assert srv.conns[0].received[1] == {"type": "subscribe", "fair": True,
                                                "snapshot": False}
            assert srv.conns[0].received[2] == {"type": "subscribe", "sports": [3],
                                                "snapshot": True}


async def _until_type(stream, kind):
    async for msg in stream:
        if msg["type"] == kind:
            return msg


async def test_unconfirmed_subscribe_is_replayed_after_reconnect(fake_server):
    got = asyncio.Event()

    async def first(c):
        await c.login()
        await c.recv()                                   # constructor subscribe
        await c.send({"type": "subscription", "filter": {"all": False, "sports": [1],
                      "events": [], "streams": [], "channels": []},
                      "fair": False, "closing": False})
        await c.recv()                                   # subscribe(sports=[2]), no echo
        await c.close(1011, "x")

    async def second(c):
        await c.login()
        restore = await c.recv()
        assert restore == {"type": "subscribe", "sports": [1], "snapshot": True}
        replay = await c.recv()
        assert replay == {"type": "subscribe", "sports": [2], "fair": True, "snapshot": True}
        got.set()
        await c.hold()

    async with fake_server(first, second) as srv:
        async with Stream("opk_test", url=srv.url, sports=[1]) as stream:
            await asyncio.wait_for(_until_type(stream, "welcome"), 5)
            for _ in range(50):
                if srv.conns and srv.conns[0].received[1:]:
                    break
                await asyncio.sleep(0.02)
            await asyncio.sleep(0.05)
            await stream.subscribe(sports=[2], fair=True)
            await drain_until(stream, got)


async def test_non_json_login_reply_reconnects(fake_server):
    async def first(c):
        await c.recv()
        await c.ws.send("<html>bad gateway</html>")
        await c.hold()

    async def second(c):
        await c.login()
        await c.hold()

    async with fake_server(first, second) as srv:
        async with Stream("opk_test", url=srv.url, sports=[1]) as stream:
            d = await asyncio.wait_for(_until_type(stream, "disconnected"), 5)
            assert d["reason"] == "unexpected"
            await asyncio.wait_for(_until_type(stream, "welcome"), 5)


async def test_ping_and_status_dropped_while_disconnected():
    stream = Stream("opk_test", url="ws://127.0.0.1:9")
    await stream.ping(1)
    await stream.status()
    await stream.request_snapshot()          # the reconnect asks for one anyway
    await stream.subscribe(sports=[1])
    assert stream._pending == [{"type": "subscribe", "sports": [1], "snapshot": True}]
    for i in range(150):
        await stream.unsubscribe(events=[i])
    assert len(stream._pending) == s._PENDING_MAX


def test_asyncio_run_returns_without_close(fake_server, tmp_path):
    """A stream left open with a full queue must not hang interpreter shutdown."""
    code = textwrap.dedent('''
        import asyncio, json, sys
        import websockets
        from oddspush import Stream

        async def handler(ws, *_):
            await ws.recv()
            await ws.send(json.dumps({"type": "welcome", "account": {}}))
            await ws.recv()
            for i in range(5000):
                await ws.send(json.dumps({"type": "keepalive", "time": i}))
            await asyncio.sleep(30)

        async def main():
            server = await websockets.serve(handler, "127.0.0.1", 0)
            port = list(server.sockets)[0].getsockname()[1]
            stream = Stream("opk_test", url=f"ws://127.0.0.1:{port}", sports=[1], queue_size=10)
            async for msg in stream:
                break
            await asyncio.sleep(0.5)          # the reader fills the queue
            server.close()

        asyncio.run(main())
        print("returned")
    ''')
    script = tmp_path / "hang.py"
    script.write_text(code)
    out = subprocess.run([sys.executable, str(script)], capture_output=True, text=True,
                         timeout=60)
    assert "returned" in out.stdout, out.stderr
