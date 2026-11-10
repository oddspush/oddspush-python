"""A fake OddsPush stream server for the tests: each connection runs a script."""

from __future__ import annotations

import asyncio
import json
from typing import Any, Awaitable, Callable, List

import pytest
import websockets

WELCOME = {"type": "welcome", "server_time": 1, "feed": {"alive": True, "ready": True},
           "limits": {"queue_size": 1000, "keepalive_s": 15, "max_events": 1000},
           "account": {"id": "acme", "plan": "trial", "expires_at": 2,
                       "limits": {"stream": True, "rest": True}}}


class Conn:
    def __init__(self, ws: Any) -> None:
        self.ws = ws
        self.received: List[dict] = []

    async def recv(self, timeout: float = 5.0) -> dict:
        msg = json.loads(await asyncio.wait_for(self.ws.recv(), timeout))
        self.received.append(msg)
        return msg

    async def recv_type(self, kind: str, timeout: float = 5.0) -> dict:
        while True:
            msg = await self.recv(timeout)
            if msg.get("type") == kind:
                return msg

    async def send(self, obj: dict) -> None:
        await self.ws.send(json.dumps(obj).encode())     # binary, like the real server

    async def login(self) -> dict:
        msg = await self.recv()
        assert msg == {"type": "login", "token": "opk_test"}
        await self.send(WELCOME)
        return msg

    async def close(self, code: int = 1000, reason: str = "") -> None:
        await self.ws.close(code, reason)

    async def hold(self) -> None:
        """Keep the connection open until the client goes away."""
        try:
            while True:
                await self.recv(timeout=60)
        except Exception:
            pass


Script = Callable[[Conn], Awaitable[None]]


class FakeServer:
    def __init__(self, *scripts: Script) -> None:
        self.scripts = list(scripts)
        self.conns: List[Conn] = []
        self.url = ""
        self._server: Any = None

    async def _handler(self, ws: Any, *_: Any) -> None:
        conn = Conn(ws)
        i = len(self.conns)
        self.conns.append(conn)
        script = self.scripts[min(i, len(self.scripts) - 1)]
        try:
            await script(conn)
        except websockets.exceptions.ConnectionClosed:
            pass

    async def __aenter__(self) -> "FakeServer":
        self._server = await websockets.serve(self._handler, "127.0.0.1", 0,
                                              compression=None)
        port = list(self._server.sockets)[0].getsockname()[1]
        self.url = f"ws://127.0.0.1:{port}"
        return self

    async def __aexit__(self, *exc: Any) -> None:
        self._server.close()
        await self._server.wait_closed()


@pytest.fixture
def fake_server():
    return FakeServer
