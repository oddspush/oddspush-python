"""``oddspush-mcp`` with the official MCP client: in process, and as a real stdio
subprocess against a local HTTP server. Skipped without ``pip install "oddspush[mcp]"``."""

import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest

mcp = pytest.importorskip("mcp")

from mcp import Client  # noqa: E402
from mcp.client.stdio import StdioServerParameters  # noqa: E402
from mcp.shared.exceptions import MCPError  # noqa: E402

from oddspush import __version__  # noqa: E402
from oddspush._mcp_tools import INSTRUCTIONS, MSG_NO_KEY, TOOLS, OddsPushTools  # noqa: E402
from oddspush.mcp_server import _key_from_env, build_server, main  # noqa: E402

KEY = "opk_" + "B" * 43


def dump(tool):
    return tool.model_dump(mode="json", by_alias=True, exclude_none=True)


@pytest.mark.parametrize("mode", ["auto", "legacy"])
async def test_in_process(mode):
    def handler(req):
        assert req.headers["x-token"] == KEY
        return httpx.Response(200, json={"events": [{"event_id": 4, "ld": 2}]})

    tools = OddsPushTools(KEY, transport=httpx.MockTransport(handler))
    async with Client(build_server(tools), mode=mode) as c:
        assert c.server_info.name == "oddspush" and c.server_info.version == __version__
        assert c.instructions == INSTRUCTIONS
        listed = await c.list_tools()
        assert [dump(x) for x in listed.tools] == TOOLS
        r = await c.call_tool("list_events", {"sport_id": 1})
        assert r.is_error is False
        assert json.loads(r.content[-1].text)["events"][0]["event_id"] == 4
        bad = await c.call_tool("get_event", {"event_id": "nope"})
        assert bad.is_error is True and bad.content[0].text.startswith("Invalid arguments")
        with pytest.raises(MCPError) as info:
            await c.call_tool("no_such_tool", {})
        assert info.value.error.code == -32602
    await tools.aclose()


def test_key_from_env():
    assert _key_from_env({"ODDSPUSH_API_KEY": f" {KEY} "}) == KEY
    assert _key_from_env({"ODDSPUSH_KEY": KEY}) == KEY
    assert _key_from_env({"ODDSPUSH_API_KEY": "", "ODDSPUSH_KEY": ""}) is None


def test_version_flag(capsys):
    with pytest.raises(SystemExit) as info:
        main(["--version"])
    assert info.value.code == 0 and __version__ in capsys.readouterr().out


class _Api(BaseHTTPRequestHandler):
    seen: list = []

    def do_GET(self):  # noqa: N802
        type(self).seen.append((self.path, dict(self.headers)))
        if self.path.startswith("/v1/closing/"):
            body, status = {"detail": "not_in_plan"}, 403
        else:
            body, status = {"n": 1, "events": [{"event_id": 77, "pre": 3}]}, 200
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


@pytest.fixture
def local_api():
    _Api.seen = []
    srv = ThreadingHTTPServer(("127.0.0.1", 0), _Api)
    th = threading.Thread(target=srv.serve_forever, daemon=True)
    th.start()
    yield f"http://127.0.0.1:{srv.server_address[1]}", _Api.seen
    srv.shutdown()
    srv.server_close()


def _params(env, errlog_path=None):
    full = {k: v for k, v in os.environ.items()
            if k not in ("ODDSPUSH_API_KEY", "ODDSPUSH_KEY", "ODDSPUSH_API_URL")}
    full.update(env)
    return StdioServerParameters(command=sys.executable, args=["-m", "oddspush.mcp_server"],
                                 env=full)


async def test_stdio_subprocess(local_api, tmp_path):
    url, seen = local_api
    errlog = open(tmp_path / "stderr.txt", "w+", encoding="utf-8")
    params = _params({"ODDSPUSH_API_KEY": KEY, "ODDSPUSH_API_URL": url})
    from mcp.client.stdio import stdio_client
    from mcp.client import ClientSession

    async with stdio_client(params, errlog=errlog) as (read, write):
        async with ClientSession(read, write) as session:
            init = await session.initialize()
            assert init.server_info.name == "oddspush"
            names = [x.name for x in (await session.list_tools()).tools]
            assert names == [x["name"] for x in TOOLS]
            r = await session.call_tool("list_events", {})
            assert json.loads(r.content[-1].text)["events"][0]["event_id"] == 77
            r = await session.call_tool("get_closing", {"event_id": 5})
            assert r.is_error is False and "7-day real-time trial" in r.content[0].text
    assert [p for p, _ in seen] == ["/v1/events", "/v1/closing/5"]
    assert all(h.get("x-token") == KEY for _, h in seen)
    errlog.seek(0)
    assert KEY not in errlog.read()
    errlog.close()


async def test_stdio_without_key():
    params = _params({})
    from mcp.client.stdio import stdio_client
    from mcp.client import ClientSession

    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            r = await session.call_tool("list_sports", {})
            assert r.is_error is False and "Soccer" in r.content[0].text
            r = await session.call_tool("get_event", {"event_id": 1})
            assert r.is_error is True and r.content[0].text == MSG_NO_KEY
