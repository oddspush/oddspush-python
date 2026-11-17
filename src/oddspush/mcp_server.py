"""``oddspush-mcp``: the OddsPush MCP server over stdio, for Claude Code, Claude Desktop,
Cursor and any other MCP client that runs local servers.

    pip install "oddspush[mcp]"
    claude mcp add oddspush --env ODDSPUSH_API_KEY=opk_... -- oddspush-mcp

The tools (``list_sports``, ``list_events``, ``get_event``, ``get_line_history``,
``get_closing``, ``list_closings``, ``get_clv``, ``recent_drops``) each call the REST API from
your own machine, with
the key in ``ODDSPUSH_API_KEY``. Like any REST request, a call from a new IP beyond your
connections takes the key over from the oldest IP: run it on the machine your bot runs on.

Environment:

- ``ODDSPUSH_API_KEY``: your key (``ODDSPUSH_KEY`` also works). Without it ``list_sports``
  works and the other tools say how to set it.
- ``ODDSPUSH_API_URL``: the REST API, ``https://feed.oddspush.com`` by default.

Nothing is written to stdout but the protocol; logs go to stderr.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Any, List, Optional

from ._constants import REST_URL
from ._mcp_tools import INSTRUCTIONS, OddsPushTools
from ._version import __version__

log = logging.getLogger("oddspush.mcp")

_INSTALL = 'the MCP server needs the "mcp" package: pip install "oddspush[mcp]"'


def _key_from_env(env: Any = None) -> Optional[str]:
    env = os.environ if env is None else env
    for name in ("ODDSPUSH_API_KEY", "ODDSPUSH_KEY"):
        value = (env.get(name) or "").strip()
        if value:
            return value
    return None


def build_server(tools: OddsPushTools) -> Any:
    """The MCP server (``mcp.server.Server``) that serves ``tools``."""
    import mcp_types as types
    from mcp.server import Server
    from mcp.shared.exceptions import MCPError

    async def list_tools(ctx: Any, params: Any) -> Any:
        return types.ListToolsResult(
            tools=[types.Tool.model_validate(t) for t in tools.definitions()])

    async def call_tool(ctx: Any, params: Any) -> Any:
        name = params.name
        if not tools.exists(name):
            # A tool that does not exist is a protocol error (JSON-RPC -32602).
            raise MCPError(code=-32602, message=f"Unknown tool: {str(name)[:64]}")
        result = await tools.call(name, params.arguments or {})
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=c["text"]) for c in result["content"]],
            isError=result["isError"])

    return Server("oddspush", version=__version__, title="OddsPush",
                  instructions=INSTRUCTIONS, website_url="https://oddspush.com",
                  on_list_tools=list_tools, on_call_tool=call_tool)


async def serve(key: Optional[str] = None, base_url: str = REST_URL) -> None:
    """Serve the tools over stdin and stdout until the client closes them."""
    from mcp.server.stdio import stdio_server

    tools = OddsPushTools(key, base_url=base_url)
    server = build_server(tools)
    try:
        async with stdio_server() as (read_stream, write_stream):
            await server.run(read_stream, write_stream, server.create_initialization_options())
    finally:
        await tools.aclose()


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="oddspush-mcp",
        description="The OddsPush MCP server over stdio. Set your key in ODDSPUSH_API_KEY.")
    parser.add_argument("--version", action="version", version=f"oddspush-mcp {__version__}")
    parser.parse_args(argv)
    logging.basicConfig(level=logging.WARNING, stream=sys.stderr,
                        format="%(name)s: %(levelname)s %(message)s")
    try:
        import anyio
        import mcp  # noqa: F401
    except ImportError:
        print(f"oddspush-mcp: {_INSTALL}", file=sys.stderr)
        return 1
    key = _key_from_env()
    base_url = (os.environ.get("ODDSPUSH_API_URL") or "").strip() or REST_URL
    try:
        anyio.run(serve, key, base_url)
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
