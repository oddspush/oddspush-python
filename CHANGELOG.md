# Changelog

## 0.3.0

- Free plan: `Stream(accept_delay=True)` logs in with `accept_delay`, and
  `Client(accept_delay=True)` / `AsyncClient(accept_delay=True)` send `X-Accept-Delay: true`.
  `stream.delay_s` says how late the odds are (30 on the free plan, 0 in real time).
- New errors: `DelayedPlan` (4110, a free key without `accept_delay=True`: fatal, and the
  message says how to fix it), `PlanChanged` (4111: the stream reconnects at once, now in real
  time), `TryAgain` (4112: retried after `retry_after_s`) and `NotReady` (the free plan's
  delayed feed filling up; a refused snapshot is asked for again). `not_in_plan` (a free key
  asking for an extra) is logged with its `features`, and raises `NotInPlan` on REST. Errors
  carry the other fields of the API's message in `details`.
- Drop alerts: `Stream(drop_alerts=True | {"min_pct": ..., "window_s": ...})`,
  `subscribe(drop_alerts=...)` / `unsubscribe(drop_alerts=True)`, `on_drop=` and
  `stream.drops()`; restored on reconnect with the rest of the confirmed filter. REST:
  `drops()` and `iter_drops()` (`/v1/drops`, paged by `cursor`).
- REST: `history()` and `iter_history()` (`/v1/events/{id}/history`, paged by `next_since`),
  `clv()` (`/v1/clv`), `status()` and `oddspush.get_status()` (`/v1/status`, sent without the
  key). Closing lines are kept for 90 days.
- `pip install "oddspush[mcp]"` adds `oddspush-mcp`, an MCP server over stdio for Claude,
  Cursor and any MCP client, with the tools (`list_sports`, `list_events`, `get_event`, `get_line_history`,
  `get_closing`, `list_closings`, `get_clv`, `recent_drops`), calling the REST API from your
  machine with the key in `ODDSPUSH_API_KEY`. Built on the official `mcp` package (2.x).
- Examples: drop alerts, and a CLV report with the line history of each bet.

## 0.2.0

- Specials and player props: `Book` keeps them (their markets have no `status`; a `closed`
  period suspends them, `settled` removes them) and `Book.is_suspended(event_id, key)` says
  whether one is suspended right now. A special update without `version` keeps the price
  held, and one with a lower `rec.version` than already applied is ignored.
- `Stream(specials=True)` and `subscribe(specials=True)` / `unsubscribe(specials=True)`:
  snapshots also include specials with their current prices. Restored on reconnect.
- A newer snapshot of a special (by `rec.version`) refreshes its suspension.
- Docs: a token works from as many IPs at a time as your connections (extra connections
  from your account page).

## 0.1.1

- Examples read the key from `ODDSPUSH_KEY`, as in the docs at oddspush.com.
- Python 3.10 or newer.

## 0.1.0

First release: `Stream`, `Client`, `AsyncClient`, `Book` and the odds helpers.
