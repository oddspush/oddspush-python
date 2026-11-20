# oddspush

Python client for the [OddsPush](https://oddspush.com) API: **real-time Pinnacle odds** over
one WebSocket, plus a REST API, with **no-vig and fair prices**, **drop alerts**, **line
history** and the **closing line** for your CLV. Includes an **MCP server** for AI agents.

- All 13 sports, live and pre-match.
- A local price book, kept up to date for you: merged by version, closed markets and
  periods removed.
- Reconnects on its own with backoff, subscribes again and catches up with a snapshot.
- Sync and asyncio REST clients.
- Pure Python, two dependencies (`websockets`, `httpx`), Python 3.10+.

You need an API key (`opk_...`). [Sign up at oddspush.com](https://oddspush.com) and you get a
free key at once (odds 30 seconds late, see [Free plan](#free-plan)); the 7-day real-time
trial and the paid plans have everything. Full API reference:
[oddspush.com/docs](https://oddspush.com/docs).

```bash
pip install oddspush
pip install "oddspush[mcp]"     # with the MCP server, oddspush-mcp
```

## Stream live odds

```python
import asyncio
from oddspush import Stream

async def main():
    async with Stream("opk_...", sports=[1], channels=["ld", "both"]) as stream:
        async for msg in stream:
            if msg["type"] == "odds" and msg["data"].get("type") == "live":
                rec = msg["data"]["rec"]
                for key, market in stream.book.markets(rec["id"]).items():
                    print(rec["id"], key, [(p["designation"], p["price"]) for p in market["prices"]])

asyncio.run(main())
```

`stream.book` always holds the current open markets of every event you follow:

```python
stream.book.event_ids()               # events in the book
stream.book.event(event_id)           # league, participants, state, startTime...
stream.book.markets(event_id)         # {"s;0;m": {...}, "s;0;ou;2.5": {...}}
stream.book.market(event_id, "s;0;m")
```

Prices are in American odds; `oddspush.american_to_decimal(-155)` gives `1.645`.

Sports: 1 Soccer, 2 Tennis, 3 Basketball, 4 Hockey, 5 American football, 6 Baseball,
7 Rugby, 8 MMA, 9 Boxing, 10 Other (volleyball and handball), 11 Esports, 12 Golf,
13 Cricket (`oddspush.SPORTS`).

Channels: `ld` and `both` are the prices (ask for both). Add `pre` for pre-match prices.
`dz` carries volatile moments: the book leaves it out, and with `dz` in your channels the
stream asks for snapshots of the book's channels only. Without `channels`, live updates come
on every channel; the book still takes only `ld` and `both`.

## No-vig and fair prices

```python
async with Stream("opk_...", sports=[1], channels=["ld", "both"], fair=True) as stream:
    async for msg in stream:
        if msg["type"] == "fair":
            for m in msg["data"]["markets"]:
                print(m["key"], m["margin"], [(p["designation"], p["fair_decimal"]) for p in m["prices"]])
```

Each `fair` message follows its `odds` update with the same markets priced without the
bookmaker's margin, two ways: `no_vig` (multiplicative) and `fair` (power method, which
corrects the favourite-longshot bias). The book keeps them too:

```python
stream.book.fair(event_id, "s;0;m")   # {"version", "margin", "prices"} or None
```

It only returns the fair price of the very market version the book holds.

## Closing line and CLV

```python
from oddspush import Client

with Client("opk_...") as api:
    bet = api.clv(1639824805, "s;0;m", "home", 2.10)   # decimal, or American: -110, 150
    print(bet["clv_pct"], bet["beat_close"])           # vs the fair closing price, in %

    line = api.closing(1639824805)          # the whole closing line (pre-match or live id)
    for row in api.iter_closings(sport_id=1, since=1793900000000):   # up to 90 days back
        print(row["event_id"], row["start_ms"])
```

Closing lines are kept for 90 days after the start. Get them as they happen with
`Stream(..., closing=True)` (`closing` messages), and work out the CLV of your own prices
with `oddspush.clv(your_decimal, fair_decimal)`.

## Line history

Every change of price, line or status of an event's markets over the last 14 days, oldest
first:

```python
with Client("opk_...") as api:
    for row in api.iter_history(1639824805, key="s;0;m", fair=True):
        print(row["time"], row["channel"], row["status"],
              [(p["designation"], p["price"]) for p in row["prices"]])
```

`history()` returns one page (`more`, `next_since`); `iter_history()` follows the pages. The
pre-match and the live version of a match are different events: ask for each.

## Drop alerts

A `drop` message when a price shortens by at least `min_pct` % within `window_s` seconds,
measured on the fair price, so a change of margin alone never alerts. Each alert has the
match (league, teams, live score and minute), the market, the leg and the price before and
after:

```python
async with Stream("opk_...", sports=[1], drop_alerts={"min_pct": 5, "window_s": 60}) as stream:
    async for drop in stream.drops():
        print(drop["home"], drop["away"], drop["market"]["key"], drop["designation"],
              drop["from"]["decimal"], "->", drop["to"]["decimal"], drop["change_pct"])
```

`drop_alerts=True` means 3 % within 60 seconds; `window_s` is one of 30, 60, 300 or 600 and
`min_pct` from 1 to 50. You can also pass `on_drop=callback`, or read `drop` messages from
the main iterator. The setting is restored on reconnect like the rest of your filter. The
drops of the last hour are on REST too:

```python
with Client("opk_...") as api:
    page = api.drops(sport_id=1, min_pct=5, window_s=60)      # oldest first, with a cursor
    new = api.drops(sport_id=1, min_pct=5, window_s=60, after=page["cursor"])  # only new ones
```

## Player props and specials

Specials (player, game and team props) are events of their own (`rec.type == "special"`,
`parentId` is the match). With `specials=True` your snapshots include them with their current
prices, so the book recovers them after a reconnect:

```python
async with Stream("opk_...", sports=[3, 5], channels=["ld", "both", "pre"],
                  specials=True) as stream:
    async for msg in stream:
        for event_id in stream.book:
            ev = stream.book.event(event_id)
            if ev.get("type") != "special":
                continue
            for key, m in stream.book.markets(event_id).items():
                names = {p["id"]: p.get("name") for p in ev.get("participants") or []}
                legs = [(names.get(p["participantId"]), p["price"]) for p in m["prices"]]
                print(ev["special"]["description"], legs,
                      "suspended" if stream.book.is_suspended(event_id, key) else "open")
```

Their markets have no `status`: a `closed` period suspends them (the book keeps them and
`is_suspended` says so) and `settled` removes them.

## REST

```python
from oddspush import Client

with Client("opk_...") as api:
    api.events(sport_id=1)                          # every event, markets per channel
    api.event(1638807209, fair=True)                # one event, ld + both merged
    api.event(1639824805, channel="pre", fair=True) # a pre-match event
    api.membership(sport_id=1)                      # pre-match events about to go live
    for row in api.iter_closings(sport_id=1):       # matches with a closing line
        print(row["event_id"], row["start_ms"])
```

`AsyncClient` has the same methods, awaitable. A `429` is retried after `retry-after`
(`max_retries=2`), then raised as `RateLimited`. Limits: 120 requests at once, then 300 a
minute per account, times its connections with extras (20 and 60 on the free plan, per
account and per IP).

`Client(...).status()` (or `oddspush.get_status()`, without a key) gives the public status:
whether the feed is flowing and the latency measured every 5 minutes by a client connected
like you.

## Free plan

A free key gets the same odds, live and pre-match, all 13 sports, over the stream and REST,
**30 seconds late**: enough to explore and to build and test a bot, not to trade live odds.
No fair prices, closing lines, CLV, line history, drop alerts or specials: those come with
the 7-day trial and the paid plans.

You have to accept the delay, so that a bot never trades on late prices without knowing:

```python
async with Stream("opk_...", sports=[1], accept_delay=True) as stream:
    async for msg in stream:
        if msg["type"] == "welcome":
            print("odds are", stream.delay_s, "seconds late")   # 30 free, 0 in real time

with Client("opk_...", accept_delay=True) as api:
    api.events(sport_id=1)
```

- Without `accept_delay=True` a free key raises `DelayedPlan` (the stream stops, close 4110).
- `accept_delay=True` is harmless on a paid key, and with it an expired key keeps receiving
  odds on the free plan instead of stopping with `SubscriptionExpired`.
- If you upgrade while connected, the stream reconnects at once (`plan_changed`) and the odds
  arrive in real time.
- A `try_again` (we could not check the key right now) is retried after `retry_after_s`.
- Asking a free key for `fair`, `closing`, `specials` or `drop_alerts` logs a warning and
  they stay off (`not_in_plan`); on REST it raises `NotInPlan`.

## MCP server for AI agents

`pip install "oddspush[mcp]"` adds `oddspush-mcp`, an MCP server (stdio) for Claude, Cursor
and any MCP client that runs local servers. Its tools: `list_sports`, `list_events`,
`get_event`, `get_line_history`, `get_closing`, `list_closings`, `get_clv` and
`recent_drops`. Each tool calls the REST API from your machine with the key in
`ODDSPUSH_API_KEY`.

```bash
claude mcp add oddspush --env ODDSPUSH_API_KEY=opk_... -- oddspush-mcp
```

Or, in a client's JSON config (Claude Desktop, Cursor...):

```json
{"mcpServers": {"oddspush": {"command": "oddspush-mcp",
                             "env": {"ODDSPUSH_API_KEY": "opk_..."}}}}
```

With a free key, `list_sports`, `list_events` and `get_event` work, 30 seconds late; the other
tools say which plans have them. Like any REST request, a call from a new IP beyond your
connections takes the key over from the oldest IP: run it on the machine your bot runs on.

## What the stream does for you

- Logs in and subscribes. After a drop it reconnects with exponential backoff (1 s, 2 s,
  4 s... up to 30 s), restores your filter and asks for a snapshot.
- Asks for a snapshot after `missed` (you fell behind) and after `feed: up` (the source
  came back), and again later if the snapshot rate limit says so.
- Yields `{"type": "disconnected", "reason": ..., "retry_in_s": ...}` each time the
  connection drops, so you can log it.
- Snapshots count against your plan's `snapshots_per_hour`. Pass `resync=False` to stop
  the automatic ones.
- Stops and raises on what retrying cannot fix: `Unauthorized`, `SubscriptionExpired`,
  `NotInPlan` (a REST-only plan), `DelayedPlan` (a free key without `accept_delay=True`) and
  `TokenReplaced`.

Change the subscription at any time:

```python
await stream.subscribe(sports=[2, 3])
await stream.unsubscribe(sports=[3])
await stream.subscribe(fair=True)     # turn fair prices on, filter unchanged, no snapshot
await stream.subscribe(drop_alerts={"min_pct": 4})
await stream.unsubscribe(drop_alerts=True)
await stream.request_snapshot()
```

In a bot that runs for days, `stream.book.prune(3600)` drops events that have had no update
for an hour and have no open market left.

### One machine per connection

A token works from as many IPs at a time as the connections in your plan: one included,
plus any extra connections you add from your account page (with your plan in one payment,
ending with it, or on their own for 30 days; 90 USD a month each). Using it from
a new IP beyond that (a stream login or a REST request) takes over from the oldest IP and
closes the stream there (`TokenReplaced`). With one connection, run your stream and your
REST calls from the same machine.

### Keep up

Read fast and do heavy work in another task: if you fall more than 15 seconds behind, the
server closes the connection and the stream reconnects. Host your bot close to Ireland (AWS
`eu-west-1`) for the lowest latency.

## Examples

See [`examples/`](https://github.com/oddspush/oddspush-python/tree/main/examples): a live
printer, a fair-price watcher, a pre-match tracker, drop alerts, a CLV report and a CLV report
with the line history of each bet.

## Links

- Website and pricing: [oddspush.com](https://oddspush.com)
- API reference: [oddspush.com/docs](https://oddspush.com/docs)
- Source and issues: [github.com/oddspush/oddspush-python](https://github.com/oddspush/oddspush-python)
- Support: support@oddspush.com

OddsPush is not affiliated with, endorsed by or sponsored by Pinnacle. MIT licensed.
