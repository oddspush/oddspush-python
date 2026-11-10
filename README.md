# oddspush

Python client for the [OddsPush](https://oddspush.com) API: **real-time Pinnacle odds** over
one WebSocket, plus a REST API, with **no-vig and fair prices** and the **closing line** for
your CLV.

- All 13 sports, live and pre-match.
- A local price book, kept up to date for you: merged by version, closed markets and
  periods removed.
- Reconnects on its own with backoff, subscribes again and catches up with a snapshot.
- Sync and asyncio REST clients.
- Pure Python, two dependencies (`websockets`, `httpx`), Python 3.9+.

You need an API token (`opk_...`). [Start a free 7-day trial](https://oddspush.com) to get
one. Full API reference: [oddspush.com/docs](https://oddspush.com/docs).

```bash
pip install oddspush
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
from oddspush import Client, clv

with Client("opk_...") as api:
    line = api.closing(1639824805)          # pre-match or live event id
    ml = next(m for m in line["markets"] if m["key"] == "s;0;m")
    home = next(p for p in ml["prices"] if p["designation"] == "home")
    print(clv(2.10, home["fair_decimal"]))  # your price vs the fair closing price
```

Or get them as they happen with `Stream(..., closing=True)` (`closing` messages).

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
minute per account.

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
  `NotInPlan` (a REST-only plan) and `TokenReplaced`.

Change the subscription at any time:

```python
await stream.subscribe(sports=[2, 3])
await stream.unsubscribe(sports=[3])
await stream.subscribe(fair=True)     # turn fair prices on, filter unchanged, no snapshot
await stream.request_snapshot()
```

In a bot that runs for days, `stream.book.prune(3600)` drops events that have had no update
for an hour and have no open market left.

### One machine per token

A token works from one IP at a time: using it from a new IP (a stream login or a REST
request) moves it there and closes the stream on the old one (`TokenReplaced`). Run your
stream and your REST calls from the same machine.

### Keep up

Read fast and do heavy work in another task: if you fall more than 15 seconds behind, the
server closes the connection and the stream reconnects. Host your bot close to Ireland (AWS
`eu-west-1`) for the lowest latency.

## Examples

See [`examples/`](https://github.com/oddspush/oddspush-python/tree/main/examples): a live
printer, a fair-price watcher, a pre-match tracker and a CLV report.

## Links

- Website and pricing: [oddspush.com](https://oddspush.com)
- API reference: [oddspush.com/docs](https://oddspush.com/docs)
- Source and issues: [github.com/oddspush/oddspush-python](https://github.com/oddspush/oddspush-python)
- Support: support@oddspush.com

OddsPush is not affiliated with, endorsed by or sponsored by Pinnacle. MIT licensed.
