"""Print soccer prices that shorten by 5 % or more within 60 seconds, with the score.

Drops are measured on the fair price (without the bookmaker's margin), so a change of
margin alone never alerts. On start it also prints the drops of the last 10 minutes from
the REST API. Trials and paid plans.

    ODDSPUSH_KEY=opk_... python drop_alerts.py
"""

import asyncio
import os
import time

from oddspush import AsyncClient, Stream


def show(d):
    state = d.get("state") or {}
    score = state.get("score") or {}
    when = f"{score.get('home', '-')}-{score.get('away', '-')}" if d.get("live") else "pre-match"
    m = d["market"]
    line = f" {m['points']}" if m.get("points") is not None else ""
    print(f"{d['home']} v {d['away']} ({when}) {m['key']}{line} {d['designation']}: "
          f"{d['from']['decimal']:.2f} -> {d['to']['decimal']:.2f} "
          f"(fair -{d['change_pct']:.1f} % in {d['window_s']} s)")


async def main():
    key = os.environ["ODDSPUSH_KEY"]
    async with AsyncClient(key) as api:
        since = int(time.time() * 1000) - 10 * 60_000
        async for d in api.iter_drops(sport_id=1, since=since, min_pct=5, window_s=60):
            show(d)
    async with Stream(key, sports=[1], channels=["ld", "both"], snapshot=False, book=False,
                      drop_alerts={"min_pct": 5, "window_s": 60}) as stream:
        async for d in stream.drops():
            show(d)


if __name__ == "__main__":
    asyncio.run(main())
