"""Print the full-game moneyline of every soccer match as it moves.

    ODDSPUSH_TOKEN=opk_... python live_odds.py
"""

import asyncio
import os

from oddspush import Stream, american_to_decimal


def teams(event):
    names = {p.get("alignment"): p.get("name") for p in event.get("participants") or []}
    return f"{names.get('home', '?')} v {names.get('away', '?')}"


async def main():
    async with Stream(os.environ["ODDSPUSH_TOKEN"], sports=[1], channels=["ld", "both"]) as stream:
        async for msg in stream:
            if msg["type"] == "disconnected":
                print("disconnected:", msg["reason"], "- retrying in", msg["retry_in_s"], "s")
            if msg["type"] != "odds" or msg["data"].get("type") != "live":
                continue
            event_id = msg["data"]["rec"]["id"]
            moneyline = stream.book.market(event_id, "s;0;m")
            if moneyline is None:
                continue
            prices = {p["designation"]: round(american_to_decimal(p["price"]), 3)
                      for p in moneyline["prices"]}
            print(teams(stream.book.event(event_id)), prices)


if __name__ == "__main__":
    asyncio.run(main())
