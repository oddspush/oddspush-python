"""Watch the no-vig and fair prices of tennis moneylines, with the bookmaker's margin.

    ODDSPUSH_KEY=opk_... python fair_prices.py
"""

import asyncio
import os

from oddspush import Stream


async def main():
    async with Stream(os.environ["ODDSPUSH_KEY"], sports=[2], channels=["ld", "both"],
                      fair=True) as stream:
        async for msg in stream:
            if msg["type"] != "fair":
                continue
            data = msg["data"]
            for market in data["markets"]:
                if market["key"] != "s;0;m":
                    continue
                legs = ", ".join(f"{p['designation']} {p['fair_decimal']:.3f} "
                                 f"(no-vig {p['no_vig_decimal']:.3f})"
                                 for p in market["prices"])
                print(f"{data['event_id']}: margin {market['margin']:.2%} | {legs}")


if __name__ == "__main__":
    asyncio.run(main())
