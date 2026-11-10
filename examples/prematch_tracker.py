"""Track the pre-match fair price of a few matches until each one really kicks off.

Pre-match prices come on the ``pre`` channel. When a match starts (which can be later than
its scheduled ``startTime``), its closing line is set and arrives as a ``closing`` message:
the pre-match prices are final from then on, so the tracker prints the close and stops
following that match.

    ODDSPUSH_TOKEN=opk_... python prematch_tracker.py 1639824805 1639824811
"""

import asyncio
import os
import sys
import time

from oddspush import Stream

GIVE_UP_S = 600     # an id that never shows up (wrong id, or already over)


def moneyline(prices):
    return ", ".join(f"{p['designation']} {p['fair_decimal']:.3f}" for p in prices)


async def main(event_ids):
    following = set(event_ids)
    seen = set()
    started = time.monotonic()
    async with Stream(os.environ["ODDSPUSH_TOKEN"], events=event_ids, channels=["pre"],
                      fair=True, closing=True) as stream:
        async for msg in stream:
            kind = msg["type"]
            if kind == "closing":
                data = msg["data"]
                if data["event_id"] in following:
                    ml = next((m for m in data["markets"] if m["key"] == "s;0;m"), None)
                    print(f"{data['event_id']} kicked off ({data['reason']}); closing fair: "
                          f"{moneyline(ml['prices']) if ml else 'no moneyline'}")
                    following.discard(data["event_id"])
                    await stream.unsubscribe(events=[data["event_id"]])
            elif kind == "snapshot" and msg.get("final"):
                # The current prices; pre-match prices can then stay still for hours.
                for event_id in sorted(following):
                    fair = stream.book.fair(event_id, "s;0;m")
                    if fair:
                        print(f"{event_id}: {moneyline(fair['prices'])}")
            elif kind == "fair":
                event_id = msg["data"]["event_id"]
                fair = stream.book.fair(event_id, "s;0;m")
                if event_id in following and fair:
                    print(f"{event_id}: {moneyline(fair['prices'])}")
            elif kind == "odds" and msg["data"].get("op") == "del":
                event_id = msg["data"]["rec"].get("id")
                if event_id in following:
                    print(f"{event_id} removed (cancelled or withdrawn)")
                    following.discard(event_id)
            for event_id in following:
                if stream.book.event(event_id) is not None:
                    seen.add(event_id)
            if time.monotonic() - started > GIVE_UP_S:
                for event_id in following - seen:
                    print(f"{event_id}: never seen, giving up")
                following &= seen
            if not following:
                return


if __name__ == "__main__":
    asyncio.run(main([int(a) for a in sys.argv[1:]]))
