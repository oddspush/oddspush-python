"""The CLV of your bets, and how each line moved from your bet to the close.

bets.csv has one bet per line: event_id,market_key,designation,decimal_odds,placed_at_ms
(for example ``1639824805,s;0;m,home,2.10,1793960000000``). ``event_id`` is the pre-match
or the live event. Closing lines are kept for 90 days, line history for 14. Trials and paid
plans.

    ODDSPUSH_KEY=opk_... python clv_history.py bets.csv
"""

import csv
import os
import sys

from oddspush import Client, NotFound, american_to_decimal


def fair_decimal(row, designation):
    """The fair decimal odds of a leg in a history row (``fair=True``), or ``None``."""
    for p in ((row.get("fair") or {}).get("prices") or []):
        if p.get("designation") == designation:
            return p.get("fair_decimal")
    return None


def main(path):
    total, n = 0.0, 0
    with Client(os.environ["ODDSPUSH_KEY"]) as api, open(path, newline="") as f:
        for event_id, key, designation, odds, placed_at in csv.reader(f):
            try:
                bet = api.clv(int(event_id), key, designation, float(odds))
            except NotFound:
                print(f"{event_id} {key} {designation}: no closing line (not started yet, "
                      "older than 90 days, or not open at kick-off)")
                continue
            total, n = total + bet["clv_pct"], n + 1
            print(f"{event_id} {key} {designation}: {float(odds):.2f} vs fair close "
                  f"{bet['closing']['fair_decimal']:.2f} -> CLV {bet['clv_pct']:+.2f} % "
                  f"({'beat' if bet['beat_close'] else 'did not beat'} the close)")

            # How the line moved after the bet: the pre-match event holds the pre-match line.
            pre_id = bet.get("event_id", int(event_id))
            moves = []
            for row in api.iter_history(pre_id, key=key, since=int(placed_at), fair=True):
                if row.get("channel") != "pre" or row.get("status") != "open":
                    continue
                leg = next((p for p in row["prices"] if p.get("designation") == designation),
                           None)
                if leg is not None:
                    moves.append((american_to_decimal(leg["price"]),
                                  fair_decimal(row, designation)))
            if moves:
                first, last = moves[0], moves[-1]
                print(f"    {len(moves)} changes after the bet: {first[0]:.2f} -> {last[0]:.2f}"
                      + (f" (fair {last[1]:.2f})" if last[1] else ""))
    if n:
        print(f"average CLV over {n} bets: {total / n:+.2f} %")


if __name__ == "__main__":
    main(sys.argv[1])
