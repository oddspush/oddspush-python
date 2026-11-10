"""Closing line value of your bets, against Pinnacle's fair closing price.

bets.csv has one bet per line: event_id,market_key,designation,decimal_odds
(for example ``1639824805,s;0;m,home,2.10``). Closing lines are kept for 7 days.

    ODDSPUSH_KEY=opk_... python clv_report.py bets.csv
"""

import csv
import os
import sys

from oddspush import Client, NotFound, clv


def main(path):
    total, n = 0.0, 0
    with Client(os.environ["ODDSPUSH_KEY"]) as api, open(path, newline="") as f:
        for event_id, key, designation, odds in csv.reader(f):
            try:
                line = api.closing(int(event_id))
            except NotFound:
                print(f"{event_id}: no closing line (not started yet, or older than 7 days)")
                continue
            market = next((m for m in line["markets"] if m["key"] == key), None)
            leg = next((p for p in (market or {}).get("prices", [])
                        if p["designation"] == designation), None)
            if leg is None:
                print(f"{event_id} {key} {designation}: not in the closing line")
                continue
            value = clv(float(odds), leg["fair_decimal"])
            total, n = total + value, n + 1
            print(f"{line['home']} v {line['away']} {key} {designation}: "
                  f"{float(odds):.2f} vs fair close {leg['fair_decimal']:.2f} -> CLV {value:+.2%}")
    if n:
        print(f"average CLV over {n} bets: {total / n:+.2%}")


if __name__ == "__main__":
    main(sys.argv[1])
