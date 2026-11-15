# Changelog

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
