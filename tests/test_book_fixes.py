"""Regression tests for the review of 0.1.0: tombstones, late snapshots, fair, prune."""

from oddspush import Book


def market(key, version, status="open", period=0):
    return {"key": key, "version": version, "status": status, "period": period,
            "prices": [{"designation": "home", "price": -110}]}


def odds(rec, channel="ld", op="upd"):
    return {"type": "odds", "data": {"type": "live", "op": op,
                                     "topic": f"matchups/reg/sp/29/live/{channel}", "rec": rec}}


def snap(*recs):
    return {"type": "snapshot", "part": 0, "final": True, "events": list(recs)}


def test_late_snapshot_does_not_revive_a_removed_event():
    b = Book()
    b.apply_message(odds({"id": 2, "markets": [market("a", 1)]}))
    b.apply_message(odds({"id": 2}, channel="pre", op="del"))
    b.apply_message(snap({"id": 2, "markets": [market("a", 1)]}))
    b.apply_message({"type": "odds", "data": {"type": "snapshot",
                                              "events": [{"id": 2, "markets": [market("a", 1)]}]}})
    assert 2 not in b
    b.apply_message(odds({"id": 2, "markets": [market("a", 3)]}))   # genuinely back
    assert b.market(2, "a")["version"] == 3


def test_late_snapshot_does_not_rewind_meta_or_close_newer_markets():
    b = Book()
    b.apply_message(odds({"id": 1, "state": {"minutes": 10}, "markets": [market("a", 300)],
                          "periods": [{"period": 0, "status": "open"}]}))
    b.apply_message(snap({"id": 1, "state": {"minutes": 8}, "league": "L",
                          "periods": [{"period": 0, "status": "closed"}],
                          "markets": [market("a", 200), market("b", 150)]}))
    assert b.event(1)["state"] == {"minutes": 10}
    assert b.event(1)["league"] == "L"                  # missing details are filled in
    assert b.market(1, "a")["version"] == 300           # newer than the snapshot: kept
    assert b.market(1, "b") is None                     # as old as the snapshot: closed


def test_first_snapshot_sets_everything():
    b = Book()
    b.apply_message(snap({"id": 1, "state": {"minutes": 8},
                          "periods": [{"period": 1, "status": "settled"}],
                          "markets": [market("a", 5, period=1), market("b", 5)]}))
    assert b.event(1)["state"] == {"minutes": 8}
    assert set(b.markets(1)) == {"b"}


def test_fair_from_another_channel_or_version_is_ignored():
    b = Book()
    b.apply_message(odds({"id": 1, "markets": [market("s;0;m", 200)]}, channel="both"))
    good = {"event_id": 1, "channel": "both",
            "markets": [{"key": "s;0;m", "version": 200, "margin": 0.03, "prices": []}]}
    assert b.apply_message({"type": "fair", "data": good}) is True
    for bad in ({**good, "channel": "dz"},
                {**good, "channel": "ld", "markets": [{**good["markets"][0], "version": 150}]}):
        assert b.apply_message({"type": "fair", "data": bad}) is False
    assert b.fair(1, "s;0;m")["margin"] == 0.03


def test_prune_drops_idle_empty_events(monkeypatch):
    import oddspush.book as book
    clock = [1000.0]
    monkeypatch.setattr(book.time, "monotonic", lambda: clock[0])
    b = Book()
    b.apply_message(odds({"id": 1, "markets": [market("a", 1)]}))
    b.apply_message(odds({"id": 2, "markets": [market("a", 1, status="closed")]}))
    clock[0] += 7200
    assert b.prune(3600) == [2]
    assert 1 in b and 2 not in b
