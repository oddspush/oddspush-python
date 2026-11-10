from oddspush import Book, channel_of


def market(key, version, status="open", period=0, prices=None, **extra):
    return {"key": key, "version": version, "status": status, "period": period,
            "prices": prices or [{"designation": "home", "price": -110},
                                 {"designation": "away", "price": -110}], **extra}


def odds(rec, channel="ld", op="upd"):
    return {"type": "odds", "data": {"type": "live", "sport_id": 1, "op": op,
                                     "topic": f"matchups/reg/sp/29/live/{channel}",
                                     "rec": rec, "ts": 1}}


def test_channel_of():
    assert channel_of("matchups/reg/sp/29/live/ld") == "ld"
    assert channel_of("x/both") == "both"
    assert channel_of("x/pre") == "pre"
    assert channel_of("x/dz") == "dz"
    assert channel_of("x/weird") == "other"
    assert channel_of(None) == "other"


def test_merge_by_version_and_late_updates():
    b = Book()
    b.apply_message(odds({"id": 7, "league": "L", "markets": [market("s;0;m", 5)]}))
    b.apply_message(odds({"id": 7, "markets": [market("s;0;m", 3, prices=[])]}))
    assert b.market(7, "s;0;m")["version"] == 5
    assert b.event(7)["league"] == "L"
    b.apply_message(odds({"id": 7, "markets": [market("s;0;m", 9)]}))
    assert b.market(7, "s;0;m")["version"] == 9


def test_closed_market_is_removed_and_not_resurrected():
    b = Book()
    b.apply_message(odds({"id": 1, "markets": [market("k", 5)]}))
    b.apply_message(odds({"id": 1, "markets": [market("k", 6, status="suspended")]}))
    assert b.market(1, "k") is None
    b.apply_message(odds({"id": 1, "markets": [market("k", 5)]}))   # late, older
    assert b.market(1, "k") is None
    b.apply_message(odds({"id": 1, "markets": [market("k", 7)]}))
    assert b.market(1, "k")["version"] == 7


def test_market_without_status_or_version_is_not_open():
    b = Book()
    b.apply_rec({"id": 1, "markets": [{"key": "sp", "prices": []}]})
    assert b.markets(1) == {}


def test_period_close_list_and_dict():
    b = Book()
    b.apply_rec({"id": 1, "markets": [market("a", 1, period=0), market("b", 1, period=1)]})
    b.apply_rec({"id": 1, "periods": [{"period": 1, "status": "settled"}]})
    assert set(b.markets(1)) == {"a"}
    b.apply_rec({"id": 1, "periods": {"0": {"status": "closed"}}})
    assert b.markets(1) == {}


def test_del_removes_event_on_any_channel():
    b = Book()
    b.apply_message(odds({"id": 1, "markets": [market("a", 1)]}))
    assert b.apply_message(odds({"id": 1}, channel="pre", op="del")) is True
    assert 1 not in b


def test_channels_filter_live_updates():
    b = Book()
    b.apply_message(odds({"id": 1, "markets": [market("a", 1)]}, channel="dz"))
    b.apply_message(odds({"id": 2, "markets": [market("a", 1)]}, channel="pre"))
    assert len(b) == 0
    pre = Book(["pre"])
    pre.apply_message(odds({"id": 2, "markets": [market("a", 1)]}, channel="pre"))
    assert pre.market(2, "a") is not None


def test_snapshot_parts_and_source_snapshot():
    b = Book()
    b.apply_message({"type": "snapshot", "part": 0, "final": True,
                     "events": [{"id": 1, "markets": [market("a", 1)]}]})
    b.apply_message({"type": "odds", "data": {"type": "snapshot",
                                              "events": [{"id": 2, "markets": [market("a", 1)]}]}})
    assert set(b.event_ids()) == {1, 2}


def test_fair_matches_version_only():
    b = Book()
    b.apply_message(odds({"id": 1, "markets": [market("s;0;m", 5)]}))
    b.apply_message({"type": "fair", "data": {"event_id": 1, "channel": "ld", "markets": [
        {"key": "s;0;m", "version": 5, "margin": 0.02, "prices": [{"fair": 0.5}]}]}})
    assert b.fair(1, "s;0;m")["margin"] == 0.02
    b.apply_message(odds({"id": 1, "markets": [market("s;0;m", 6)]}))
    assert b.fair(1, "s;0;m") is None      # the fair price was for version 5


def test_snapshot_inline_fair():
    b = Book()
    m = market("s;0;m", 5, fair={"margin": 0.03, "prices": [{"fair": 0.4}]})
    b.apply_message({"type": "snapshot", "events": [{"id": 1, "markets": [m]}]})
    assert "fair" not in b.market(1, "s;0;m")
    assert b.fair(1, "s;0;m") == {"version": 5, "margin": 0.03, "prices": [{"fair": 0.4}]}


def test_markets_returns_a_copy():
    b = Book()
    b.apply_rec({"id": 1, "markets": [market("a", 1)]})
    b.markets(1).clear()
    assert b.market(1, "a") is not None
