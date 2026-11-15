"""Specials and player props: no market `status`, suspension by period, versionless updates."""

from oddspush import Book


def special(rec_id=50, periods=None, markets=None):
    rec = {"id": rec_id, "type": "special", "parentId": 7,
           "special": {"description": "Total Points", "category": "Player Props"},
           "participants": [{"id": 51, "name": "Over"}, {"id": 52, "name": "Under"}],
           "markets": markets if markets is not None else [spm()]}
    if periods is not None:
        rec["periods"] = periods
    return rec


def spm(key="s;0;ou", version=10, price=-110, **extra):
    m = {"key": key, "type": "total", "period": 0,
         "prices": [{"participantId": 51, "price": price, "points": 24.5},
                    {"participantId": 52, "price": -110, "points": 24.5}], **extra}
    if version is not None:
        m["version"] = version
    return m


def odds(rec, channel="both", op="upd"):
    return {"type": "odds", "data": {"type": "live", "sport_id": 3, "op": op,
                                     "topic": f"matchups/spc/sp/29/live/{channel}",
                                     "rec": rec, "ts": 1}}


def test_a_special_market_without_status_is_open():
    b = Book()
    b.apply_message(odds(special(periods=[{"period": 0, "status": "open"}])))
    assert b.market(50, "s;0;ou")["prices"][0]["participantId"] == 51
    assert not b.is_suspended(50, "s;0;ou")
    # A regular event's market without status is still not open.
    b.apply_rec({"id": 1, "markets": [{"key": "sp", "prices": []}]})
    assert b.markets(1) == {}


def test_closed_period_suspends_a_special_and_open_reopens_it():
    b = Book()
    b.apply_message(odds(special(periods=[{"period": 0, "status": "open"}])))
    # Suspended: updates keep coming, often without version (repeating the price held).
    b.apply_message(odds(special(periods=[{"period": 0, "status": "closed"}],
                                 markets=[spm(version=None, price=-110)])))
    assert b.market(50, "s;0;ou")["version"] == 10
    assert b.is_suspended(50, "s;0;ou")
    # A versioned price change while suspended is applied.
    b.apply_message(odds(special(periods=[{"period": 0, "status": "closed"}],
                                 markets=[spm(version=10.5, price=-130)])))
    assert b.market(50, "s;0;ou")["prices"][0]["price"] == -130
    b.apply_message(odds(special(periods=[{"period": 0, "status": "open"}],
                                 markets=[spm(version=11, price=-125)])))
    assert not b.is_suspended(50, "s;0;ou")
    assert b.market(50, "s;0;ou")["version"] == 11


def test_settled_period_removes_a_special():
    b = Book()
    b.apply_message(odds(special(periods=[{"period": 0, "status": "open"}])))
    b.apply_message(odds(special(periods=[{"period": 0, "status": "settled"}], markets=[])))
    assert b.markets(50) == {}


def test_empty_markets_update_is_not_a_removal():
    b = Book()
    b.apply_message(odds(special(periods=[{"period": 0, "status": "open"}])))
    b.apply_message(odds(special(periods=[{"period": 0, "status": "open"}], markets=[])))
    assert b.market(50, "s;0;ou") is not None


def test_a_versioned_update_older_than_the_held_one_is_still_ignored():
    b = Book()
    b.apply_message(odds(special(markets=[spm(version=10, price=-110)])))
    b.apply_message(odds(special(markets=[spm(version=9, price=-200)])))
    assert b.market(50, "s;0;ou")["prices"][0]["price"] == -110


def test_regular_event_closed_period_still_removes():
    b = Book()
    b.apply_rec({"id": 1, "markets": [{"key": "a", "version": 1, "status": "open", "period": 0}]})
    b.apply_rec({"id": 1, "periods": [{"period": 0, "status": "closed"}]})
    assert b.markets(1) == {}
    assert not b.is_suspended(1, "a")


def test_versioned_unversioned_versioned():
    """v100 open -> no version, suspended -> v101 reopened; a late v99 is ignored."""
    b = Book()
    b.apply_message(odds(dict(special(periods=[{"period": 0, "status": "open"}],
                                      markets=[spm(version=100, price=-110)]), version=5)))
    b.apply_message(odds(dict(special(periods=[{"period": 0, "status": "closed"}],
                                      markets=[spm(version=None, price=-110)]), version=6)))
    assert b.market(50, "s;0;ou")["version"] == 100 and b.is_suspended(50, "s;0;ou")
    b.apply_message(odds(dict(special(periods=[{"period": 0, "status": "open"}],
                                      markets=[spm(version=101, price=-125)]), version=7)))
    assert b.market(50, "s;0;ou")["version"] == 101 and not b.is_suspended(50, "s;0;ou")
    b.apply_message(odds(dict(special(periods=[{"period": 0, "status": "closed"}],
                                      markets=[spm(version=99, price=-200)]), version=6)))
    assert b.market(50, "s;0;ou")["prices"][0]["price"] == -125
    assert not b.is_suspended(50, "s;0;ou")      # the stale period status was not applied


def test_unversioned_price_is_taken_when_nothing_is_held():
    b = Book()
    b.apply_message(odds(special(markets=[spm(version=None, price=-140)])))
    assert b.market(50, "s;0;ou")["prices"][0]["price"] == -140
    b.apply_message(odds(special(markets=[spm(version=7, price=-150)])))
    assert b.market(50, "s;0;ou")["version"] == 7


def test_a_newer_snapshot_refreshes_a_special_suspension():
    b = Book()
    b.apply_message(odds(dict(special(periods=[{"period": 0, "status": "open"}]), version=5)))
    b.apply_message(odds(dict(special(periods=[{"period": 0, "status": "closed"}],
                                      markets=[]), version=6)))
    assert b.is_suspended(50, "s;0;ou")
    # The reopen (rv 7) was missed; the resync snapshot brings it.
    b.apply_message({"type": "snapshot", "part": 0, "final": True, "events": [
        dict(special(periods=[{"period": 0, "status": "open"}],
                     markets=[spm(version=11, price=-125)]), version=7)]})
    assert not b.is_suspended(50, "s;0;ou")
    assert b.market(50, "s;0;ou")["version"] == 11


def test_regular_market_without_version_still_applies():
    b = Book()
    b.apply_rec({"id": 1, "markets": [{"key": "a", "version": 1, "status": "open", "period": 0,
                                       "prices": [1]}]})
    b.apply_rec({"id": 1, "markets": [{"key": "a", "status": "open", "period": 0,
                                       "prices": [2]}]})
    assert b.market(1, "a")["prices"] == [2]
    b.apply_rec({"id": 1, "markets": [{"key": "a", "status": "closed", "period": 0}]})
    assert b.market(1, "a") is None
