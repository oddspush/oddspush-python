import pytest

from oddspush import american_to_decimal, clv, decimal_to_american, implied_probability
from oddspush.odds import margin, no_vig


def test_american_decimal_roundtrip():
    assert american_to_decimal(450) == pytest.approx(5.5)
    assert american_to_decimal(-155) == pytest.approx(1.645161, rel=1e-6)
    assert american_to_decimal(100) == pytest.approx(2.0)
    assert american_to_decimal(-100) == pytest.approx(2.0)
    for a in (-1000, -155, -100, 100, 150, 3000):
        assert decimal_to_american(american_to_decimal(a)) == pytest.approx(a if a != -100 else 100)


@pytest.mark.parametrize("bad", [0, 50, -99.9])
def test_american_invalid(bad):
    with pytest.raises(ValueError):
        american_to_decimal(bad)


def test_decimal_invalid():
    with pytest.raises(ValueError):
        decimal_to_american(1.0)


def test_margin_and_no_vig_match_docs():
    # The example from the API docs: -155 / +450 / +300, margin 0.039661.
    assert margin([-155, 450, 300]) == pytest.approx(0.039661, abs=1e-6)
    nv = no_vig([-155, 450, 300])
    assert nv == pytest.approx([0.584655, 0.174882, 0.240463], abs=1e-6)
    assert sum(nv) == pytest.approx(1.0)
    assert implied_probability(100) == pytest.approx(0.5)


def test_clv():
    assert clv(2.10, 2.0) == pytest.approx(0.05)
    with pytest.raises(ValueError):
        clv(2.0, 1.0)
