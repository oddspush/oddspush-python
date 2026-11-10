"""Odds arithmetic: American and decimal odds, implied probability, margin and CLV.

The API sends prices in American odds and, with ``fair`` on, the no-vig and fair prices
already worked out. These helpers cover the rest: converting your own prices and
measuring your closing line value.
"""

from __future__ import annotations

from typing import Iterable, List


def american_to_decimal(american: float) -> float:
    """``-155`` -> ``1.6452``; ``+450`` -> ``5.5``. ``|american|`` must be at least 100."""
    a = float(american)
    if a >= 100:
        return 1.0 + a / 100.0
    if a <= -100:
        return 1.0 + 100.0 / -a
    raise ValueError(f"not valid American odds: {american!r}")


def decimal_to_american(decimal: float) -> float:
    """``2.5`` -> ``150``; ``1.5`` -> ``-200``. ``decimal`` must be above 1."""
    d = float(decimal)
    if d <= 1.0:
        raise ValueError(f"not valid decimal odds: {decimal!r}")
    if d >= 2.0:
        return (d - 1.0) * 100.0
    return -100.0 / (d - 1.0)


def implied_probability(american: float) -> float:
    """The probability the price implies, margin included: ``1 / decimal odds``."""
    return 1.0 / american_to_decimal(american)


def margin(prices: Iterable[float]) -> float:
    """The bookmaker's margin of a market, from the American prices of all its legs:
    the sum of their implied probabilities minus 1."""
    return sum(implied_probability(p) for p in prices) - 1.0


def no_vig(prices: Iterable[float]) -> List[float]:
    """No-vig probabilities, multiplicative method: each implied probability divided by
    their sum. The API sends these as ``no_vig`` (with ``fair`` on)."""
    implied = [implied_probability(p) for p in prices]
    total = sum(implied)
    return [q / total for q in implied]


def clv(decimal_odds: float, closing_decimal: float) -> float:
    """Closing line value of a bet: ``decimal_odds / closing_decimal - 1``.

    ``closing_decimal`` is usually the ``fair_decimal`` of the same market and leg in the
    closing line. ``0.05`` means you beat the close by 5 %.
    """
    if closing_decimal <= 1.0:
        raise ValueError(f"not valid decimal odds: {closing_decimal!r}")
    return float(decimal_odds) / float(closing_decimal) - 1.0
