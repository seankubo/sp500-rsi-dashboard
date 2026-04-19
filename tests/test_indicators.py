import pandas as pd

from indicators import calculate_rsi


def test_calculate_rsi_returns_series_with_expected_bounds():
    close = pd.Series(
        [100, 101, 102, 103, 102, 101, 102, 104, 105, 106, 105, 104, 107, 109, 108, 110]
    )
    rsi = calculate_rsi(close, window=14).dropna()

    assert not rsi.empty
    assert (rsi >= 0).all()
    assert (rsi <= 100).all()


def test_calculate_rsi_rejects_empty_series():
    empty = pd.Series(dtype=float)
    try:
        calculate_rsi(empty, window=14)
    except ValueError as exc:
        assert "must not be empty" in str(exc)
    else:
        raise AssertionError("Expected ValueError for empty series")
