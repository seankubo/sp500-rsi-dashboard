import pandas as pd

from dashboard import build_rsi_figure


def test_build_rsi_figure_has_expected_trace_and_range():
    dates = pd.date_range("2026-01-01", periods=3, freq="D")
    data = pd.DataFrame({"RSI_14": [45.0, 50.0, 55.0]}, index=dates)
    fig = build_rsi_figure(data, "^GSPC")

    assert len(fig.data) == 1
    assert fig.data[0].name == "RSI (14-day)"
    assert fig.layout.yaxis.range == (0, 100)
