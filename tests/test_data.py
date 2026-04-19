"""Unit tests for `data.py` (PostgreSQL-only).

These tests mock SQLAlchemy engines/connections so a live Postgres instance is not required.
"""

from unittest.mock import MagicMock, patch

import pandas as pd

from data import (
    authenticate_user,
    create_user_with_password,
    fetch_price_data,
    init_db,
)


@patch("data.yf.download")
def test_fetch_price_data_returns_dataframe_and_persists(mock_download):
    index = pd.date_range("2026-01-01", periods=2, freq="D")
    mock_download.return_value = pd.DataFrame(
        {
            "Open": [5990.0, 6010.0],
            "High": [6010.0, 6030.0],
            "Low": [5980.0, 6000.0],
            "Close": [6000.0, 6020.0],
            "Adj Close": [6000.0, 6020.0],
            "Volume": [1_000_000, 1_200_000],
        },
        index=index,
    )

    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    engine.connect.return_value.__enter__.return_value = conn

    with patch("data._pg_engine", return_value=engine), patch(
        "data.pd.read_sql_query", return_value=pd.DataFrame()
    ):
        result = fetch_price_data("^GSPC", "1mo", "1d", database_url="postgresql+psycopg://x")

    assert not result.empty
    assert "Close" in result.columns


@patch("data.yf.download")
def test_fetch_price_data_reads_postgres_fallback_when_empty_download(mock_download):
    index = pd.date_range("2026-01-01", periods=2, freq="D")
    first_payload = pd.DataFrame(
        {
            "Open": [5990.0, 6010.0],
            "High": [6010.0, 6030.0],
            "Low": [5980.0, 6000.0],
            "Close": [6000.0, 6020.0],
            "Adj Close": [6000.0, 6020.0],
            "Volume": [1_000_000, 1_200_000],
        },
        index=index,
    )
    mock_download.side_effect = [first_payload, pd.DataFrame()]

    engine = MagicMock()
    conn = MagicMock()
    engine.begin.return_value.__enter__.return_value = conn
    engine.connect.return_value.__enter__.return_value = conn

    # On fallback (empty download), load_price_data should return non-empty DataFrame.
    fallback_frame = pd.DataFrame(
        {"Date": index, "Open": [1.0, 2.0], "High": [1.0, 2.0], "Low": [1.0, 2.0], "Close": [1.0, 2.0], "Adj Close": [1.0, 2.0], "Volume": [1.0, 2.0]}
    )
    with patch("data._pg_engine", return_value=engine), patch(
        "data.pd.read_sql_query", return_value=fallback_frame
    ):
        fetch_price_data("^GSPC", "1mo", "1d", database_url="postgresql+psycopg://x")
        fallback_result = fetch_price_data("^GSPC", "1mo", "1d", database_url="postgresql+psycopg://x")

    assert not fallback_result.empty
    assert "Close" in fallback_result.columns


@patch("data.yf.download")
def test_fetch_price_data_raises_when_empty_and_no_cache(mock_download):
    mock_download.return_value = pd.DataFrame()
    try:
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__.return_value = conn
        engine.connect.return_value.__enter__.return_value = conn
        with patch("data._pg_engine", return_value=engine), patch(
            "data.pd.read_sql_query", return_value=pd.DataFrame()
        ):
            fetch_price_data("^GSPC", "1mo", "1d", database_url="postgresql+psycopg://x")
    except ValueError as exc:
        assert "No data returned" in str(exc)
    else:
        raise AssertionError("Expected ValueError for empty download result")

def test_init_db_requires_database_url():
    try:
        init_db(database_url=None)
    except ValueError as exc:
        assert "DATABASE_URL is required" in str(exc)
    else:
        raise AssertionError("Expected ValueError when DATABASE_URL is missing")


def test_authenticate_user_returns_none_on_empty_inputs():
    assert authenticate_user("", "x", database_url="postgresql+psycopg://x") is None
    assert authenticate_user("u", "", database_url="postgresql+psycopg://x") is None


def test_create_user_with_password_requires_password():
    try:
        create_user_with_password("u", "", database_url="postgresql+psycopg://x")
    except ValueError as exc:
        assert "password must be non-empty" in str(exc)
    else:
        raise AssertionError("Expected ValueError for empty password")
