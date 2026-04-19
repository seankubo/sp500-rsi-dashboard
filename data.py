"""Data access helpers for market prices and portfolios (PostgreSQL only)."""

from __future__ import annotations

import os
import hashlib
import hmac
import secrets

import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

PRICE_COLUMNS = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]

_PG_INIT = """
CREATE TABLE IF NOT EXISTS prices (
    symbol TEXT NOT NULL,
    date DATE NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    adj_close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    PRIMARY KEY (symbol, date)
)
"""

_PG_USERS = """
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_salt BYTEA,
    password_hash BYTEA,
    dify_api_key TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
"""

_PG_ACCOUNTS = """
CREATE TABLE IF NOT EXISTS accounts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    base_currency TEXT NOT NULL DEFAULT 'USD',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, name)
)
"""

_PG_HOLDINGS = """
CREATE TABLE IF NOT EXISTS holdings (
    account_id INTEGER NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    symbol TEXT NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    avg_cost DOUBLE PRECISION NOT NULL,
    invested_total DOUBLE PRECISION NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, symbol)
)
"""


def _effective_database_url(explicit: str | None) -> str | None:
    """Return Postgres URL from explicit arg, else DATABASE_URL env."""
    if explicit is not None:
        return explicit.strip() or None
    return os.getenv("DATABASE_URL", "").strip() or None


def _pg_engine(database_url: str) -> Engine:
    return create_engine(database_url, pool_pre_ping=True)


def init_db(
    *,
    database_url: str | None = None,
) -> None:
    """Create schema for market data cache and portfolio tables (PostgreSQL)."""
    url = _effective_database_url(database_url)
    if not url:
        raise ValueError("DATABASE_URL is required (PostgreSQL only).")
    engine = _pg_engine(url)
    with engine.begin() as conn:
        conn.execute(text(_PG_INIT))
        conn.execute(text(_PG_USERS))
        conn.execute(text(_PG_ACCOUNTS))
        conn.execute(text(_PG_HOLDINGS))
        # Backward-compatible migration if the table existed before auth was added.
        conn.execute(
            text("ALTER TABLE users ADD COLUMN IF NOT EXISTS password_salt BYTEA")
        )
        conn.execute(
            text("ALTER TABLE users ADD COLUMN IF NOT EXISTS password_hash BYTEA")
        )
        conn.execute(text("ALTER TABLE users ADD COLUMN IF NOT EXISTS dify_api_key TEXT"))
    engine.dispose()


def _hash_password(password: str, *, salt: bytes | None = None) -> tuple[bytes, bytes]:
    if salt is None:
        salt = secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000)
    return salt, dk


def create_user_with_password(
    username: str,
    password: str,
    *,
    database_url: str | None = None,
) -> int:
    """Create a user with a password and return user id."""
    name = username.strip()
    if not name:
        raise ValueError("username must be non-empty")
    if not password:
        raise ValueError("password must be non-empty")
    url = _effective_database_url(database_url)
    if not url:
        raise ValueError("DATABASE_URL is required (PostgreSQL only).")
    salt, ph = _hash_password(password)
    engine = _pg_engine(url)
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                INSERT INTO users (username, password_salt, password_hash)
                VALUES (:u, :s, :h)
                RETURNING id
                """
            ),
            {"u": name, "s": salt, "h": ph},
        ).first()
        uid = int(row[0]) if row else 0
    engine.dispose()
    return uid


def authenticate_user(
    username: str,
    password: str,
    *,
    database_url: str | None = None,
) -> int | None:
    """Return user id if credentials are valid; otherwise None."""
    name = username.strip()
    if not name or not password:
        return None
    url = _effective_database_url(database_url)
    if not url:
        raise ValueError("DATABASE_URL is required (PostgreSQL only).")
    engine = _pg_engine(url)
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, password_salt, password_hash
                FROM users
                WHERE username = :u
                """
            ),
            {"u": name},
        ).first()
    engine.dispose()
    if not row:
        return None
    uid = int(row[0])
    salt = row[1]
    stored = row[2]
    if salt is None or stored is None:
        return None
    _, candidate = _hash_password(password, salt=bytes(salt))
    if hmac.compare_digest(bytes(stored), candidate):
        return uid
    return None


def get_user_dify_api_key(
    user_id: int,
    *,
    database_url: str | None = None,
) -> str | None:
    """Return the stored Dify API key for user_id, if set."""
    url = _effective_database_url(database_url)
    if not url:
        raise ValueError("DATABASE_URL is required (PostgreSQL only).")
    engine = _pg_engine(url)
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT dify_api_key FROM users WHERE id = :id"),
            {"id": int(user_id)},
        ).first()
    engine.dispose()
    if not row:
        return None
    value = row[0]
    if value is None:
        return None
    return str(value)


def set_user_dify_api_key(
    user_id: int,
    dify_api_key: str,
    *,
    database_url: str | None = None,
) -> None:
    """Persist Dify API key for user_id."""
    key = dify_api_key.strip()
    if not key:
        raise ValueError("dify_api_key must be non-empty")
    url = _effective_database_url(database_url)
    if not url:
        raise ValueError("DATABASE_URL is required (PostgreSQL only).")
    engine = _pg_engine(url)
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE users SET dify_api_key = :k WHERE id = :id"),
            {"k": key, "id": int(user_id)},
        )
    engine.dispose()


def assert_account_owned_by_user(
    account_id: int,
    user_id: int,
    *,
    database_url: str | None = None,
) -> None:
    """Raise if account_id does not belong to user_id."""
    url = _effective_database_url(database_url)
    if not url:
        raise ValueError("DATABASE_URL is required (PostgreSQL only).")
    engine = _pg_engine(url)
    with engine.connect() as conn:
        ok = conn.execute(
            text("SELECT 1 FROM accounts WHERE id = :aid AND user_id = :uid"),
            {"aid": account_id, "uid": user_id},
        ).first()
    engine.dispose()
    if not ok:
        raise PermissionError("Account access denied.")


def ensure_user_password(
    username: str,
    password: str,
    *,
    database_url: str | None = None,
    overwrite: bool = False,
) -> int:
    """
    Ensure a user exists and has a password.

    - If user does not exist: create with password.
    - If user exists but has no password set: set password.
    - If overwrite=True: reset password even if already set.

    Returns the user id.
    """
    name = username.strip()
    if not name:
        raise ValueError("username must be non-empty")
    if not password:
        raise ValueError("password must be non-empty")
    url = _effective_database_url(database_url)
    if not url:
        raise ValueError("DATABASE_URL is required (PostgreSQL only).")

    salt, ph = _hash_password(password)
    engine = _pg_engine(url)
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, password_hash
                FROM users
                WHERE username = :u
                """
            ),
            {"u": name},
        ).first()

        if row is None:
            uid = int(
                conn.execute(
                    text(
                        """
                        INSERT INTO users (username, password_salt, password_hash)
                        VALUES (:u, :s, :h)
                        RETURNING id
                        """
                    ),
                    {"u": name, "s": salt, "h": ph},
                ).scalar_one()
            )
            engine.dispose()
            return uid

        uid = int(row[0])
        existing_hash = row[1]
        if existing_hash is None or overwrite:
            conn.execute(
                text(
                    """
                    UPDATE users
                    SET password_salt = :s,
                        password_hash = :h
                    WHERE id = :id
                    """
                ),
                {"s": salt, "h": ph, "id": uid},
            )
    engine.dispose()
    return uid


def _normalize_downloaded_data(data: pd.DataFrame) -> pd.DataFrame:
    """Normalize yfinance data shape to a single-index OHLCV frame."""
    frame = data.copy()
    if isinstance(frame.columns, pd.MultiIndex):
        frame.columns = frame.columns.get_level_values(0)
    frame.index = pd.to_datetime(frame.index)
    for col in PRICE_COLUMNS:
        if col not in frame.columns:
            frame[col] = pd.NA
    return frame[PRICE_COLUMNS].sort_index()


def _rows_for_insert(symbol: str, normalized: pd.DataFrame) -> list[dict[str, object]]:
    out = _normalize_downloaded_data(normalized).reset_index().rename(columns={"index": "Date"})
    out["symbol"] = symbol
    out = out.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
    )
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
    return out[
        ["symbol", "date", "open", "high", "low", "close", "adj_close", "volume"]
    ].to_dict("records")


_PG_UPSERT = text(
    """
    INSERT INTO prices(symbol, date, open, high, low, close, adj_close, volume)
    VALUES(:symbol, :date, :open, :high, :low, :close, :adj_close, :volume)
    ON CONFLICT (symbol, date) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        adj_close = EXCLUDED.adj_close,
        volume = EXCLUDED.volume
    """
)


def save_price_data(
    symbol: str,
    data: pd.DataFrame,
    *,
    database_url: str | None = None,
) -> None:
    """Persist downloaded prices (PostgreSQL)."""
    records = _rows_for_insert(symbol, data)
    url = _effective_database_url(database_url)
    if not url:
        raise ValueError("DATABASE_URL is required (PostgreSQL only).")
    engine = _pg_engine(url)
    with engine.begin() as conn:
        for row in records:
            conn.execute(_PG_UPSERT, row)
    engine.dispose()

_LOAD_PG = text(
    """
    SELECT
        date AS "Date",
        open AS "Open",
        high AS "High",
        low AS "Low",
        close AS "Close",
        adj_close AS "Adj Close",
        volume AS "Volume"
    FROM prices
    WHERE symbol = :symbol
    ORDER BY date
    """
)


def load_price_data(
    symbol: str,
    *,
    database_url: str | None = None,
) -> pd.DataFrame:
    """Load cached prices for symbol (PostgreSQL)."""
    url = _effective_database_url(database_url)
    if not url:
        raise ValueError("DATABASE_URL is required (PostgreSQL only).")
    engine = _pg_engine(url)
    with engine.connect() as conn:
        frame = pd.read_sql_query(_LOAD_PG, conn, params={"symbol": symbol})
    engine.dispose()
    if frame.empty:
        return frame
    frame["Date"] = pd.to_datetime(frame["Date"])
    return frame.set_index("Date")


def fetch_price_data(
    symbol: str = "^GSPC",
    period: str = "1y",
    interval: str = "1d",
    *,
    database_url: str | None = None,
) -> pd.DataFrame:
    """Fetch OHLCV data, persist to Postgres, fallback to cached prices in Postgres."""
    try:
        data = yf.download(symbol, period=period, interval=interval, progress=False)
    except Exception:
        data = pd.DataFrame()

    if not data.empty:
        normalized = _normalize_downloaded_data(data)
        save_price_data(
            symbol,
            normalized,
            database_url=database_url,
        )
        return normalized

    cached = load_price_data(symbol, database_url=database_url)
    if cached.empty:
        raise ValueError(f"No data returned for symbol: {symbol}")
    return cached


def list_accounts(
    user_id: int,
    *,
    database_url: str | None = None,
) -> list[tuple[int, str]]:
    """Return (id, name) for accounts belonging to user_id."""
    url = _effective_database_url(database_url)
    if not url:
        raise ValueError("DATABASE_URL is required (PostgreSQL only).")
    engine = _pg_engine(url)
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, name FROM accounts WHERE user_id = :uid ORDER BY id"),
            {"uid": user_id},
        ).fetchall()
    engine.dispose()
    return [(int(r[0]), str(r[1])) for r in rows]


def create_account(
    user_id: int,
    account_name: str,
    *,
    base_currency: str = "USD",
    database_url: str | None = None,
) -> int:
    """Insert an account and return id."""
    aname = account_name.strip()
    if not aname:
        raise ValueError("account name must be non-empty")
    url = _effective_database_url(database_url)
    if not url:
        raise ValueError("DATABASE_URL is required (PostgreSQL only).")
    engine = _pg_engine(url)
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                INSERT INTO accounts (user_id, name, base_currency)
                VALUES (:uid, :n, :bc)
                RETURNING id
                """
            ),
            {"uid": user_id, "n": aname, "bc": base_currency},
        ).first()
        aid = int(row[0]) if row else 0
    engine.dispose()
    return aid


def get_or_create_account_by_name(
    user_id: int,
    account_name: str,
    *,
    base_currency: str = "USD",
    database_url: str | None = None,
) -> int:
    """Return account id, creating the row if missing."""
    aname = account_name.strip()
    if not aname:
        raise ValueError("account name must be non-empty")
    url = _effective_database_url(database_url)
    if not url:
        raise ValueError("DATABASE_URL is required (PostgreSQL only).")
    engine = _pg_engine(url)
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT id FROM accounts WHERE user_id = :uid AND name = :n"),
            {"uid": user_id, "n": aname},
        ).first()
        if row:
            aid = int(row[0])
        else:
            aid = int(
                conn.execute(
                    text(
                        """
                        INSERT INTO accounts (user_id, name, base_currency)
                        VALUES (:uid, :n, :bc)
                        RETURNING id
                        """
                    ),
                    {"uid": user_id, "n": aname, "bc": base_currency},
                ).scalar_one()
            )
    engine.dispose()
    return aid


def load_holdings(
    account_id: int,
    *,
    database_url: str | None = None,
) -> pd.DataFrame:
    """Load holdings for an account as a DataFrame."""
    url = _effective_database_url(database_url)
    if not url:
        raise ValueError("DATABASE_URL is required (PostgreSQL only).")
    engine = _pg_engine(url)
    with engine.connect() as conn:
        frame = pd.read_sql_query(
            text(
                """
                SELECT symbol, quantity, avg_cost, invested_total
                FROM holdings
                WHERE account_id = :aid
                ORDER BY symbol
                """
            ),
            conn,
            params={"aid": account_id},
        )
    engine.dispose()
    return frame


def apply_virtual_purchase(
    account_id: int,
    symbol: str,
    quantity: float,
    unit_price: float,
    *,
    database_url: str | None = None,
) -> None:
    """Accumulate shares and weighted-average cost for a virtual purchase."""
    if quantity <= 0:
        raise ValueError("quantity must be positive")
    if unit_price <= 0:
        raise ValueError("unit_price must be positive")
    sym = symbol.strip().upper()
    if not sym:
        raise ValueError("symbol must be non-empty")

    add_cost = float(quantity) * float(unit_price)
    url = _effective_database_url(database_url)
    if not url:
        raise ValueError("DATABASE_URL is required (PostgreSQL only).")
    engine = _pg_engine(url)
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT quantity, avg_cost, invested_total
                FROM holdings
                WHERE account_id = :aid AND symbol = :s
                """
            ),
            {"aid": account_id, "s": sym},
        ).mappings().first()
        if row is None:
            conn.execute(
                text(
                    """
                    INSERT INTO holdings (
                        account_id, symbol, quantity, avg_cost, invested_total
                    )
                    VALUES (:aid, :s, :q, :ac, :inv)
                    """
                ),
                {
                    "aid": account_id,
                    "s": sym,
                    "q": float(quantity),
                    "ac": float(unit_price),
                    "inv": add_cost,
                },
            )
        else:
            q0 = float(row["quantity"])
            inv0 = float(row["invested_total"])
            q1 = q0 + float(quantity)
            inv1 = inv0 + add_cost
            avg1 = inv1 / q1 if q1 else float(unit_price)
            conn.execute(
                text(
                    """
                    UPDATE holdings
                    SET quantity = :q,
                        avg_cost = :ac,
                        invested_total = :inv,
                        updated_at = NOW()
                    WHERE account_id = :aid AND symbol = :s
                    """
                ),
                {
                    "q": q1,
                    "ac": avg1,
                    "inv": inv1,
                    "aid": account_id,
                    "s": sym,
                },
            )
    engine.dispose()
