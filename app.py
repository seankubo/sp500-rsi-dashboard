"""Streamlit app entry point for S&P 500 RSI dashboard."""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st

from api import send_dify_chat_message
from dashboard import build_portfolio_pie_figure, render_stock_section
from data import (
    apply_virtual_purchase,
    assert_account_owned_by_user,
    create_account,
    create_user_with_password,
    ensure_user_password,
    get_user_dify_api_key,
    fetch_price_data,
    authenticate_user,
    init_db,
    list_accounts,
    load_holdings,
    set_user_dify_api_key,
)
from indicators import calculate_rsi
from telemetry import get_telemetry_logger, setup_azure_monitor

DEFAULT_CHATBOT_SERVICE_URL = "https://api.dify.ai/v1"


def _resolve_database_url() -> str | None:
    env = os.getenv("DATABASE_URL", "").strip()
    if env:
        return env
    try:
        return str(st.secrets["DATABASE_URL"]).strip() or None
    except Exception:
        return None


def _resolve_dify_api_key(ui_value: str) -> str | None:
    if ui_value.strip():
        return ui_value.strip()
    env_key = os.getenv("DIFY_API_KEY", "").strip()
    if env_key:
        return env_key
    try:
        secret_key = str(st.secrets["DIFY_API_KEY"]).strip()
    except Exception:
        return None
    return secret_key or None


@st.cache_data(show_spinner=False)
def get_sp500_symbols() -> list[str]:
    """Fetch S&P 500 constituent symbols and format for Yahoo Finance."""
    # Use CSV source to avoid optional HTML parsers (lxml/bs4) on locked-down machines.
    table = pd.read_csv(
        "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
    )
    symbols = table["Symbol"].astype(str).str.replace(".", "-", regex=False).tolist()
    # Keep deterministic ordering and deduplicate.
    return list(dict.fromkeys(symbols))


@st.cache_data(show_spinner=False)
def get_top_rsi_symbols(symbols: list[str], top_n: int = 10) -> list[str]:
    """Return symbols sorted by highest latest RSI."""
    ranking: list[tuple[str, float]] = []
    for symbol in symbols:
        try:
            prices = fetch_price_data(symbol=symbol, period="6mo", interval="1d")
            rsi_series = calculate_rsi(prices["Close"], window=14).dropna()
            if not rsi_series.empty:
                ranking.append((symbol, float(rsi_series.iloc[-1])))
        except Exception:
            continue

    if not ranking:
        return symbols[:top_n]
    ranking.sort(key=lambda item: item[1], reverse=True)
    return [symbol for symbol, _ in ranking[:top_n]]


@st.dialog("Send message to chatbot")
def chatbot_modal(symbol: str, latest_rsi: float) -> None:
    st.markdown(
        """
        <style>
        div[data-testid="stDialog"] div[role="dialog"] {
            width: min(1100px, 95vw);
        }
        .difyStreamBox {
            max-height: 500vh;
            overflow-y: auto;
            padding: 12px 14px;
            border: 1px solid rgba(49, 51, 63, 0.2);
            border-radius: 10px;
            background: transparent;
            white-space: pre-wrap;
            overflow-wrap: anywhere;
            word-break: break-word;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.write(f"Symbol: `{symbol}`")
    st.caption(
        "Uses Dify **workflow** API (`/workflows/run`). Set `DIFY_API_KEY` in the environment "
        "or `.streamlit/secrets.toml`, or paste a key below."
    )

    default_endpoint = st.session_state.get("chatbot_endpoint") or os.getenv(
        "CHATBOT_SERVICE_URL", DEFAULT_CHATBOT_SERVICE_URL
    )
    endpoint = st.text_input(
        "Dify API base URL",
        value=default_endpoint,
        placeholder="https://api.dify.ai/v1",
    )
    # Per-user Dify key: stored in DB once used.
    current_user_id = int(st.session_state.get("auth_user_id") or 0)
    stored_key = None
    if current_user_id:
        try:
            stored_key = get_user_dify_api_key(current_user_id, database_url=_resolve_database_url())
        except Exception:
            stored_key = None
    api_key_input = st.text_input(
        "Dify API key (optional if configured elsewhere)",
        value=st.session_state.get("dify_api_key_input", stored_key or ""),
        type="password",
        help="Overrides DIFY_API_KEY / secrets for this session when non-empty.",
    )
    default_prompt = (
        "Generate a report with the given RSI\n\n"
        f"Symbol: {symbol}\n"
        f"RSI(14): {latest_rsi:.2f}\n"
    )
    message = st.text_area(
        "Prompt",
        height=140,
        value=default_prompt,
        placeholder="Write your question or instructions here...",
    )

    if st.button("Send", type="primary"):
        if not endpoint.strip():
            st.error("Please provide the Dify API base URL.")
            return
        if not message.strip():
            st.error("Please enter a message before sending.")
            return
        st.session_state["chatbot_endpoint"] = endpoint.strip()
        st.session_state["dify_api_key_input"] = api_key_input
        try:
            # Persist user-entered key to DB (per logged-in user).
            if current_user_id and api_key_input.strip():
                try:
                    set_user_dify_api_key(
                        current_user_id,
                        api_key_input,
                        database_url=_resolve_database_url(),
                    )
                except Exception:
                    pass
            user_tag = str(st.session_state.get("auth_user_id") or "streamlit-dashboard")
            stream = send_dify_chat_message(
                message.strip(),
                stock_list=symbol,
                base_url=endpoint.strip(),
                api_key=_resolve_dify_api_key(api_key_input or (stored_key or "")),
                user=user_tag,
                response_mode="streaming",
            )
            status = st.info("Streaming from Dify…")
            out = ""
            box = st.empty()
            for token in stream:
                out += token
                box.markdown(f"<div class='difyStreamBox'>{out}</div>", unsafe_allow_html=True)
            status.success("Completed.")
        except Exception as exc:
            log = get_telemetry_logger()
            if log:
                log.exception("Dify chat-messages request failed.")
            st.error(f"Failed to send message: {exc}")


@st.dialog("Virtual purchase")
def purchase_modal(
    symbol: str,
    latest_close: float,
    *,
    database_url: str,
    account_id: int,
) -> None:
    st.caption("Virtual only — no broker execution. Stored for the active account.")
    st.write(f"Symbol: `{symbol}`")
    st.metric("Latest close (USD)", f"${latest_close:,.4f}")
    qty = st.number_input(
        "Shares to buy",
        min_value=0.0001,
        value=1.0,
        step=0.1,
        format="%.4f",
    )
    est = float(qty) * float(latest_close)
    st.metric("Estimated cost (USD)", f"${est:,.2f}")
    if st.button("Purchase", type="primary"):
        try:
            apply_virtual_purchase(
                account_id,
                symbol,
                float(qty),
                float(latest_close),
                database_url=database_url,
            )
        except Exception as exc:
            st.error(f"Could not record purchase: {exc}")
            return
        st.success("Purchase recorded.")
        st.rerun()


def _logout() -> None:
    for k in ["auth_user_id", "auth_username", "active_account_id"]:
        if k in st.session_state:
            del st.session_state[k]


def _require_login(database_url: str) -> tuple[int, str]:
    """Render login/register UI if needed; return (user_id, username)."""
    uid = st.session_state.get("auth_user_id")
    uname = st.session_state.get("auth_username")
    if isinstance(uid, int) and isinstance(uname, str) and uname:
        return uid, uname

    st.subheader("Login")
    with st.form("login_form", clear_on_submit=False):
        username = st.text_input("Username", key="login_username")
        password = st.text_input("Password", type="password", key="login_password")
        submitted = st.form_submit_button("Login", type="primary")
    if submitted:
        user_id = authenticate_user(username, password, database_url=database_url)
        if user_id is None:
            st.error("Invalid username/password.")
        else:
            st.session_state["auth_user_id"] = int(user_id)
            st.session_state["auth_username"] = username.strip()
            st.rerun()

    st.divider()
    st.subheader("Register")
    with st.form("register_form", clear_on_submit=False):
        r_user = st.text_input("New username", key="reg_username")
        r_pass = st.text_input("New password", type="password", key="reg_password")
        r_pass2 = st.text_input("Confirm password", type="password", key="reg_password2")
        reg = st.form_submit_button("Create account", type="secondary")
    if reg:
        if not r_user.strip():
            st.error("Username required.")
        elif not r_pass:
            st.error("Password required.")
        elif r_pass != r_pass2:
            st.error("Passwords do not match.")
        else:
            try:
                new_id = create_user_with_password(
                    r_user.strip(), r_pass, database_url=database_url
                )
            except Exception as exc:
                st.error(str(exc))
            else:
                st.session_state["auth_user_id"] = int(new_id)
                st.session_state["auth_username"] = r_user.strip()
                st.success("User created.")
                st.rerun()

    # Do not stop the whole app; allow read-only browsing when logged out.
    return None, None


def _maybe_logged_in() -> tuple[int | None, str | None]:
    uid = st.session_state.get("auth_user_id")
    uname = st.session_state.get("auth_username")
    if isinstance(uid, int) and isinstance(uname, str) and uname:
        return uid, uname
    return None, None


def run() -> None:
    st.set_page_config(page_title="S&P 500 RSI Dashboard", layout="wide")
    setup_azure_monitor()
    st.title("S&P 500 RSI Dashboard")

    database_url = _resolve_database_url()
    if not database_url:
        st.error(
            "PostgreSQL is required. Set `DATABASE_URL` in the environment or "
            "`.streamlit/secrets.toml`."
        )
        return
    user_id: int | None = None
    username: str | None = None
    purchase_enabled = False
    init_db(database_url=database_url)

    with st.sidebar:
        st.subheader("Login")
        user_id, username = _maybe_logged_in()
        if user_id is None:
            st.caption("Sign in to enable **chat** and **virtual purchase**.")
            _require_login(database_url)
        else:
            st.caption(f"Signed in as `{username}`")
            if st.button("Logout", type="secondary"):
                _logout()
                st.rerun()
    purchase_enabled = user_id is not None

    try:
        all_symbols = get_sp500_symbols()
    except Exception as exc:
        st.warning(f"Could not fetch full S&P 500 symbol list: {exc}")
        all_symbols = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "JPM", "V", "XOM"]

    # Fast defaults: avoid scanning the entire S&P500 on app start.
    default_symbols = ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "JPM", "V", "XOM"]
    with st.sidebar:
        slow_defaults = st.toggle(
            "Use Top-RSI defaults (slow)",
            value=False,
            help="Scans many symbols to find highest RSI. This can take a long time.",
        )
    if slow_defaults:
        default_symbols = get_top_rsi_symbols(all_symbols, top_n=10)
    symbols = st.multiselect(
        "Select symbols",
        options=all_symbols,
        default=default_symbols,
        help="Defaults to a fast preselected list (enable Top-RSI defaults in the sidebar if needed).",
    )

    is_default_selection = (
        len(symbols) == len(default_symbols)
        and set(symbols) == set(default_symbols)
    )
    if is_default_selection:
        st.header("Top 10 RSI symbols")

    if not symbols:
        st.warning("Please select at least one symbol.")
        return

    active_account_id = None
    if user_id is not None:
        with st.sidebar:
            st.subheader("Portfolio account")
            active_account_id = st.session_state.get("active_account_id")
            accounts = list_accounts(user_id, database_url=database_url)
            acc_labels = [f"{n} ({aid})" for aid, n in accounts]
            acc_ids = [aid for aid, _ in accounts]
            if not acc_ids:
                st.info("No accounts yet. Create one below.")
            default_a_idx = (
                acc_ids.index(active_account_id) if active_account_id in acc_ids else 0
            )
            pick_a = st.selectbox(
                "Account",
                options=range(len(acc_ids)),
                format_func=lambda i: acc_labels[i],
                index=default_a_idx,
                key="pick_account_idx",
            )
            active_account_id = acc_ids[pick_a] if acc_ids else None
            st.session_state["active_account_id"] = active_account_id

            new_acc = st.text_input("New account name", key="new_account_name")
            if st.button("Create account", key="create_account_btn"):
                if not new_acc.strip():
                    st.error("Enter an account name.")
                else:
                    try:
                        create_account(user_id, new_acc.strip(), database_url=database_url)
                        st.success("Account created.")
                        st.rerun()
                    except Exception as exc:
                        st.error(str(exc))

    main_col, portfolio_col = st.columns([0.72, 0.28], gap="large")
    latest_close_by_symbol: dict[str, float] = {}

    with main_col:
        for symbol in symbols:
            with st.container():
                try:
                    prices = fetch_price_data(
                        symbol=symbol, period="1y", interval="1d", database_url=database_url
                    )
                    prices["RSI_14"] = calculate_rsi(prices["Close"], window=14)
                    close_series = prices["Close"].dropna()
                    if not close_series.empty:
                        latest_close_by_symbol[symbol] = float(close_series.iloc[-1])
                    rsi_series = prices["RSI_14"].dropna()
                    latest_rsi = float(rsi_series.iloc[-1]) if not rsi_series.empty else float("nan")
                    open_chat, open_purchase = render_stock_section(
                        prices.dropna(subset=["RSI_14"]),
                        symbol,
                        chat_enabled=user_id is not None,
                        purchase_enabled=purchase_enabled,
                    )
                    lc = latest_close_by_symbol.get(symbol)
                    if (
                        open_purchase
                        and purchase_enabled
                        and active_account_id is not None
                        and lc is not None
                    ):
                        try:
                            assert_account_owned_by_user(
                                int(active_account_id),
                                int(user_id),
                                database_url=database_url,
                            )
                        except Exception as exc:
                            st.error(str(exc))
                            continue
                        purchase_modal(
                            symbol,
                            lc,
                            database_url=database_url,
                            account_id=active_account_id,
                        )
                    elif open_chat and user_id is not None:
                        chatbot_modal(symbol, latest_rsi)
                except Exception as exc:  # pragma: no cover - UI fallback
                    log = get_telemetry_logger()
                    if log:
                        log.exception("Failed to load price data for %s.", symbol)
                    st.error(f"Could not load {symbol}: {exc}")

    with portfolio_col:
        st.subheader("Portfolio")
        if user_id is None:
            st.caption("Log in to view portfolio, buy stocks, and use chat.")
            return
        if active_account_id is None:
            st.caption("Select or create an account to view holdings.")
            return
        assert_account_owned_by_user(int(active_account_id), int(user_id), database_url=database_url)
        holdings = load_holdings(active_account_id, database_url=database_url)
        if holdings.empty:
            st.caption("No virtual holdings yet. Use the payments icon on a chart.")
        else:
            price_by_symbol = dict(latest_close_by_symbol)
            for sym in holdings["symbol"].astype(str).tolist():
                if sym in price_by_symbol:
                    continue
                try:
                    px = fetch_price_data(sym, period="5d", interval="1d", database_url=database_url)
                    cs = px["Close"].dropna()
                    if not cs.empty:
                        price_by_symbol[sym] = float(cs.iloc[-1])
                except Exception:
                    continue
            values: list[float] = []
            labels: list[str] = []
            total = 0.0
            for row in holdings.itertuples(index=False):
                sym = str(row.symbol)
                q = float(row.quantity)
                p = price_by_symbol.get(sym)
                if p is None:
                    continue
                v = q * p
                labels.append(sym)
                values.append(v)
                total += v
            st.metric("Total value (USD, mark-to-market)", f"${total:,.2f}")
            if labels and values and total > 0:
                st.plotly_chart(build_portfolio_pie_figure(labels, values), use_container_width=True)
            elif not labels:
                st.caption(
                    "Could not price holdings (market data unavailable). "
                    "Try again after prices load."
                )


if __name__ == "__main__":
    run()
