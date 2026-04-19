"""Plotting and Streamlit UI helpers."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


def build_rsi_figure(data: pd.DataFrame, symbol: str) -> go.Figure:
    """Build an RSI trend chart with overbought/oversold markers."""
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=data.index,
            y=data["RSI_14"],
            mode="lines",
            name="RSI (14-day)",
            line={"color": "#1f77b4", "width": 2},
            hovertemplate="Date: %{x|%Y-%m-%d}<br>RSI: %{y:.2f}<extra></extra>",
        )
    )

    fig.add_hline(y=70, line_dash="dash", line_color="red")
    fig.add_hline(y=30, line_dash="dash", line_color="green")

    fig.update_layout(
        title=f"{symbol} RSI Trend",
        xaxis_title="Date",
        yaxis_title="RSI",
        yaxis={"range": [0, 100]},
        hovermode="x unified",
        template="plotly_white",
        height=350,
        margin={"l": 40, "r": 20, "t": 30, "b": 40},
    )
    return fig


def render_stock_section(
    data: pd.DataFrame,
    symbol: str,
    *,
    chat_enabled: bool = True,
    purchase_enabled: bool = True,
) -> tuple[bool, bool]:
    """Title row; currency opens purchase modal when enabled; chatbot opens chat modal when enabled."""
    if purchase_enabled and chat_enabled:
        title_col, purchase_col, chat_col = st.columns(
            [1, 0.07, 0.07], vertical_alignment="center"
        )
    elif purchase_enabled and not chat_enabled:
        title_col, purchase_col = st.columns([1, 0.08], vertical_alignment="center")
        chat_col = None
    elif (not purchase_enabled) and chat_enabled:
        title_col, chat_col = st.columns([1, 0.08], vertical_alignment="center")
        purchase_col = None
    else:
        title_col = st.columns([1], vertical_alignment="center")[0]
        purchase_col = None
        chat_col = None
    with title_col:
        st.markdown(f"### {symbol} RSI Trend")
    open_purchase = False
    if purchase_col is not None:
        with purchase_col:
            open_purchase = st.button(
                "",
                key=f"open_purchase_{symbol}",
                icon=":material/payments:",
                help="Virtual purchase",
                type="tertiary",
            )
    open_chat = False
    if chat_col is not None:
        with chat_col:
            open_chat = st.button(
                "",
                key=f"open_chat_{symbol}",
                icon=":material/smart_toy:",
                help="Open chatbot",
                type="tertiary",
            )
    st.plotly_chart(build_rsi_figure(data, symbol), use_container_width=True)
    return open_chat, open_purchase


def build_portfolio_pie_figure(labels: list[str], values: list[float]) -> go.Figure:
    """Allocation pie chart by current market value."""
    fig = go.Figure(
        data=[
            go.Pie(
                labels=labels,
                values=values,
                hole=0.35,
                textinfo="label+percent",
            )
        ]
    )
    fig.update_layout(
        title="Holdings by value",
        template="plotly_white",
        height=320,
        margin={"l": 20, "r": 20, "t": 40, "b": 20},
        showlegend=True,
    )
    return fig
