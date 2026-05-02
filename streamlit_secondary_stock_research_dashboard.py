from __future__ import annotations

from html import escape
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "5_output"
FEATURE_PATH = ROOT / "4_ready_for_analysis" / "tickerlist.xlsx"

KEY_FIELDS = [
    "valuation_gap",
    "roic",
    "rev_growth_2025",
    "ebit_growth_2025",
    "debt_to_assets",
    "ev_to_ebitda",
    "price_to_earnings",
    "price_to_book",
    "earnings_surprise",
    "stock_volatility",
    "3_month",
    "google_trends",
]

VALUATION_FIELDS = ["price_to_book", "ev_to_ebitda", "price_to_earnings", "valuation_gap"]

MODEL_TENDENCY = {
    "price_to_book": "High has been favorable in this run",
    "ev_to_ebitda": "High has been favorable, likely mixed with growth/quality",
    "price_to_earnings": "Weak/mixed; high is mildly favorable",
    "valuation_gap": "Incomplete and weak signal so far",
}

DISPLAY_NAMES = {
    "ticker": "Ticker",
    "name": "Name",
    "country": "Country",
    "sector": "Sector",
    "industry": "Industry",
    "xgb_rank": "XGB rank",
    "xgb_prediction_raw": "XGB predicted",
    "linear_rank": "Linear rank",
    "linear_prediction_raw": "Linear predicted",
    "rank_gap": "Linear rank minus XGB rank",
    "returns_2025": "Actual 2025 return",
    "valuation_gap": "Valuation gap",
    "roic": "ROIC",
    "rev_growth_2025": "Revenue growth 2025",
    "ebit_growth_2025": "EBIT growth 2025",
    "debt_to_assets": "Debt/assets",
    "ev_to_ebitda": "EV/EBITDA",
    "price_to_earnings": "P/E",
    "price_to_book": "P/B",
    "earnings_surprise": "Earnings surprise",
    "stock_volatility": "Stock volatility",
    "3_month": "3 month momentum",
    "google_trends": "Google Trends",
    "missing_key_fields": "Missing key fields",
    "missing_key_count": "Missing key count",
    "price_to_book_position": "P/B position",
    "ev_to_ebitda_position": "EV/EBITDA position",
    "price_to_earnings_position": "P/E position",
    "valuation_gap_position": "Valuation gap position",
    "momentum_read": "Momentum read",
}


st.set_page_config(
    page_title="Secondary Stock Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .block-container { padding-top: 1rem; padding-bottom: 2rem; }
    [data-testid="stMetricValue"] { font-size: 1.25rem; }
    [data-testid="stMetricLabel"] { font-size: 0.8rem; }
    div[data-testid="stDataFrame"] { border: 1px solid #d9dee7; border-radius: 4px; }
    .header {
        border-left: 6px solid #2563eb;
        background: #171923;
        padding: 1rem 1.2rem;
        margin-bottom: 1rem;
    }
    .header-title { font-size: 1.8rem; font-weight: 750; color: #f8fafc; }
    .header-subtitle { color: #b8c0cc; margin-top: 0.2rem; }
    .section { border-top: 1px solid #d9dee7; padding-top: 0.8rem; margin-top: 1rem; }
    .metric-grid {
        display: grid;
        grid-template-columns: repeat(6, minmax(0, 1fr));
        gap: 0.75rem;
        margin: 0.75rem 0 0.4rem 0;
    }
    .metric-card {
        min-height: 78px;
        border: 1px solid #303642;
        border-radius: 4px;
        background: #171923;
        padding: 0.72rem 0.82rem;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }
    .metric-label {
        color: #aab2bf;
        font-size: 0.78rem;
        line-height: 1.1rem;
    }
    .metric-value {
        color: #f8fafc;
        font-size: 1.15rem;
        font-weight: 720;
        line-height: 1.35rem;
        overflow-wrap: anywhere;
    }
    .notice {
        border: 1px solid #6b5d22;
        border-left: 5px solid #d6b22a;
        border-radius: 4px;
        background: #24210f;
        color: #f8e7a1;
        padding: 0.85rem 1rem;
        margin: 0.8rem 0 1rem 0;
    }
    @media (max-width: 1100px) {
        .metric-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    }
    @media (max-width: 700px) {
        .metric-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def cache_token(path: Path) -> float | None:
    return path.stat().st_mtime if path.exists() else None


@st.cache_data(show_spinner=False)
def read_csv(path_text: str, token: float | None) -> pd.DataFrame:
    path = Path(path_text)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


@st.cache_data(show_spinner=False)
def read_excel(path_text: str, token: float | None) -> pd.DataFrame:
    path = Path(path_text)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_excel(path)


def pct(value: object) -> str:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return "-"
    return f"{float(number) * 100:.1f}%"


def metric_cards(items: list[tuple[str, str]]) -> str:
    cards = []
    for label, value in items:
        cards.append(
            "<div class='metric-card'>"
            f"<div class='metric-label'>{escape(label)}</div>"
            f"<div class='metric-value'>{escape(value)}</div>"
            "</div>"
        )
    return "<div class='metric-grid'>" + "".join(cards) + "</div>"


def numeric_value(value: object) -> float | None:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return None if pd.isna(number) else float(number)


def percentile_rank(series: pd.Series, value: object) -> float | None:
    number = numeric_value(value)
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if number is None or numeric.empty:
        return None
    return float((numeric <= number).mean() * 100)


def position_label(percentile: float | None) -> str:
    if percentile is None:
        return "Missing"
    if percentile < 33:
        return "Low"
    if percentile <= 66:
        return "Middle"
    return "High"


def momentum_read(row: pd.Series, df: pd.DataFrame) -> str:
    price_pct = percentile_rank(df["3_month"], row.get("3_month")) if "3_month" in df.columns else None
    trends_pct = percentile_rank(df["google_trends"], row.get("google_trends")) if "google_trends" in df.columns else None
    if price_pct is None and trends_pct is None:
        return "Missing"
    if price_pct is None:
        combined = trends_pct
        basis = "Google Trends only"
    elif trends_pct is None:
        combined = price_pct
        basis = "3M only"
    else:
        combined = (price_pct + trends_pct) / 2
        basis = "3M + Trends"
    if combined >= 70:
        label = "Strong"
    elif combined >= 40:
        label = "Mixed"
    else:
        label = "Weak"
    return f"{label} ({basis})"


def column_config(columns: list[str]) -> dict[str, object]:
    config: dict[str, object] = {}
    for col in columns:
        label = DISPLAY_NAMES.get(col, col)
        if col in {"xgb_prediction_raw", "linear_prediction_raw", "returns_2025"}:
            config[col] = st.column_config.NumberColumn(label, format="%.1%")
        elif col in {"xgb_rank", "linear_rank", "rank_gap", "missing_key_count"}:
            config[col] = st.column_config.NumberColumn(label, format="%.0f")
        elif col in KEY_FIELDS:
            config[col] = st.column_config.NumberColumn(label, format="%.3f")
        else:
            config[col] = label
    return config


def build_universe(xgb: pd.DataFrame, linear: pd.DataFrame, features: pd.DataFrame) -> pd.DataFrame:
    if xgb.empty:
        return pd.DataFrame()

    base = xgb.rename(
        columns={
            "prediction_raw": "xgb_prediction_raw",
            "prediction_rank": "xgb_rank",
        }
    ).copy()

    if not linear.empty:
        linear_cols = [col for col in ["ticker", "prediction_raw", "prediction_rank"] if col in linear.columns]
        linear_part = linear[linear_cols].rename(
            columns={
                "prediction_raw": "linear_prediction_raw",
                "prediction_rank": "linear_rank",
            }
        )
        base = base.merge(linear_part, on="ticker", how="left")

    if not features.empty and "ticker" in features.columns:
        feature_cols = ["ticker", *[col for col in KEY_FIELDS if col in features.columns]]
        base = base.merge(features[feature_cols], on="ticker", how="left", suffixes=("", "_feature"))

    for col in base.columns:
        if col not in {"ticker", "name", "country", "sector", "industry", "model_split"}:
            numeric = pd.to_numeric(base[col], errors="coerce")
            if numeric.notna().any():
                base[col] = numeric

    if {"linear_rank", "xgb_rank"}.issubset(base.columns):
        base["rank_gap"] = base["linear_rank"] - base["xgb_rank"]

    present_fields = [col for col in KEY_FIELDS if col in base.columns]
    base["missing_key_count"] = base[present_fields].isna().sum(axis=1)
    base["missing_key_fields"] = base[present_fields].apply(
        lambda row: ", ".join(DISPLAY_NAMES.get(col, col) for col, value in row.items() if pd.isna(value)),
        axis=1,
    )
    base.loc[base["missing_key_fields"] == "", "missing_key_fields"] = "None"

    for col in VALUATION_FIELDS:
        if col in base.columns:
            base[f"{col}_position"] = [
                position_label(percentile_rank(base[col], value))
                for value in base[col]
            ]
    base["momentum_read"] = [momentum_read(row, base) for _, row in base.iterrows()]
    return base


def filter_universe(df: pd.DataFrame, countries: list[str], sectors: list[str], industries: list[str], search: str) -> pd.DataFrame:
    out = df.copy()
    if countries and "country" in out.columns:
        out = out[out["country"].isin(countries)]
    if sectors and "sector" in out.columns:
        out = out[out["sector"].isin(sectors)]
    if industries and "industry" in out.columns:
        out = out[out["industry"].isin(industries)]
    if search:
        haystack = out.get("ticker", pd.Series("", index=out.index)).astype(str) + " " + out.get("name", pd.Series("", index=out.index)).astype(str)
        out = out[haystack.str.contains(search, case=False, na=False)]
    return out


def ranked_table(df: pd.DataFrame, limit: int) -> pd.DataFrame:
    cols = [
        "ticker",
        "name",
        "country",
        "sector",
        "industry",
        "xgb_rank",
        "xgb_prediction_raw",
        "returns_2025",
        "linear_rank",
        "linear_prediction_raw",
        "rank_gap",
        "valuation_gap",
        "roic",
        "rev_growth_2025",
        "ebit_growth_2025",
        "price_to_earnings",
        "price_to_earnings_position",
        "ev_to_ebitda",
        "ev_to_ebitda_position",
        "price_to_book",
        "price_to_book_position",
        "3_month",
        "google_trends",
        "momentum_read",
        "missing_key_count",
        "missing_key_fields",
    ]
    visible = [col for col in cols if col in df.columns]
    return df.sort_values("xgb_rank", na_position="last")[visible].head(limit)


def valuation_table(row: pd.Series, peers: pd.DataFrame, universe_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for col in VALUATION_FIELDS:
        if col not in row.index:
            continue
        value = row.get(col)
        universe_pct = percentile_rank(universe_df[col], value) if col in universe_df.columns else None
        peer_pct = percentile_rank(peers[col], value) if col in peers.columns else None
        rows.append(
            {
                "field": DISPLAY_NAMES.get(col, col),
                "value": value,
                "universe_position": position_label(universe_pct),
                "peer_position": position_label(peer_pct),
                "model_tendency": MODEL_TENDENCY.get(col, "Not assessed"),
            }
        )
    return pd.DataFrame(rows)


def valuation_ranked_table(df: pd.DataFrame, limit: int) -> pd.DataFrame:
    cols = [
        "ticker",
        "name",
        "sector",
        "xgb_rank",
        "xgb_prediction_raw",
        "returns_2025",
        "price_to_book",
        "price_to_book_position",
        "ev_to_ebitda",
        "ev_to_ebitda_position",
        "price_to_earnings",
        "price_to_earnings_position",
        "valuation_gap",
        "valuation_gap_position",
        "momentum_read",
        "missing_key_fields",
    ]
    visible = [col for col in cols if col in df.columns]
    return df.sort_values("xgb_rank", na_position="last")[visible].head(limit)


def field_table(row: pd.Series, peers: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for col in KEY_FIELDS:
        if col not in row.index:
            continue
        value = row.get(col)
        peer_median = pd.to_numeric(peers[col], errors="coerce").median() if col in peers.columns else None
        rows.append(
            {
                "field": DISPLAY_NAMES.get(col, col),
                "value": value,
                "peer_median": peer_median,
                "missing": pd.isna(value),
            }
        )
    return pd.DataFrame(rows)


def show_data_coverage(df: pd.DataFrame) -> None:
    rows = []
    for col in KEY_FIELDS:
        if col not in df.columns:
            continue
        missing = int(df[col].isna().sum())
        rows.append(
            {
                "field": DISPLAY_NAMES.get(col, col),
                "available": int(df[col].notna().sum()),
                "missing": missing,
                "missing_pct": missing / len(df) if len(df) else 0,
            }
        )
    coverage = pd.DataFrame(rows).sort_values("missing", ascending=False)
    st.dataframe(
        coverage,
        width="stretch",
        hide_index=True,
        column_config={
            "field": "Field",
            "available": st.column_config.NumberColumn("Available", format="%d"),
            "missing": st.column_config.NumberColumn("Missing", format="%d"),
            "missing_pct": st.column_config.NumberColumn("Missing %", format="%.1%"),
        },
    )
    chart = coverage.set_index("field")["missing"]
    st.bar_chart(chart, width="stretch")


xgb = read_csv(str(OUTPUT_DIR / "xgb_ranked_predictions.csv"), cache_token(OUTPUT_DIR / "xgb_ranked_predictions.csv"))
linear = read_csv(str(OUTPUT_DIR / "linear_ranked_predictions.csv"), cache_token(OUTPUT_DIR / "linear_ranked_predictions.csv"))
features = read_excel(str(FEATURE_PATH), cache_token(FEATURE_PATH))
universe = build_universe(xgb, linear, features)

st.markdown(
    """
    <div class="header">
        <div class="header-title">Secondary Stock Dashboard</div>
        <div class="header-subtitle">Plain ranked output, ticker checks, and data completeness. No synthetic scoring layer.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

if universe.empty:
    st.info("No ranked prediction file found. Run the model pipeline first.")
    st.stop()

country_options = sorted([x for x in universe.get("country", pd.Series(dtype=str)).dropna().unique() if str(x)])
sector_options = sorted([x for x in universe.get("sector", pd.Series(dtype=str)).dropna().unique() if str(x)])
industry_options = sorted([x for x in universe.get("industry", pd.Series(dtype=str)).dropna().unique() if str(x)])

with st.sidebar:
    view = st.radio("View", ["Ranked list", "Relative valuation", "Ticker detail", "Data coverage"])
    st.divider()
    countries = st.multiselect("Country", country_options)
    sectors = st.multiselect("Sector", sector_options)
    industries = st.multiselect("Industry", industry_options)
    search = st.text_input("Ticker or name")
    row_limit = st.slider("Rows", 10, 250, 50, step=10)
    if st.button("Refresh data"):
        st.cache_data.clear()
        st.rerun()

filtered = filter_universe(universe, countries, sectors, industries, search)
if filtered.empty:
    st.info("No rows match the current filters.")
    st.stop()

top = filtered.sort_values("xgb_rank", na_position="last").iloc[0]
if view != "Ticker detail":
    actual_count = int(filtered["returns_2025"].notna().sum()) if "returns_2025" in filtered else 0
    top_actual = pct(top.get("returns_2025")) if "returns_2025" in filtered else "-"
    st.markdown(
        metric_cards(
            [
                ("Rows", f"{len(filtered):,}"),
                ("Top ticker", str(top.get("ticker", "-"))),
                ("Top XGB predicted", pct(top.get("xgb_prediction_raw"))),
                ("Top actual return", top_actual),
                ("Actual returns available", f"{actual_count:,}"),
                ("Missing DCF / Trends", f"{filtered['valuation_gap'].isna().sum():,} / {filtered['google_trends'].isna().sum():,}" if {"valuation_gap", "google_trends"}.issubset(filtered.columns) else "-"),
            ]
        ),
        unsafe_allow_html=True,
    )

if view == "Ranked list":
    st.markdown("<div class='section'></div>", unsafe_allow_html=True)
    st.subheader("Ranked Stocks")
    table = ranked_table(filtered, row_limit)
    st.dataframe(table, width="stretch", hide_index=True, column_config=column_config(table.columns.tolist()))

    st.markdown("<div class='section'></div>", unsafe_allow_html=True)
    left, right = st.columns(2)
    if "rank_gap" in filtered.columns:
        with left:
            st.subheader("XGBoost ranks much higher than linear")
            cols = ["ticker", "name", "xgb_rank", "linear_rank", "rank_gap", "xgb_prediction_raw", "linear_prediction_raw", "missing_key_fields"]
            visible = [col for col in cols if col in filtered.columns]
            xgb_higher = filtered[filtered["rank_gap"] >= 250][visible].sort_values("xgb_rank").head(row_limit)
            st.dataframe(xgb_higher, width="stretch", hide_index=True, column_config=column_config(xgb_higher.columns.tolist()))
        with right:
            st.subheader("Both models rank in top 100")
            cols = ["ticker", "name", "xgb_rank", "linear_rank", "xgb_prediction_raw", "linear_prediction_raw", "missing_key_fields"]
            visible = [col for col in cols if col in filtered.columns]
            both = filtered[(filtered["xgb_rank"] <= 100) & (filtered["linear_rank"] <= 100)][visible].sort_values("xgb_rank").head(row_limit)
            st.dataframe(both, width="stretch", hide_index=True, column_config=column_config(both.columns.tolist()))

elif view == "Relative valuation":
    st.markdown("<div class='section'></div>", unsafe_allow_html=True)
    st.subheader("Relative Valuation")
    st.caption("Position describes low/middle/high versus the current filtered universe. It is not automatically good or bad.")
    table = valuation_ranked_table(filtered, row_limit)
    st.dataframe(table, width="stretch", hide_index=True, column_config=column_config(table.columns.tolist()))

    st.markdown("<div class='section'></div>", unsafe_allow_html=True)
    tendency = pd.DataFrame(
        [
            {
                "field": DISPLAY_NAMES.get(field, field),
                "current_model_tendency": tendency_text,
            }
            for field, tendency_text in MODEL_TENDENCY.items()
        ]
    )
    st.subheader("Current model tendency")
    st.dataframe(tendency, width="stretch", hide_index=True)

    left, right = st.columns(2)
    with left:
        st.subheader("P/B versus predicted return")
        if {"price_to_book", "xgb_prediction_raw"}.issubset(filtered.columns):
            chart_df = filtered[["price_to_book", "xgb_prediction_raw"]].dropna()
            if not chart_df.empty:
                fig, ax = plt.subplots(figsize=(7, 4))
                ax.scatter(chart_df["price_to_book"], chart_df["xgb_prediction_raw"], alpha=0.45, s=18)
                ax.set_xlabel("P/B")
                ax.set_ylabel("XGB predicted return")
                ax.grid(alpha=0.25)
                st.pyplot(fig, clear_figure=True)
    with right:
        st.subheader("EV/EBITDA versus predicted return")
        if {"ev_to_ebitda", "xgb_prediction_raw"}.issubset(filtered.columns):
            chart_df = filtered[["ev_to_ebitda", "xgb_prediction_raw"]].dropna()
            if not chart_df.empty:
                fig, ax = plt.subplots(figsize=(7, 4))
                ax.scatter(chart_df["ev_to_ebitda"], chart_df["xgb_prediction_raw"], alpha=0.45, s=18)
                ax.set_xlabel("EV/EBITDA")
                ax.set_ylabel("XGB predicted return")
                ax.grid(alpha=0.25)
                st.pyplot(fig, clear_figure=True)

elif view == "Ticker detail":
    selector = filtered[["ticker", "name", "xgb_rank"]].copy()
    selector["label"] = selector["ticker"].astype(str)
    selector.loc[selector["name"].notna() & (selector["name"].astype(str) != ""), "label"] = (
        selector["ticker"].astype(str) + " - " + selector["name"].astype(str)
    )
    selector = selector.sort_values("xgb_rank", na_position="last")
    selected = st.selectbox("Ticker", selector["label"].tolist())
    selected_ticker = selector.loc[selector["label"] == selected, "ticker"].iloc[0]
    row = filtered[filtered["ticker"] == selected_ticker].iloc[0]

    st.markdown("<div class='section'></div>", unsafe_allow_html=True)
    st.subheader(f"{row.get('ticker')} - {row.get('name', '')}")
    st.caption(" | ".join(str(x) for x in [row.get("country", ""), row.get("sector", ""), row.get("industry", "")] if pd.notna(x) and str(x)))

    st.markdown(
        metric_cards(
            [
                ("XGB rank", f"{row.get('xgb_rank'):.0f}" if pd.notna(row.get("xgb_rank")) else "-"),
                ("XGB predicted", pct(row.get("xgb_prediction_raw"))),
                ("Actual return", pct(row.get("returns_2025"))),
                ("Linear rank", f"{row.get('linear_rank'):.0f}" if pd.notna(row.get("linear_rank")) else "-"),
                ("Linear predicted", pct(row.get("linear_prediction_raw"))),
                ("Missing fields", f"{row.get('missing_key_count'):.0f}" if pd.notna(row.get("missing_key_count")) else "-"),
            ]
        ),
        unsafe_allow_html=True,
    )

    if row.get("missing_key_fields") != "None":
        st.markdown(
            f"<div class='notice'>Missing key data: {escape(str(row.get('missing_key_fields')))}</div>",
            unsafe_allow_html=True,
        )

    if pd.notna(row.get("industry")) and "industry" in filtered.columns:
        peers = filtered[filtered["industry"] == row.get("industry")]
    elif pd.notna(row.get("sector")) and "sector" in filtered.columns:
        peers = filtered[filtered["sector"] == row.get("sector")]
    else:
        peers = filtered

    left, right = st.columns([1.1, 0.9])
    with left:
        st.subheader("Relative valuation")
        valuation = valuation_table(row, peers, filtered)
        st.dataframe(
            valuation,
            width="stretch",
            hide_index=True,
            column_config={
                "field": "Field",
                "value": st.column_config.NumberColumn("Ticker value", format="%.3f"),
                "universe_position": "Universe position",
                "peer_position": "Peer position",
                "model_tendency": "Current model tendency",
            },
        )

        st.subheader("Financial values versus peers")
        values = field_table(row, peers)
        st.dataframe(
            values,
            width="stretch",
            hide_index=True,
            column_config={
                "field": "Field",
                "value": st.column_config.NumberColumn("Ticker value", format="%.3f"),
                "peer_median": st.column_config.NumberColumn("Peer median", format="%.3f"),
                "missing": "Missing",
            },
        )
    with right:
        st.subheader("Model comparison")
        comparison = pd.DataFrame(
            [
                {"model": "XGBoost", "rank": row.get("xgb_rank"), "prediction": row.get("xgb_prediction_raw")},
                {"model": "Linear", "rank": row.get("linear_rank"), "prediction": row.get("linear_prediction_raw")},
            ]
        )
        st.dataframe(
            comparison,
            width="stretch",
            hide_index=True,
            column_config={
                "model": "Model",
                "rank": st.column_config.NumberColumn("Rank", format="%.0f"),
                "prediction": st.column_config.NumberColumn("Prediction", format="%.1%"),
            },
        )
        st.subheader("Momentum")
        st.metric("Momentum read", str(row.get("momentum_read", "-")))
        momentum_rows = [
            {"field": DISPLAY_NAMES.get("3_month"), "value": row.get("3_month")},
            {"field": DISPLAY_NAMES.get("google_trends"), "value": row.get("google_trends")},
        ]
        st.dataframe(
            pd.DataFrame(momentum_rows),
            width="stretch",
            hide_index=True,
            column_config={
                "field": "Field",
                "value": st.column_config.NumberColumn("Value", format="%.3f"),
            },
        )

else:
    st.markdown("<div class='section'></div>", unsafe_allow_html=True)
    st.subheader("Data Completeness")
    show_data_coverage(filtered)

    st.markdown("<div class='section'></div>", unsafe_allow_html=True)
    left, right = st.columns(2)
    with left:
        st.subheader("Highest-ranked stocks missing valuation gap")
        if "valuation_gap" in filtered.columns:
            cols = ["ticker", "name", "xgb_rank", "xgb_prediction_raw", "sector", "industry"]
            visible = [col for col in cols if col in filtered.columns]
            missing_dcf = filtered[filtered["valuation_gap"].isna()][visible].sort_values("xgb_rank").head(row_limit)
            st.dataframe(missing_dcf, width="stretch", hide_index=True, column_config=column_config(missing_dcf.columns.tolist()))
    with right:
        st.subheader("Highest-ranked stocks missing Google Trends")
        if "google_trends" in filtered.columns:
            cols = ["ticker", "name", "xgb_rank", "xgb_prediction_raw", "sector", "industry"]
            visible = [col for col in cols if col in filtered.columns]
            missing_trends = filtered[filtered["google_trends"].isna()][visible].sort_values("xgb_rank").head(row_limit)
            st.dataframe(missing_trends, width="stretch", hide_index=True, column_config=column_config(missing_trends.columns.tolist()))

    if {"xgb_prediction_raw", "3_month"}.issubset(filtered.columns):
        st.markdown("<div class='section'></div>", unsafe_allow_html=True)
        st.subheader("Prediction versus 3 month momentum")
        chart_df = filtered[["xgb_prediction_raw", "3_month"]].dropna()
        if not chart_df.empty:
            fig, ax = plt.subplots(figsize=(8, 4))
            ax.scatter(chart_df["3_month"], chart_df["xgb_prediction_raw"], alpha=0.45, s=18)
            ax.set_xlabel("3 month momentum")
            ax.set_ylabel("XGB predicted return")
            ax.grid(alpha=0.25)
            st.pyplot(fig, clear_figure=True)
