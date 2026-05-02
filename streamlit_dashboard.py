from __future__ import annotations

import json
import time
from html import escape
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "5_output"
PREDICTIONS_DIR = OUTPUT_DIR / "predictions"
PLOTS_DIR = OUTPUT_DIR / "xgb_plots"
MACRO_VERDICT_PATH = ROOT / "macro_verdict.txt"
FINANCIAL_SHEET_ID = "1MYCq9bQ5Vj-xocHtfgNVODDvZJLQE1rCcdUMMx5CBgE"
FINANCIAL_SHEET_TAB = "Input data"
FINANCIAL_SHEET_TICKER_CELL = "B1"
GOOGLE_CREDENTIALS_PATH = ROOT / "0_ingestion" / "stock-ingestion-494417-17cbf0e7891b.json"
FINANCIAL_CACHE_DIR = ROOT / "0_ingestion" / "stock_statement_cache"


st.set_page_config(
    page_title="ML Stock Dashboard",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)


st.markdown(
    """
    <style>
    .block-container { padding-top: 1.6rem; padding-bottom: 2rem; }
    [data-testid="stMetricValue"] { font-size: 1.55rem; }
    [data-testid="stMetricLabel"] { font-size: 0.82rem; }
    div[data-testid="stDataFrame"] { border: 1px solid #d7dde5; border-radius: 6px; }
    .small-muted { color: #5f6b7a; font-size: 0.86rem; }
    .stock-card-grid {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 0.75rem;
        margin: 0.75rem 0 0.9rem 0;
    }
    .stock-card {
        min-height: 88px;
        border: 1px solid #d7dde5;
        border-radius: 6px;
        padding: 0.72rem 0.82rem;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }
    .stock-card-label { color: #5f6b7a; font-size: 0.78rem; line-height: 1.1rem; }
    .stock-card-value { font-size: 1.15rem; font-weight: 720; line-height: 1.35rem; overflow-wrap: anywhere; }
    .financial-card-grid {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 0.75rem;
        margin: 0.75rem 0 1rem 0;
    }
    .financial-card {
        min-height: 94px;
        border: 1px solid #d7dde5;
        border-radius: 6px;
        padding: 0.78rem 0.86rem;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }
    .financial-card-group { color: #5f6b7a; font-size: 0.72rem; line-height: 1rem; text-transform: uppercase; }
    .financial-card-label { color: #334155; font-size: 0.82rem; line-height: 1.1rem; }
    .financial-card-value { font-size: 1.22rem; font-weight: 720; line-height: 1.35rem; overflow-wrap: anywhere; }
    .dashboard-divider { margin-top: 1.1rem; }
    .stock-notice {
        border: 1px solid #d9bf57;
        border-left: 5px solid #d9a600;
        border-radius: 6px;
        padding: 0.75rem 0.9rem;
        margin: 0.6rem 0 1rem 0;
        background: rgba(217, 166, 0, 0.10);
    }
    @media (max-width: 1100px) { .stock-card-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
    @media (max-width: 700px) { .stock-card-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
    @media (max-width: 1100px) { .financial-card-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
    @media (max-width: 700px) { .financial-card-grid { grid-template-columns: repeat(1, minmax(0, 1fr)); } }
    </style>
    """,
    unsafe_allow_html=True,
)


def file_mtime(path: Path) -> str:
    if not path.exists():
        return "missing"
    return pd.Timestamp(path.stat().st_mtime, unit="s").strftime("%Y-%m-%d %H:%M:%S")


def file_cache_token(path: Path) -> float | None:
    if not path.exists():
        return None
    return path.stat().st_mtime


def file_size(path: Path) -> str:
    if not path.exists():
        return "missing"
    return f"{path.stat().st_size:,} bytes"


def run_sources() -> dict[str, Path]:
    sources = {"Current output": OUTPUT_DIR}
    if PREDICTIONS_DIR.exists():
        for folder in sorted(PREDICTIONS_DIR.iterdir(), reverse=True):
            if folder.is_dir():
                sources[f"Archive: {folder.name}"] = folder
    return sources


@st.cache_data(show_spinner=False)
def read_csv(path_text: str, cache_token: float | None) -> pd.DataFrame:
    path = Path(path_text)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


@st.cache_data(show_spinner=False)
def read_excel(path_text: str, cache_token: float | None) -> pd.DataFrame:
    path = Path(path_text)
    if not path.exists():
        return pd.DataFrame()
    return pd.read_excel(path)


@st.cache_data(show_spinner=False)
def read_text(path_text: str, cache_token: float | None) -> str:
    path = Path(path_text)
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def source_plot_dir(source_label: str, source_path: Path) -> Path:
    if source_path == OUTPUT_DIR:
        return PLOTS_DIR
    archive_name = source_path.name
    archived = PLOTS_DIR / archive_name
    return archived if archived.exists() else PLOTS_DIR


def format_pct(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value) * 100:.1f}%"


def normalize_prediction_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "prediction_rank" in out.columns:
        out["prediction_rank"] = pd.to_numeric(out["prediction_rank"], errors="coerce")
    for col in ["prediction_raw", "prediction_transformed", "returns_2025", "returns_2025_ihs"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    for col in ["ticker", "name", "country", "industry", "sector", "model_split"]:
        if col in out.columns:
            out[col] = out[col].fillna("").astype(str)
    return out


def filter_predictions(df: pd.DataFrame, *, countries: list[str], sectors: list[str], industries: list[str], splits: list[str], search: str) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if countries and "country" in out.columns:
        out = out[out["country"].isin(countries)]
    if sectors and "sector" in out.columns:
        out = out[out["sector"].isin(sectors)]
    if industries and "industry" in out.columns:
        out = out[out["industry"].isin(industries)]
    if splits and "model_split" in out.columns:
        out = out[out["model_split"].isin(splits)]
    if search:
        needle = search.strip().lower()
        cols = [c for c in ["ticker", "name"] if c in out.columns]
        if cols:
            mask = False
            for col in cols:
                mask = mask | out[col].str.lower().str.contains(needle, na=False)
            out = out[mask]
    return out


def metric_from_run_log(run_log: pd.DataFrame, name: str) -> float | None:
    if run_log.empty or name not in run_log.columns:
        return None
    value = pd.to_numeric(run_log.iloc[-1][name], errors="coerce")
    return None if pd.isna(value) else float(value)


def show_metric_row(run_log: pd.DataFrame, xgb_df: pd.DataFrame, linear_df: pd.DataFrame) -> None:
    cols = st.columns(6)
    cols[0].metric("Rows scored", f"{len(xgb_df):,}" if not xgb_df.empty else "-")
    cols[1].metric("Features", f"{int(metric_from_run_log(run_log, 'feature_count') or 0):,}" if not run_log.empty else "-")
    cols[2].metric("Test RMSE", f"{metric_from_run_log(run_log, 'test_rmse'):.3f}" if metric_from_run_log(run_log, "test_rmse") is not None else "-")
    cols[3].metric("Test R2", f"{metric_from_run_log(run_log, 'test_r2'):.3f}" if metric_from_run_log(run_log, "test_r2") is not None else "-")
    if not xgb_df.empty and "prediction_raw" in xgb_df.columns:
        cols[4].metric("Top XGB prediction", format_pct(xgb_df["prediction_raw"].max()))
    else:
        cols[4].metric("Top XGB prediction", "-")
    if not linear_df.empty and "prediction_raw" in linear_df.columns:
        cols[5].metric("Top linear prediction", format_pct(linear_df["prediction_raw"].max()))
    else:
        cols[5].metric("Top linear prediction", "-")


def display_prediction_table(df: pd.DataFrame, limit: int) -> None:
    if df.empty:
        st.info("No prediction file found for this source.")
        return
    cols = [
        "prediction_rank",
        "ticker",
        "name",
        "country",
        "sector",
        "industry",
        "prediction_raw",
        "returns_2025",
        "model_split",
        "has_training_target",
    ]
    visible = [c for c in cols if c in df.columns]
    st.dataframe(
        df.sort_values("prediction_rank", na_position="last")[visible].head(limit),
        width="stretch",
        hide_index=True,
        column_config={
            "prediction_rank": st.column_config.NumberColumn("Rank", format="%.0f"),
            "prediction_raw": st.column_config.NumberColumn("Predicted return", format="%.2f"),
            "returns_2025": st.column_config.NumberColumn("Actual return", format="%.2f"),
        },
    )


def show_bar_chart(df: pd.DataFrame, label_col: str, value_col: str, title: str, limit: int = 20) -> None:
    if df.empty or label_col not in df.columns or value_col not in df.columns:
        st.info(f"No data available for {title.lower()}.")
        return
    chart_df = df[[label_col, value_col]].dropna().head(limit).set_index(label_col)
    st.bar_chart(chart_df, width="stretch")


def show_plot_gallery(plot_dir: Path) -> None:
    pngs = sorted(plot_dir.glob("*.png"))
    if not pngs:
        st.info(f"No PNG plots found in {plot_dir}.")
        return
    selected = st.selectbox("Plot", [p.name for p in pngs])
    selected_path = plot_dir / selected
    st.image(str(selected_path), width="stretch")


def data_completeness_score(df: pd.DataFrame) -> pd.Series:
    useful_cols = [
        "xgb_prediction_raw",
        "linear_prediction_raw",
        "returns_2025",
        "momentum_2024",
        "3_mth_momentum",
        "3_month",
        "roic",
        "rev_growth_2025",
        "ebit_growth_2025",
        "debt_to_assets",
        "ev_to_ebitda",
        "valuation_gap",
        "earnings_surprise",
        "price_to_book",
        "price_to_earnings",
        "stock_volatility",
        "google_trends",
    ]
    available = [col for col in useful_cols if col in df.columns]
    if not available:
        return pd.Series(0.0, index=df.index)
    return df[available].notna().mean(axis=1) * 100


def build_portfolio_suggestion(
    stock_universe: pd.DataFrame,
    *,
    size: int,
    sort_mode: str,
) -> pd.DataFrame:
    if stock_universe.empty or "ticker" not in stock_universe.columns:
        return pd.DataFrame()

    candidates = stock_universe.copy()
    candidates["data_completeness"] = data_completeness_score(candidates)

    if sort_mode == "Linear rank" and "linear_rank" in candidates.columns:
        candidates["portfolio_sort_score"] = pd.to_numeric(candidates["linear_rank"], errors="coerce")
        candidates = candidates.sort_values("portfolio_sort_score", na_position="last")
    elif sort_mode == "Combined rank" and {"xgb_rank", "linear_rank"}.issubset(candidates.columns):
        candidates["portfolio_sort_score"] = (
            pd.to_numeric(candidates["xgb_rank"], errors="coerce").rank(method="average")
            + pd.to_numeric(candidates["linear_rank"], errors="coerce").rank(method="average")
        ) / 2
        candidates = candidates.sort_values("portfolio_sort_score", na_position="last")
    elif sort_mode == "DCF gap" and "valuation_gap" in candidates.columns:
        candidates["portfolio_sort_score"] = pd.to_numeric(candidates["valuation_gap"], errors="coerce")
        candidates = candidates.sort_values("portfolio_sort_score", ascending=False, na_position="last")
    elif sort_mode == "Momentum" and "3_mth_momentum" in candidates.columns:
        candidates["portfolio_sort_score"] = pd.to_numeric(candidates["3_mth_momentum"], errors="coerce")
        candidates = candidates.sort_values("portfolio_sort_score", ascending=False, na_position="last")
    elif sort_mode == "Momentum" and "3_month" in candidates.columns:
        candidates["portfolio_sort_score"] = pd.to_numeric(candidates["3_month"], errors="coerce")
        candidates = candidates.sort_values("portfolio_sort_score", ascending=False, na_position="last")
    elif sort_mode == "Google Trends" and "google_trends" in candidates.columns:
        candidates["portfolio_sort_score"] = pd.to_numeric(candidates["google_trends"], errors="coerce")
        candidates = candidates.sort_values("portfolio_sort_score", ascending=False, na_position="last")
    elif "xgb_rank" in candidates.columns:
        candidates["portfolio_sort_score"] = pd.to_numeric(candidates["xgb_rank"], errors="coerce")
        candidates = candidates.sort_values("portfolio_sort_score", na_position="last")
    elif "xgb_prediction_raw" in candidates.columns:
        candidates["portfolio_sort_score"] = pd.to_numeric(candidates["xgb_prediction_raw"], errors="coerce")
        candidates = candidates.sort_values("portfolio_sort_score", ascending=False, na_position="last")

    portfolio = candidates.head(size).copy()
    if portfolio.empty:
        return pd.DataFrame()
    portfolio["suggested_weight"] = 1 / len(portfolio)
    return portfolio


def build_stock_universe(xgb_df: pd.DataFrame, linear_df: pd.DataFrame, feature_df: pd.DataFrame) -> pd.DataFrame:
    if xgb_df.empty:
        base = linear_df.copy()
    else:
        base = xgb_df.copy()

    if base.empty:
        return base

    base = base.rename(
        columns={
            "prediction_raw": "xgb_prediction_raw",
            "prediction_transformed": "xgb_prediction_transformed",
            "prediction_rank": "xgb_rank",
        }
    )

    if not linear_df.empty:
        linear_cols = [c for c in ["ticker", "baseline_model", "prediction_raw", "prediction_rank"] if c in linear_df.columns]
        linear_part = linear_df[linear_cols].rename(
            columns={
                "prediction_raw": "linear_prediction_raw",
                "prediction_rank": "linear_rank",
            }
        )
        base = base.merge(linear_part, on="ticker", how="left")

    if not feature_df.empty and "ticker" in feature_df.columns:
        feature_cols = [c for c in feature_df.columns if c not in {"name", "country", "industry", "sector"}]
        feature_part = feature_df[feature_cols].copy()
        base = base.merge(feature_part, on="ticker", how="left", suffixes=("", "_feature"))

    for col in base.columns:
        if col not in {"ticker", "name", "country", "industry", "sector", "model_split", "baseline_model"}:
            numeric = pd.to_numeric(base[col], errors="coerce")
            if numeric.notna().any():
                base[col] = numeric
    return base


def percentile_rank(series: pd.Series, value: float | int | None) -> float | None:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if value is None or pd.isna(value) or numeric.empty:
        return None
    return float((numeric <= float(value)).mean() * 100)


def numeric_value(value: object) -> float | None:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return None if pd.isna(number) else float(number)


RELATIVE_VALUATION_FIELDS = ["price_to_book", "ev_to_ebitda", "price_to_earnings"]

MODEL_TENDENCY = {
    "price_to_book": "High has been favorable in the current run",
    "ev_to_ebitda": "High has been favorable, likely mixed with growth/quality",
    "price_to_earnings": "Weak/mixed; high is mildly favorable",
}

STOCK_LABELS = {
    "price_to_book": "P/B",
    "ev_to_ebitda": "EV/EBITDA",
    "price_to_earnings": "P/E",
    "valuation_gap": "DCF valuation gap",
    "3_month": "3 month momentum",
    "3_mth_momentum": "3 month momentum",
    "google_trends": "Google Trends",
    "earnings_surprise": "Earnings surprise",
}


def position_label(percentile: float | None) -> str:
    if percentile is None:
        return "Missing"
    if percentile < 33:
        return "Low"
    if percentile <= 66:
        return "Middle"
    return "High"


def stock_metric_cards(items: list[tuple[str, str]]) -> None:
    cards = []
    for label, value in items:
        cards.append(
            "<div class='stock-card'>"
            f"<div class='stock-card-label'>{escape(label)}</div>"
            f"<div class='stock-card-value'>{escape(value)}</div>"
            "</div>"
        )
    st.markdown("<div class='stock-card-grid'>" + "".join(cards) + "</div>", unsafe_allow_html=True)


def missing_key_fields(row: pd.Series) -> str:
    fields = []
    momentum_col = "3_mth_momentum" if "3_mth_momentum" in row.index else "3_month"
    for col in ["valuation_gap", "earnings_surprise", "google_trends", momentum_col, "roic", "rev_growth_2025", "ebit_growth_2025", "stock_volatility"]:
        if col in row.index and pd.isna(row.get(col)):
            fields.append(STOCK_LABELS.get(col, col))
    return ", ".join(fields)


def momentum_read(row: pd.Series, universe: pd.DataFrame) -> str:
    momentum_col = "3_mth_momentum" if "3_mth_momentum" in universe.columns else "3_month"
    price_pct = percentile_rank(universe[momentum_col], row.get(momentum_col)) if momentum_col in universe.columns else None
    trends_pct = percentile_rank(universe["google_trends"], row.get("google_trends")) if "google_trends" in universe.columns else None
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


def macro_support_score(macro_text: str) -> tuple[float | None, str]:
    text = (macro_text or "").lower()
    if "final market stance: bullish" in text or "macro verdict: bullish" in text:
        return 100.0, "Bullish"
    if "final market stance: bearish" in text or "macro verdict: bearish" in text:
        return 0.0, "Bearish"
    if "final market stance: neutral" in text or "macro verdict: neutral" in text:
        return 50.0, "Neutral"
    return None, "Missing"


def momentum_assessment(row: pd.Series, universe: pd.DataFrame, macro_text: str) -> dict[str, str]:
    momentum_col = "3_mth_momentum" if "3_mth_momentum" in universe.columns else "3_month"
    price_pct = percentile_rank(universe[momentum_col], row.get(momentum_col)) if momentum_col in universe.columns else None
    trends_pct = percentile_rank(universe["google_trends"], row.get("google_trends")) if "google_trends" in universe.columns else None
    macro_score, macro_label = macro_support_score(macro_text)

    weighted_parts = []
    if price_pct is not None:
        weighted_parts.append((price_pct, 0.50))
    if trends_pct is not None:
        weighted_parts.append((trends_pct, 0.30))
    if macro_score is not None:
        weighted_parts.append((macro_score, 0.20))

    if not weighted_parts:
        score = None
    else:
        total_weight = sum(weight for _, weight in weighted_parts)
        score = sum(value * weight for value, weight in weighted_parts) / total_weight

    if score is None:
        label = "Missing"
    elif score >= 75:
        label = "Strong"
    elif score >= 55:
        label = "Constructive"
    elif score >= 40:
        label = "Mixed"
    else:
        label = "Weak"

    return {
        "label": label,
        "score": "-" if score is None else f"{score:.0f}",
        "price": "-" if price_pct is None else f"{price_pct:.0f}",
        "trends": "-" if trends_pct is None else f"{trends_pct:.0f}",
        "macro": macro_label,
    }


def dcf_valuation_read(value: float | int | None) -> str:
    numeric = numeric_value(value)
    if numeric is None:
        return "Missing"
    if numeric > 1.05:
        return "Undervalued"
    if numeric < 0.95:
        return "Overvalued"
    return "Close to fair value"


def percentile_text(series: pd.Series, value: float | int | None) -> str:
    percentile = percentile_rank(series, value)
    return "-" if percentile is None else f"{percentile:.0f}"


def relative_valuation_rows(row: pd.Series, peers: pd.DataFrame, universe: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for col in RELATIVE_VALUATION_FIELDS:
        if col not in row.index:
            continue
        value = row.get(col)
        universe_pct = percentile_rank(universe[col], value) if col in universe.columns else None
        peer_pct = percentile_rank(peers[col], value) if col in peers.columns else None
        rows.append(
            {
                "field": STOCK_LABELS.get(col, col),
                "value": value,
                "universe_position": position_label(universe_pct),
                "peer_position": position_label(peer_pct),
                "model_tendency": MODEL_TENDENCY.get(col, "Not assessed"),
            }
        )
    return pd.DataFrame(rows)


def financial_cache_path(ticker: str) -> Path:
    safe = "".join(ch if ch.isalnum() or ch in "-_." else "_" for ch in ticker.upper())
    return FINANCIAL_CACHE_DIR / f"{safe}.json"


def get_financial_sheet_service():
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError as exc:
        raise RuntimeError("Google Sheets packages are not installed in this environment.") from exc
    if not GOOGLE_CREDENTIALS_PATH.exists():
        raise RuntimeError(f"Missing Google credentials file: {GOOGLE_CREDENTIALS_PATH}")
    credentials = service_account.Credentials.from_service_account_file(
        GOOGLE_CREDENTIALS_PATH,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=credentials)


def write_financial_sheet_ticker(service, ticker: str) -> None:
    service.spreadsheets().values().update(
        spreadsheetId=FINANCIAL_SHEET_ID,
        range=f"{FINANCIAL_SHEET_TAB}!{FINANCIAL_SHEET_TICKER_CELL}",
        valueInputOption="USER_ENTERED",
        body={"values": [[ticker]]},
    ).execute()


def read_financial_sheet_values(service) -> list[list[object]]:
    response = service.spreadsheets().values().get(
        spreadsheetId=FINANCIAL_SHEET_ID,
        range=f"{FINANCIAL_SHEET_TAB}!A1:AB80",
        valueRenderOption="UNFORMATTED_VALUE",
    ).execute()
    return response.get("values", [])


def sheet_is_ready(values: list[list[object]], ticker: str) -> bool:
    if not values or len(values[0]) < 2 or str(values[0][1]).strip().upper() != ticker.upper():
        return False
    flat_values = [str(cell).upper() for row in values for cell in row]
    if any("#NAME?" in cell or "LOADING" in cell for cell in flat_values):
        return False
    return any(len(row) > 1 and str(row[0]).strip().lower() == "revenue" and numeric_value(row[1]) is not None for row in values)


def fetch_financial_sheet_values(ticker: str) -> list[list[object]]:
    service = get_financial_sheet_service()
    write_financial_sheet_ticker(service, ticker)
    deadline = time.time() + 60
    values: list[list[object]] = []
    while time.time() < deadline:
        values = read_financial_sheet_values(service)
        if sheet_is_ready(values, ticker):
            return values
        time.sleep(3)
    raise RuntimeError(f"Timed out waiting for {ticker} statement sheet to recalculate.")


def parse_statement_section(values: list[list[object]], statement: str, pairs: list[tuple[int, int]], years: list[int]) -> pd.DataFrame:
    rows = []
    skip_labels = {"", "line item", "date", "reported currency", "filing date", "fiscal year", "period"}
    for label_col, value_col in pairs:
        year = years[pairs.index((label_col, value_col))]
        for sheet_row in values[4:]:
            label = str(sheet_row[label_col]).strip() if len(sheet_row) > label_col else ""
            if label.lower() in skip_labels:
                continue
            value = sheet_row[value_col] if len(sheet_row) > value_col else None
            if label and numeric_value(value) is not None:
                rows.append({"statement": statement, "year": year, "line_item": label, "value": numeric_value(value)})
    return pd.DataFrame(rows)


def parse_derived_financial_rows(values: list[list[object]]) -> pd.DataFrame:
    rows = []
    for sheet_row in values[41:49]:
        label = str(sheet_row[0]).strip() if len(sheet_row) > 0 else ""
        if not label or label.lower() == "year":
            continue
        for idx, year in enumerate([2022, 2023, 2024, 2025], start=1):
            value = sheet_row[idx] if len(sheet_row) > idx else None
            if numeric_value(value) is not None:
                rows.append({"statement": "Derived", "year": year, "line_item": label, "value": numeric_value(value)})
    return pd.DataFrame(rows)


def parse_financial_sheet(values: list[list[object]]) -> pd.DataFrame:
    years = [2022, 2023, 2024, 2025]
    frames = [
        parse_statement_section(values, "Income Statement", [(0, 1), (2, 3), (4, 5), (6, 7)], years),
        parse_statement_section(values, "Balance Sheet", [(9, 10), (11, 12), (13, 14), (15, 16)], years),
        parse_statement_section(values, "Cash Flow", [(18, 19), (20, 21), (22, 23), (24, 25)], years),
        parse_derived_financial_rows(values),
    ]
    return pd.concat([frame for frame in frames if not frame.empty], ignore_index=True) if any(not frame.empty for frame in frames) else pd.DataFrame()


def load_cached_financial_sheet(ticker: str) -> pd.DataFrame:
    path = financial_cache_path(ticker)
    if not path.exists():
        return pd.DataFrame()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return pd.DataFrame(payload.get("rows", []))
    except (json.JSONDecodeError, OSError):
        return pd.DataFrame()


def save_cached_financial_sheet(ticker: str, df: pd.DataFrame) -> None:
    FINANCIAL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "ticker": ticker,
        "saved_at": pd.Timestamp.utcnow().isoformat(),
        "rows": df.to_dict(orient="records"),
    }
    financial_cache_path(ticker).write_text(json.dumps(payload, indent=2), encoding="utf-8")


def get_financial_sheet_data(ticker: str, refresh: bool) -> tuple[pd.DataFrame, str]:
    if not refresh:
        cached = load_cached_financial_sheet(ticker)
        if not cached.empty:
            return cached, "cached"
    values = fetch_financial_sheet_values(ticker)
    parsed = parse_financial_sheet(values)
    if parsed.empty:
        raise RuntimeError(f"No statement rows could be parsed for {ticker}.")
    save_cached_financial_sheet(ticker, parsed)
    return parsed, "refreshed"


def financial_series(financials: pd.DataFrame, line_items: list[str], statement: str | None = None) -> pd.DataFrame:
    df = financials[financials["line_item"].isin(line_items)].copy()
    if statement:
        df = df[df["statement"] == statement]
    if df.empty:
        return pd.DataFrame()
    pivot = df.pivot_table(index="year", columns="line_item", values="value", aggfunc="first").sort_index()
    pivot.index = pivot.index.astype(int).astype(str)
    pivot.index.name = "Year"
    return pivot


def latest_financial_value(series_df: pd.DataFrame, column: str) -> float | None:
    if series_df.empty or column not in series_df.columns:
        return None
    values = pd.to_numeric(series_df[column], errors="coerce").dropna()
    return None if values.empty else float(values.iloc[-1])


def financial_ratio(numerator: pd.DataFrame, numerator_col: str, denominator: pd.DataFrame, denominator_col: str) -> pd.Series:
    if numerator.empty or denominator.empty or numerator_col not in numerator.columns or denominator_col not in denominator.columns:
        return pd.Series(dtype="float64")
    joined = pd.concat(
        [
            pd.to_numeric(numerator[numerator_col], errors="coerce"),
            pd.to_numeric(denominator[denominator_col], errors="coerce"),
        ],
        axis=1,
        keys=["numerator", "denominator"],
    )
    joined = joined[joined["denominator"].notna() & (joined["denominator"] != 0)]
    return joined["numerator"] / joined["denominator"]


def latest_series_value(series: pd.Series) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    return None if values.empty else float(values.iloc[-1])


def format_large_number(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    value = float(value)
    sign = "-" if value < 0 else ""
    value = abs(value)
    if value >= 1_000_000_000:
        return f"{sign}{value / 1_000_000_000:.2f}B"
    if value >= 1_000_000:
        return f"{sign}{value / 1_000_000:.1f}M"
    return f"{sign}{value:,.0f}"


def format_ratio(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value):.2f}x"


def format_financial_pct(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "-"
    return f"{float(value) * 100:.1f}%"


def financial_metric_cards(items: list[tuple[str, str, str]]) -> None:
    cards = []
    for group, label, value in items:
        cards.append(
            "<div class='financial-card'>"
            f"<div class='financial-card-group'>{escape(group)}</div>"
            f"<div class='financial-card-label'>{escape(label)}</div>"
            f"<div class='financial-card-value'>{escape(value)}</div>"
            "</div>"
        )
    st.markdown("<div class='financial-card-grid'>" + "".join(cards) + "</div>", unsafe_allow_html=True)


def show_percent_line_chart(df: pd.DataFrame, title: str, clip_abs: float = 2.0) -> None:
    if df.empty:
        st.info(f"No {title.lower()} ratios available.")
        return
    chart_df = df.dropna(how="all").copy()
    if chart_df.empty:
        st.info(f"No {title.lower()} ratios available.")
        return
    chart_df = chart_df.reset_index().rename(columns={chart_df.index.name or "index": "Year"})
    melted = chart_df.melt("Year", var_name="Ratio", value_name="Value").dropna()
    if melted.empty:
        st.info(f"No {title.lower()} ratios available.")
        return
    melted["Display value"] = melted["Value"].clip(lower=-clip_abs, upper=clip_abs)
    chart = (
        alt.Chart(melted)
        .mark_line(point=True)
        .encode(
            x=alt.X("Year:N", title=None),
            y=alt.Y("Display value:Q", title=None, axis=alt.Axis(format=".0%")),
            color=alt.Color("Ratio:N", title="Ratio"),
            tooltip=[
                alt.Tooltip("Year:N", title=""),
                alt.Tooltip("Ratio:N", title="Ratio"),
                alt.Tooltip("Value:Q", title="Value", format=".1%"),
                alt.Tooltip("Display value:Q", title="Displayed", format=".1%"),
            ],
        )
        .properties(height=320)
    )
    st.altair_chart(chart, width="stretch")


def show_financial_statement_charts(selected_ticker: str) -> None:
    st.markdown("#### Financial Statement Tear Sheet")
    col_a, col_b = st.columns([0.25, 0.75])
    with col_a:
        refresh = st.checkbox("Refresh from Google Sheet", value=False)
    with col_b:
        load_clicked = st.button("Load statement charts", key=f"load_financials_{selected_ticker}")

    cached = load_cached_financial_sheet(selected_ticker)
    if cached.empty and not load_clicked:
        st.caption("Load on demand to avoid refreshing SheetsFinance for every stock selection.")
        return

    try:
        if load_clicked or not cached.empty:
            with st.spinner(f"Loading statements for {selected_ticker}..."):
                financials, source = get_financial_sheet_data(selected_ticker, refresh=refresh or cached.empty)
    except RuntimeError as exc:
        st.error(str(exc))
        return

    st.caption(f"Statement data source: {source}")
    revenue = financial_series(financials, ["Revenue", "Gross Profit", "Operating Income", "Net Income"], "Income Statement")
    margins = financial_series(financials, ["Gross profitability", "Operating profit margin", "ROIC"], "Derived")
    cash_flow = financial_series(financials, ["Operating Cash Flow", "Capital Expenditure", "Free Cash Flow", "Net Income"], "Cash Flow")
    balance = financial_series(financials, ["Total Assets", "Total Debt", "Total Equity", "Cash And Cash Equivalents", "Total Current Assets", "Total Current Liabilities"], "Balance Sheet")
    valuation = financial_series(financials, ["Price to book ratio", "Current ratio", "FreeCashFlowYield"], "Derived")

    gross_margin = financial_ratio(revenue, "Gross Profit", revenue, "Revenue")
    net_margin = financial_ratio(revenue, "Net Income", revenue, "Revenue")
    fcf_margin = financial_ratio(cash_flow, "Free Cash Flow", revenue, "Revenue")
    fcf_to_net_income = financial_ratio(cash_flow, "Free Cash Flow", revenue, "Net Income")
    capex_to_revenue = financial_ratio(cash_flow, "Capital Expenditure", revenue, "Revenue").abs()
    debt_to_assets = financial_ratio(balance, "Total Debt", balance, "Total Assets")
    cash_to_assets = financial_ratio(balance, "Cash And Cash Equivalents", balance, "Total Assets")
    revenue_growth = pd.to_numeric(revenue["Revenue"], errors="coerce").pct_change() if "Revenue" in revenue.columns else pd.Series(dtype="float64")
    gross_profit_growth = pd.to_numeric(revenue["Gross Profit"], errors="coerce").pct_change() if "Gross Profit" in revenue.columns else pd.Series(dtype="float64")
    operating_income_growth = pd.to_numeric(revenue["Operating Income"], errors="coerce").pct_change() if "Operating Income" in revenue.columns else pd.Series(dtype="float64")
    fcf_growth = pd.to_numeric(cash_flow["Free Cash Flow"], errors="coerce").pct_change() if "Free Cash Flow" in cash_flow.columns else pd.Series(dtype="float64")
    percentage_ratios = pd.DataFrame(
        {
            "Gross margin": gross_margin,
            "Operating margin": margins["Operating profit margin"] if "Operating profit margin" in margins.columns else pd.Series(dtype="float64"),
            "Net margin": net_margin,
            "ROIC": margins["ROIC"] if "ROIC" in margins.columns else pd.Series(dtype="float64"),
            "Gross profitability": margins["Gross profitability"] if "Gross profitability" in margins.columns else pd.Series(dtype="float64"),
            "FCF margin": fcf_margin,
            "FCF yield": valuation["FreeCashFlowYield"] if "FreeCashFlowYield" in valuation.columns else pd.Series(dtype="float64"),
            "FCF / net income": fcf_to_net_income,
            "Capex / revenue": capex_to_revenue,
            "Revenue growth": revenue_growth,
            "Debt / assets": debt_to_assets,
            "Cash / assets": cash_to_assets,
            "Gross profit growth": gross_profit_growth,
            "Operating income growth": operating_income_growth,
            "FCF growth": fcf_growth,
        }
    ).dropna(how="all")

    st.markdown("##### Percentage Ratios")
    if percentage_ratios.empty:
        st.info("No percentage ratio rows available.")
    else:
        ratio_left, ratio_right = st.columns(2)
        with ratio_left:
            profitability_cols = [
                c for c in ["Gross margin", "Operating margin", "Net margin", "ROIC", "Gross profitability"] if c in percentage_ratios.columns
            ]
            st.markdown("###### Profitability")
            show_percent_line_chart(percentage_ratios[profitability_cols], "profitability", clip_abs=1.5)
        with ratio_right:
            cash_cols = [c for c in ["FCF margin", "FCF yield", "FCF / net income", "Capex / revenue"] if c in percentage_ratios.columns]
            st.markdown("###### Cash Generation")
            show_percent_line_chart(percentage_ratios[cash_cols], "cash generation", clip_abs=1.5)

        ratio_left, ratio_right = st.columns(2)
        with ratio_left:
            reinvestment_cols = [c for c in ["Capex / revenue", "Debt / assets", "Cash / assets"] if c in percentage_ratios.columns]
            st.markdown("###### Reinvestment and Leverage")
            show_percent_line_chart(percentage_ratios[reinvestment_cols], "reinvestment and leverage", clip_abs=1.0)
        with ratio_right:
            growth_cols = [c for c in ["Revenue growth", "Gross profit growth", "Operating income growth", "FCF growth"] if c in percentage_ratios.columns]
            st.markdown("###### Growth and Cash Expansion")
            show_percent_line_chart(percentage_ratios[growth_cols], "growth and cash expansion", clip_abs=2.0)




def describe_comp_score(score: float | None) -> str:
    if score is None:
        return "No peer score available."
    if score >= 0.85:
        return "Top industry peer percentile."
    if score >= 0.65:
        return "Better than most industry peers."
    if score >= 0.35:
        return "Around the middle of industry peers."
    if score >= 0.15:
        return "Worse than most industry peers."
    return "Bottom industry peer percentile."


def valuation_comp_rows(row: pd.Series) -> pd.DataFrame:
    pairs = [
        ("P/E", "price_to_earnings", "comps_price_earnings"),
        ("EV/EBITDA", "ev_to_ebitda", "comps_ev_ebitda"),
    ]
    rows = []
    for label, stock_col, comp_col in pairs:
        stock_multiple = numeric_value(row.get(stock_col))
        comp_signal = numeric_value(row.get(comp_col))
        rows.append(
            {
                "multiple": label,
                "stock_multiple": stock_multiple,
                "industry_peer_score": comp_signal,
                "description": describe_comp_score(comp_signal),
            }
        )
    return pd.DataFrame(rows)


def show_portfolio_suggestion(stock_universe: pd.DataFrame) -> None:
    if stock_universe.empty:
        st.info("No stock universe available.")
        return

    controls = st.columns(2)
    with controls[0]:
        sort_mode = st.selectbox(
            "Sort by",
            ["XGB rank", "Linear rank", "Combined rank", "DCF gap", "Momentum", "Google Trends"],
        )
    with controls[1]:
        portfolio_size = st.slider("Number of stocks", 10, 80, 40, step=5)

    portfolio = build_portfolio_suggestion(
        stock_universe,
        size=portfolio_size,
        sort_mode=sort_mode,
    )
    if portfolio.empty:
        st.info("No stocks passed the current portfolio filters.")
        return

    avg_pred = pd.to_numeric(portfolio.get("xgb_prediction_raw"), errors="coerce").mean()
    avg_actual = pd.to_numeric(portfolio.get("returns_2025"), errors="coerce").mean() if "returns_2025" in portfolio.columns else None
    stock_metric_cards(
        [
            ("Holdings", f"{len(portfolio):.0f}"),
            ("Equal weight", format_pct(portfolio["suggested_weight"].iloc[0])),
            ("Avg XGB predicted", format_pct(avg_pred)),
            ("Avg actual return", format_pct(avg_actual)),
        ]
    )

    st.markdown("#### Suggested Holdings")
    display_cols = [
        "suggested_weight",
        "portfolio_sort_score",
        "xgb_rank",
        "linear_rank",
        "ticker",
        "name",
        "country",
        "sector",
        "industry",
        "xgb_prediction_raw",
        "linear_prediction_raw",
        "returns_2025",
        "data_completeness",
        "roic",
        "valuation_gap",
        "momentum_2024",
        "3_mth_momentum",
        "google_trends",
    ]
    visible = [col for col in display_cols if col in portfolio.columns]
    st.dataframe(
        portfolio[visible],
        width="stretch",
        hide_index=True,
        column_config={
            "suggested_weight": st.column_config.NumberColumn("Weight", format="%.1%"),
            "portfolio_sort_score": st.column_config.NumberColumn("Sort score", format="%.3f"),
            "xgb_rank": st.column_config.NumberColumn("XGB rank", format="%.0f"),
            "linear_rank": st.column_config.NumberColumn("Linear rank", format="%.0f"),
            "xgb_prediction_raw": st.column_config.NumberColumn("XGB predicted", format="%.1%"),
            "linear_prediction_raw": st.column_config.NumberColumn("Linear predicted", format="%.1%"),
            "returns_2025": st.column_config.NumberColumn("Actual return", format="%.1%"),
            "data_completeness": st.column_config.NumberColumn("Data completeness", format="%.0f"),
            "roic": st.column_config.NumberColumn("ROIC", format="%.3f"),
            "valuation_gap": st.column_config.NumberColumn("DCF gap", format="%.3f"),
            "momentum_2024": st.column_config.NumberColumn("Momentum 2024", format="%.3f"),
            "3_mth_momentum": st.column_config.NumberColumn("3M momentum", format="%.3f"),
            "google_trends": st.column_config.NumberColumn("Google Trends", format="%.3f"),
        },
    )
    st.caption("Rule-based dashboard suggestion from the model-ranked universe, not portfolio advice.")


def stock_selector(stock_universe: pd.DataFrame, filtered_xgb: pd.DataFrame, key: str) -> str | None:
    if stock_universe.empty or "ticker" not in stock_universe.columns:
        return None

    selector_df = stock_universe[["ticker", "name"]].copy()
    selector_df["label"] = selector_df["ticker"].fillna("").astype(str)
    selector_df.loc[selector_df["name"].notna() & (selector_df["name"].astype(str) != ""), "label"] = (
        selector_df["ticker"].astype(str) + " - " + selector_df["name"].astype(str)
    )
    selector_df = selector_df.sort_values("ticker")
    default_index = 0
    if not filtered_xgb.empty and "ticker" in filtered_xgb.columns:
        top_ticker = filtered_xgb.sort_values("prediction_rank", na_position="last").iloc[0]["ticker"]
        matches = selector_df.index[selector_df["ticker"] == top_ticker].tolist()
        if matches:
            default_index = selector_df.index.get_loc(matches[0])
    selected_label = st.selectbox("Stock", selector_df["label"].tolist(), index=default_index, key=key)
    return selector_df.loc[selector_df["label"] == selected_label, "ticker"].iloc[0]


def show_stock_profile(stock_universe: pd.DataFrame, selected_ticker: str, macro_text: str) -> None:
    if stock_universe.empty:
        st.info("No stock data available.")
        return

    match = stock_universe[stock_universe["ticker"] == selected_ticker]
    if match.empty:
        st.info("Choose a stock from the selector.")
        return

    row = match.iloc[0]
    company_name = row.get("name", "") or selected_ticker
    st.subheader(f"{selected_ticker} - {company_name}")
    st.caption(
        " | ".join(
            str(x)
            for x in [row.get("country", ""), row.get("sector", ""), row.get("industry", "")]
            if pd.notna(x) and str(x)
        )
    )

    stock_metric_cards(
        [
            ("XGB rank", f"{row.get('xgb_rank', '-'):.0f}" if pd.notna(row.get("xgb_rank", None)) else "-"),
            ("XGB predicted return", format_pct(row.get("xgb_prediction_raw"))),
            ("Actual return", format_pct(row.get("returns_2025"))),
            ("Linear rank", f"{row.get('linear_rank', '-'):.0f}" if pd.notna(row.get("linear_rank", None)) else "-"),
            ("Linear predicted return", format_pct(row.get("linear_prediction_raw"))),
            ("ROIC percentile", percentile_text(stock_universe["roic"], row.get("roic")) if "roic" in stock_universe.columns else "-"),
        ]
    )

    missing = missing_key_fields(row)
    if missing:
        st.markdown(
            f"<div class='stock-notice'>Missing key data: {escape(missing)}</div>",
            unsafe_allow_html=True,
        )

    momentum = momentum_assessment(row, stock_universe, macro_text)
    st.markdown("#### Momentum Assessment")
    stock_metric_cards(
        [
            ("Assessment", momentum["label"]),
            ("Momentum score", momentum["score"]),
            ("3M momentum percentile", momentum["price"]),
            ("Google Trends percentile", momentum["trends"]),
            ("Macro support", momentum["macro"]),
            ("Signal type", "Dashboard only"),
        ]
    )

    sector = row.get("sector", "")
    industry = row.get("industry", "")
    sector_peers = stock_universe[stock_universe["sector"] == sector] if sector else pd.DataFrame()
    industry_peers = stock_universe[stock_universe["industry"] == industry] if industry else pd.DataFrame()
    valuation_peers = industry_peers if not industry_peers.empty else sector_peers if not sector_peers.empty else stock_universe

    dcf_value = numeric_value(row.get("valuation_gap"))
    st.markdown("#### DCF and Relative Valuation")
    dcf_left, relative_right = st.columns([0.35, 0.65])
    with dcf_left:
        st.metric("DCF gap", f"{dcf_value:.3f}" if dcf_value is not None else "-", dcf_valuation_read(dcf_value))
        st.caption("1.0 means fairly priced by DCF. Below 1.0 means overvalued; above 1.0 means undervalued.")

    with relative_right:
        valuation = relative_valuation_rows(row, valuation_peers, stock_universe)
        st.dataframe(
            valuation,
            width="stretch",
            hide_index=True,
            column_config={
                "field": "Relative multiple",
                "value": st.column_config.NumberColumn("Ticker value", format="%.3f"),
                "universe_position": "Universe position",
                "peer_position": "Peer position",
                "model_tendency": "Current model tendency",
            },
        )

    st.markdown("#### Business Snapshot")
    feature_order = [
        "momentum_2024",
        "roic",
        "rev_growth_2025",
        "ebit_growth_2025",
        "debt_to_assets",
        "ev_to_ebitda",
        "price_to_earnings",
        "price_to_book",
        "earnings_surprise",
        "stock_volatility",
        "3_mth_momentum",
        "3_month",
        "google_trends",
    ]
    labels = {
        "momentum_2024": "Momentum 2024",
        "roic": "ROIC",
        "rev_growth_2025": "Revenue growth 2025",
        "ebit_growth_2025": "EBIT growth 2025",
        "debt_to_assets": "Debt to assets",
        "ev_to_ebitda": "EV/EBITDA",
        "price_to_earnings": "P/E",
        "price_to_book": "P/B",
        "earnings_surprise": "Earnings surprise",
        "stock_volatility": "Stock volatility",
        "3_mth_momentum": "3 month",
        "3_month": "3 month",
        "google_trends": "Google Trends",
    }
    rows = []
    for col in feature_order:
        if col not in stock_universe.columns:
            continue
        value = row.get(col)
        rows.append(
            {
                "metric": labels.get(col, col),
                "stock": value,
                "universe_percentile": percentile_rank(stock_universe[col], value),
            }
        )
    snapshot = pd.DataFrame(rows)
    st.dataframe(
        snapshot,
        width="stretch",
        hide_index=True,
        column_config={
            "metric": "Metric",
            "stock": st.column_config.NumberColumn("Stock", format="%.3f"),
            "universe_percentile": st.column_config.NumberColumn("Universe percentile", format="%.0f"),
        },
    )

    st.markdown("#### Prediction Comps")
    peer_scope = st.radio("Peer group", ["Industry", "Sector", "All stocks"], horizontal=True)
    if peer_scope == "Industry" and not industry_peers.empty:
        peers = industry_peers
    elif peer_scope == "Sector" and not sector_peers.empty:
        peers = sector_peers
    else:
        peers = stock_universe

    comp_cols = [
        "ticker",
        "name",
        "xgb_rank",
        "xgb_prediction_raw",
        "linear_rank",
        "linear_prediction_raw",
        "price_to_earnings",
        "comps_price_earnings",
        "ev_to_ebitda",
        "comps_ev_ebitda",
        "returns_2025",
    ]
    visible = [c for c in comp_cols if c in peers.columns]
    comps = peers[visible].copy()
    if "xgb_prediction_raw" in comps.columns:
        comps = comps.sort_values("xgb_prediction_raw", ascending=False, na_position="last")
    st.dataframe(
        comps.head(30),
        width="stretch",
        hide_index=True,
        column_config={
            "xgb_rank": st.column_config.NumberColumn("XGB rank", format="%.0f"),
            "xgb_prediction_raw": st.column_config.NumberColumn("XGB predicted", format="%.3f"),
            "linear_rank": st.column_config.NumberColumn("Linear rank", format="%.0f"),
            "linear_prediction_raw": st.column_config.NumberColumn("Linear predicted", format="%.3f"),
            "price_to_earnings": st.column_config.NumberColumn("P/E", format="%.2f"),
            "comps_price_earnings": st.column_config.NumberColumn("P/E peer score", format="%.3f"),
            "ev_to_ebitda": st.column_config.NumberColumn("EV/EBITDA", format="%.2f"),
            "comps_ev_ebitda": st.column_config.NumberColumn("EV/EBITDA peer score", format="%.3f"),
            "returns_2025": st.column_config.NumberColumn("Actual return", format="%.3f"),
        },
    )


sources = run_sources()
source_label = st.sidebar.selectbox("Output source", list(sources.keys()))
source_path = sources[source_label]
plot_dir = source_plot_dir(source_label, source_path)

if st.sidebar.button("Refresh data"):
    st.cache_data.clear()
    st.rerun()

st.sidebar.caption(f"Predictions updated: {file_mtime(source_path / 'xgb_ranked_predictions.csv')}")
st.sidebar.caption(f"Plots folder: {plot_dir}")

xgb_path = source_path / "xgb_ranked_predictions.csv"
linear_path = source_path / "linear_ranked_predictions.csv"
importance_path = source_path / "xgb_feature_importance.csv"
coefficients_path = source_path / "linear_coefficients.csv"
run_log_path = source_path / "xgb_run_log.csv"
xgb_summary_path = source_path / "xgb_summary.txt"
linear_summary_path = source_path / "linear_summary.txt"
feature_data_path = ROOT / "4_ready_for_analysis" / "tickerlist.xlsx"

xgb = normalize_prediction_frame(read_csv(str(xgb_path), file_cache_token(xgb_path)))
linear = normalize_prediction_frame(read_csv(str(linear_path), file_cache_token(linear_path)))
importance = read_csv(str(importance_path), file_cache_token(importance_path))
coefficients = read_csv(str(coefficients_path), file_cache_token(coefficients_path))
run_log = read_csv(str(run_log_path), file_cache_token(run_log_path))
xgb_summary = read_text(str(xgb_summary_path), file_cache_token(xgb_summary_path))
linear_summary = read_text(str(linear_summary_path), file_cache_token(linear_summary_path))
macro_verdict = read_text(str(MACRO_VERDICT_PATH), file_cache_token(MACRO_VERDICT_PATH))
feature_data = read_excel(str(feature_data_path), file_cache_token(feature_data_path))
stock_universe = build_stock_universe(xgb, linear, feature_data)

with st.sidebar.expander("Loaded files", expanded=True):
    st.caption(f"XGB: {xgb_path}")
    st.caption(f"XGB modified: {file_mtime(xgb_path)}")
    st.caption(f"XGB size: {file_size(xgb_path)}")
    st.caption(f"Linear modified: {file_mtime(linear_path)}")
    st.caption(f"Feature data modified: {file_mtime(feature_data_path)}")
    if not xgb.empty and "ticker" in xgb.columns:
        top_loaded = ", ".join(xgb["ticker"].head(5).astype(str).tolist())
        st.caption(f"Top loaded XGB: {top_loaded}")

filter_base = xgb if not xgb.empty else linear
country_options = sorted([x for x in filter_base.get("country", pd.Series(dtype=str)).dropna().unique() if x])
sector_options = sorted([x for x in filter_base.get("sector", pd.Series(dtype=str)).dropna().unique() if x])
industry_options = sorted([x for x in filter_base.get("industry", pd.Series(dtype=str)).dropna().unique() if x])
split_options = sorted([x for x in xgb.get("model_split", pd.Series(dtype=str)).dropna().unique() if x])

countries = st.sidebar.multiselect("Country", country_options)
sectors = st.sidebar.multiselect("Sector", sector_options)
industries = st.sidebar.multiselect("Industry", industry_options)
splits = st.sidebar.multiselect("XGBoost split", split_options)
search = st.sidebar.text_input("Ticker or company")
row_limit = st.sidebar.slider("Rows shown", 10, 250, 50, step=10)

filtered_xgb = filter_predictions(
    xgb,
    countries=countries,
    sectors=sectors,
    industries=industries,
    splits=splits,
    search=search,
)
filtered_linear = filter_predictions(
    linear,
    countries=countries,
    sectors=sectors,
    industries=industries,
    splits=[],
    search=search,
)

st.title("ML Stock Dashboard")
st.markdown(f"<div class='small-muted'>Source: {source_label}</div>", unsafe_allow_html=True)

show_metric_row(run_log, filtered_xgb, filtered_linear)

overview_tab, rankings_tab, portfolio_tab, model_tab, plots_tab, stock_tab, financials_tab, reports_tab = st.tabs(
    ["Overview", "Rankings", "Portfolio", "Model", "Plots", "Individual Stock", "Financials", "Reports"]
)

with overview_tab:
    left, right = st.columns([1.2, 1])
    with left:
        st.subheader("Top XGBoost Picks")
        display_prediction_table(filtered_xgb, row_limit)
    with right:
        st.subheader("Sector Exposure")
        if not filtered_xgb.empty and "sector" in filtered_xgb.columns:
            sector_counts = filtered_xgb["sector"].replace("", "Unknown").value_counts().rename_axis("sector").reset_index(name="count")
            st.bar_chart(sector_counts.set_index("sector"), width="stretch")
        else:
            st.info("No sector data available.")

    if macro_verdict:
        with st.expander("Macro Verdict", expanded=False):
            st.text(macro_verdict)

with rankings_tab:
    model_choice = st.radio("Ranking table", ["XGBoost", "Linear baseline", "Side-by-side"], horizontal=True)
    if model_choice == "XGBoost":
        display_prediction_table(filtered_xgb, row_limit)
    elif model_choice == "Linear baseline":
        display_prediction_table(filtered_linear, row_limit)
    else:
        if filtered_xgb.empty or filtered_linear.empty:
            st.info("Both ranked prediction files are needed for side-by-side comparison.")
        else:
            compare = filtered_xgb.merge(
                filtered_linear[["ticker", "prediction_raw", "prediction_rank"]],
                on="ticker",
                how="inner",
                suffixes=("_xgb", "_linear"),
            )
            compare["rank_gap"] = compare["prediction_rank_linear"] - compare["prediction_rank_xgb"]
            visible = [
                "ticker",
                "name",
                "country",
                "sector",
                "prediction_rank_xgb",
                "prediction_raw_xgb",
                "prediction_rank_linear",
                "prediction_raw_linear",
                "rank_gap",
            ]
            st.dataframe(
                compare[visible].sort_values("prediction_rank_xgb").head(row_limit),
                width="stretch",
                hide_index=True,
            )

with portfolio_tab:
    show_portfolio_suggestion(stock_universe)

with model_tab:
    left, right = st.columns(2)
    with left:
        st.subheader("XGBoost Feature Importance")
        show_bar_chart(importance, "Feature", "Gain", "XGBoost feature importance", 25)
        st.dataframe(importance, width="stretch", hide_index=True)
    with right:
        st.subheader("Linear Coefficients")
        sort_col = "abs_coefficient" if "abs_coefficient" in coefficients.columns else "coefficient"
        if not coefficients.empty and sort_col in coefficients.columns:
            coef_sorted = coefficients.sort_values(sort_col, ascending=False)
        else:
            coef_sorted = coefficients
        show_bar_chart(coef_sorted, "feature", "coefficient", "linear coefficients", 25)
        st.dataframe(coef_sorted, width="stretch", hide_index=True)

    st.subheader("Run History")
    if run_log.empty:
        st.info("No XGBoost run log found.")
    else:
        st.dataframe(run_log, width="stretch", hide_index=True)
        latest = run_log.iloc[-1]
        params_text = latest.get("best_params_json", "")
        if isinstance(params_text, str) and params_text:
            with st.expander("Latest Best Parameters"):
                try:
                    st.json(json.loads(params_text))
                except json.JSONDecodeError:
                    st.code(params_text)

with plots_tab:
    st.subheader("Generated XGBoost Plots")
    show_plot_gallery(plot_dir)

with stock_tab:
    if stock_universe.empty or "ticker" not in stock_universe.columns:
        st.info("No individual stock data available.")
    else:
        selected_ticker = stock_selector(stock_universe, filtered_xgb, "stock_profile_selector")
        if selected_ticker:
            show_stock_profile(stock_universe, selected_ticker, macro_verdict)

with financials_tab:
    if stock_universe.empty or "ticker" not in stock_universe.columns:
        st.info("No financial statement data available.")
    else:
        selected_ticker = stock_selector(stock_universe, filtered_xgb, "financials_selector")
        if selected_ticker:
            show_financial_statement_charts(selected_ticker)

with reports_tab:
    left, right = st.columns(2)
    with left:
        st.subheader("XGBoost Summary")
        st.code(xgb_summary or "Missing xgb_summary.txt")
    with right:
        st.subheader("Linear Summary")
        st.code(linear_summary or "Missing linear_summary.txt")

    log_files = sorted((OUTPUT_DIR / "logs").glob("ml_pipeline*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
    if log_files:
        st.subheader("Latest Pipeline Log")
        selected_log = st.selectbox("Log file", [p.name for p in log_files])
        selected_log_path = OUTPUT_DIR / "logs" / selected_log
        st.code(read_text(str(selected_log_path), file_cache_token(selected_log_path))[-12000:])

