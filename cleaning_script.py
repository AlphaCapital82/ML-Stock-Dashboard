import os
import json
import numpy as np
import pandas as pd

# --- Input / Output ---
INPUT_PATH = os.path.join("0_needs_processing", "tickerlist.xlsx")
SHEET_NAME = "Sheet1"
OUTPUT_DIR = "1_cleaned"
OUTPUT_FILE = "tickerlist_cleaned.xlsx"
FEATURE_CONFIG_PATH = os.path.join("3_feature_and_variable_transformation", "model_feature_config.json")

# --- Columns ---
DEFAULT_Y_COL = "returns_2025"
DEFAULT_X_COLS = [
    "roic",
    "rev_growth_2025",
    "debt_to_assets",
    "ev_to_ebitda",
    "price_to_book",
    "earnings_surprise",
    "google_trends",
    "ebit_growth_2025",
]

# --- Cleaning / Feature Engineering ---
PERCENT_TO_FRACTION = False
WINSOR_LO = 0.02
WINSOR_HI = 0.98

NA_TOKENS = {
    "",
    "na",
    "n/a",
    "nan",
    "null",
    "-",
    "--",
    "#n/a",
    "#div/0!",
    "none",
    "inf",
    "-inf",
}


def dedupe(items: list[str]) -> list[str]:
    out = []
    for item in items:
        if item and item not in out:
            out.append(item)
    return out


def load_cleaning_columns() -> tuple[str, list[str]]:
    if not os.path.exists(FEATURE_CONFIG_PATH):
        return DEFAULT_Y_COL, DEFAULT_X_COLS.copy()

    with open(FEATURE_CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    target_col = cfg.get("raw_target_column", DEFAULT_Y_COL)
    feature_cols = []
    feature_cols.extend(cfg.get("raw_feature_columns", []))
    feature_cols.extend(c for c in cfg.get("ihs_columns", []) if c != target_col)
    for pair in cfg.get("interactions", {}).values():
        if isinstance(pair, list):
            feature_cols.extend(pair)

    feature_cols = [c for c in dedupe(feature_cols) if c != target_col]
    return target_col, feature_cols or DEFAULT_X_COLS.copy()


def parse_number(x, percent_to_fraction=PERCENT_TO_FRACTION):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return np.nan
    if isinstance(x, (int, float, np.integer, np.floating)):
        v = float(x)
        return np.nan if np.isinf(v) else v

    s = str(x).strip()
    if not s or s.lower() in NA_TOKENS:
        return np.nan

    s = s.replace("\u00A0", "").replace(" ", "")

    is_paren_neg = s.startswith("(") and s.endswith(")")
    if is_paren_neg:
        s = s[1:-1]

    is_percent = "%" in s
    if is_percent:
        s = s.replace("%", "")

    for token in ["$", "\u20ac", "\u00a3", "kr", "NOK", "USD", "â‚¬", "Â£", "Ã¢â€šÂ¬", "Ã‚Â£"]:
        s = s.replace(token, "")

    for minus in ["\u2212", "\u2013", "\u2014", "âˆ’", "â€“", "â€”", "Ã¢Ë†â€™"]:
        s = s.replace(minus, "-")

    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")

    try:
        val = float(s)
    except ValueError:
        return np.nan

    if is_paren_neg:
        val = -val
    if np.isinf(val):
        return np.nan
    if is_percent and percent_to_fraction:
        val /= 100.0
    return val


def winsorize_series(series: pd.Series, lower_q=WINSOR_LO, upper_q=WINSOR_HI) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    nonnull = s.dropna()
    if nonnull.empty:
        return s
    lo = nonnull.quantile(lower_q)
    hi = nonnull.quantile(upper_q)
    return s.clip(lo, hi)


def build_actions_report(actions: list[dict], input_rows: int) -> str:
    lines = []
    lines.append("CLEANING ACTIONS")
    lines.append(f"Rows processed: {input_rows}")
    lines.append("")
    lines.append("Parse step (per column):")
    for a in actions:
        if a["step"] != "parse":
            continue
        lines.append(
            f"- {a['column']}: missing {a['missing_before']} -> {a['missing_after']}, "
            f"new_missing_from_parse={a['new_missing_from_parse']}"
        )
    lines.append("")
    lines.append("Winsorization step (per column):")
    for a in actions:
        if a["step"] != "winsorize":
            continue
        lines.append(
            f"- {a['column']}: lo={a['lo']:.6g}, hi={a['hi']:.6g}, "
            f"low_clipped={a['low_clipped']}, high_clipped={a['high_clipped']}"
        )
    return "\n".join(lines)


def clean_and_engineer(df: pd.DataFrame, target_col: str, feature_cols: list[str]) -> tuple[pd.DataFrame, list[dict]]:
    df = df.copy()
    actions = []
    all_num_cols = dedupe([target_col] + feature_cols)

    for c in all_num_cols:
        if c in df.columns:
            before = df[c]
            parsed = before.map(parse_number)
            actions.append(
                {
                    "step": "parse",
                    "column": c,
                    "missing_before": int(before.isna().sum()),
                    "missing_after": int(parsed.isna().sum()),
                    "new_missing_from_parse": int((before.notna() & parsed.isna()).sum()),
                }
            )
            df[c] = parsed

    for c in feature_cols:
        if c in df.columns:
            s = pd.to_numeric(df[c], errors="coerce")
            nonnull = s.dropna()
            if nonnull.empty:
                continue
            lo = float(nonnull.quantile(WINSOR_LO))
            hi = float(nonnull.quantile(WINSOR_HI))
            actions.append(
                {
                    "step": "winsorize",
                    "column": c,
                    "lo": lo,
                    "hi": hi,
                    "low_clipped": int((s < lo).sum()),
                    "high_clipped": int((s > hi).sum()),
                }
            )
            df[c] = s.clip(lo, hi)

    return df, actions


def main() -> None:
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")

    df = pd.read_excel(INPUT_PATH, sheet_name=SHEET_NAME)
    target_col, feature_cols = load_cleaning_columns()
    cleaned_df, actions = clean_and_engineer(df, target_col, feature_cols)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    cleaned_df.to_excel(output_path, index=False)
    print(f"Saved cleaned file to: {output_path}")

    actions_report = build_actions_report(actions, len(df))
    actions_report_path = os.path.join(OUTPUT_DIR, os.path.splitext(OUTPUT_FILE)[0] + "_actions.txt")
    with open(actions_report_path, "w", encoding="utf-8") as f:
        f.write(actions_report + "\n")
    print(f"Saved cleaning actions report to: {actions_report_path}")


if __name__ == "__main__":
    main()
