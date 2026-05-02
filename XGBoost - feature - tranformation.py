import json
import os

import numpy as np
import pandas as pd

# --- Paths ---
INPUT_PATH = os.path.join("1_cleaned", "tickerlist_cleaned.xlsx")
INPUT_SHEET = "Sheet1"
OUTPUT_DIR = "4_ready_for_analysis"
OUTPUT_FILE = "tickerlist.xlsx"
REPORT_DIR = "3_feature_and_variable_transformation"
REPORT_FILE = "feature_transformation_report.txt"
MODEL_CONFIG_FILE = "model_config.json"
FEATURE_CONFIG_FILE = "model_feature_config.json"

DEFAULT_FEATURE_CONFIG = {
    "target_column": "returns_2025_ihs",
    "raw_target_column": "returns_2025",
    "ihs_columns": [
        "returns_2025",
        "roic",
        "rev_growth_2025",
        "price_to_book",
        "earnings_surprise",
        "google_trends",
        "ebit_growth_2025",
    ],
    "raw_feature_columns": ["debt_to_assets", "ev_to_ebitda"],
    "transformed_feature_columns": [
        "roic_ihs",
        "rev_growth_2025_ihs",
        "price_to_book_ihs",
        "earnings_surprise_ihs",
        "google_trends_ihs",
        "ebit_growth_2025_ihs",
    ],
    "interactions": {
        "int_rev_growth_2025_x_price_to_book": ["rev_growth_2025", "price_to_book"],
        "int_ev_to_ebitda_x_price_to_book": ["ev_to_ebitda", "price_to_book"],
        "int_rev_growth_2025_x_ev_to_ebitda": ["rev_growth_2025", "ev_to_ebitda"],
        "int_debt_to_assets_x_price_to_book": ["debt_to_assets", "price_to_book"],
    },
    "pdp_2d_pairs": [
        ["rev_growth_2025_ihs", "price_to_book_ihs"],
        ["ev_to_ebitda", "price_to_book_ihs"],
    ],
}


def ihs_transform(series: pd.Series) -> pd.Series:
    x = pd.to_numeric(series, errors="coerce")
    return np.log(x + np.sqrt(x * x + 1.0))


def load_feature_config() -> dict:
    config_path = os.path.join(REPORT_DIR, FEATURE_CONFIG_FILE)
    if not os.path.exists(config_path):
        os.makedirs(REPORT_DIR, exist_ok=True)
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_FEATURE_CONFIG, f, indent=2)
        return DEFAULT_FEATURE_CONFIG.copy()

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    merged = DEFAULT_FEATURE_CONFIG.copy()
    merged.update(cfg)
    return merged


def main() -> None:
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(f"Input file not found: {INPUT_PATH}")

    df = pd.read_excel(INPUT_PATH, sheet_name=INPUT_SHEET)
    cfg = load_feature_config()
    actions = []
    warnings = []

    # Create IHS transformed features.
    for col in cfg.get("ihs_columns", []):
        if col in df.columns:
            out_col = f"{col}_ihs"
            df[out_col] = ihs_transform(df[col])
            actions.append(f"Created IHS feature: {out_col}")
        else:
            warnings.append(f"IHS source column missing: {col}")

    # Create interactions. Fill missing with 0 to avoid propagating NaN into interaction terms.
    for out_col, pair in cfg.get("interactions", {}).items():
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            warnings.append(f"Interaction skipped because pair is invalid: {out_col}")
            continue
        a, b = pair
        if a in df.columns and b in df.columns:
            df[out_col] = pd.to_numeric(df[a], errors="coerce").fillna(0) * pd.to_numeric(df[b],
                                                                                          errors="coerce").fillna(0)
            actions.append(f"Created interaction: {out_col} = {a} * {b} (NaN->0 before multiply)")
        else:
            warnings.append(f"Interaction source columns missing for {out_col}: {a}, {b}")

    requested_features = []
    requested_features.extend(cfg.get("raw_feature_columns", []))
    requested_features.extend(cfg.get("transformed_feature_columns", []))
    requested_features.extend(cfg.get("interactions", {}).keys())
    feature_columns = []
    for col in requested_features:
        if col in df.columns and col not in feature_columns:
            feature_columns.append(col)
        elif col not in df.columns:
            warnings.append(f"Model feature missing after transformation: {col}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    df.to_excel(output_path, index=False)

    os.makedirs(REPORT_DIR, exist_ok=True)
    report_path = os.path.join(REPORT_DIR, REPORT_FILE)
    config_path = os.path.join(REPORT_DIR, MODEL_CONFIG_FILE)
    model_config = {
        "target_column": cfg.get("target_column", "returns_2025_ihs"),
        "raw_target_column": cfg.get("raw_target_column", "returns_2025"),
        "feature_columns": feature_columns,
        "pdp_2d_pairs": cfg.get("pdp_2d_pairs", []),
    }
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(model_config, f, indent=2)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("FEATURE TRANSFORMATION REPORT\n")
        f.write(f"Input: {INPUT_PATH}\n")
        f.write(f"Output: {output_path}\n")
        f.write(f"Rows: {len(df)}\n")
        f.write(f"Columns: {len(df.columns)}\n\n")
        f.write("Applied actions:\n")
        for item in actions:
            f.write(f"- {item}\n")
        if warnings:
            f.write("\nWarnings:\n")
            for item in warnings:
                f.write(f"- {item}\n")
        f.write("\nModel features:\n")
        for c in feature_columns:
            f.write(f"- {c}\n")
        f.write("\nFinal columns:\n")
        for c in df.columns:
            f.write(f"- {c}\n")

    print(f"Saved transformed dataset to: {output_path}")
    print(f"Saved transformation report to: {report_path}")
    print(f"Saved model config to: {config_path}")
    if warnings:
        print(f"Warnings: {len(warnings)}. See report for details.")


if __name__ == "__main__":
    main()
