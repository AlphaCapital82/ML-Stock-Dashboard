import itertools
import json
import os

import numpy as np
import pandas as pd
from scipy.stats import shapiro
from statsmodels.stats.outliers_influence import variance_inflation_factor

# --- INPUT ---
EXCEL_PATH = os.path.join("1_cleaned", "tickerlist_cleaned.xlsx")
SHEET_NAME = "Sheet1"
REPORT_PATH = os.path.join("2_diagnosis", "diagnostics_report.txt")
FEATURE_CONFIG_PATH = os.path.join("3_feature_and_variable_transformation", "model_feature_config.json")

DEFAULT_TARGET_COL = "returns_2025"
DEFAULT_FEATURES = [
    "roic",
    "rev_growth_2025",
    "debt_to_assets",
    "ev_to_ebitda",
    "price_to_book",
    "earnings_surprise",
    "google_trends",
    "ebit_growth_2025",
]


def dedupe(items: list[str]) -> list[str]:
    out = []
    for item in items:
        if item and item not in out:
            out.append(item)
    return out


def load_diagnostic_columns() -> tuple[str, list[str]]:
    if not os.path.exists(FEATURE_CONFIG_PATH):
        return DEFAULT_TARGET_COL, DEFAULT_FEATURES.copy()

    with open(FEATURE_CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    target_col = cfg.get("raw_target_column", DEFAULT_TARGET_COL)
    feature_cols = []
    feature_cols.extend(cfg.get("raw_feature_columns", []))
    feature_cols.extend(c for c in cfg.get("ihs_columns", []) if c != target_col)
    for pair in cfg.get("interactions", {}).values():
        if isinstance(pair, list):
            feature_cols.extend(pair)

    feature_cols = [c for c in dedupe(feature_cols) if c != target_col]
    return target_col, feature_cols or DEFAULT_FEATURES.copy()


def calculate_vif(df: pd.DataFrame, variables: list[str]) -> pd.DataFrame:
    variables = [v for v in variables if v in df.columns]
    if df.empty or len(variables) < 2:
        return pd.DataFrame(columns=["feature", "VIF"])
    out = pd.DataFrame()
    out["feature"] = variables
    out["VIF"] = [variance_inflation_factor(df[variables].values, i) for i in range(len(variables))]
    return out


def detect_outliers(df: pd.DataFrame, column: str) -> tuple[int, int]:
    q1 = df[column].quantile(0.25)
    q3 = df[column].quantile(0.75)
    iqr = q3 - q1
    iqr_outliers = ((df[column] < (q1 - 1.5 * iqr)) | (df[column] > (q3 + 1.5 * iqr))).sum()
    z_scores = ((df[column] - df[column].mean()) / df[column].std()).abs()
    z_outliers = (z_scores > 3).sum()
    return int(iqr_outliers), int(z_outliers)


def suggest_drop_from_pair(df: pd.DataFrame, a: str, b: str, target_col: str) -> tuple[str, str]:
    miss_a = float(df[a].isna().mean())
    miss_b = float(df[b].isna().mean())
    corr_a = abs(float(pd.to_numeric(df[target_col], errors="coerce").corr(pd.to_numeric(df[a], errors="coerce"))))
    corr_b = abs(float(pd.to_numeric(df[target_col], errors="coerce").corr(pd.to_numeric(df[b], errors="coerce"))))
    score_a = corr_a - miss_a
    score_b = corr_b - miss_b
    if score_a >= score_b:
        return b, a  # drop, keep
    return a, b


def ihs_candidate_flags(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    rows = []
    for c in cols:
        s = pd.to_numeric(df[c], errors="coerce").dropna()
        if s.empty:
            continue
        skew = float(s.skew())
        kurt = float(s.kurt())
        recommend = abs(skew) >= 1.0 or kurt >= 3.0
        reason = []
        if abs(skew) >= 1.0:
            reason.append(f"|skew|={abs(skew):.2f}>=1")
        if kurt >= 3.0:
            reason.append(f"kurt={kurt:.2f}>=3")
        rows.append(
            {
                "variable": c,
                "skew": skew,
                "kurtosis": kurt,
                "ihs_candidate": recommend,
                "reason": "; ".join(reason) if reason else "none",
            }
        )
    return pd.DataFrame(rows).sort_values(by=["ihs_candidate", "kurtosis"], ascending=[False, False])


def interaction_candidates(df: pd.DataFrame, features: list[str], target: str, min_n: int = 300) -> pd.DataFrame:
    y = pd.to_numeric(df[target], errors="coerce")
    out = []
    for a, b in itertools.combinations(features, 2):
        xa = pd.to_numeric(df[a], errors="coerce")
        xb = pd.to_numeric(df[b], errors="coerce")
        pair = pd.DataFrame({"a": xa, "b": xb, "y": y}).dropna()
        if len(pair) < min_n:
            continue
        pair_corr = float(pair["a"].corr(pair["b"]))
        if abs(pair_corr) > 0.85:
            continue
        prod = pair["a"] * pair["b"]
        c = float(prod.corr(pair["y"]))
        out.append(
            {
                "interaction": f"{a} x {b}",
                "n": int(len(pair)),
                "corr_interaction_with_target": c,
                "pair_corr": pair_corr,
            }
        )
    if not out:
        return pd.DataFrame(columns=["interaction", "n", "corr_interaction_with_target", "pair_corr"])
    df_out = pd.DataFrame(out)
    return df_out.reindex(df_out["corr_interaction_with_target"].abs().sort_values(ascending=False).index)


def main() -> None:
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
    target_col, configured_features = load_diagnostic_columns()
    configured_cols = dedupe([target_col] + configured_features)
    missing_cols = [col for col in configured_cols if col not in df.columns]
    all_cols = [col for col in configured_cols if col in df.columns]
    features = [col for col in configured_features if col in df.columns]

    if target_col not in df.columns:
        raise ValueError(f"Target column missing from cleaned file: {target_col}")

    for col in all_cols:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", "."), errors="coerce")

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)

    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("=== DIAGNOSTICS REPORT ===\n\n")
        f.write(f"Target column: {target_col}\n")
        f.write("Configured features:\n")
        for feature in configured_features:
            f.write(f"- {feature}\n")
        if missing_cols:
            f.write("\nMissing configured columns skipped:\n")
            for col in missing_cols:
                f.write(f"- {col}\n")
        f.write("\n")

        f.write("1. Missing Values:\n")
        f.write(df[all_cols].isnull().sum().to_string())
        f.write("\n\n")

        f.write("2. Summary Statistics:\n")
        f.write(df[all_cols].describe().to_string())
        f.write("\n\n")

        f.write("3. Skewness:\n")
        for var in all_cols:
            f.write(f"{var}: {df[var].skew():.6f}\n")
        f.write("\n")

        f.write("4. Normality (Shapiro-Wilk p-values):\n")
        for var in all_cols:
            try:
                x = df[var].dropna()
                pval = shapiro(x.sample(n=min(5000, x.shape[0]), random_state=1))[1] if len(x) >= 3 else np.nan
                f.write(f"{var}: {pval:.3e}\n")
            except Exception as e:
                f.write(f"{var}: Error ({e})\n")
        f.write("\n")

        f.write("5. VIF (Predictors Only):\n")
        vif_features = [c for c in features if df[c].notna().sum() > 1]
        vif_base = df[vif_features].dropna() if vif_features else pd.DataFrame()
        vif_df = calculate_vif(vif_base, vif_features)
        if vif_df.empty:
            f.write("Skipped: fewer than two complete numeric predictor columns after dropping missing values.")
        else:
            f.write(vif_df.sort_values("VIF", ascending=False).to_string(index=False))
        f.write("\n\n")

        f.write("6. Outlier Summary:\n")
        f.write(f"{'Variable':>22}  {'IQR-outliers':>13}  {'Z-outliers':>10}\n")
        for var in all_cols:
            iqr_outliers, z_outliers = detect_outliers(df, var)
            f.write(f"{var:>22}  {iqr_outliers:13}  {z_outliers:10}\n")
        f.write("\n")

        f.write("7. Collinearity Suggestions:\n")
        corr = df[features].corr().abs()
        high_pairs = []
        for a, b in itertools.combinations(features, 2):
            c = corr.loc[a, b]
            if pd.notna(c) and c >= 0.80:
                high_pairs.append((a, b, float(c)))

        if not high_pairs:
            f.write("No feature pairs above |corr| >= 0.80.\n\n")
        else:
            f.write("High-correlation pairs (|corr| >= 0.80):\n")
            for a, b, c in sorted(high_pairs, key=lambda x: x[2], reverse=True):
                drop, keep = suggest_drop_from_pair(df, a, b, target_col)
                f.write(f"- {a} vs {b}: |corr|={c:.3f}. Suggested drop: {drop} (keep: {keep}).\n")
            f.write("\n")

        f.write("8. IHS Transformation Candidates:\n")
        ihs_df = ihs_candidate_flags(df, all_cols)
        f.write(ihs_df.to_string(index=False))
        f.write("\n\n")

        f.write("9. Suggested Interaction Candidates (Exploratory):\n")
        int_df = interaction_candidates(df, features, target_col, min_n=300)
        if int_df.empty:
            f.write("No interaction candidates met thresholds.\n")
        else:
            f.write(int_df.head(10).to_string(index=False))
            f.write("\n")


if __name__ == "__main__":
    main()
