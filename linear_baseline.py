import json
import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import ElasticNetCV, LinearRegression, RidgeCV
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


FILE_PATH = os.path.join("4_ready_for_analysis", "tickerlist.xlsx")
SHEET_NAME = "Sheet1"
MODEL_CONFIG_PATH = os.path.join("3_feature_and_variable_transformation", "model_config.json")
OUTPUT_DIR = "5_output"

PREDICTIONS_CSV_PATH = os.path.join(OUTPUT_DIR, "linear_predictions.csv")
RANKED_PREDICTIONS_CSV_PATH = os.path.join(OUTPUT_DIR, "linear_ranked_predictions.csv")
COEFFICIENTS_CSV_PATH = os.path.join(OUTPUT_DIR, "linear_coefficients.csv")
SUMMARY_TXT_PATH = os.path.join(OUTPUT_DIR, "linear_summary.txt")

DEFAULT_TARGET_COL = "returns_2025_ihs"
DEFAULT_RAW_TARGET_COL = "returns_2025"
DEFAULT_FEATURES = [
    "roic_ihs",
    "rev_growth_2025_ihs",
    "debt_to_assets",
    "ev_to_ebitda",
    "price_to_book_ihs",
    "earnings_surprise_ihs",
    "google_trends_ihs",
    "ebit_growth_2025_ihs",
    "int_rev_growth_2025_x_price_to_book",
    "int_ev_to_ebitda_x_price_to_book",
    "int_rev_growth_2025_x_ev_to_ebitda",
    "int_debt_to_assets_x_price_to_book",
]

RANDOM_STATE = 42
TEST_SIZE = 0.20
VAL_SIZE_WITHIN_TRAIN = 0.125


def rmse(y_true, y_pred) -> float:
    return float(np.sqrt(mean_squared_error(y_true, y_pred)))


def inverse_target_transform(values: np.ndarray, target_col: str) -> np.ndarray:
    if target_col.endswith("_ihs"):
        return np.sinh(values)
    return values


def load_model_config() -> dict:
    if os.path.exists(MODEL_CONFIG_PATH):
        with open(MODEL_CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "target_column": DEFAULT_TARGET_COL,
        "raw_target_column": DEFAULT_RAW_TARGET_COL,
        "feature_columns": DEFAULT_FEATURES,
    }


def coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def evaluate(name: str, model, x_train, y_train, x_val, y_val, x_test, y_test) -> dict:
    metrics = {"model": name}
    for split, x, y in [
        ("train", x_train, y_train),
        ("val", x_val, y_val),
        ("test", x_test, y_test),
    ]:
        pred = model.predict(x)
        metrics[f"{split}_rmse"] = rmse(y, pred)
        metrics[f"{split}_r2"] = float(r2_score(y, pred))
    return metrics


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if not os.path.exists(FILE_PATH):
        raise FileNotFoundError(f"File not found: {FILE_PATH}")

    source_df = pd.read_excel(FILE_PATH, sheet_name=SHEET_NAME)
    cfg = load_model_config()
    target_col = cfg.get("target_column", DEFAULT_TARGET_COL)
    raw_target_col = cfg.get("raw_target_column", DEFAULT_RAW_TARGET_COL)
    features = cfg.get("feature_columns", DEFAULT_FEATURES)

    missing = [col for col in [target_col] + features if col not in source_df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    df = source_df.dropna(subset=[target_col]).copy()
    df = coerce_numeric(df, [target_col] + features)
    df = df.dropna(subset=[target_col]).copy()
    skipped_empty_features = [col for col in features if df[col].notna().sum() == 0]
    features = [col for col in features if col not in skipped_empty_features]
    if not features:
        raise ValueError("No usable features remain after removing all-empty columns.")

    x = df[features].copy()
    y = df[target_col].copy()

    x_train_val, x_test, y_train_val, y_test = train_test_split(
        x, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )
    x_train, x_val, y_train, y_val = train_test_split(
        x_train_val, y_train_val, test_size=VAL_SIZE_WITHIN_TRAIN, random_state=RANDOM_STATE
    )

    models = {
        "linear_regression": Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                ("model", LinearRegression()),
            ]
        ),
        "ridge": Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                ("model", RidgeCV(alphas=np.logspace(-4, 4, 25))),
            ]
        ),
        "elastic_net": Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                ("model", ElasticNetCV(
                    alphas=np.logspace(-4, 2, 25),
                    l1_ratio=[0.1, 0.3, 0.5, 0.7, 0.9],
                    cv=5,
                    random_state=RANDOM_STATE,
                    max_iter=20000,
                )),
            ]
        ),
    }

    rows = []
    fitted = {}
    for name, model in models.items():
        model.fit(x_train, y_train)
        fitted[name] = model
        rows.append(evaluate(name, model, x_train, y_train, x_val, y_val, x_test, y_test))

    metrics_df = pd.DataFrame(rows).sort_values("test_rmse")
    best_name = str(metrics_df.iloc[0]["model"])
    best_model = fitted[best_name]

    all_x = source_df[features].copy()
    all_x = coerce_numeric(all_x, features)
    pred_transformed = best_model.predict(all_x)
    pred_raw = inverse_target_transform(pred_transformed, target_col)

    id_cols = [
        "ticker",
        "name",
        "country",
        "industry",
        "sector",
        raw_target_col,
        target_col,
    ]
    id_cols = [col for col in id_cols if col in source_df.columns]
    predictions = source_df[id_cols].copy()
    predictions["baseline_model"] = best_name
    predictions["prediction_transformed"] = pred_transformed
    predictions["prediction_raw"] = pred_raw
    predictions["prediction_rank"] = predictions["prediction_raw"].rank(ascending=False, method="first")
    predictions.to_csv(PREDICTIONS_CSV_PATH, index=False, encoding="utf-8-sig")
    predictions.sort_values("prediction_raw", ascending=False).to_csv(
        RANKED_PREDICTIONS_CSV_PATH,
        index=False,
        encoding="utf-8-sig",
    )

    model_step = best_model.named_steps["model"]
    coefs = getattr(model_step, "coef_", np.zeros(len(features)))
    coefficients = pd.DataFrame(
        {
            "feature": features,
            "coefficient": coefs,
            "abs_coefficient": np.abs(coefs),
        }
    ).sort_values("abs_coefficient", ascending=False)
    coefficients.to_csv(COEFFICIENTS_CSV_PATH, index=False, encoding="utf-8-sig")

    with open(SUMMARY_TXT_PATH, "w", encoding="utf-8") as f:
        f.write("LINEAR BASELINE SUMMARY\n")
        f.write(f"Generated UTC: {datetime.now(timezone.utc).isoformat(timespec='seconds')}\n")
        f.write(f"Input file: {FILE_PATH}\n")
        f.write(f"Rows scored: {len(predictions)}\n")
        f.write(f"Rows with target used for modeling: {len(df)}\n")
        f.write(f"Target column: {target_col}\n")
        f.write(f"Raw target column: {raw_target_col}\n")
        f.write(f"Feature count: {len(features)}\n")
        if skipped_empty_features:
            f.write(f"Skipped all-empty features: {', '.join(skipped_empty_features)}\n")
        f.write(f"Best baseline model: {best_name}\n\n")
        f.write("Metrics:\n")
        f.write(metrics_df.to_string(index=False))
        f.write("\n\nOutput files:\n")
        f.write(f"- {PREDICTIONS_CSV_PATH}\n")
        f.write(f"- {RANKED_PREDICTIONS_CSV_PATH}\n")
        f.write(f"- {COEFFICIENTS_CSV_PATH}\n")

    print(f"Best baseline model: {best_name}")
    print(metrics_df.to_string(index=False))
    print(f"Saved baseline predictions to: {PREDICTIONS_CSV_PATH}")
    print(f"Saved ranked baseline predictions to: {RANKED_PREDICTIONS_CSV_PATH}")
    print(f"Saved baseline coefficients to: {COEFFICIENTS_CSV_PATH}")
    print(f"Saved baseline summary to: {SUMMARY_TXT_PATH}")


if __name__ == "__main__":
    main()
