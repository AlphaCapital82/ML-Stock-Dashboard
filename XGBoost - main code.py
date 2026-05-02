import os
import json
import csv
import warnings
from datetime import datetime, timezone
from typing import List

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import xgboost as xgb
from xgboost import XGBRegressor, plot_importance

from sklearn.model_selection import RandomizedSearchCV, train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.inspection import PartialDependenceDisplay


# =========================
# Configuration
# =========================
FILE_PATH = os.path.join("4_ready_for_analysis", "tickerlist.xlsx")
SHEET_NAME = "Sheet1"
MODEL_CONFIG_PATH = os.path.join("3_feature_and_variable_transformation", "model_config.json")

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
DEFAULT_PDP_2D_PAIRS = [
    ["rev_growth_2025_ihs", "price_to_book_ihs"],
    ["ev_to_ebitda", "price_to_book_ihs"],
]

RANDOM_STATE = 42

# 3-way split for clean early stopping + clean test
TEST_SIZE = 0.20
VAL_SIZE_WITHIN_TRAIN = 0.125  # 12.5% of remaining 80% => 10% of total as val

SAVE_PLOTS = True
PLOTS_DIR = os.path.join("5_output", "xgb_plots")
RUN_LOG_PATH = os.path.join("5_output", "xgb_run_log.csv")
PREDICTIONS_CSV_PATH = os.path.join("5_output", "xgb_predictions.csv")
RANKED_PREDICTIONS_CSV_PATH = os.path.join("5_output", "xgb_ranked_predictions.csv")
FEATURE_IMPORTANCE_CSV_PATH = os.path.join("5_output", "xgb_feature_importance.csv")
SUMMARY_TXT_PATH = os.path.join("5_output", "xgb_summary.txt")

# Hyperparameter search space
PARAM_GRID = {
    "n_estimators": [300, 500, 800],
    "learning_rate": [0.02, 0.03, 0.05],
    "max_depth": [2, 3, 4],
    "min_child_weight": [3, 5, 8],
    "gamma": [0, 0.03, 0.05, 0.10],
    "subsample": [0.75, 0.85, 0.95],
    "colsample_bytree": [0.6, 0.75, 0.9],
    "reg_alpha": [0, 0.001, 0.01, 0.05],
    "reg_lambda": [1, 2, 5],
}
DEFAULT_N_ITER = 100
DEFAULT_CV_FOLDS = 5


# =========================
# Helpers
# =========================
def ensure_dir(path: str) -> None:
    if SAVE_PLOTS:
        os.makedirs(path, exist_ok=True)


def save_or_show(fig: plt.Figure, filename: str) -> None:
    if SAVE_PLOTS:
        out_path = os.path.join(PLOTS_DIR, filename)
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
    else:
        plt.show()


def coerce_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def safe_name(s: str) -> str:
    return (
        s.replace("/", "_")
        .replace("\\", "_")
        .replace(":", "_")
        .replace("*", "_")
        .replace("?", "_")
        .replace('"', "_")
        .replace("<", "_")
        .replace(">", "_")
        .replace("|", "_")
        .replace(" ", "_")
    )


def rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float(np.sqrt(mean_squared_error(y_true, y_pred)))


def print_metrics(tag: str, y_true: pd.Series, y_pred: np.ndarray) -> None:
    y_true_np = np.asarray(y_true)
    r = rmse(y_true_np, y_pred)
    r2 = float(r2_score(y_true_np, y_pred))
    print(f"{tag} RMSE: {r:.6f}")
    print(f"{tag} R-squared: {r2:.6f}")


def build_tuning_suggestions(train_rmse: float, val_rmse: float, test_rmse: float) -> List[str]:
    suggestions: List[str] = []
    if test_rmse <= 0:
        return suggestions

    overfit_gap = test_rmse - train_rmse
    rel_gap = overfit_gap / max(test_rmse, 1e-9)
    val_test_gap = abs(val_rmse - test_rmse) / max(test_rmse, 1e-9)

    if rel_gap > 0.20:
        suggestions.append("Possible overfitting: decrease max_depth and/or increase min_child_weight.")
        suggestions.append("Try stronger regularization: increase reg_alpha and reg_lambda.")
        suggestions.append("Lower learning_rate slightly and keep early stopping.")
    elif rel_gap < 0.05 and test_rmse > val_rmse * 1.03:
        suggestions.append("Possible underfitting: allow slightly more complexity (max_depth or lower min_child_weight).")
        suggestions.append("Try reducing regularization a bit if train/val are both high.")
    else:
        suggestions.append("Model balance looks reasonable; tune around current best parameters.")

    if val_test_gap > 0.10:
        suggestions.append("Validation and test differ a lot; consider more robust split or repeated runs with different seeds.")

    return suggestions


def append_run_log(
    file_path: str,
    target_col: str,
    feature_count: int,
    train_rmse: float,
    val_rmse: float,
    test_rmse: float,
    train_r2: float,
    val_r2: float,
    test_r2: float,
    best_params: dict,
    suggestions: List[str],
) -> None:
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    exists = os.path.exists(file_path)
    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "timestamp_utc",
                "target_col",
                "feature_count",
                "train_rmse",
                "val_rmse",
                "test_rmse",
                "train_r2",
                "val_r2",
                "test_r2",
                "best_params_json",
                "suggestions",
            ],
        )
        if not exists:
            writer.writeheader()
        writer.writerow(
            {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "target_col": target_col,
                "feature_count": feature_count,
                "train_rmse": f"{train_rmse:.6f}",
                "val_rmse": f"{val_rmse:.6f}",
                "test_rmse": f"{test_rmse:.6f}",
                "train_r2": f"{train_r2:.6f}",
                "val_r2": f"{val_r2:.6f}",
                "test_r2": f"{test_r2:.6f}",
                "best_params_json": json.dumps(best_params, sort_keys=True),
                "suggestions": " | ".join(suggestions),
            }
        )


def inverse_target_transform(values: np.ndarray, target_col: str) -> np.ndarray:
    if target_col.endswith("_ihs"):
        return np.sinh(values)
    return values


def save_dashboard_outputs(
    source_df: pd.DataFrame,
    model_df: pd.DataFrame,
    features: list[str],
    target_col: str,
    raw_target_col: str,
    tuned_model: XGBRegressor,
    train_medians: pd.Series,
    split_by_index: dict[int, str],
    importance_df: pd.DataFrame,
    metrics: dict[str, float],
    best_params: dict,
    suggestions: list[str],
) -> None:
    os.makedirs("5_output", exist_ok=True)

    all_x = source_df[features].copy()
    all_x = coerce_numeric(all_x, features).fillna(train_medians)
    pred_transformed = tuned_model.predict(all_x)
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
    id_cols = [c for c in id_cols if c in source_df.columns]
    predictions = source_df[id_cols].copy()
    predictions["prediction_transformed"] = pred_transformed
    predictions["prediction_raw"] = pred_raw
    predictions["model_split"] = [split_by_index.get(idx, "unscored") for idx in source_df.index]
    predictions["has_training_target"] = source_df.index.isin(model_df.index)
    predictions["prediction_rank"] = predictions["prediction_raw"].rank(ascending=False, method="first")

    predictions.to_csv(PREDICTIONS_CSV_PATH, index=False, encoding="utf-8-sig")
    predictions.sort_values("prediction_raw", ascending=False).to_csv(
        RANKED_PREDICTIONS_CSV_PATH,
        index=False,
        encoding="utf-8-sig",
    )

    if not importance_df.empty:
        importance_df.to_csv(FEATURE_IMPORTANCE_CSV_PATH, index=False, encoding="utf-8-sig")

    with open(SUMMARY_TXT_PATH, "w", encoding="utf-8") as f:
        f.write("XGBOOST MODEL SUMMARY\n")
        f.write(f"Generated UTC: {datetime.now(timezone.utc).isoformat(timespec='seconds')}\n")
        f.write(f"Input file: {FILE_PATH}\n")
        f.write(f"Rows scored: {len(predictions)}\n")
        f.write(f"Rows with target used for modeling: {len(model_df)}\n")
        f.write(f"Target column: {target_col}\n")
        f.write(f"Raw target column: {raw_target_col}\n")
        f.write(f"Feature count: {len(features)}\n\n")
        f.write("Metrics:\n")
        for key, value in metrics.items():
            f.write(f"- {key}: {value:.6f}\n")
        f.write("\nBest parameters:\n")
        f.write(json.dumps(best_params, indent=2, sort_keys=True))
        f.write("\n\nTuning suggestions:\n")
        for suggestion in suggestions:
            f.write(f"- {suggestion}\n")
        f.write("\nOutput files:\n")
        f.write(f"- {PREDICTIONS_CSV_PATH}\n")
        f.write(f"- {RANKED_PREDICTIONS_CSV_PATH}\n")
        f.write(f"- {FEATURE_IMPORTANCE_CSV_PATH}\n")
        f.write(f"- {RUN_LOG_PATH}\n")


def load_model_config() -> dict:
    if os.path.exists(MODEL_CONFIG_PATH):
        with open(MODEL_CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        print(f"Loaded model config: {MODEL_CONFIG_PATH}")
        return cfg
    print(f"Model config not found, using defaults: {MODEL_CONFIG_PATH}")
    return {
        "target_column": DEFAULT_TARGET_COL,
        "raw_target_column": DEFAULT_RAW_TARGET_COL,
        "feature_columns": DEFAULT_FEATURES,
        "pdp_2d_pairs": DEFAULT_PDP_2D_PAIRS,
        "hyperparameter_tuning": {
            "param_grid": PARAM_GRID,
            "n_iter": DEFAULT_N_ITER,
            "cv_folds": DEFAULT_CV_FOLDS,
        },
    }


# =========================
# Main
# =========================
def main() -> None:
    warnings.filterwarnings("once")

    ensure_dir(PLOTS_DIR)

    if not os.path.exists(FILE_PATH):
        raise FileNotFoundError(f"File not found: {FILE_PATH}")

    source_df = pd.read_excel(FILE_PATH, sheet_name=SHEET_NAME)
    df = source_df.copy()
    cfg = load_model_config()
    target_col = cfg.get("target_column", DEFAULT_TARGET_COL)
    raw_target_col = cfg.get("raw_target_column", DEFAULT_RAW_TARGET_COL)
    features = cfg.get("feature_columns", DEFAULT_FEATURES)
    pairs_2d = cfg.get("pdp_2d_pairs", DEFAULT_PDP_2D_PAIRS)
    tuning_cfg = cfg.get("hyperparameter_tuning", {})
    param_grid = tuning_cfg.get("param_grid", PARAM_GRID)
    n_iter = int(tuning_cfg.get("n_iter", DEFAULT_N_ITER))
    configured_cv = int(tuning_cfg.get("cv_folds", DEFAULT_CV_FOLDS))

    if target_col not in df.columns:
        raise ValueError(f"Missing target column '{target_col}'. Found columns: {list(df.columns)}")

    # Keep only rows with a target
    df = df.dropna(subset=[target_col]).copy()

    missing_features = [c for c in features if c not in df.columns]
    if missing_features:
        raise ValueError(
            f"Missing required model features from config: {missing_features}\n"
            f"Found columns: {list(df.columns)}"
        )

    # Coerce numeric
    df = coerce_numeric(df, [target_col] + features)
    df = df.dropna(subset=[target_col]).copy()

    # Prepare X/y
    X = df[features].copy()
    y = df[target_col].copy()

    # Split: train_val vs test
    X_train_val, X_test, y_train_val, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )

    # Split train_val: train vs val (for early stopping)
    X_train, X_val, y_train, y_val = train_test_split(
        X_train_val, y_train_val, test_size=VAL_SIZE_WITHIN_TRAIN, random_state=RANDOM_STATE
    )
    split_by_index = {idx: "train" for idx in X_train.index}
    split_by_index.update({idx: "validation" for idx in X_val.index})
    split_by_index.update({idx: "test" for idx in X_test.index})

    # Median imputation based on TRAIN ONLY
    train_medians = X_train.median(numeric_only=True)
    X_train_f = X_train.fillna(train_medians)
    X_val_f = X_val.fillna(train_medians)
    X_test_f = X_test.fillna(train_medians)

    print(f"Rows: total={len(df)}, train={len(X_train_f)}, val={len(X_val_f)}, test={len(X_test_f)}")
    print(f"Features: {len(features)}")

    # =========================
    # Initial model quick check
    # =========================
    initial_model = XGBRegressor(
        n_estimators=600,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=1.0,
        reg_alpha=0.0,
        min_child_weight=3,
        gamma=0.0,
        objective="reg:squarederror",
        random_state=RANDOM_STATE,
        n_jobs=1,
        early_stopping_rounds=25, # Moved here
    )

    initial_model.fit(
        X_train_f, y_train,
        eval_set=[(X_val_f, y_val)],
        verbose=False,
    )

    print("\nInitial Model Evaluation")
    print_metrics("Train", y_train, initial_model.predict(X_train_f))
    print_metrics("Val", y_val, initial_model.predict(X_val_f))
    print_metrics("Test", y_test, initial_model.predict(X_test_f))

    # Initial importance plot
    fig, ax = plt.subplots(figsize=(10, 6))
    plot_importance(initial_model, importance_type="gain", max_num_features=20, ax=ax)
    ax.set_title("Feature Importance (Initial Model, Gain)")
    plt.tight_layout()
    save_or_show(fig, "importance_initial_gain.png")

    # =========================
    # Hyperparameter tuning (CV on TRAIN ONLY)
    # =========================
    # Adaptive folds (prevents CV errors on small samples)
    cv_folds = min(configured_cv, max(3, len(X_train_f)))
    if cv_folds < 3:
        raise ValueError("Not enough training rows for CV. Increase dataset size or reduce split sizes.")

    search = RandomizedSearchCV(
        estimator=XGBRegressor(
            objective="reg:squarederror",
            random_state=RANDOM_STATE,
            n_jobs=1,
        ),
        param_distributions=param_grid,
        n_iter=n_iter,
        cv=cv_folds,
        scoring="neg_mean_squared_error",
        verbose=1,
        n_jobs=1,
        random_state=RANDOM_STATE,
    )
    search.fit(X_train_f, y_train)

    best_params = dict(search.best_params_)
    print("\nBest Parameters (CV on Train):")
    for k in sorted(best_params.keys()):
        print(f"{k}: {best_params[k]}")

    # =========================
    # Fit tuned model with early stopping on VAL
    # =========================
    tuned_model = XGBRegressor(
        **best_params,
        objective="reg:squarederror",
        random_state=RANDOM_STATE,
        n_jobs=1,
        early_stopping_rounds=25, # Moved here
    )

    tuned_model.fit(
        X_train_f, y_train,
        eval_set=[(X_val_f, y_val)],
        verbose=False,
    )

    print("\nTuned Model Evaluation (XGBRegressor)")
    y_train_pred = tuned_model.predict(X_train_f)
    y_val_pred = tuned_model.predict(X_val_f)
    y_test_pred = tuned_model.predict(X_test_f)

    train_rmse = rmse(y_train, y_train_pred)
    val_rmse = rmse(y_val, y_val_pred)
    test_rmse = rmse(y_test, y_test_pred)
    train_r2 = float(r2_score(np.asarray(y_train), y_train_pred))
    val_r2 = float(r2_score(np.asarray(y_val), y_val_pred))
    test_r2 = float(r2_score(np.asarray(y_test), y_test_pred))

    print_metrics("Train", y_train, y_train_pred)
    print_metrics("Val", y_val, y_val_pred)
    print_metrics("Test", y_test, y_test_pred)
    print("Generalization Gap (RMSE, Train vs Test):", abs(train_rmse - test_rmse))

    tuning_suggestions = build_tuning_suggestions(train_rmse, val_rmse, test_rmse)
    print("\nTuning Suggestions After This Run:")
    for s in tuning_suggestions:
        print(f"- {s}")

    append_run_log(
        RUN_LOG_PATH,
        target_col=target_col,
        feature_count=len(features),
        train_rmse=train_rmse,
        val_rmse=val_rmse,
        test_rmse=test_rmse,
        train_r2=train_r2,
        val_r2=val_r2,
        test_r2=test_r2,
        best_params=best_params,
        suggestions=tuning_suggestions,
    )
    print(f"Saved run log row to: {RUN_LOG_PATH}")

    # =========================
    # Booster gain importance (reliable names)
    # =========================
    booster = tuned_model.get_booster()
    booster.feature_names = features

    booster_scores = booster.get_score(importance_type="gain")
    sorted_scores = dict(sorted(booster_scores.items(), key=lambda kv: kv[1], reverse=True))

    print("\nFeature Importance (Gain)")
    for feat, score in sorted_scores.items():
        print(f"{feat}: {score:.4f}")

    if sorted_scores:
        importance_df = pd.DataFrame(sorted_scores.items(), columns=["Feature", "Gain"]).sort_values("Gain", ascending=False)
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.barh(importance_df["Feature"].head(20)[::-1], importance_df["Gain"].head(20)[::-1])
        ax.set_title("Feature Importance (Tuned Model, Gain)")
        ax.set_xlabel("Total Gain")
        ax.set_ylabel("Feature")
        plt.tight_layout()
        save_or_show(fig, "importance_tuned_gain.png")
    else:
        importance_df = pd.DataFrame(columns=["Feature", "Gain"])

    metrics = {
        "train_rmse": train_rmse,
        "val_rmse": val_rmse,
        "test_rmse": test_rmse,
        "train_r2": train_r2,
        "val_r2": val_r2,
        "test_r2": test_r2,
    }
    save_dashboard_outputs(
        source_df=source_df,
        model_df=df,
        features=features,
        target_col=target_col,
        raw_target_col=raw_target_col,
        tuned_model=tuned_model,
        train_medians=train_medians,
        split_by_index=split_by_index,
        importance_df=importance_df,
        metrics=metrics,
        best_params=best_params,
        suggestions=tuning_suggestions,
    )
    print(f"Saved dashboard predictions to: {PREDICTIONS_CSV_PATH}")
    print(f"Saved ranked predictions to: {RANKED_PREDICTIONS_CSV_PATH}")
    print(f"Saved feature importance to: {FEATURE_IMPORTANCE_CSV_PATH}")
    print(f"Saved model summary to: {SUMMARY_TXT_PATH}")

    # =========================
    # PDP plots (use TRAIN FILLED)
    # =========================
    print("\nStarting PDP generation...")
    print("PDP output directory (absolute):", os.path.abspath(PLOTS_DIR))

    # Make sure directory exists
    os.makedirs(PLOTS_DIR, exist_ok=True)

    # Speed and stability knobs
    GRID_RESOLUTION_1D = 30
    GRID_RESOLUTION_2D = 25

    # 1D PDP for every feature
    for i, feat in enumerate(features, start=1):
        try:
            fig, ax = plt.subplots(figsize=(7, 4))

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                PartialDependenceDisplay.from_estimator(
                    tuned_model,
                    X_train_f,
                    [feat],
                    ax=ax,
                    grid_resolution=GRID_RESOLUTION_1D,
                    percentiles=(0.02, 0.98),
                    kind="average",
                )

            ax.set_title(f"PDP (1D): {feat}")
            plt.tight_layout()

            filename = f"pdp_1d_{safe_name(feat)}.png"
            out_path = os.path.join(PLOTS_DIR, filename)
            fig.savefig(out_path, dpi=150, bbox_inches="tight")
            plt.close(fig)

            print(f"[{i}/{len(features)}] Saved 1D PDP:", out_path)

        except Exception as e:
            # Do not stop the whole run if one feature fails
            print(f"[{i}/{len(features)}] FAILED 1D PDP for {feat}: {repr(e)}")
            try:
                plt.close(fig)
            except Exception:
                pass

    # 2D PDP for key pairs
    for pair in pairs_2d:
        if not isinstance(pair, (list, tuple)) or len(pair) != 2:
            continue
        a, b = pair
        if a not in features or b not in features:
            continue

        try:
            fig, ax = plt.subplots(figsize=(8, 6))

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                PartialDependenceDisplay.from_estimator(
                    tuned_model,
                    X_train_f,
                    [(a, b)],
                    ax=ax,
                    grid_resolution=GRID_RESOLUTION_2D,
                    percentiles=(0.02, 0.98),
                    kind="average",
                )

            ax.set_title(f"PDP (2D): {a} x {b}")
            plt.tight_layout()

            filename = f"pdp_2d_{safe_name(a)}_x_{safe_name(b)}.png"
            out_path = os.path.join(PLOTS_DIR, filename)
            fig.savefig(out_path, dpi=150, bbox_inches="tight")
            plt.close(fig)

            print("Saved 2D PDP:", out_path)

        except Exception as e:
            print(f"FAILED 2D PDP for {a} x {b}: {repr(e)}")
            try:
                plt.close(fig)
            except Exception:
                pass

    print("Finished PDP generation.")


    # =========================
    # Optional: raw scale RMSE (aligned)
    # =========================
    if raw_target_col in df.columns:
        y_test_raw = pd.to_numeric(df.loc[X_test.index, raw_target_col], errors="coerce")
        y_pred_raw = pd.Series(np.sinh(y_test_pred), index=y_test_raw.index)

        mask = y_test_raw.notna() & y_pred_raw.notna()
        if mask.sum() > 0:
            raw_rmse = float(np.sqrt(mean_squared_error(y_test_raw[mask], y_pred_raw[mask])))
            print("\nApprox RMSE on raw returns_2025 scale (inverse sinh):", raw_rmse)
        else:
            print("\nApprox RMSE on raw returns_2025 scale: cannot compute (no aligned non-missing values).")


if __name__ == "__main__":
    main()
