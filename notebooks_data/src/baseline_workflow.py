from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, RegressorMixin, clone
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.model_selection import KFold
from sklearn.tree import DecisionTreeRegressor
from torch import alpha_dropout
from xgboost import XGBRegressor

from src.helper import (
    apply_cleaning_policy,
    fit_cleaning_policy,
    out_of_fold_scores,
    prepare_xy,
    score_predictions,
    snapshot_info,
)
from src.pipe import Model_pipeline
from src.process import get_pipeline_config


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"

TRAIN_PATH = DATA_DIR / "apartments_raw_data.csv"
TEST_PATH = DATA_DIR / "apartments_raw_data_test.csv"

DEFAULT_TARGET = "price_total"
TEXT_COLUMNS = ["title", "description", "meta_description"]
DATASET_VERSION = "postgresql_split_snapshot_from_eda"
DATA_SCOPE = "Czech Republic"
FEATURE_SET = "structured_baseline_no_text"

CV = KFold(n_splits=5, shuffle=True, random_state=42)
CLEANING_POLICIES = [
    "invalid_only",
    "light_trim",
    "strong_ppm_filter_train_only",
]

MODEL_DESCRIPTIONS = {
    "Global median": "Predict the same global median price for every listing.",
    "Linear regression on log-log scale": (
        "Linear model on log(price) with the current structured preprocessing pipeline."
    ),
    "Ridge regression on log-log scale": (
        "L2-regularized linear model on log(price) with the current structured preprocessing pipeline."
    ),
    "Decision tree baseline": "Single-tree baseline on log(price) with structured features.",
    "XGBoost baseline": "Tree-based baseline on the current structured preprocessing pipeline.",
}


@dataclass
class LockedEvaluation:
    model: object
    cleaned_test_df: pd.DataFrame
    y_test: pd.Series
    predictions: np.ndarray
    metrics: dict


@dataclass
class RegionalLockedEvaluation:
    global_model: object
    region_models: dict
    cleaned_test_df: pd.DataFrame
    y_test: pd.Series
    predictions: np.ndarray
    metrics: dict
    routing_summary: pd.DataFrame


def _native_categorical_fallback_mask(
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    categorical_cols: list[str],
) -> pd.Series:
    fallback_mask = pd.Series(False, index=X_test.index)

    for col in categorical_cols:
        if col not in X_train.columns or col not in X_test.columns:
            continue

        seen_values = pd.Index(X_train[col].dropna().unique())
        if seen_values.empty:
            fallback_mask |= X_test[col].notna()
            continue

        fallback_mask |= X_test[col].notna() & ~X_test[col].isin(seen_values)

    return fallback_mask


def _align_native_categorical_columns(
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    categorical_cols: list[str],
) -> pd.DataFrame:
    X_aligned = X_test.copy()

    for col in categorical_cols:
        if col not in X_train.columns or col not in X_aligned.columns:
            continue

        train_categories = pd.Index(X_train[col].dropna().unique()).tolist()
        X_aligned[col] = pd.Categorical(X_aligned[col], categories=train_categories)

    return X_aligned


class GlobalMedianRegressor(BaseEstimator, RegressorMixin):
    def fit(self, X, y):
        del X
        self.global_median_ = float(np.median(np.asarray(y)))
        return self

    def predict(self, X):
        return np.full(len(X), self.global_median_, dtype=float)


class SmearingLogTargetRegressor(BaseEstimator, RegressorMixin):
    def __init__(self, regressor):
        self.regressor = regressor

    def fit(self, X, y):
        y = np.asarray(y, dtype=float)
        if np.any(y <= 0):
            raise ValueError("SmearingLogTargetRegressor requires strictly positive targets.")

        self.regressor_ = clone(self.regressor)
        y_log = np.log(y)
        self.regressor_.fit(X, y_log)

        train_log_predictions = self.regressor_.predict(X)
        residuals = y_log - train_log_predictions
        self.smearing_factor_ = float(np.mean(np.exp(residuals)))
        self.log_residual_variance_ = float(np.var(residuals, ddof=1))
        return self

    def predict(self, X):
        log_predictions = self.regressor_.predict(X)
        return np.exp(log_predictions) * self.smearing_factor_


def build_log_target_model(model, model_type: str):
    config = get_pipeline_config().copy()
    base = Model_pipeline(config=config, model_type=model_type, model=model)
    return SmearingLogTargetRegressor(regressor=base)



def build_linear_model():
    return build_log_target_model(LinearRegression(), model_type="linear")


def build_ridge_model():
    return build_log_target_model(Ridge(alpha=1.0), model_type="linear")


def build_decision_tree_model():
    return build_log_target_model(
        DecisionTreeRegressor(max_depth=12, min_samples_leaf=5, random_state=42),
        model_type="tree",
    )


def build_xgb_model():
    return build_log_target_model(
        model=XGBRegressor(
            n_estimators=300,
            learning_rate=0.05,
            max_depth=6,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_lambda=1.0,
            random_state=42,
            enable_categorical=True,
            n_jobs=-1,
        ),
        model_type="tree_modern",
    )


MODEL_BUILDERS = {
    "Global median": GlobalMedianRegressor,
    "Linear regression on log-log scale": build_linear_model,
    "Ridge regression on log-log scale": build_ridge_model,
    "Decision tree baseline": build_decision_tree_model,
    "XGBoost baseline": build_xgb_model,
}


def build_dataset_card(
    train_path: Path = TRAIN_PATH,
    test_path: Path = TEST_PATH,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            snapshot_info(train_path, "train", "v_data_train"),
            snapshot_info(test_path, "test", "v_data_test"),
        ]
    )


def build_model_catalog() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"model": model_name, "description": MODEL_DESCRIPTIONS[model_name]}
            for model_name in MODEL_BUILDERS
        ]
    )


def select_cleaning_policy(
    train_raw: pd.DataFrame,
    target: str = DEFAULT_TARGET,
    cv=CV,
):
    rows = []
    for policy_name in CLEANING_POLICIES:
        metrics, _ = out_of_fold_scores(
            train_raw,
            build_xgb_model,
            policy_name,
            cv,
            target=target,
        )
        rows.append(
            {
                "cleaning_policy": policy_name,
                "eligible_for_final_lock": policy_name != "strong_ppm_filter_train_only",
                **metrics,
            }
        )

    policy_results = pd.DataFrame(rows).sort_values("MedAPE").reset_index(drop=True)
    selected_policy = (
        policy_results.loc[policy_results["eligible_for_final_lock"]]
        .sort_values("MedAPE")
        .iloc[0]["cleaning_policy"]
    )
    return policy_results, selected_policy


def run_model_cv_comparison(
    train_raw: pd.DataFrame,
    cleaning_policy: str,
    target: str = DEFAULT_TARGET,
    cv=CV,
):
    rows = []
    for model_name, model_builder in MODEL_BUILDERS.items():
        metrics, _ = out_of_fold_scores(
            train_raw,
            model_builder,
            cleaning_policy,
            cv,
            target=target,
        )
        rows.append(
            {
                "dataset_version": DATASET_VERSION,
                "scope": DATA_SCOPE,
                "cleaning_policy": cleaning_policy,
                "feature_set": FEATURE_SET,
                "model": model_name,
                **metrics,
            }
        )

    cv_results = pd.DataFrame(rows).sort_values("MedAPE").reset_index(drop=True)
    locked_model_name = cv_results.iloc[0]["model"]
    locked_builder = MODEL_BUILDERS[locked_model_name]
    return cv_results, locked_model_name, locked_builder


def fit_model_on_test(
    train_raw: pd.DataFrame,
    test_raw: pd.DataFrame,
    model_builder,
    cleaning_policy: str,
    target: str = DEFAULT_TARGET,
):
    cleaned_train, cleaning_params = fit_cleaning_policy(
        train_raw,
        cleaning_policy,
        target=target,
    )
    cleaned_test = apply_cleaning_policy(test_raw, cleaning_params)

    X_train, y_train = prepare_xy(cleaned_train, target=target)
    X_test, y_test = prepare_xy(cleaned_test, target=target)

    model = model_builder()
    model.fit(X_train, y_train)
    predictions = model.predict(X_test)
    metrics = score_predictions(y_test, predictions)
    return LockedEvaluation(
        model=model,
        cleaned_test_df=cleaned_test,
        y_test=y_test,
        predictions=predictions,
        metrics=metrics,
    )


def fit_region_specialized_model_on_test(
    train_raw: pd.DataFrame,
    test_raw: pd.DataFrame,
    model_builder,
    cleaning_policy: str,
    region_col: str = "locality_region_id",
    min_region_train_size: int = 200,
    native_categorical_cols: list[str] | None = None,
    target: str = DEFAULT_TARGET,
):
    cleaned_train, cleaning_params = fit_cleaning_policy(
        train_raw,
        cleaning_policy,
        target=target,
    )
    cleaned_test = apply_cleaning_policy(test_raw, cleaning_params)

    X_train, y_train = prepare_xy(cleaned_train, target=target)
    X_test, y_test = prepare_xy(cleaned_test, target=target)

    global_model = model_builder()
    global_model.fit(X_train, y_train)
    predictions = pd.Series(
        global_model.predict(X_test),
        index=cleaned_test.index,
        dtype=float,
    )

    trained_region_models = {}
    trained_region_keys = set()
    native_categorical_cols = native_categorical_cols or get_pipeline_config().get(
        "cat_features",
        [],
    )
    train_region_counts = cleaned_train[region_col].value_counts(dropna=False)
    test_region_counts = cleaned_test[region_col].value_counts(dropna=False)
    routing_counts = {}

    for region_value, train_rows in train_region_counts.items():
        if pd.isna(region_value) or train_rows < min_region_train_size:
            continue

        test_mask = cleaned_test[region_col] == region_value
        if not test_mask.any():
            continue

        train_mask = cleaned_train[region_col] == region_value
        region_train_df = cleaned_train.loc[train_mask].copy()
        region_test_df = cleaned_test.loc[test_mask].copy()

        X_region_train, y_region_train = prepare_xy(region_train_df, target=target)
        X_region_test, _ = prepare_xy(region_test_df, target=target)
        fallback_mask = _native_categorical_fallback_mask(
            X_region_train,
            X_region_test,
            native_categorical_cols,
        )
        X_region_test_safe = X_region_test.loc[~fallback_mask].copy()

        routing_counts[region_value] = {
            "test_rows_region_model": int((~fallback_mask).sum()),
            "test_rows_global_fallback": int(fallback_mask.sum()),
        }
        if X_region_test_safe.empty:
            continue

        X_region_test_safe = _align_native_categorical_columns(
            X_region_train,
            X_region_test_safe,
            native_categorical_cols,
        )

        region_model = model_builder()
        region_model.fit(X_region_train, y_region_train)
        predictions.loc[X_region_test_safe.index] = region_model.predict(X_region_test_safe)
        trained_region_models[region_value] = region_model
        trained_region_keys.add(region_value)

    routing_rows = []
    for region_value, test_rows in test_region_counts.items():
        train_rows = int(train_region_counts.get(region_value, 0))
        region_routing = routing_counts.get(
            region_value,
            {
                "test_rows_region_model": 0,
                "test_rows_global_fallback": int(test_rows),
            },
        )
        uses_region_model = region_routing["test_rows_region_model"] > 0
        fallback_rows = region_routing["test_rows_global_fallback"]

        if train_rows < min_region_train_size:
            prediction_source = "global_fallback_insufficient_train_rows"
        elif uses_region_model and fallback_rows:
            prediction_source = "mixed_region_model_and_global_fallback"
        elif uses_region_model:
            prediction_source = "region_model"
        else:
            prediction_source = "global_fallback_unseen_native_categories"

        routing_rows.append(
            {
                "region": region_value,
                "train_rows": train_rows,
                "test_rows": int(test_rows),
                "uses_region_model": uses_region_model,
                "test_rows_region_model": region_routing["test_rows_region_model"],
                "test_rows_global_fallback": fallback_rows,
                "prediction_source": prediction_source,
                "train_rows_required": min_region_train_size,
            }
        )

    routing_summary = pd.DataFrame(routing_rows).sort_values(
        ["uses_region_model", "train_rows", "test_rows", "region"],
        ascending=[False, False, False, True],
    ).reset_index(drop=True)

    metrics = score_predictions(y_test, predictions.to_numpy())
    return RegionalLockedEvaluation(
        global_model=global_model,
        region_models=trained_region_models,
        cleaned_test_df=cleaned_test,
        y_test=y_test,
        predictions=predictions.to_numpy(),
        metrics=metrics,
        routing_summary=routing_summary,
    )


def run_model_test_comparison(
    train_raw: pd.DataFrame,
    test_raw: pd.DataFrame,
    cleaning_policy: str,
    locked_model_name: str | None = None,
    target: str = DEFAULT_TARGET,
):
    rows = []
    for model_name, model_builder in MODEL_BUILDERS.items():
        evaluation = fit_model_on_test(
            train_raw=train_raw,
            test_raw=test_raw,
            model_builder=model_builder,
            cleaning_policy=cleaning_policy,
            target=target,
        )
        rows.append(
            {
                "dataset_version": DATASET_VERSION,
                "scope": DATA_SCOPE,
                "cleaning_policy": cleaning_policy,
                "feature_set": FEATURE_SET,
                "model": model_name,
                "is_locked_model": model_name == locked_model_name,
                "n_scored": len(evaluation.y_test),
                **evaluation.metrics,
            }
        )

    return pd.DataFrame(rows).sort_values("MedAPE").reset_index(drop=True)


def build_final_test_table(
    model_name: str,
    cleaning_policy: str,
    evaluation: LockedEvaluation,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "model": model_name,
                "cleaning_policy": cleaning_policy,
                "n_scored": len(evaluation.y_test),
                **evaluation.metrics,
            }
        ]
    )


def subgroup_summary(
    df: pd.DataFrame,
    group_col: str,
    min_group_size: int = 0,
) -> pd.DataFrame:
    rows = []
    for key, group in df.groupby(group_col, observed=True):
        if len(group) < min_group_size:
            continue
        rows.append(
            {
                "segment": group_col,
                "group": key,
                "n": len(group),
                **score_predictions(group["actual_price"], group["prediction"]),
            }
        )

    return pd.DataFrame(rows).sort_values("MedAPE").reset_index(drop=True)


def _build_price_bands(actual_price: pd.Series) -> pd.Series:
    labels = ["Q1", "Q2", "Q3", "Q4"]
    try:
        return pd.qcut(actual_price, q=4, labels=labels)
    except ValueError:
        bands = pd.qcut(actual_price, q=4, duplicates="drop")
        if not hasattr(bands.dtype, "categories"):
            return bands.astype(str)
        n_categories = len(bands.dtype.categories)
        renamed = labels[:n_categories]
        return bands.cat.rename_categories(renamed)


def build_diagnostics(
    cleaned_test_df: pd.DataFrame,
    y_test: pd.Series,
    predictions: np.ndarray,
):
    evaluation_df = cleaned_test_df.copy()
    evaluation_df["actual_price"] = np.asarray(y_test)
    evaluation_df["prediction"] = predictions
    evaluation_df["signed_error"] = evaluation_df["prediction"] - evaluation_df["actual_price"]
    evaluation_df["abs_error"] = evaluation_df["signed_error"].abs()
    evaluation_df["ape"] = evaluation_df["abs_error"] / evaluation_df["actual_price"]

    if "usable_area_m2" in evaluation_df.columns:
        area = evaluation_df["usable_area_m2"].replace(0, np.nan)
        evaluation_df["actual_price_per_m2"] = evaluation_df["actual_price"] / area
        evaluation_df["predicted_price_per_m2"] = evaluation_df["prediction"] / area

    evaluation_df["error_direction"] = np.where(
        evaluation_df["signed_error"] >= 0,
        "overprediction",
        "underprediction",
    )
    evaluation_df["price_band"] = _build_price_bands(evaluation_df["actual_price"])

    region_summary = subgroup_summary(evaluation_df, "locality_region_id")
    category_summary = subgroup_summary(evaluation_df, "category_sub")
    price_band_summary = subgroup_summary(evaluation_df, "price_band")

    preferred_columns = [
        "web_link",
        "district_id",
        "locality_region_id",
        "category_sub",
        "usable_area_m2",
        "total_area_m2",
        "floor_number",
        "total_floors",
        "ownership_type",
        "construction_type",
        "building_condition",
        "energy_class",
        "is_furnished",
        "location_type",
        "has_elevator",
        "has_garage",
        "has_cellar",
        "has_loggia",
        "actual_price",
        "prediction",
        "signed_error",
        "abs_error",
        "ape",
        "actual_price_per_m2",
        "predicted_price_per_m2",
        "error_direction",
    ]
    available_columns = [col for col in preferred_columns if col in evaluation_df.columns]
    worst_predictions = evaluation_df.loc[:, available_columns].sort_values(
        ["ape", "abs_error"],
        ascending=[False, False],
    )

    return (
        evaluation_df,
        region_summary,
        category_summary,
        price_band_summary,
        worst_predictions,
    )


def save_output_tables(
    dataset_card: pd.DataFrame,
    policy_results: pd.DataFrame,
    cv_results: pd.DataFrame,
    final_test_table: pd.DataFrame,
    region_summary: pd.DataFrame,
    category_summary: pd.DataFrame,
    price_band_summary: pd.DataFrame,
    worst_predictions: pd.DataFrame,
    test_results: pd.DataFrame | None = None,
    output_dir: Path = Path("artifacts/baseline"),
):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    outputs = {
        "dataset_card.csv": dataset_card,
        "cleaning_policy_cv_results.csv": policy_results,
        "baseline_model_cv_results.csv": cv_results,
        "final_test_metrics.csv": final_test_table,
        "test_region_summary.csv": region_summary,
        "test_category_summary.csv": category_summary,
        "test_price_band_summary.csv": price_band_summary,
        "test_worst_predictions.csv": worst_predictions,
    }
    if test_results is not None:
        outputs["baseline_model_test_results.csv"] = test_results

    for filename, frame in outputs.items():
        frame.to_csv(output_dir / filename, index=False)

    return outputs
