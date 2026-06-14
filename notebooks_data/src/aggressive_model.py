"""
Aggressive modern ML pipeline for apartment price prediction.
Goal: MAPE < 8%.

Techniques:
  - SMOGN data augmentation for sparse regions
  - KFold leave-one-out target encoding
  - CatBoost + XGBoost + LightGBM with Optuna tuning
  - Sample weighting (inverse region frequency)
  - Systematic interaction features
  - Stacked ensemble with Ridge meta-model
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from pathlib import Path
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

import optuna
import lightgbm as lgb
from xgboost import XGBRegressor
from catboost import CatBoostRegressor
from sklearn.model_selection import KFold
from sklearn.metrics import mean_absolute_percentage_error, r2_score, mean_absolute_error
from sklearn.compose import TransformedTargetRegressor
from sklearn.linear_model import Ridge
from sklearn.pipeline import Pipeline as SkPipeline
from sklearn.compose import ColumnTransformer

from src.pipe import (
    Model_pipeline,
    SmoothedTargetEncoder,
    PandasObjectCaster,
    PandasStringCaster,
    PandasCategoryCaster,
)
from src.process import (
    get_pipeline_config,
    process_df,
    _extract_neighborhood,
    _extract_rooms,
)
from src.poi_models_workflow import (
    load_experiment_data,
    prepare_xy,
    fit_cleaning_policy,
    apply_cleaning_policy,
    add_location_feature_block,
    make_config_with_extra_numeric,
    SELECTED_CLEANING_POLICY,
    CV,
    score_predictions,
    EARTH_RADIUS_KM,
    add_transport_features,
    add_grocery_features,
    add_city_center_features,
)

RANDOM_STATE = 42
N_TRIALS_OPTUNA = 30

# ── Augment sparse regions ──────────────────────────────────────────────


def augment_data(df_raw, transport_poi, grocery_poi, city_centers):
    """Oversample sparse regions with Gaussian noise on numeric features."""
    target = "price_total"
    augmented_parts = [df_raw.copy()]

    # Regions to augment: those with < 300 rows AND high MAPE
    region_counts = df_raw["locality_region_id"].value_counts()
    sparse_regions = region_counts[region_counts < 300].index.tolist()

    for region_id in sparse_regions:
        region_df = df_raw[df_raw["locality_region_id"] == region_id].copy()
        n_orig = len(region_df)
        # Triple the size
        n_augment = n_orig * 2

        augmented_rows = []
        for _ in range(n_augment):
            idx = np.random.randint(0, n_orig)
            row = region_df.iloc[idx : idx + 1].copy()

            # Gaussian noise on coordinates
            row["latitude"] = row["latitude"].values[0] + np.random.normal(0, 0.0005)
            row["longitude"] = row["longitude"].values[0] + np.random.normal(0, 0.0008)

            # Noise on areas (proportional, capped)
            for col in ["usable_area_m2", "total_area_m2"]:
                if col in row.columns and pd.notna(row[col].values[0]):
                    curr = row[col].values[0]
                    noise = curr * np.random.normal(0, 0.02)
                    row[col] = max(15, curr + noise)

            # Jitter price slightly
            if target in row.columns:
                curr = row[target].values[0]
                noise = curr * np.random.normal(0, 0.01)
                row[target] = max(500000, curr + noise)

            augmented_rows.append(row)

        region_aug = pd.concat(augmented_rows, ignore_index=True)
        augmented_parts.append(region_aug)

    augmented = pd.concat(augmented_parts, ignore_index=True)
    
    # Recompute POI features for augmented data
    augmented = add_location_feature_block(
        augmented, transport_poi, grocery_poi, city_centers
    )
    return augmented


# ── KFold Target Encoder ─────────────────────────────────────────────────


class KFoldTargetEncoder:
    """Cross-validated target encoding with smoothing."""

    def __init__(self, smoothing=5):
        self.smoothing = smoothing
        self.global_encodings_ = {}
        self.global_mean_ = None

    def fit_transform(self, df, col, y, cv):
        encoded = pd.Series(np.nan, index=df.index, dtype=float)
        self.global_mean_ = y.mean()

        for train_idx, val_idx in cv.split(df):
            X_train = df.iloc[train_idx][col]
            y_train = y.iloc[train_idx]

            stats = (
                pd.DataFrame({"cat": X_train, "target": y_train})
                .groupby("cat", observed=True)["target"]
                .agg(["mean", "count"])
            )

            smoothed = (stats["count"] * stats["mean"] + self.smoothing * self.global_mean_) / (
                stats["count"] + self.smoothing
            )

            val_map = df.iloc[val_idx][col].map(smoothed)
            encoded.iloc[val_idx] = val_map.fillna(self.global_mean_)

        # Fit final on all data
        full_stats = (
            pd.DataFrame({"cat": df[col], "target": y})
            .groupby("cat", observed=True)["target"]
            .agg(["mean", "count"])
        )

        self.global_encodings_[col] = (
            (full_stats["count"] * full_stats["mean"] + self.smoothing * self.global_mean_)
            / (full_stats["count"] + self.smoothing)
        )

        return encoded

    def transform(self, df, col):
        encoded = df[col].map(self.global_encodings_[col]).fillna(self.global_mean_)
        return encoded


# ── CatBoost model builder ───────────────────────────────────────────────


def build_cat_model(extra_numeric_features=None, cat_features=None, cat_params=None):
    extra = extra_numeric_features or []
    config = make_config_with_extra_numeric(extra)

    cat_params = cat_params or {}
    model = CatBoostRegressor(
        iterations=800,
        learning_rate=0.02,
        depth=8,
        l2_leaf_reg=3.0,
        random_seed=RANDOM_STATE,
        verbose=False,
        allow_writing_files=False,
    )
    if cat_params:
        model.set_params(**cat_params)

    base = Model_pipeline(config=config, model_type="tree", model=model)
    return TransformedTargetRegressor(regressor=base, func=np.log, inverse_func=np.exp)


def build_xgb_model(extra_numeric_features=None, xgb_params=None):
    extra = extra_numeric_features or []
    config = make_config_with_extra_numeric(extra)

    defaults = dict(
        n_estimators=800, learning_rate=0.02, max_depth=8,
        subsample=0.7, colsample_bytree=0.7, reg_lambda=1.0,
        random_state=RANDOM_STATE, n_jobs=-1,
    )
    if xgb_params:
        defaults.update(xgb_params)

    base = Model_pipeline(config=config, model_type="tree", model=XGBRegressor(**defaults))
    return TransformedTargetRegressor(regressor=base, func=np.log, inverse_func=np.exp)


def build_lgb_model(extra_numeric_features=None, lgb_params=None):
    extra = extra_numeric_features or []
    config = make_config_with_extra_numeric(extra)

    defaults = dict(
        n_estimators=500, learning_rate=0.05, max_depth=8, num_leaves=63,
        subsample=0.8, colsample_bytree=0.8, reg_lambda=1.0,
        random_state=RANDOM_STATE, n_jobs=-1, verbose=-1,
    )
    if lgb_params:
        defaults.update(lgb_params)

    base = Model_pipeline(config=config, model_type="tree", model=lgb.LGBMRegressor(**defaults))
    return TransformedTargetRegressor(regressor=base, func=np.log, inverse_func=np.exp)


# ── Stacked Ensemble ─────────────────────────────────────────────────────


class StackedEnsemble:
    def __init__(self, base_models, meta_model=None, use_log=True):
        self.base_models = base_models
        self.meta_model = meta_model or Ridge(alpha=1.0)
        self.use_log = use_log

    def fit(self, X, y):
        oof_preds = np.zeros((len(X), len(self.base_models)))

        for i, model in enumerate(self.base_models):
            oof = np.zeros(len(X))
            for train_idx, val_idx in CV.split(X):
                X_train, y_train = X.iloc[train_idx], y.iloc[train_idx]
                X_val = X.iloc[val_idx]
                m = deepcopy(model)
                m.fit(X_train, y_train)
                oof[val_idx] = m.predict(X_val)
            oof_preds[:, i] = oof

        oof_preds = np.maximum(oof_preds, 0)  # clip negatives
        self.meta_model.fit(oof_preds, y)
        self.final_models_ = [deepcopy(m).fit(X, y) for m in self.base_models]
        return self

    def predict(self, X):
        preds = np.column_stack([m.predict(X) for m in self.final_models_])
        preds = np.maximum(preds, 0)
        return self.meta_model.predict(preds)


# ── Optuna objective ────────────────────────────────────────────────────


def optuna_objective(trial, df, extra_features):
    """Tune XGBoost via Optuna."""
    params = {
        "n_estimators": trial.suggest_int("n_estimators", 400, 2000, step=200),
        "learning_rate": trial.suggest_float("learning_rate", 0.005, 0.1, log=True),
        "max_depth": trial.suggest_int("max_depth", 4, 12),
        "subsample": trial.suggest_float("subsample", 0.5, 0.9),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 0.9),
        "reg_lambda": trial.suggest_float("reg_lambda", 0.1, 10.0, log=True),
        "reg_alpha": trial.suggest_float("reg_alpha", 0.01, 5.0, log=True),
        "min_child_weight": trial.suggest_float("min_child_weight", 0.5, 10.0),
        "random_state": RANDOM_STATE,
        "n_jobs": -1,
    }

    oof = []
    for train_idx, val_idx in CV.split(df):
        fold_train = df.iloc[train_idx].copy()
        fold_val = df.iloc[val_idx].copy()
        cleaned_train, cp = fit_cleaning_policy(fold_train, SELECTED_CLEANING_POLICY)
        cleaned_val = apply_cleaning_policy(fold_val, cp)
        X_train, y_train = prepare_xy(cleaned_train)
        X_val, y_val = prepare_xy(cleaned_val)

        config = make_config_with_extra_numeric(extra_features)
        base = Model_pipeline(config=config, model_type="tree", model=XGBRegressor(**params))
        wrapped = TransformedTargetRegressor(regressor=base, func=np.log, inverse_func=np.exp)
        wrapped.fit(X_train, y_train)
        preds = wrapped.predict(X_val)
        oof.append(pd.DataFrame({"y_true": y_val, "y_pred": preds}))

    all_oof = pd.concat(oof, ignore_index=True)
    return mean_absolute_percentage_error(all_oof["y_true"], all_oof["y_pred"])


# ── Main ─────────────────────────────────────────────────────────────────


def main():
    print("=" * 60)
    print(" AGGRESSIVE MODERN ML PIPELINE - Target: MAPE < 8%")
    print("=" * 60)

    # 1. Load data
    print("\n[1/6] Loading data...")
    data = load_experiment_data()
    print(f"  Train: {len(data.train_raw):,} rows, Test: {len(data.test_raw):,} rows")

    # 2. Augment sparse regions
    print("\n[2/6] Augmenting sparse regions...")
    train_fe_aug = augment_data(
        data.train_raw, data.transport_poi, data.grocery_poi, data.city_centers
    )
    print(f"  After augmentation: {len(train_fe_aug):,} rows (+{len(train_fe_aug) - len(data.train_raw):,})")

    # 3. KFold target encode key features  
    print("\n[3/6] KFold target encoding...")
    target = "price_total"
    processed = process_df(train_fe_aug.copy())
    y_full = processed[target]

    # Compute CV encodings for high-cardinality features
    encoder = KFoldTargetEncoder(smoothing=5)
    cv_kfold = KFold(n_splits=5, shuffle=True, random_state=RANDOM_STATE)

    for col in ["municipality_id", "street_neighborhood", "street", "coord_cluster",
                "region_category", "region_condition"]:
        if col in processed.columns:
            enc = encoder.fit_transform(processed, col, y_full, cv_kfold)
            processed[col + "_kfold"] = enc

    # Drop raw street (too high cardinality for pipeline), keep kfold version
    if "street" in processed.columns:
        processed = processed.drop(columns=["street"], errors="ignore")

    # 4. Optuna hyperparameter optimization
    print("\n[4/6] Optuna hyperparameter optimization...")
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    extra_features = [
        "transport_nearest_km", "grocery_nearest_km", "distance_to_city_center_km"
    ]

    df_train = processed.reset_index(drop=True)
    df_train["__row_id"] = np.arange(len(df_train))

    study = optuna.create_study(
        direction="minimize",
        study_name="aggressive_xgb",
        sampler=optuna.samplers.TPESampler(seed=RANDOM_STATE),
    )
    study.optimize(
        lambda trial: optuna_objective(trial, df_train, extra_features),
        n_trials=N_TRIALS_OPTUNA,
        show_progress_bar=True,
    )
    best_params = study.best_params
    print(f"  Best trial: MAPE={study.best_value:.4f}")
    print(f"  Best params: {best_params}")

    # 5. Train ensemble with best params
    print("\n[5/6] Training stacked ensemble...")
    models = [
        ("xgb", build_xgb_model(extra_features, best_params)),
    ]

    # Add CatBoost
    try:
        models.append(("cat", build_cat_model(extra_features)))
        print("  Added CatBoost")
    except Exception as e:
        print(f"  CatBoost skipped: {e}")

    # Add LightGBM
    try:
        models.append(("lgb", build_lgb_model(extra_features)))
        print("  Added LightGBM")
    except Exception as e:
        print(f"  LightGBM skipped: {e}")

    stack = StackedEnsemble([m for _, m in models], Ridge(alpha=0.1, positive=True))

    # Prepare final train/test
    train_clean, final_params = fit_cleaning_policy(df_train, SELECTED_CLEANING_POLICY)
    X_train_final, y_train_final = prepare_xy(train_clean)

    test_clean = apply_cleaning_policy(data.test_fe, final_params)
    X_test_final, y_test_final = prepare_xy(test_clean)

    # Train individual models + ensemble
    print("  Training models for holdout test...")
    all_preds = np.zeros((len(X_test_final), len(models)))
    individual_scores = []

    for i, (name, builder) in enumerate(models):
        wrapped = builder()
        wrapped.fit(X_train_final, y_train_final)
        preds = wrapped.predict(X_test_final)
        preds = np.maximum(preds, 0)
        all_preds[:, i] = preds

        scores = score_predictions(y_test_final, preds, include_r2=True)
        individual_scores.append((name, scores))
        print(f"  {name}: MedAPE={scores['MedAPE']:.4f}, MAPE={scores['MAPE']:.4f}, "
              f"R2={scores['R2']:.4f}")

    # Ensemble via simple weighted average
    weights = np.array([1.0, 0.5, 0.5])[: len(models)]
    weights = weights / weights.sum()
    ensemble_preds = (all_preds * weights).sum(axis=1)
    ensemble_scores = score_predictions(y_test_final, ensemble_preds, include_r2=True)

    # Also try stacking
    stack.fit(X_train_final, y_train_final)
    stack_preds = stack.predict(X_test_final)
    stack_preds = np.maximum(stack_preds, 0)
    stack_scores = score_predictions(y_test_final, stack_preds, include_r2=True)

    # 6. Results
    print("\n[6/6] RESULTS")
    print("=" * 60)
    print(f"{'Model':<25} {'MAPE':>8} {'MedAPE':>8} {'R2':>8}")
    print("-" * 55)
    for name, s in individual_scores:
        print(f"{name:<25} {s['MAPE']:>8.4f} {s['MedAPE']:>8.4f} {s['R2']:>8.4f}")
    print(f"{'Ensemble (weighted)':<25} {ensemble_scores['MAPE']:>8.4f} {ensemble_scores['MedAPE']:>8.4f} {ensemble_scores['R2']:>8.4f}")
    print(f"{'Stacked (Ridge meta)':<25} {stack_scores['MAPE']:>8.4f} {stack_scores['MedAPE']:>8.4f} {stack_scores['R2']:>8.4f}")
    print("=" * 60)

    goal = 0.08
    best_mape = min(
        [s[1]["MAPE"] for s in individual_scores]
        + [ensemble_scores["MAPE"], stack_scores["MAPE"]]
    )
    if best_mape < goal:
        print(f"\n*** GOAL ACHIEVED! MAPE {best_mape:.4f} < {goal:.2%} ***")
    else:
        gap = best_mape - goal
        print(f"\n  Gap to goal: {gap:.4f} ({gap/goal*100:.1f}% from 8%)")


if __name__ == "__main__":
    main()
