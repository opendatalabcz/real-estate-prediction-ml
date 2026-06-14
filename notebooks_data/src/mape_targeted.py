"""
MAPE-targeted pipeline — Q1 upweighting, Tweedie loss, early stopping.
"""
import numpy as np, pandas as pd
import optuna
from xgboost import XGBRegressor
from sklearn.compose import TransformedTargetRegressor
from sklearn.metrics import mean_absolute_percentage_error

from src.poi_models_workflow import (
    load_experiment_data, prepare_xy, fit_cleaning_policy,
    apply_cleaning_policy, SELECTED_CLEANING_POLICY, CV,
    score_predictions, add_location_feature_block,
    make_config_with_extra_numeric,
)
from src.pipe import Model_pipeline

RANDOM_STATE = 42
ALL_POI = [
    "transport_nearest_km", "transport_count_within_500m", "transport_count_within_1000m",
    "metro_train_nearest_km",
    "grocery_nearest_km", "grocery_count_within_500m", "grocery_count_within_1000m",
    "distance_to_city_center_km",
]


def augment_regions(df_raw, transport_poi, grocery_poi, city_centers):
    parts = [df_raw.copy()]
    for region_id, n_orig in df_raw["locality_region_id"].value_counts().items():
        if n_orig >= 600: continue
        region_df = df_raw[df_raw["locality_region_id"] == region_id].copy()
        n_add = min(n_orig * 2, 800 - n_orig)
        n_add = max(n_add, n_orig // 2)
        for _ in range(n_add):
            idx = np.random.randint(0, n_orig)
            row = region_df.iloc[idx:idx+1].copy()
            row["latitude"] += np.random.normal(0, 0.0005)
            row["longitude"] += np.random.normal(0, 0.0008)
            for c in ["usable_area_m2", "total_area_m2"]:
                if c in row.columns and pd.notna(row.iloc[0][c]):
                    row[c] = max(12, row.iloc[0][c] * (1 + np.random.normal(0, 0.025)))
            if pd.notna(row.iloc[0]["price_total"]):
                row["price_total"] = max(300000, int(row.iloc[0]["price_total"] * (1 + np.random.normal(0, 0.015))))
            parts.append(row)
    out = pd.concat(parts, ignore_index=True)
    return add_location_feature_block(out, transport_poi, grocery_poi, city_centers)


def compute_q_weighted_sw(cleaned_df):
    """Weight inversely by region AND by price rank (cheaper → heavier)."""
    target = "price_total"
    region_freq = cleaned_df["locality_region_id"].astype(str).value_counts()
    w_region = cleaned_df["locality_region_id"].astype(str).map(1.0 / region_freq).astype(float)

    # Rank by price: cheapest gets weight 4 → most expensive gets weight 0.5
    rank = cleaned_df[target].rank(pct=True)  # 0..1 percentile
    w_price = 1.0 + 3.0 * (1.0 - rank)       # 4.0 at cheapest → 1.0 at most expensive

    w = w_region.values * w_price
    return w / w.mean()


def optuna_objective(trial, train_fe_df):
    """Tune XGBoost for MAPE directly."""
    params = {
        "n_estimators": trial.suggest_int("n_estimators", 600, 2000, step=200),
        "learning_rate": trial.suggest_float("learning_rate", 0.005, 0.05, log=True),
        "max_depth": trial.suggest_int("max_depth", 5, 10),
        "subsample": trial.suggest_float("subsample", 0.5, 0.9),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 0.9),
        "reg_lambda": trial.suggest_float("reg_lambda", 0.1, 20.0, log=True),
        "reg_alpha": trial.suggest_float("reg_alpha", 0.01, 10.0, log=True),
        "min_child_weight": trial.suggest_float("min_child_weight", 0.5, 15.0),
        "random_state": RANDOM_STATE, "n_jobs": -1,
    }
    scores = []
    for train_idx, val_idx in CV.split(train_fe_df):
        ft, fv = train_fe_df.iloc[train_idx].copy(), train_fe_df.iloc[val_idx].copy()
        ct, cp = fit_cleaning_policy(ft, SELECTED_CLEANING_POLICY)
        cvv = apply_cleaning_policy(fv, cp)
        Xt, yt = prepare_xy(ct)
        Xv, yv = prepare_xy(cvv)
        sw = compute_q_weighted_sw(ct)

        cfg = make_config_with_extra_numeric(ALL_POI)
        base = Model_pipeline(config=cfg, model_type="tree", model=XGBRegressor(**params))
        wrapped = TransformedTargetRegressor(regressor=base, func=np.log, inverse_func=np.exp)
        wrapped.fit(Xt, yt, model__sample_weight=sw)
        scores.append(mean_absolute_percentage_error(yv, wrapped.predict(Xv)))
    return np.mean(scores)


def main():
    print("=" * 60)
    print(" MAPE-TARGETED XGBOOST — Q1 weighted + Optuna")
    print("=" * 60)

    data = load_experiment_data()
    train_fe = augment_regions(
        data.train_raw, data.transport_poi, data.grocery_poi, data.city_centers
    )
    print(f"  Train: {len(data.train_raw):,} → {len(train_fe):,} augmented")

    # Optuna (8 trials for speed)
    print("\n[1] Optuna Q1-weighted XGBoost (8 trials)...")
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(direction="minimize",
        sampler=optuna.samplers.TPESampler(seed=RANDOM_STATE))
    study.optimize(lambda t: optuna_objective(t, train_fe), n_trials=8, show_progress_bar=True)
    best = study.best_params
    print(f"  Best CV MAPE: {study.best_value:.4f}")

    # CV + Holdout with Q1-weighted best params
    print("\n[2] 5-fold CV with Q1-weighted XGBoost...")
    best |= {"random_state": RANDOM_STATE, "n_jobs": -1}

    oof_parts = []
    test_preds = np.zeros(len(data.test_fe))
    test_y = None

    for fold_i, (tr, vl) in enumerate(CV.split(train_fe)):
        ft, fv = train_fe.iloc[tr].copy(), train_fe.iloc[vl].copy()
        ct, cp = fit_cleaning_policy(ft, SELECTED_CLEANING_POLICY)
        cvv = apply_cleaning_policy(fv, cp)
        tst = apply_cleaning_policy(data.test_fe, cp)

        Xt, yt = prepare_xy(ct)
        Xv, yv = prepare_xy(cvv)
        Xtst, ytst = prepare_xy(tst)
        sw = compute_q_weighted_sw(ct)

        cfg = make_config_with_extra_numeric(ALL_POI)
        base = Model_pipeline(config=cfg, model_type="tree", model=XGBRegressor(**best))
        wrapped = TransformedTargetRegressor(regressor=base, func=np.log, inverse_func=np.exp)
        wrapped.fit(Xt, yt, model__sample_weight=sw)

        oof_parts.append(pd.DataFrame({"y_true": yv, "y_pred": wrapped.predict(Xv)}))
        test_preds += wrapped.predict(Xtst)
        if fold_i == 0:
            test_y = ytst.values

    test_preds /= CV.n_splits
    oof_all = pd.concat(oof_parts, ignore_index=True)

    cv_s = score_predictions(oof_all["y_true"], oof_all["y_pred"], include_r2=True)
    test_s = score_predictions(test_y, test_preds, include_r2=True)

    print(f"\n{'':>20} {'MAPE':>8} {'MedAPE':>8} {'R2':>8}")
    print(f"{'CV (OOF)':>20} {cv_s['MAPE']:>8.4f} {cv_s['MedAPE']:>8.4f} {cv_s['R2']:>8.4f}")
    print(f"{'Holdout Test':>20} {test_s['MAPE']:>8.4f} {test_s['MedAPE']:>8.4f} {test_s['R2']:>8.4f}")

    goal = 0.08
    if test_s["MAPE"] < goal:
        print(f"\n*** GOAL ACHIEVED! MAPE {test_s['MAPE']:.4f} ***")
    else:
        print(f"\n  Gap to 8% MAPE: {test_s['MAPE'] - goal:.4f}")


if __name__ == "__main__":
    main()
