"""
CatBoost-native pipeline v2 — fixed data flow.
Target: MAPE < 8%.
"""
import numpy as np, pandas as pd
import optuna
from xgboost import XGBRegressor
from catboost import CatBoostRegressor
from sklearn.compose import TransformedTargetRegressor
from sklearn.metrics import mean_absolute_percentage_error

from src.poi_models_workflow import (
    load_experiment_data, prepare_xy, fit_cleaning_policy,
    apply_cleaning_policy, SELECTED_CLEANING_POLICY, CV,
    score_predictions, add_location_feature_block,
    make_config_with_extra_numeric, remove_invalid_rows,
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
    """Oversample regions with < 600 rows. Target: ~800 rows per sparse region."""
    parts = [df_raw.copy()]
    for region_id, n_orig in df_raw["locality_region_id"].value_counts().items():
        if n_orig >= 600:
            continue
        region_df = df_raw[df_raw["locality_region_id"] == region_id].copy()
        # Add up to 2x synthetic data
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


def compute_sw_from_clean(cleaned):
    """Sample weight = 1/region_freq, normalized."""
    if "locality_region_id" not in cleaned.columns:
        return None
    region_freq = cleaned["locality_region_id"].astype(str).value_counts()
    w = cleaned["locality_region_id"].astype(str).map(1.0 / region_freq)
    return np.array(w / w.mean())


def xgb_model_from_params(params):
    extra = ALL_POI
    cfg = make_config_with_extra_numeric(extra)
    base = Model_pipeline(config=cfg, model_type="tree", model=XGBRegressor(**params))
    return TransformedTargetRegressor(regressor=base, func=np.log, inverse_func=np.exp)


def optuna_objective(trial, train_fe_df):
    params = {
        "n_estimators": trial.suggest_int("n_estimators", 400, 1500, step=200),
        "learning_rate": trial.suggest_float("learning_rate", 0.005, 0.1, log=True),
        "max_depth": trial.suggest_int("max_depth", 5, 10),
        "subsample": trial.suggest_float("subsample", 0.5, 0.9),
        "colsample_bytree": trial.suggest_float("colsample_bytree", 0.5, 0.9),
        "reg_lambda": trial.suggest_float("reg_lambda", 0.01, 10.0, log=True),
        "reg_alpha": trial.suggest_float("reg_alpha", 0.001, 5.0, log=True),
        "min_child_weight": trial.suggest_float("min_child_weight", 0.5, 10.0),
        "random_state": RANDOM_STATE, "n_jobs": -1,
    }
    scores = []
    for train_idx, val_idx in CV.split(train_fe_df):
        ft, fv = train_fe_df.iloc[train_idx].copy(), train_fe_df.iloc[val_idx].copy()
        ct, cp = fit_cleaning_policy(ft, SELECTED_CLEANING_POLICY)
        cvv = apply_cleaning_policy(fv, cp)
        Xt, yt = prepare_xy(ct)
        Xv, yv = prepare_xy(cvv)
        sw = compute_sw_from_clean(ct)

        wrapped = xgb_model_from_params(params)
        wrapped.fit(Xt, yt, model__sample_weight=sw)
        scores.append(mean_absolute_percentage_error(yv, wrapped.predict(Xv)))
    return np.mean(scores)


def main():
    print("=" * 60)
    print(" CATBOOST + XGBOOST ENSEMBLE — Target: MAPE < 8%")
    print("=" * 60)

    # 1. Load + augment
    data = load_experiment_data()
    print(f"\n  Train: {len(data.train_raw):,} → ", end="")
    train_aug = augment_regions(
        data.train_raw, data.transport_poi, data.grocery_poi, data.city_centers
    )
    print(f"{len(train_aug):,} augmented")

    # 2. Optuna scan (fast: 12 trials)
    print("\n[1] Optuna XGBoost tuning (12 trials)...")
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(
        direction="minimize",
        sampler=optuna.samplers.TPESampler(seed=RANDOM_STATE),
    )
    study.optimize(
        lambda t: optuna_objective(t, train_aug),
        n_trials=12,
        show_progress_bar=True,
    )
    best_xgb_params = study.best_params
    best_xgb_params |= {"random_state": RANDOM_STATE, "n_jobs": -1}
    print(f"  Best CV MAPE: {study.best_value:.4f}")

    # 3. CV + holdout for ensemble
    print("\n[2] 5-fold CV with sample weights...")

    oof_xgb, oof_cat = [], []
    test_preds_xgb = np.zeros(len(data.test_fe))
    test_preds_cat = np.zeros(len(data.test_fe))
    test_y = []

    for fold_i, (tr, vl) in enumerate(CV.split(train_aug)):
        ft = train_aug.iloc[tr].copy()
        fv = train_aug.iloc[vl].copy()
        ct, cp = fit_cleaning_policy(ft, SELECTED_CLEANING_POLICY)
        cvv = apply_cleaning_policy(fv, cp)
        cv_test = apply_cleaning_policy(data.test_fe, cp)

        Xt, yt = prepare_xy(ct)
        Xv, yv = prepare_xy(cvv)
        Xtest, ytest = prepare_xy(cv_test)
        sw = compute_sw_from_clean(ct)

        # CatBoost categorical columns — all object/string/category dtypes
        cat_col_names = [
            c for c in Xt.columns
            if Xt[c].dtype.name in ("object", "category", "string")
        ]

        # Convert to string for CatBoost
        Xt_cat, Xv_cat, Xtest_cat = Xt.copy(), Xv.copy(), Xtest.copy()
        for col in cat_col_names:
            Xt_cat[col] = Xt_cat[col].astype(str)
            Xv_cat[col] = Xv_cat[col].astype(str)
            Xtest_cat[col] = Xtest_cat[col].astype(str)

        # XGBoost
        xgb_w = xgb_model_from_params(best_xgb_params)
        xgb_w.fit(Xt, yt, model__sample_weight=sw)
        px = xgb_w.predict(Xv)
        tx = xgb_w.predict(Xtest)
        oof_xgb.append(pd.DataFrame({"y_true": yv, "y_pred": px}))
        test_preds_xgb += tx

        # CatBoost (manual log-transform to avoid clone issue)
        cat_model = CatBoostRegressor(
            iterations=1000, learning_rate=0.02, depth=8,
            l2_leaf_reg=3.0, random_seed=RANDOM_STATE,
            verbose=False, allow_writing_files=False,
        )
        yt_log = np.log(np.maximum(yt, 1))
        cat_model.fit(Xt_cat, yt_log, sample_weight=sw, cat_features=cat_col_names)
        pc = np.exp(cat_model.predict(Xv_cat))
        tc = np.exp(cat_model.predict(Xtest_cat))
        oof_cat.append(pd.DataFrame({"y_true": yv, "y_pred": pc}))
        test_preds_cat += tc

        if fold_i == 0:
            test_y = ytest.values

    test_preds_xgb /= CV.n_splits
    test_preds_cat /= CV.n_splits

    # 4. Find optimal blend via CV OOF
    print("\n[3] Optimizing ensemble blend...")
    oof_all_xgb = pd.concat(oof_xgb, ignore_index=True)
    oof_all_cat = pd.concat(oof_cat, ignore_index=True)

    best_mape = float("inf")
    best_w = 0.5
    for w in np.linspace(0, 1, 21):
        blend = w * oof_all_xgb["y_pred"] + (1 - w) * oof_all_cat["y_pred"]
        m = mean_absolute_percentage_error(oof_all_xgb["y_true"], blend)
        if m < best_mape:
            best_mape = m
            best_w = w

    test_blend = best_w * test_preds_xgb + (1 - best_w) * test_preds_cat
    test_blend = np.maximum(test_blend, 0)

    # 5. Results
    print("\n[4] RESULTS")
    print("=" * 60)
    s_x = score_predictions(test_y, test_preds_xgb, include_r2=True)
    s_c = score_predictions(test_y, test_preds_cat, include_r2=True)
    s_b = score_predictions(test_y, test_blend, include_r2=True)

    print(f"{'Model':<30} {'MAPE':>8} {'MedAPE':>8} {'R2':>8}")
    print("-" * 56)
    print(f"{'XGBoost (tuned+weighted)':<30} {s_x['MAPE']:>8.4f} {s_x['MedAPE']:>8.4f} {s_x['R2']:>8.4f}")
    print(f"{'CatBoost':<30} {s_c['MAPE']:>8.4f} {s_c['MedAPE']:>8.4f} {s_c['R2']:>8.4f}")
    print(f"{'Ensemble (XGB{w:.2f}+CAT{1-w:.2f})':<30} {s_b['MAPE']:>8.4f} {s_b['MedAPE']:>8.4f} {s_b['R2']:>8.4f}")
    print("=" * 60)

    goal = 0.08
    for name, s, mape in [("XGBoost", s_x, s_x["MAPE"]), ("CatBoost", s_c, s_c["MAPE"]), ("Ensemble", s_b, s_b["MAPE"])]:
        if mape < goal:
            print(f"\n✓ GOAL ACHIEVED: {name} MAPE {mape:.4f} < 8%")
            return

    best_mape_all = min(s["MAPE"] for s in [s_x, s_c, s_b])
    gap = best_mape_all - goal
    print(f"\n  ~ Gap to 8%: {gap:.4f} more needed")


if __name__ == "__main__":
    main()
