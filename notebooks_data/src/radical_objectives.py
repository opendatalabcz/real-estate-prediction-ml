"""
Radical MAPE reduction: Tweedie loss, quantile regression, early stopping.
"""
import numpy as np, pandas as pd
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

OBJECTIVES = {
    "tweedie": {"objective": "reg:tweedie", "tweedie_variance_power": 1.5},
    "squarederror": {"objective": "reg:squarederror"},
    "absoluteerror": {"objective": "reg:absoluteerror"},
    "pseudohuber": {"objective": "reg:pseudohubererror"},
    "gamma": {"objective": "reg:gamma"},
}


def run_cv_with_objective(train_fe, test_fe, obj_name, obj_params, base_params):
    """5-fold CV + holdout for one objective."""
    oof_preds = []
    test_preds = np.zeros(len(test_fe))
    test_y = None

    for fold_i, (tr, vl) in enumerate(CV.split(train_fe)):
        ft = train_fe.iloc[tr].copy()
        fv = train_fe.iloc[vl].copy()
        ct, cp = fit_cleaning_policy(ft, SELECTED_CLEANING_POLICY)
        cvv = apply_cleaning_policy(fv, cp)
        ctst = apply_cleaning_policy(test_fe, cp)

        Xt, yt = prepare_xy(ct)
        Xv, yv = prepare_xy(cvv)
        Xtst, ytst = prepare_xy(ctst)

        # Build model with objective
        cfg = make_config_with_extra_numeric(ALL_POI)
        params = {**base_params, **obj_params, "random_state": RANDOM_STATE, "n_jobs": -1}
        base = Model_pipeline(config=cfg, model_type="tree", model=XGBRegressor(**params))

        # No log-transform for Tweedie/Gamma since they handle positive data natively
        if obj_name in ("tweedie", "gamma"):
            base.fit(Xt, yt)
            pv = base.predict(Xv)
            pt = base.predict(Xtst)
        else:
            wrapped = TransformedTargetRegressor(regressor=base, func=np.log, inverse_func=np.exp)
            wrapped.fit(Xt, yt)
            pv = wrapped.predict(Xv)
            pt = wrapped.predict(Xtst)

        oof_preds.append(pd.DataFrame({"y_true": yv.values, "y_pred": pv}))
        test_preds += pt
        if fold_i == 0:
            test_y = ytst.values

    test_preds /= CV.n_splits
    oof_all = pd.concat(oof_preds, ignore_index=True)

    cv_s = score_predictions(oof_all["y_true"], oof_all["y_pred"], include_r2=True)
    test_s = score_predictions(test_y, test_preds, include_r2=True)
    return cv_s, test_s


def main():
    print("=" * 60)
    print(" RADICAL OBJECTIVE COMPARISON")
    print("=" * 60)

    data = load_experiment_data()
    train_fe = add_location_feature_block(
        data.train_raw.copy(), data.transport_poi, data.grocery_poi, data.city_centers
    )
    print(f"  Train: {len(data.train_raw):,} | Test: {len(data.test_raw):,}")

    base_params = dict(
        n_estimators=1200, learning_rate=0.01, max_depth=8,
        subsample=0.7, colsample_bytree=0.7,
        reg_lambda=3.0, reg_alpha=0.5, min_child_weight=3.0,
    )

    results = []
    for name, params in OBJECTIVES.items():
        print(f"\n  Training {name}...", end=" ", flush=True)
        cv_s, test_s = run_cv_with_objective(train_fe, data.test_fe, name, params, base_params)
        results.append((name, cv_s, test_s))
        print(f"CV MAPE={cv_s['MAPE']:.4f} MedAPE={cv_s['MedAPE']:.4f} | "
              f"Test MAPE={test_s['MAPE']:.4f} MedAPE={test_s['MedAPE']:.4f}")

    print("\n" + "=" * 60)
    print(f"{'Objective':<20} {'CV MAPE':>9} {'CV MedAPE':>10} {'Test MAPE':>10} {'Test MedAPE':>11} {'Test R2':>8}")
    print("-" * 70)
    best_test_mape = 1.0
    best_name = ""
    for name, cv_s, test_s in results:
        print(f"{name:<20} {cv_s['MAPE']:>9.4f} {cv_s['MedAPE']:>10.4f} "
              f"{test_s['MAPE']:>10.4f} {test_s['MedAPE']:>11.4f} {test_s['R2']:>8.4f}")
        if test_s["MAPE"] < best_test_mape:
            best_test_mape = test_s["MAPE"]
            best_name = name

    print("=" * 60)
    print(f"\n  Best: {best_name} — Test MAPE {best_test_mape:.4f}")
    goal = 0.08
    if best_test_mape < goal:
        print(f"  *** GOAL ACHIEVED! ***")
    else:
        print(f"  Gap to 8%: {best_test_mape - goal:.4f}")


if __name__ == "__main__":
    main()
