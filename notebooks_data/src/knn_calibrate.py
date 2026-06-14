"""
KNN-localized bias correction: for each prediction, find k nearest training neighbors
and adjust by their observed error ratio.
"""
import numpy as np, pandas as pd
from sklearn.neighbors import NearestNeighbors
from xgboost import XGBRegressor
from sklearn.preprocessing import StandardScaler
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

XGB_PARAMS = dict(
    n_estimators=800, learning_rate=0.02, max_depth=8,
    subsample=0.7, colsample_bytree=0.7, reg_lambda=1.0,
    random_state=RANDOM_STATE, n_jobs=-1,
)


def knn_calibrate_preds(X_train_np, oof_true, oof_pred, X_calib_np, calib_preds, k=50):
    """
    For each calibration point, find k nearest training neighbors.
    Return calibrated predictions = raw * median(true/pred) of neighbors.
    """
    # Scale features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(np.vstack([X_train_np, X_calib_np]))
    X_train_scaled = X_scaled[:len(X_train_np)]
    X_calib_scaled = X_scaled[len(X_train_np):]

    # Compute error ratios for training points
    ratios = oof_true.values / np.maximum(oof_pred.values, 100)

    # Find k nearest neighbors
    nn = NearestNeighbors(n_neighbors=k, metric="euclidean")
    nn.fit(X_train_scaled)
    distances, indices = nn.kneighbors(X_calib_scaled)

    # For each calibration point, compute median ratio of neighbors
    calibrated = np.zeros(len(calib_preds))
    for i, idx in enumerate(indices):
        neighbor_ratios = ratios[idx]
        correction = np.median(neighbor_ratios)
        calibrated[i] = calib_preds[i] * correction

    return np.maximum(calibrated, 100)


def main():
    print("=" * 60)
    print(" KNN-LOCAL BIAS CORRECTION")
    print("=" * 60)

    data = load_experiment_data()
    train_fe = add_location_feature_block(
        data.train_raw.copy(), data.transport_poi, data.grocery_poi, data.city_centers
    )
    print(f"  Train: {len(data.train_raw):,} | Test: {len(data.test_raw):,}")

    # Collect OOF predictions and test predictions
    oof_parts = []
    test_preds_raw = np.zeros(len(data.test_fe))
    Xtest_collected = None
    test_y = None

    for fold_i, (tr, vl) in enumerate(CV.split(train_fe)):
        ft = train_fe.iloc[tr].copy()
        fv = train_fe.iloc[vl].copy()
        ct, cp = fit_cleaning_policy(ft, SELECTED_CLEANING_POLICY)
        cvv = apply_cleaning_policy(fv, cp)
        ctst = apply_cleaning_policy(data.test_fe, cp)

        Xt, yt = prepare_xy(ct)
        Xv, yv = prepare_xy(cvv)
        Xtst, ytst = prepare_xy(ctst)

        cfg = make_config_with_extra_numeric(ALL_POI)
        base = Model_pipeline(config=cfg, model_type="tree", model=XGBRegressor(**XGB_PARAMS))
        wrapped = TransformedTargetRegressor(regressor=base, func=np.log, inverse_func=np.exp)
        wrapped.fit(Xt, yt)

        pv = wrapped.predict(Xv)
        pt = wrapped.predict(Xtst)

        oof_parts.append(pd.DataFrame({
            "y_true": yv.values, "y_pred": pv, **{c: Xv[c].values for c in Xv.columns}
        }))

        test_preds_raw += pt
        if fold_i == 0:
            test_y = ytst.values
            Xtest_collected = Xtst.copy()

    test_preds_raw /= CV.n_splits
    oof_all = pd.concat(oof_parts, ignore_index=True)

    raw_cv = score_predictions(oof_all["y_true"], oof_all["y_pred"], include_r2=True)
    raw_test = score_predictions(test_y, test_preds_raw, include_r2=True)
    print(f"\n  Raw CV:  MAPE={raw_cv['MAPE']:.4f} MedAPE={raw_cv['MedAPE']:.4f} R2={raw_cv['R2']:.4f}")
    print(f"  Raw Test: MAPE={raw_test['MAPE']:.4f} MedAPE={raw_test['MedAPE']:.4f} R2={raw_test['R2']:.4f}")

    # KNN calibration — use only numeric columns for distance
    num_cols = oof_all.select_dtypes(include=[np.number]).columns.tolist()
    num_cols = [c for c in num_cols if c not in ["y_true", "y_pred"]]

    X_train_num = oof_all[num_cols].fillna(0).values.astype(np.float64)
    X_test_num = Xtest_collected[num_cols].fillna(0).values.astype(np.float64)

    # Tune k
    print(f"\n  KNN calibration (tuning k)...")
    best_k, best_mape = 30, 1.0
    for k in [10, 20, 30, 50, 75, 100, 150]:
        calib_cv = knn_calibrate_preds(
            X_train_num, oof_all["y_true"], oof_all["y_pred"],
            X_train_num, oof_all["y_pred"].values, k=k
        )
        mape_cv = mean_absolute_percentage_error(oof_all["y_true"], calib_cv)
        if mape_cv < best_mape:
            best_mape, best_k = mape_cv, k
        print(f"    k={k:>3}: CV MAPE={mape_cv:.4f}")

    # Apply best k on test
    print(f"\n  Applying k={best_k} on test...")
    calib_test = knn_calibrate_preds(
        X_train_num, oof_all["y_true"], oof_all["y_pred"],
        X_test_num, test_preds_raw, k=best_k
    )

    calib_cv = knn_calibrate_preds(
        X_train_num, oof_all["y_true"], oof_all["y_pred"],
        X_train_num, oof_all["y_pred"].values, k=best_k
    )

    cv_cal = score_predictions(oof_all["y_true"], calib_cv, include_r2=True)
    test_cal = score_predictions(test_y, calib_test, include_r2=True)

    print(f"\n  {'':>25} {'MAPE':>8} {'MedAPE':>8} {'R2':>8}")
    print(f"  {'Raw test':>25} {raw_test['MAPE']:>8.4f} {raw_test['MedAPE']:>8.4f} {raw_test['R2']:>8.4f}")
    print(f"  {'KNN calibrated test':>25} {test_cal['MAPE']:>8.4f} {test_cal['MedAPE']:>8.4f} {test_cal['R2']:>8.4f}")

    print("=" * 60)
    goal = 0.08
    if test_cal["MAPE"] < goal:
        print(f"\n*** GOAL ACHIEVED! MAPE {test_cal['MAPE']:.4f} ***")
    else:
        print(f"\n  Gap to 8%: {test_cal['MAPE'] - goal:.4f}")


if __name__ == "__main__":
    main()
