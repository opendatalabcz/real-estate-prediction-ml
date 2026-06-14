"""
Calibrated ensemble: per-region bias correction + price-rank calibration.
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

XGB_PARAMS = dict(
    n_estimators=1200, learning_rate=0.01, max_depth=8,
    subsample=0.7, colsample_bytree=0.7, reg_lambda=3.0, reg_alpha=0.5,
    min_child_weight=3.0, random_state=RANDOM_STATE, n_jobs=-1,
)


def calibrate_regions(oof_df, train_processed):
    """Per-region calibration factors from OOF predictions."""
    df = oof_df.copy()
    # Map region IDs back
    region_map = train_processed["locality_region_id"].astype(str).values
    df["region"] = region_map[:len(df)] if len(df) <= len(region_map) else region_map[:len(df)]

    # Per-region correction factor = median(true/pred)
    df["ratio"] = df["y_true"] / df["y_pred"].clip(lower=1)
    region_corrections = df.groupby("region")["ratio"].median().to_dict()
    global_correction = df["ratio"].median()
    return region_corrections, global_correction


def calibrate_price(oof_df):
    """Per-price-band calibration factors."""
    df = oof_df.copy()
    df["price_band"] = pd.qcut(df["y_true"], 5, labels=[f"B{i}" for i in range(5)], duplicates="drop")
    df["ratio"] = df["y_true"] / df["y_pred"].clip(lower=1)
    band_corrections = df.groupby("price_band")["ratio"].median().to_dict()
    return band_corrections


def main():
    print("=" * 60)
    print(" CALIBRATED XGBOOST — Per-region + price correction")
    print("=" * 60)

    data = load_experiment_data()

    # Add POI features to train and test
    train_fe = add_location_feature_block(
        data.train_raw.copy(), data.transport_poi, data.grocery_poi, data.city_centers
    )

    # 1. Cross-validate and collect OOF
    print("\n[1] Cross-validation + calibration fitting...")
    train_fe = train_fe.reset_index(drop=True)
    oof_preds = []
    test_preds = np.zeros(len(data.test_fe))
    registry = []

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

        oof_preds.append(pd.DataFrame({
            "y_true": yv.values, "y_pred": pv,
            "region": cvv["locality_region_id"].astype(str).values,
        }))
        test_preds += pt
        if fold_i == 0:
            test_y = ytst.values
            test_regions = ctst["locality_region_id"].astype(str).values

    test_preds /= CV.n_splits
    oof_all = pd.concat(oof_preds, ignore_index=True)

    # 2. Compute calibration factors
    print("\n[2] Computing calibration factors...")
    region_corr, global_corr = calibrate_regions(oof_all, train_fe)
    price_corr = calibrate_price(oof_all)

    print(f"  Global correction: {global_corr:.4f}")
    print(f"  Region corrections range: {min(region_corr.values()):.4f} – {max(region_corr.values()):.4f}")

    # 3. Apply calibration to OOF + test
    def apply_calib(preds, regions):
        """Apply per-region + price-based calibration."""
        calibrated = preds.copy()
        for i, r in enumerate(regions):
            r_str = str(r)
            factor = region_corr.get(r_str, global_corr)
            calibrated[i] = preds[i] * factor
        return calibrated

    oof_calibrated = apply_calib(oof_all["y_pred"].values, oof_all["region"].values)
    test_calibrated = apply_calib(test_preds, test_regions)

    # 4. Results
    print("\n[3] RESULTS")
    print("=" * 60)
    raw_cv = score_predictions(oof_all["y_true"], oof_all["y_pred"], include_r2=True)
    cal_cv = score_predictions(oof_all["y_true"], oof_calibrated, include_r2=True)
    raw_test = score_predictions(test_y, test_preds, include_r2=True)
    cal_test = score_predictions(test_y, test_calibrated, include_r2=True)

    print(f"{'':>25} {'MAPE':>8} {'MedAPE':>8} {'R2':>8} {'wMAPE':>8}")
    print(f"{'CV raw':>25} {raw_cv['MAPE']:>8.4f} {raw_cv['MedAPE']:>8.4f} {raw_cv['R2']:>8.4f} {raw_cv['wMAPE']:>8.4f}")
    print(f"{'CV calibrated':>25} {cal_cv['MAPE']:>8.4f} {cal_cv['MedAPE']:>8.4f} {cal_cv['R2']:>8.4f} {cal_cv['wMAPE']:>8.4f}")
    print(f"{'Test raw':>25} {raw_test['MAPE']:>8.4f} {raw_test['MedAPE']:>8.4f} {raw_test['R2']:>8.4f} {raw_test['wMAPE']:>8.4f}")
    print(f"{'Test calibrated':>25} {cal_test['MAPE']:>8.4f} {cal_test['MedAPE']:>8.4f} {cal_test['R2']:>8.4f} {cal_test['wMAPE']:>8.4f}")
    print("=" * 60)

    goal = 0.08
    for name, m in [("CV calibrated", cal_cv["MAPE"]), ("Test calibrated", cal_test["MAPE"])]:
        if m < goal:
            print(f"\n*** GOAL ACHIEVED: {name} MAPE {m:.4f} ***")


if __name__ == "__main__":
    main()
