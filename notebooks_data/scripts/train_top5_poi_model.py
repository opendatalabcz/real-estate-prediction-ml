"""
Train baseline vs top-5 POI XGBoost using Model_pipeline + KFold CV OOF.

Top-5 POI features (forward-selected):
  1. transport_quality_score
  2. city_center_travel_time_min
  3. convenience_nearest_km
  4. grocery_avg_3_nearest_km
  5. supermarket_nearest_km

Uses same preprocessing (process_df, Model_pipeline) as the main experiment.
"""

import json
import sys
from copy import deepcopy
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.model_selection import KFold
from sklearn.neighbors import BallTree
from sklearn.compose import TransformedTargetRegressor
from xgboost import XGBRegressor

_SRC = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(_SRC))
sys.path.insert(0, str(_SRC.parent))

from process import process_df, get_pipeline_config
from pipe import Model_pipeline
from helper import score_predictions  # noqa: E402
from helper import remove_invalid_rows  # noqa: E402

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
ARTIFACTS_DIR = PROJECT_ROOT / "artifacts" / "top5_poi_model"
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)

TRAIN_PATH = DATA_DIR / "apartments_raw_data.csv"
TEST_PATH = DATA_DIR / "apartments_raw_data_test.csv"
TRANSPORT_POI_PATH = DATA_DIR / "public_transport.csv"
GROCERY_POI_PATH = DATA_DIR / "brand_stores.csv"
CITY_CENTERS_PATH = DATA_DIR / "city_centers.csv"
TRAVEL_PATH = DATA_DIR / "city_center_travel_times.csv"

TARGET = "price_total"
EARTH_RADIUS_KM = 6371.0088
RANDOM_STATE = 42
CV = KFold(n_splits=5, shuffle=True, random_state=RANDOM_STATE)

TOP5_POI = [
    "transport_quality_score",
    "city_center_travel_time_min",
    "convenience_nearest_km",
    "grocery_avg_3_nearest_km",
    "supermarket_nearest_km",
]

# ---------------------------------------------------------------------------
# POI feature engineering (BallTree)
# ---------------------------------------------------------------------------


def build_transport_quality_score(
    apt_lat: np.ndarray, apt_lon: np.ndarray,
    transport_df: pd.DataFrame,
    w_metro: float = 10.0, w_tram: float = 1.0, w_bus: float = 0.0,
) -> np.ndarray:
    """Compute transport quality score Q = w/(1+d) for nearest transport of each type."""
    scores = np.zeros(len(apt_lat))
    apt_rad = np.radians(np.column_stack([apt_lat, apt_lon]))

    for kind, weight in [("subway", w_metro), ("tram_stop", w_tram), ("bus_stop", w_bus)]:
        if weight == 0:
            continue
        mask = transport_df["poi_kind"].str.lower() == kind
        if not mask.any():
            continue
        poi_rad = np.radians(
            transport_df.loc[mask, ["latitude", "longitude"]].astype(float).values
        )
        tree = BallTree(poi_rad, metric="haversine")
        dist_km, _ = tree.query(apt_rad, k=1)
        dist_km = dist_km.flatten() * EARTH_RADIUS_KM
        scores += weight / (1.0 + dist_km)
    return scores


def compute_poi_features(df: pd.DataFrame, transport: pd.DataFrame, grocery: pd.DataFrame) -> pd.DataFrame:
    """Compute 5 POI features on a single DataFrame using BallTree queries."""
    lat = df["latitude"].values
    lon = df["longitude"].values

    df["transport_quality_score"] = build_transport_quality_score(lat, lon, transport)

    grocery_rad = np.radians(grocery[["latitude", "longitude"]].astype(float).values)
    apt_rad = np.radians(np.column_stack([lat, lon]))
    tree = BallTree(grocery_rad, metric="haversine")
    dist_km_3, _ = tree.query(apt_rad, k=3)
    dist_km_3 *= EARTH_RADIUS_KM

    sup_mask = grocery["poi_kind"].str.lower() == "supermarket"
    con_mask = ~sup_mask

    if sup_mask.any():
        sup_rad = np.radians(grocery.loc[sup_mask, ["latitude", "longitude"]].astype(float).values)
        sup_tree = BallTree(sup_rad, metric="haversine")
        sup_d, _ = sup_tree.query(apt_rad, k=1)
        df["supermarket_nearest_km"] = sup_d.flatten() * EARTH_RADIUS_KM

    if con_mask.any():
        con_rad = np.radians(grocery.loc[con_mask, ["latitude", "longitude"]].astype(float).values)
        con_tree = BallTree(con_rad, metric="haversine")
        con_d, _ = con_tree.query(apt_rad, k=1)
        df["convenience_nearest_km"] = con_d.flatten() * EARTH_RADIUS_KM

    df["grocery_avg_3_nearest_km"] = np.mean(dist_km_3, axis=1)

    if TRAVEL_PATH.exists():
        travel = pd.read_csv(TRAVEL_PATH)
        travel_map = dict(zip(travel["listing_id"], travel["travel_time_min"]))
        df["city_center_travel_time_min"] = df["id"].map(travel_map)

    return df


# ---------------------------------------------------------------------------
# Model builder
# ---------------------------------------------------------------------------


def build_model(extra_numeric_features):
    """Build Model_pipeline wrapped in TransformedTargetRegressor(log/exp)."""
    config = deepcopy(get_pipeline_config())
    config["num_features"] = list(dict.fromkeys(
        config["num_features"] + (extra_numeric_features or [])
    ))
    base = Model_pipeline(
        config=config,
        model_type="tree",
        model=XGBRegressor(
            n_estimators=800, learning_rate=0.025,
            max_depth=12, max_leaves=63, grow_policy="lossguide",
            subsample=0.7, colsample_bytree=0.8,
            reg_lambda=1.0, random_state=RANDOM_STATE, n_jobs=-1,
        ),
    )
    return TransformedTargetRegressor(regressor=base, func=np.log, inverse_func=np.exp)


# ---------------------------------------------------------------------------
# KFold CV with OOF predictions
# ---------------------------------------------------------------------------


def oof_scores(df_raw, extra_features):
    """
    KFold CV with OOF predictions.
    df_raw already has POI features computed.
    Inside each fold: process_df -> fit -> predict.
    """
    df = remove_invalid_rows(df_raw).reset_index(drop=True).copy()
    df["__row_id"] = np.arange(len(df))
    oof_parts = []

    for train_idx, val_idx in CV.split(df):
        fold_train = df.iloc[train_idx].copy()
        fold_val = df.iloc[val_idx].copy()

        X_train = process_df(fold_train).drop(columns=[TARGET, "__row_id"], errors="ignore")
        y_train = fold_train[TARGET]
        X_val = process_df(fold_val).drop(columns=[TARGET, "__row_id"], errors="ignore")
        y_val = fold_val[TARGET]

        model = build_model(extra_features)
        model.fit(X_train, y_train)
        preds = model.predict(X_val)

        oof_parts.append(pd.DataFrame({
            "row_id": fold_val["__row_id"].values,
            "y_true": y_val.values,
            "y_pred": preds,
        }))

    oof = pd.concat(oof_parts, ignore_index=True).sort_values("row_id")
    scores = score_predictions(oof["y_true"], oof["y_pred"])
    scores["n_scored"] = len(oof)
    return scores, oof


def fit_and_score_test(df_train_raw, df_test_raw, extra_features):
    """Fit on full train, predict on test, return scores."""
    df_train = remove_invalid_rows(df_train_raw)
    df_test = remove_invalid_rows(df_test_raw)
    X_train = process_df(df_train).drop(columns=[TARGET], errors="ignore")
    y_train = df_train[TARGET]
    X_test = process_df(df_test).drop(columns=[TARGET], errors="ignore")
    y_test = df_test[TARGET]

    model = build_model(extra_features)
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    scores = score_predictions(y_test, preds)
    scores["n_scored"] = len(y_test)
    return scores


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    print("Loading data...")
    train_raw = pd.read_csv(TRAIN_PATH)
    test_raw = pd.read_csv(TEST_PATH)
    print(f"  train: {len(train_raw)}, test: {len(test_raw)}")

    print("Loading POI databases...")
    transport = pd.read_csv(TRANSPORT_POI_PATH)
    grocery = pd.read_csv(GROCERY_POI_PATH)

    print("Computing POI features...")
    train_raw = compute_poi_features(train_raw, transport, grocery)
    test_raw = compute_poi_features(test_raw, transport, grocery)

    print("\n--- OOF CV: Baseline ---")
    baseline_oof, _ = oof_scores(train_raw, extra_features=[])
    for k in ["MedAPE", "MAPE", "wMAPE", "MAE", "R2"]:
        print(f"  {k}: {baseline_oof[k]:.5f}")

    print("\n--- OOF CV: Top-5 POI ---")
    top5_oof, _ = oof_scores(train_raw, extra_features=TOP5_POI)
    for k in ["MedAPE", "MAPE", "wMAPE", "MAE", "R2"]:
        print(f"  {k}: {top5_oof[k]:.5f}")

    print("\n--- Test: Baseline ---")
    baseline_test = fit_and_score_test(train_raw, test_raw, extra_features=[])
    for k in ["MedAPE", "MAPE", "wMAPE", "MAE", "R2"]:
        print(f"  {k}: {baseline_test[k]:.5f}")

    print("\n--- Test: Top-5 POI ---")
    top5_test = fit_and_score_test(train_raw, test_raw, extra_features=TOP5_POI)
    for k in ["MedAPE", "MAPE", "wMAPE", "MAE", "R2"]:
        print(f"  {k}: {top5_test[k]:.5f}")

    results = {
        "baseline_oof": {k: round(v, 6) if isinstance(v, float) else v for k, v in baseline_oof.items()},
        "top5_oof": {k: round(v, 6) if isinstance(v, float) else v for k, v in top5_oof.items()},
        "baseline_test": {k: round(v, 6) if isinstance(v, float) else v for k, v in baseline_test.items()},
        "top5_test": {k: round(v, 6) if isinstance(v, float) else v for k, v in top5_test.items()},
    }

    with open(ARTIFACTS_DIR / "results.json", "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nResults saved to {ARTIFACTS_DIR / 'results.json'}")


if __name__ == "__main__":
    main()
