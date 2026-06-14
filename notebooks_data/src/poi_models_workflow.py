from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.compose import TransformedTargetRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, r2_score
from sklearn.model_selection import KFold
from sklearn.neighbors import BallTree

from src.pipe import Model_pipeline
from src.process import get_pipeline_config, process_df


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"

TRAIN_PATH = DATA_DIR / "apartments_raw_data.csv"
TEST_PATH = DATA_DIR / "apartments_raw_data_test.csv"
TRANSPORT_POI_PATH = DATA_DIR / "public_transport.csv"
GROCERY_POI_PATH = DATA_DIR / "brand_stores.csv"
CITY_CENTERS_PATH = DATA_DIR / "city_centers.csv"

TARGET = "price_total"
CV = KFold(n_splits=5, shuffle=True, random_state=42)
EARTH_RADIUS_KM = 6371.0088
SELECTED_CLEANING_POLICY = "invalid_only"

ALL_ENGINEERED_FEATURES = [
    "transport_nearest_km",
    "transport_count_within_500m",
    "transport_count_within_1000m",
    "metro_train_nearest_km",
    "grocery_nearest_km",
    "grocery_count_within_500m",
    "grocery_count_within_1000m",
    "distance_to_city_center_km",
]

DEFAULT_FEATURE_VARIANTS = {
    "A_baseline_only": [],
    "B_transport": [
        "transport_nearest_km",
        "transport_count_within_500m",
        "transport_count_within_1000m",
        "metro_train_nearest_km",
    ],
    "C_retail": [
        "grocery_nearest_km",
        "grocery_count_within_500m",
        "grocery_count_within_1000m",
    ],
    "D_city_center": [
        "distance_to_city_center_km",
    ],
    "E_all_location_features": ALL_ENGINEERED_FEATURES.copy(),
}

MINIMAL_FAST_VARIANTS = {
    "A_baseline_only": [],
    "B_nearest_transport_only": ["transport_nearest_km"],
    "C_nearest_grocery_only": ["grocery_nearest_km"],
    "D_city_center_only": ["distance_to_city_center_km"],
    "E_three_core_features": [
        "transport_nearest_km",
        "grocery_nearest_km",
        "distance_to_city_center_km",
    ],
}


@dataclass
class ExperimentData:
    train_raw: pd.DataFrame
    test_raw: pd.DataFrame
    transport_poi: pd.DataFrame
    grocery_poi: pd.DataFrame
    city_centers: pd.DataFrame
    train_fe: pd.DataFrame
    test_fe: pd.DataFrame
    poi_validation_report: pd.Series


def medape(y_true, y_pred):
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)
    return np.median(np.abs((y_true - y_pred) / y_true))


def wmape(y_true, y_pred):
    y_true = np.asarray(y_true)
    y_pred = np.asarray(y_pred)
    return np.abs(y_true - y_pred).sum() / np.abs(y_true).sum()


def score_predictions(y_true, y_pred, include_r2=True):
    scores = {
        "MedAPE": medape(y_true, y_pred),
        "MAPE": mean_absolute_percentage_error(y_true, y_pred),
        "wMAPE": wmape(y_true, y_pred),
        "MAE": mean_absolute_error(y_true, y_pred),
    }
    if include_r2:
        scores["R2"] = r2_score(y_true, y_pred)
    return scores


def ensure_required_files() -> None:
    for path in [
        TRAIN_PATH,
        TEST_PATH,
        TRANSPORT_POI_PATH,
        GROCERY_POI_PATH,
        CITY_CENTERS_PATH,
    ]:
        if not path.exists():
            raise FileNotFoundError(f"Missing required file: {path}")


def remove_invalid_rows(df):
    data = df.copy()

    if "updated" in data.columns:
        data["updated"] = pd.to_datetime(
            data["updated"],
            errors="coerce",
            dayfirst=True,
        )

    if "web_link" in data.columns:
        if "updated" in data.columns:
            data = data.sort_values(["updated"]).drop_duplicates(
                subset="web_link", keep="last"
            )
        else:
            data = data.drop_duplicates(subset="web_link", keep="last")

    valid_target = data[TARGET].notna() & (data[TARGET] > 0)
    usable_ok = data["usable_area_m2"].isna() | (data["usable_area_m2"] > 0)
    total_ok = data["total_area_m2"].isna() | (data["total_area_m2"] > 0)
    coords_ok = data["latitude"].between(48.0, 51.5) & data["longitude"].between(12.0, 19.0)

    return data.loc[valid_target & usable_ok & total_ok & coords_ok].copy()


def fit_cleaning_policy(df, policy):
    cleaned = remove_invalid_rows(df)
    params = {"policy": policy}

    if policy == "invalid_only":
        return cleaned, params

    if policy == "light_trim":
        clip_cols = [
            "usable_area_m2",
            "total_area_m2",
            "loggia_area_m2",
            "cellar_area_m2",
            "floor_number",
            "total_floors",
        ]
        params["clip_bounds"] = {}
        for col in clip_cols:
            if col in cleaned.columns:
                low = cleaned[col].quantile(0.01)
                high = cleaned[col].quantile(0.99)
                params["clip_bounds"][col] = (low, high)
        return apply_cleaning_policy(cleaned, params), params

    if policy == "strong_ppm_filter_train_only":
        area_basis = cleaned["usable_area_m2"].fillna(cleaned["total_area_m2"])
        ppm = cleaned[TARGET] / area_basis
        params["ppm_bounds"] = (ppm.quantile(0.01), ppm.quantile(0.99))
        low, high = params["ppm_bounds"]
        keep = ppm.between(low, high)
        return cleaned.loc[keep].copy(), params

    raise ValueError(f"Unknown policy: {policy}")


def apply_cleaning_policy(df, params):
    cleaned = remove_invalid_rows(df)
    policy = params["policy"]

    if policy == "light_trim":
        for col, (low, high) in params.get("clip_bounds", {}).items():
            if col in cleaned.columns:
                cleaned[col] = cleaned[col].clip(lower=low, upper=high)

    return cleaned


def prepare_xy(df):
    processed = process_df(df.copy())
    X = processed.drop(columns=[TARGET, "__row_id"], errors="ignore")
    y = processed[TARGET].copy()
    return X, y


def load_poi_table(path, lat_col="latitude", lon_col="longitude"):
    poi = pd.read_csv(path).copy()
    required = {lat_col, lon_col}
    missing = required - set(poi.columns)
    if missing:
        raise ValueError(f"{path} is missing required columns: {missing}")

    poi = poi.rename(columns={lat_col: "latitude", lon_col: "longitude"})
    poi = poi.dropna(subset=["latitude", "longitude"]).copy()
    return poi


def build_balltree(poi_df):
    coords_rad = np.radians(poi_df[["latitude", "longitude"]].to_numpy())
    return BallTree(coords_rad, metric="haversine")


def _distance_and_counts(listing_df, poi_df, prefix, radii_km=(0.5, 1.0)):
    out = pd.DataFrame(index=listing_df.index)

    if poi_df.empty:
        out[f"{prefix}_nearest_km"] = np.nan
        for radius_km in radii_km:
            radius_name = int(radius_km * 1000)
            out[f"{prefix}_count_within_{radius_name}m"] = np.nan
        return out

    mask = listing_df["latitude"].notna() & listing_df["longitude"].notna()
    out[f"{prefix}_nearest_km"] = np.nan
    for radius_km in radii_km:
        radius_name = int(radius_km * 1000)
        out[f"{prefix}_count_within_{radius_name}m"] = np.nan

    if mask.sum() == 0:
        return out

    tree = build_balltree(poi_df)
    listing_coords_rad = np.radians(
        listing_df.loc[mask, ["latitude", "longitude"]].to_numpy()
    )

    dist_rad, _ = tree.query(listing_coords_rad, k=1)
    out.loc[mask, f"{prefix}_nearest_km"] = dist_rad[:, 0] * EARTH_RADIUS_KM

    for radius_km in radii_km:
        radius_name = int(radius_km * 1000)
        counts = tree.query_radius(
            listing_coords_rad,
            r=radius_km / EARTH_RADIUS_KM,
            count_only=True,
        )
        out.loc[mask, f"{prefix}_count_within_{radius_name}m"] = counts.astype(float)

    return out


def add_transport_features(listing_df, transport_poi):
    features = _distance_and_counts(
        listing_df=listing_df,
        poi_df=transport_poi,
        prefix="transport",
        radii_km=(0.5, 1.0),
    )

    if "poi_kind" in transport_poi.columns:
        poi_kind = transport_poi["poi_kind"].astype(str).str.lower()
        metro_train_mask = (
            poi_kind.str.contains("metro", na=False)
            | poi_kind.str.contains("train", na=False)
            | poi_kind.str.contains("rail", na=False)
            | poi_kind.str.contains("station", na=False)
        )
        metro_train_poi = transport_poi.loc[metro_train_mask].copy()
        extra = _distance_and_counts(
            listing_df=listing_df,
            poi_df=metro_train_poi,
            prefix="metro_train",
            radii_km=(),
        )
        features["metro_train_nearest_km"] = extra["metro_train_nearest_km"]
    else:
        features["metro_train_nearest_km"] = np.nan

    return features


def add_grocery_features(listing_df, grocery_poi):
    return _distance_and_counts(
        listing_df=listing_df,
        poi_df=grocery_poi,
        prefix="grocery",
        radii_km=(0.5, 1.0),
    )


def haversine_km(lat1, lon1, lat2, lon2):
    lat1 = np.radians(lat1)
    lon1 = np.radians(lon1)
    lat2 = np.radians(lat2)
    lon2 = np.radians(lon2)

    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (
        np.sin(dlat / 2.0) ** 2
        + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    )
    c = 2 * np.arcsin(np.sqrt(a))
    return EARTH_RADIUS_KM * c


def add_city_center_features(listing_df, city_centers_df):
    required = {"locality_region_id", "center_latitude", "center_longitude"}
    missing = required - set(city_centers_df.columns)
    if missing:
        raise ValueError(f"city_centers.csv is missing required columns: {missing}")

    merged = listing_df.merge(
        city_centers_df[["locality_region_id", "center_latitude", "center_longitude"]],
        on="locality_region_id",
        how="left",
    )

    out = pd.DataFrame(index=listing_df.index)
    valid = (
        merged["latitude"].notna()
        & merged["longitude"].notna()
        & merged["center_latitude"].notna()
        & merged["center_longitude"].notna()
    )

    out["distance_to_city_center_km"] = np.nan
    out.loc[valid, "distance_to_city_center_km"] = haversine_km(
        merged.loc[valid, "latitude"].to_numpy(),
        merged.loc[valid, "longitude"].to_numpy(),
        merged.loc[valid, "center_latitude"].to_numpy(),
        merged.loc[valid, "center_longitude"].to_numpy(),
    )
    return out


def add_location_feature_block(df, transport_poi, grocery_poi, city_centers_df):
    transport_features = add_transport_features(df, transport_poi)
    grocery_features = add_grocery_features(df, grocery_poi)
    center_features = add_city_center_features(df, city_centers_df)
    return pd.concat([df.copy(), transport_features, grocery_features, center_features], axis=1)


def validate_engineered_features(df, feature_cols):
    missing_cols = [col for col in feature_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing engineered features after creation: {missing_cols}")

    all_nan_cols = [col for col in feature_cols if df[col].notna().sum() == 0]
    if all_nan_cols:
        raise ValueError(
            f"These engineered features are entirely missing. Check POI inputs: {all_nan_cols}"
        )


def build_poi_validation_report(train_raw, transport_poi, grocery_poi, city_centers):
    transport_missing_coords = int(transport_poi[["latitude", "longitude"]].isna().sum().sum())
    grocery_missing_coords = int(grocery_poi[["latitude", "longitude"]].isna().sum().sum())
    train_region_ids = set(train_raw["locality_region_id"].dropna().astype(int).astype(str))
    center_region_ids = set(city_centers["locality_region_id"].dropna().astype(int).astype(str))
    missing_center_regions = sorted(train_region_ids - center_region_ids)

    report = pd.Series(
        {
            "transport_rows": len(transport_poi),
            "grocery_rows": len(grocery_poi),
            "transport_missing_lat_lon": transport_missing_coords,
            "grocery_missing_lat_lon": grocery_missing_coords,
            "city_center_rows": len(city_centers),
            "city_center_missing_region_ids": len(missing_center_regions),
        }
    )
    return report, missing_center_regions


def load_experiment_data() -> ExperimentData:
    ensure_required_files()

    train_raw = pd.read_csv(TRAIN_PATH)
    test_raw = pd.read_csv(TEST_PATH)
    transport_poi = load_poi_table(TRANSPORT_POI_PATH)
    grocery_poi = load_poi_table(GROCERY_POI_PATH)
    city_centers = pd.read_csv(CITY_CENTERS_PATH)

    report, missing_center_regions = build_poi_validation_report(
        train_raw=train_raw,
        transport_poi=transport_poi,
        grocery_poi=grocery_poi,
        city_centers=city_centers,
    )
    if missing_center_regions:
        raise ValueError(f"city_centers.csv is missing region ids: {missing_center_regions}")

    train_fe = add_location_feature_block(train_raw, transport_poi, grocery_poi, city_centers)
    test_fe = add_location_feature_block(test_raw, transport_poi, grocery_poi, city_centers)
    validate_engineered_features(train_fe, ALL_ENGINEERED_FEATURES)

    return ExperimentData(
        train_raw=train_raw,
        test_raw=test_raw,
        transport_poi=transport_poi,
        grocery_poi=grocery_poi,
        city_centers=city_centers,
        train_fe=train_fe,
        test_fe=test_fe,
        poi_validation_report=report,
    )


class HierarchicalMedianRegressor(BaseEstimator, RegressorMixin):
    def __init__(self, primary_cols, fallback_col=None):
        self.primary_cols = primary_cols
        self.fallback_col = fallback_col

    def fit(self, X, y):
        df = X.copy()
        df["_target"] = np.asarray(y)
        self.global_median_ = np.median(y)
        self.primary_map_ = (
            df.groupby(self.primary_cols, observed=True)["_target"].median().to_dict()
        )
        if self.fallback_col is not None:
            self.fallback_map_ = (
                df.groupby(self.fallback_col, observed=True)["_target"].median().to_dict()
            )
        else:
            self.fallback_map_ = {}
        return self

    def predict(self, X):
        preds = []
        for _, row in X.iterrows():
            key = tuple(row[col] for col in self.primary_cols)
            if len(self.primary_cols) == 1:
                key = key[0]

            if key in self.primary_map_:
                preds.append(self.primary_map_[key])
            elif self.fallback_col is not None and row[self.fallback_col] in self.fallback_map_:
                preds.append(self.fallback_map_[row[self.fallback_col]])
            else:
                preds.append(self.global_median_)
        return np.asarray(preds)


def make_config_with_extra_numeric(extra_numeric_features):
    config = deepcopy(get_pipeline_config())
    config["num_features"] = list(dict.fromkeys(config["num_features"] + extra_numeric_features))
    return config


def build_linear_model(extra_numeric_features=None):
    extra_numeric_features = extra_numeric_features or []
    config = make_config_with_extra_numeric(extra_numeric_features)
    base = Model_pipeline(config=config, model_type="linear", model=LinearRegression())
    return TransformedTargetRegressor(regressor=base, func=np.log, inverse_func=np.exp)


def build_xgb_model(extra_numeric_features=None):
    from xgboost import XGBRegressor

    extra_numeric_features = extra_numeric_features or []
    config = make_config_with_extra_numeric(extra_numeric_features)
    base = Model_pipeline(
        config=config,
        model_type="tree",
        model=XGBRegressor(
            n_estimators=800,
            learning_rate=0.02,
            max_depth=8,
            subsample=0.7,
            colsample_bytree=0.7,
            reg_lambda=1.0,
            random_state=42,
            n_jobs=-1,
        ),
    )
    return TransformedTargetRegressor(regressor=base, func=np.log, inverse_func=np.exp)


def get_reference_models():
    return {
        "Hierarchical median baseline": lambda: HierarchicalMedianRegressor(
            primary_cols=["locality_region_id", "category_sub"],
            fallback_col="locality_region_id",
        ),
        "Linear regression on log-price": lambda: build_linear_model([]),
        "XGBoost baseline": lambda: build_xgb_model([]),
    }


def out_of_fold_scores(df, model_builder, cleaning_policy):
    df = df.reset_index(drop=True).copy()
    df["__row_id"] = np.arange(len(df))
    oof_parts = []

    for train_idx, val_idx in CV.split(df):
        fold_train = df.iloc[train_idx].copy()
        fold_val = df.iloc[val_idx].copy()

        cleaned_train, cleaning_params = fit_cleaning_policy(fold_train, cleaning_policy)
        cleaned_val = apply_cleaning_policy(fold_val, cleaning_params)

        X_train, y_train = prepare_xy(cleaned_train)
        X_val, y_val = prepare_xy(cleaned_val)

        model = model_builder()
        model.fit(X_train, y_train)
        preds = model.predict(X_val)

        oof_parts.append(
            pd.DataFrame(
                {
                    "row_id": cleaned_val["__row_id"].to_numpy(),
                    "y_true": y_val.to_numpy(),
                    "y_pred": preds,
                }
            )
        )

    oof = pd.concat(oof_parts, ignore_index=True).sort_values("row_id")
    scores = score_predictions(oof["y_true"], oof["y_pred"])
    scores["n_scored"] = len(oof)
    return scores, oof


def fit_locked_model(train_df, test_df, model_builder, cleaning_policy):
    cleaned_train, cleaning_params = fit_cleaning_policy(train_df, cleaning_policy)
    cleaned_test = apply_cleaning_policy(test_df, cleaning_params)

    X_train, y_train = prepare_xy(cleaned_train)
    X_test, y_test = prepare_xy(cleaned_test)

    model = model_builder()
    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    return model, cleaned_test, preds, score_predictions(y_test, preds)


def run_reference_baseline_stack(train_fe, cleaning_policy=SELECTED_CLEANING_POLICY):
    rows = []
    for model_name, model_builder in get_reference_models().items():
        metrics, _ = out_of_fold_scores(
            df=train_fe,
            model_builder=model_builder,
            cleaning_policy=cleaning_policy,
        )
        rows.append(
            {
                "feature_variant": "A_baseline_only",
                "model": model_name,
                **metrics,
            }
        )
    return pd.DataFrame(rows).sort_values(["MedAPE", "MAPE"]).reset_index(drop=True)


def run_feature_variant_cv(
    train_fe,
    feature_variants,
    cleaning_policy=SELECTED_CLEANING_POLICY,
):
    experiment_rows = []
    oof_store = {}

    for variant_name, extra_features in feature_variants.items():
        validate_engineered_features(train_fe, extra_features)
        model_builders = {
            "Linear regression on log-price": lambda ef=extra_features: build_linear_model(ef),
            "XGBoost baseline": lambda ef=extra_features: build_xgb_model(ef),
        }

        for model_name, model_builder in model_builders.items():
            metrics, oof = out_of_fold_scores(
                df=train_fe,
                model_builder=model_builder,
                cleaning_policy=cleaning_policy,
            )
            experiment_rows.append(
                {
                    "feature_variant": variant_name,
                    "n_added_features": len(extra_features),
                    "added_features": ", ".join(extra_features) if extra_features else "(none)",
                    "model": model_name,
                    **metrics,
                }
            )
            oof_store[(variant_name, model_name)] = oof

    variant_cv_results = pd.DataFrame(experiment_rows).sort_values(
        ["model", "MedAPE", "MAPE"]
    ).reset_index(drop=True)
    return variant_cv_results, oof_store


def add_uplift_vs_baseline(results_df):
    outputs = []

    for model_name, group in results_df.groupby("model", observed=True):
        baseline_row = group.loc[group["feature_variant"] == "A_baseline_only"]
        if baseline_row.empty:
            continue
        baseline_row = baseline_row.iloc[0]

        for _, row in group.iterrows():
            new_row = row.to_dict()
            for metric in ["MedAPE", "MAPE", "wMAPE", "MAE"]:
                new_row[f"delta_{metric}_vs_baseline"] = row[metric] - baseline_row[metric]
            new_row["delta_R2_vs_baseline"] = row["R2"] - baseline_row["R2"]
            outputs.append(new_row)

    return pd.DataFrame(outputs).sort_values(["model", "MedAPE"]).reset_index(drop=True)


def select_best_variant_per_model(variant_cv_results):
    return (
        variant_cv_results.sort_values(["model", "MedAPE", "MAPE", "wMAPE"])
        .groupby("model", observed=True)
        .first()
        .reset_index()
    )


def get_variant_features(feature_variants, variant_name):
    return feature_variants[variant_name]


def run_final_test_comparison(
    train_fe,
    test_fe,
    best_variant_per_model,
    feature_variants,
    cleaning_policy=SELECTED_CLEANING_POLICY,
):
    final_test_rows = []

    _, _, _, hier_scores = fit_locked_model(
        train_df=train_fe,
        test_df=test_fe,
        model_builder=lambda: HierarchicalMedianRegressor(
            primary_cols=["locality_region_id", "category_sub"],
            fallback_col="locality_region_id",
        ),
        cleaning_policy=cleaning_policy,
    )
    final_test_rows.append(
        {
            "model": "Hierarchical median baseline",
            "feature_variant": "A_baseline_only",
            "n_added_features": 0,
            **hier_scores,
        }
    )

    for _, row in best_variant_per_model.iterrows():
        model_name = row["model"]
        variant_name = row["feature_variant"]
        extra_features = get_variant_features(feature_variants, variant_name)

        if model_name == "Linear regression on log-price":
            builder = lambda ef=extra_features: build_linear_model(ef)
        elif model_name == "XGBoost baseline":
            builder = lambda ef=extra_features: build_xgb_model(ef)
        else:
            continue

        _, _, _, test_scores = fit_locked_model(
            train_df=train_fe,
            test_df=test_fe,
            model_builder=builder,
            cleaning_policy=cleaning_policy,
        )
        final_test_rows.append(
            {
                "model": model_name,
                "feature_variant": variant_name,
                "n_added_features": len(extra_features),
                **test_scores,
            }
        )

    return pd.DataFrame(final_test_rows).sort_values(["MedAPE", "MAPE"]).reset_index(drop=True)


def run_robustness_checks(train_fe, feature_variants):
    rows = []
    for variant_name, extra_features in feature_variants.items():
        if not extra_features:
            continue

        subset = train_fe[extra_features].copy()
        rows.append(
            {
                "feature_variant": variant_name,
                "n_features": len(extra_features),
                "mean_missing_fraction": subset.isna().mean().mean(),
                "max_missing_fraction": subset.isna().mean().max(),
                "high_corr_pairs_abs_ge_0_90": int(
                    (
                        subset.corr(numeric_only=True)
                        .where(
                            np.triu(
                                np.ones((subset.shape[1], subset.shape[1])),
                                k=1,
                            ).astype(bool)
                        )
                        .abs()
                        >= 0.90
                    ).sum().sum()
                )
                if subset.shape[1] >= 2
                else 0,
            }
        )

    return pd.DataFrame(rows).reset_index(drop=True)


def subgroup_summary(df, group_col):
    rows = []
    for key, group in df.groupby(group_col, observed=True):
        if len(group) < 20:
            continue
        rows.append(
            {
                "segment": group_col,
                "group": key,
                "n": len(group),
                **score_predictions(group["price_total"], group["prediction"]),
            }
        )
    return pd.DataFrame(rows).sort_values("MedAPE").reset_index(drop=True)


def get_locked_xgb_artifacts(
    train_fe,
    test_fe,
    best_variant_per_model,
    feature_variants,
    cleaning_policy=SELECTED_CLEANING_POLICY,
):
    best_xgb_row = best_variant_per_model.loc[
        best_variant_per_model["model"] == "XGBoost baseline"
    ]
    if best_xgb_row.empty:
        return None

    best_xgb_variant = best_xgb_row.iloc[0]["feature_variant"]
    best_xgb_features = get_variant_features(feature_variants, best_xgb_variant)

    locked_xgb_model, locked_xgb_test_df, locked_xgb_preds, locked_xgb_scores = fit_locked_model(
        train_df=train_fe,
        test_df=test_fe,
        model_builder=lambda ef=best_xgb_features: build_xgb_model(ef),
        cleaning_policy=cleaning_policy,
    )

    X_test_locked, y_test_locked = prepare_xy(locked_xgb_test_df)
    evaluation_df = X_test_locked[["locality_region_id", "category_sub"]].copy()
    evaluation_df["price_total"] = y_test_locked.values
    evaluation_df["prediction"] = locked_xgb_preds
    evaluation_df["price_band"] = pd.qcut(
        evaluation_df["price_total"],
        q=4,
        labels=["Q1", "Q2", "Q3", "Q4"],
    )

    return {
        "variant_name": best_xgb_variant,
        "extra_features": best_xgb_features,
        "model": locked_xgb_model,
        "test_df": locked_xgb_test_df,
        "predictions": locked_xgb_preds,
        "scores": locked_xgb_scores,
        "evaluation_df": evaluation_df,
    }


def extract_locked_xgb_feature_importances(train_fe, locked_xgb_model, cleaning_policy=SELECTED_CLEANING_POLICY):
    cleaned_train_full, _ = fit_cleaning_policy(train_fe, cleaning_policy)
    X_train_full, y_train_full = prepare_xy(cleaned_train_full)
    del X_train_full, y_train_full

    regressor = locked_xgb_model.regressor_
    preprocessor = regressor.pipeline_.named_steps["preprocessing"]
    model = regressor.pipeline_.named_steps["model"]

    transformed_feature_names = preprocessor.get_feature_names_out()
    return (
        pd.DataFrame(
            {
                "feature": transformed_feature_names,
                "importance": model.feature_importances_,
            }
        )
        .sort_values("importance", ascending=False)
        .reset_index(drop=True)
    )


def plot_core_feature_distributions(train_fe):
    import matplotlib.pyplot as plt

    feature_specs = [
        ("transport_nearest_km", "Nearest transport (km)", "#1f77b4"),
        ("grocery_nearest_km", "Nearest grocery (km)", "#2a9d8f"),
        ("distance_to_city_center_km", "City center distance (km)", "#e76f51"),
    ]

    fig, axes = plt.subplots(1, len(feature_specs), figsize=(15, 4.2))
    for ax, (column, title, color) in zip(axes, feature_specs):
        series = train_fe[column].dropna()
        ax.hist(series, bins=30, color=color, alpha=0.85, edgecolor="white")
        ax.set_title(title)
        ax.set_xlabel("Kilometers")
        ax.set_ylabel("Listings")
        if not series.empty:
            ax.axvline(series.median(), color="black", linestyle="--", linewidth=1)
            ax.text(
                0.98,
                0.95,
                f"median={series.median():.2f}",
                transform=ax.transAxes,
                ha="right",
                va="top",
                fontsize=9,
                bbox={"facecolor": "white", "alpha": 0.8, "edgecolor": "none"},
            )

    fig.suptitle("Core location-feature distributions", fontsize=13)
    fig.tight_layout()
    return fig


def plot_transport_kind_counts(transport_poi, top_n=8):
    import matplotlib.pyplot as plt

    counts = (
        transport_poi["poi_kind"]
        .fillna("(missing)")
        .value_counts(dropna=False)
        .head(top_n)
        .sort_values(ascending=True)
    )

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.barh(counts.index.astype(str), counts.values, color="#4c78a8")
    ax.set_title(f"Top {len(counts)} public transport POI kinds")
    ax.set_xlabel("POI count")
    ax.set_ylabel("")
    fig.tight_layout()
    return fig


def plot_variant_metric(results_df, metric="MedAPE"):
    import matplotlib.pyplot as plt

    plot_df = (
        results_df[["feature_variant", "model", metric]]
        .pivot(index="feature_variant", columns="model", values=metric)
        .sort_index()
    )

    fig, ax = plt.subplots(figsize=(10, 4.8))
    plot_df.plot(kind="bar", ax=ax, width=0.82, color=["#457b9d", "#e76f51", "#6c757d"][: len(plot_df.columns)])
    ax.set_title(f"{metric} by feature variant")
    ax.set_xlabel("Feature variant")
    ax.set_ylabel(metric)
    ax.tick_params(axis="x", rotation=25)
    ax.legend(title="")
    fig.tight_layout()
    return fig


def plot_variant_uplift(variant_cv_uplift, metric="delta_MedAPE_vs_baseline"):
    import matplotlib.pyplot as plt

    plot_df = variant_cv_uplift.loc[
        variant_cv_uplift["feature_variant"] != "A_baseline_only",
        ["feature_variant", "model", metric],
    ].copy()
    plot_df["label"] = plot_df["model"] + " | " + plot_df["feature_variant"]
    plot_df = plot_df.sort_values(metric, ascending=False)

    fig, ax = plt.subplots(figsize=(10, 5.2))
    colors = ["#d62828" if value > 0 else "#2a9d8f" for value in plot_df[metric]]
    ax.barh(plot_df["label"], plot_df[metric], color=colors)
    ax.axvline(0, color="black", linewidth=1)
    ax.set_title(f"{metric} vs baseline")
    ax.set_xlabel(metric)
    ax.set_ylabel("")
    fig.tight_layout()
    return fig


def plot_holdout_metric(final_test_table, metric="MedAPE"):
    import matplotlib.pyplot as plt

    plot_df = final_test_table.sort_values(metric, ascending=True)
    labels = plot_df["model"] + "\n" + plot_df["feature_variant"]

    fig, ax = plt.subplots(figsize=(8.5, 4.8))
    ax.barh(labels, plot_df[metric], color=["#6c757d", "#457b9d", "#e76f51"][: len(plot_df)])
    ax.set_title(f"Holdout {metric} for locked variants")
    ax.set_xlabel(metric)
    ax.set_ylabel("")
    fig.tight_layout()
    return fig


def plot_robustness_overview(robustness_table):
    import matplotlib.pyplot as plt

    if robustness_table.empty:
        fig, ax = plt.subplots(figsize=(6, 2.5))
        ax.text(0.5, 0.5, "No robustness data available.", ha="center", va="center")
        ax.axis("off")
        return fig

    plot_df = robustness_table.set_index("feature_variant")[
        ["mean_missing_fraction", "max_missing_fraction"]
    ]

    fig, ax = plt.subplots(figsize=(9, 4.5))
    plot_df.plot(kind="bar", ax=ax, width=0.8, color=["#a8dadc", "#1d3557"])
    ax.set_title("Missingness in engineered feature variants")
    ax.set_xlabel("Feature variant")
    ax.set_ylabel("Fraction missing")
    ax.tick_params(axis="x", rotation=25)
    ax.legend(title="")
    fig.tight_layout()
    return fig


def plot_feature_importance(feature_importance, top_n=20):
    import matplotlib.pyplot as plt

    plot_df = feature_importance.head(top_n).iloc[::-1]

    fig, ax = plt.subplots(figsize=(9, max(5, top_n * 0.28)))
    ax.barh(plot_df["feature"], plot_df["importance"], color="#f4a261")
    ax.set_title(f"Top {len(plot_df)} XGBoost feature importances")
    ax.set_xlabel("Importance")
    ax.set_ylabel("")
    fig.tight_layout()
    return fig


def plot_subgroup_metric(summary_df, metric="MedAPE", top_n=12):
    import matplotlib.pyplot as plt

    plot_df = summary_df.sort_values(metric, ascending=True).head(top_n).iloc[::-1]

    fig, ax = plt.subplots(figsize=(8.5, max(4, len(plot_df) * 0.35)))
    ax.barh(plot_df["group"].astype(str), plot_df[metric], color="#8ecae6")
    ax.set_title(f"Best subgroup {metric} values")
    ax.set_xlabel(metric)
    ax.set_ylabel("")
    fig.tight_layout()
    return fig


def save_output_tables(
    reference_table,
    variant_cv_results,
    variant_cv_uplift,
    best_variant_per_model,
    final_test_table,
    robustness_table,
    output_dir=Path("."),
):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    outputs = {
        "reference_baseline_metrics.csv": reference_table,
        "location_feature_variant_cv_results.csv": variant_cv_results,
        "location_feature_variant_cv_uplift.csv": variant_cv_uplift,
        "locked_best_variant_per_model.csv": best_variant_per_model,
        "final_test_comparison_locked_variants.csv": final_test_table,
        "feature_engineering_robustness_checks.csv": robustness_table,
    }

    for filename, frame in outputs.items():
        frame.to_csv(output_dir / filename, index=False)

    return outputs
