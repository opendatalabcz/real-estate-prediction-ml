import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb

from src.process import process_df, get_pipeline_config
from src.pipe import Model_pipeline
from src.poi_helper import PoiHelper
from config import MODEL_PATH, CLEANING_PARAMS_PATH

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class PredictLoader:
    def __init__(self):
        self.model = None
        self.cleaning_params = None
        self.preprocessor = None
        self.booster = None
        self.feat_names = None
        self.poi = None
        self._load()

    def _load(self):
        self.model = joblib.load(MODEL_PATH)
        self.cleaning_params = joblib.load(CLEANING_PARAMS_PATH)

        r = self.model.regressor_
        xgb_model = r.pipeline_.named_steps["model"]

        xgb_model.save_model("/tmp/_price_predictor_xgb.json")
        self.booster = xgb.Booster(model_file="/tmp/_price_predictor_xgb.json")

        self.preprocessor = r.pipeline_.named_steps["preprocessing"]
        self.feat_names = list(self.preprocessor.get_feature_names_out())

        self.poi = PoiHelper(
            transport_path=PROJECT_ROOT / "data" / "public_transport.csv",
            grocery_path=PROJECT_ROOT / "data" / "brand_stores.csv",
            city_centers_path=PROJECT_ROOT / "data" / "city_centers.csv",
        )

        # 95th percentile of |log(actual) - log(pred)| from CV on training data
        self.conformal_q = 0.1257
        self._init_calibration()

    def _apply_removal_policy(self, df):
        cleaned = df.copy()
        policy = self.cleaning_params.get("policy")
        if policy == "light_trim":
            for col, (low, high) in self.cleaning_params.get("clip_bounds", {}).items():
                if col in cleaned.columns:
                    cleaned[col] = cleaned[col].clip(lower=low, upper=high)
        return cleaned

    def _init_calibration(self):
        self.calib_bands = np.array([
            1_000_000, 3_000_000, 5_000_000, 7_000_000, 9_000_000,
            11_000_000, 13_500_000, 17_500_000, 22_500_000, 35_000_000,
        ])
        self.calib_factors = np.array([
            0.6557, 0.9132, 0.9756, 1.0111, 1.0384,
            1.0194, 1.0672, 1.0183, 1.0267, 1.1429,
        ])

    def _calibrate_price(self, raw_price: float) -> float:
        factor = float(np.interp(raw_price, self.calib_bands, self.calib_factors,
                                 left=self.calib_factors[0], right=self.calib_factors[-1]))
        return raw_price * factor

    def predict(self, input_dict: dict) -> dict:
        df = pd.DataFrame([input_dict])

        df["__row_id"] = 0
        df["total_area_m2_was_missing"] = 0

        df = self._apply_removal_policy(df)

        # Add POI features
        lat = float(input_dict["latitude"])
        lng = float(input_dict["longitude"])
        region_id = int(input_dict["locality_region_id"])
        poi_features = self.poi.compute(lat, lng, region_id)
        for k, v in poi_features.items():
            df[k] = v

        processed = process_df(df.copy())

        X = processed.drop(columns=["price_total", "__row_id"], errors="ignore")

        price = float(self.model.predict(X)[0])

        area = float(input_dict.get("usable_area_m2", 1))

        Xt = self.preprocessor.transform(X)
        dmat = xgb.DMatrix(Xt, enable_categorical=True)
        contribs = self.booster.predict(dmat, pred_contribs=True)

        shap_values = []
        for name, val in zip(self.feat_names, contribs[0][:-1]):
            short_name = name.split("__", 1)[1] if "__" in name else name
            shap_values.append({
                "feature": short_name,
                "full_name": name,
                "value_log": float(val),
            })

        shap_values.sort(key=lambda x: -abs(x["value_log"]))

        total_shap_log = sum(sv["value_log"] for sv in shap_values)
        bias_log = float(np.log(price)) - total_shap_log
        base_value_czk = float(np.exp(bias_log))

        for sv in shap_values:
            multiplier = float(np.exp(sv["value_log"]))
            pct_impact = (multiplier - 1) * 100
            sv["value_log"] = round(sv["value_log"], 4)
            sv["multiplier"] = round(multiplier, 4)
            sv["pct_impact"] = round(pct_impact, 2)

        calibrated_price = self._calibrate_price(price)
        price_per_m2 = round(calibrated_price / area, 0) if area > 0 else 0

        log_price = np.log(max(calibrated_price, 1))
        confidence_lower = float(np.exp(log_price - self.conformal_q))
        confidence_upper = float(np.exp(log_price + self.conformal_q))

        return {
            "predicted_price_czk": round(calibrated_price, 0),
            "price_per_m2": price_per_m2,
            "confidence_lower_czk": round(confidence_lower, 0),
            "confidence_upper_czk": round(confidence_upper, 0),
            "conformal_q": round(self.conformal_q, 4),
            "base_value_czk": round(base_value_czk, 0),
            "shap_values": shap_values,
        }
