import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin
from typing import List
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import (
    OneHotEncoder,
    OrdinalEncoder,
    StandardScaler,
    FunctionTransformer
)
from sklearn.impute import SimpleImputer


def fill_total_area(X):
    """Custom function to fill total_area with usable_area if missing."""
    X = X.copy()
    if 'total_area_m2' in X.columns and 'usable_area_m2' in X.columns:
        X['total_area_m2'] = X['total_area_m2'].fillna(X['usable_area_m2'])
    return X


def identity_feature_names(transformer, input_features):
    return input_features


class SmoothedTargetEncoder(BaseEstimator, TransformerMixin):
    def __init__(self, smoothing=20):
        self.smoothing = smoothing
        self.mapping_ = None
        self.global_mean_ = None

    def fit(self, X, y):
        X = pd.DataFrame(X, columns=["district_id"])
        y = pd.Series(y)

        self.global_mean_ = y.mean()

        stats = (
            pd.DataFrame({"district_id": X["district_id"], "price_total": y})
            .groupby("district_id", observed=True)["price_total"]
            .agg(["mean", "count"])
        )

        smooth = (
            (stats["count"] * stats["mean"] +
             self.smoothing * self.global_mean_)
            / (stats["count"] + self.smoothing)
        )

        self.mapping_ = smooth
        return self

    def transform(self, X):
        X = pd.DataFrame(X, columns=["district_id"])
        encoded = X["district_id"].map(self.mapping_)
        encoded = encoded.fillna(self.global_mean_)
        return encoded.to_frame()


def get_ordinal_pipeline(
    num_features: List[str],
    bool_features: List[str],
    model_type: str = "tree"
) -> Pipeline:
    """
    Returns preprocessing pipeline with:
    - Custom total_area imputation
    - Ordinal encoding for energy_class
    - One-hot for nominal categoricals
    - Proper scaling depending on model type
    """

    if model_type not in {"tree", "linear"}:
        raise ValueError("model_type must be 'tree' or 'linear'")

    area_imputer = FunctionTransformer(
        fill_total_area,
        validate=False,
        feature_names_out=identity_feature_names  # <--- ADD THIS
    )

    num_steps = [
        ("imputer", SimpleImputer(strategy="median"))
    ]

    if model_type == "linear":
        num_steps.append(("scaler", StandardScaler()))

    num_transformer = Pipeline(steps=num_steps)

    energy_categories = ["G", "F", "E", "D", "C", "B", "A", "missing"]

    energy_steps = [
        ("imputer", SimpleImputer(strategy="constant", fill_value="missing")),
        (
            "ordinal",
            OrdinalEncoder(
                categories=[energy_categories],
                handle_unknown="use_encoded_value",
                unknown_value=-1
            ),
        ),
    ]

    if model_type == "linear":
        energy_steps.append(("scaler", StandardScaler()))

    energy_transformer = Pipeline(steps=energy_steps)

    other_cat_features = [
        "category_sub",
        "locality_region_id",
        "construction_type",
        "building_condition",
        "ownership_type",
        "location_type",
        "is_furnished",
    ]
    district_transformer = Pipeline([
        ("target_mean", SmoothedTargetEncoder(smoothing=5))
    ])

    cat_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            (
                "onehot",
                OneHotEncoder(
                    handle_unknown="ignore",
                    sparse_output=(model_type == "tree")
                ),
            ),
        ]
    )

    bool_transformer = Pipeline(
        steps=[
            ("to_int", FunctionTransformer(lambda x: x.astype(int))),
            ("imputer", SimpleImputer(strategy="most_frequent")),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", num_transformer, num_features),
            ("energy", energy_transformer, ["energy_class"]),
            ("cat", cat_transformer, other_cat_features),
            ("bool", bool_transformer, bool_features),
            ("district", district_transformer, ["district_id"])
        ],
        remainder="drop",
        verbose_feature_names_out=False
    )

    return Pipeline(
        steps=[
            ("area_fill", area_imputer),
            ("preprocessing", preprocessor),
        ]
    )


class Model_pipeline:
    def __init__(self, num_features: List[str], bool_features: List[str], model_type: str = "tree", model=None):
        self.pipeline = Pipeline(
            steps=[
                ("preprocessing", get_ordinal_pipeline(num_features, bool_features, model_type)),
                ("model", model)
            ]
        )

    def fit(self, X, y):
        self.pipeline.fit(X, y)
        return self

    def predict(self, X):
        return self.pipeline.predict(X)

    def get_score(self, X, y, scoring, scale_back=False):
        y_pred = self.predict(X)
        if scale_back:
            y = np.expm1(y.copy())
            y_pred = np.expm1(y_pred.copy())

        return scoring(y, y_pred)
