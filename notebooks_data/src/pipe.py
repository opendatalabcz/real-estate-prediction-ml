from sklearn.feature_selection import SelectFromModel
import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from typing import List, Optional
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import (
    OneHotEncoder,
    OrdinalEncoder,
    StandardScaler,
    FunctionTransformer
)
from sklearn.impute import SimpleImputer
from sklearn.base import BaseEstimator, RegressorMixin
import unicodedata
from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
from sklearn.impute import KNNImputer


class PandasCategoryCaster(BaseEstimator, TransformerMixin):
    """
    Sanitizes strings to ASCII and casts columns to pandas 'category' dtype.
    This prevents XGBoost UnicodeDecodeErrors on prediction.
    """

    def __init__(self, fill_missing: str | None = None):
        self.fill_missing = fill_missing

    def fit(self, X, y=None):
        self._is_fitted = True
        return self

    def __sklearn_is_fitted__(self):
        return getattr(self, "_is_fitted", False)

    def _sanitize_string(self, text):
        if not isinstance(text, str):
            return text

        text = unicodedata.normalize('NFKD', text)

        text = text.encode('ascii', 'ignore').decode('utf-8')

        return text.strip().lower().replace(" ", "_").replace("-", "_")

    def transform(self, X):
        X_cat = X.copy()
        for col in X_cat.columns:
            X_cat[col] = X_cat[col].astype(object)
            # First, sanitize the strings
            X_cat[col] = X_cat[col].apply(self._sanitize_string)
            if self.fill_missing is not None:
                X_cat[col] = X_cat[col].fillna(self.fill_missing)
            # Then cast to category
            X_cat[col] = X_cat[col].astype('category')
        return X_cat

    def get_feature_names_out(self, input_features=None):
        return input_features


class PandasStringCaster(BaseEstimator, TransformerMixin):
    """Casts selected columns to pandas string dtype and preserves missing values."""

    def fit(self, X, y=None):
        self._is_fitted = True
        return self

    def __sklearn_is_fitted__(self):
        return getattr(self, "_is_fitted", False)

    def transform(self, X):
        X_str = X.copy()
        for col in X_str.columns:
            X_str[col] = X_str[col].astype("string")
            X_str[col] = X_str[col].replace({pd.NA: np.nan})
        return X_str

    def get_feature_names_out(self, input_features=None):
        return input_features


class PandasObjectCaster(BaseEstimator, TransformerMixin):
    """Converts pandas extension dtypes to plain object dtype with np.nan missing values."""

    def fit(self, X, y=None):
        self._is_fitted = True
        return self

    def __sklearn_is_fitted__(self):
        return getattr(self, "_is_fitted", False)

    def transform(self, X):
        X_obj = X.copy()
        for col in X_obj.columns:
            X_obj[col] = X_obj[col].astype(object)
            X_obj[col] = X_obj[col].where(pd.notna(X_obj[col]), np.nan)
        return X_obj

    def get_feature_names_out(self, input_features=None):
        return input_features


class SmoothedTargetEncoder(BaseEstimator, TransformerMixin):
    """Generically target-encodes a single categorical feature."""

    def __init__(self, smoothing=20):
        self.smoothing = smoothing
        self.mapping_ = None
        self.global_mean_ = None

    def fit(self, X, y):
        if isinstance(X, pd.DataFrame):
            X_series = X.iloc[:, 0]
        else:
            X_series = pd.Series(X.ravel())
        y = pd.Series(y)

        self.global_mean_ = y.mean()

        stats = (
            pd.DataFrame({"cat_feature": X_series, "target": y})
            .groupby("cat_feature", observed=True)["target"]
            .agg(["mean", "count"])
        )

        smooth = (
            (stats["count"] * stats["mean"] + self.smoothing * self.global_mean_)
            / (stats["count"] + self.smoothing)
        )

        self.mapping_ = smooth
        return self

    def transform(self, X):
        if isinstance(X, pd.DataFrame):
            X_series = X.iloc[:, 0]
        else:
            X_series = pd.Series(X.ravel())

        encoded = X_series.map(self.mapping_)
        encoded = encoded.astype(float)
        encoded = encoded.fillna(self.global_mean_)
        return encoded.to_frame()

    def get_feature_names_out(self, input_features=None):
        """Passes the input feature names straight through."""
        return input_features


def cast_to_float(X):
    return X.astype(float)


def cast_to_int(X):
    return X.astype(int)


def get_pipeline(
    num_features: List[str],
    log_num_features: List[str],
    cat_features: List[str],
    structural_features: List[str],
    bool_features: List[str],
    ordinal_features: List[str] = None,
    ordinal_categories: List[List[str]] = None,
    target_encoded_features: List[str] = None,
    model_type: str = "tree"


) -> Pipeline:
    """Returns a generalized preprocessing pipeline."""

    if model_type not in {"tree", "linear", "tree_modern"}:
        raise ValueError("model_type must be 'tree' or 'linear'")

    transformers = []

    if num_features:
        num_steps = []
        num_steps.append(("imputer", SimpleImputer(strategy="median")))
        if model_type == "linear":
            num_steps.append(("scaler", StandardScaler()))
        num_transformer = Pipeline(num_steps) if num_steps else "passthrough"
        transformers.append(("num", num_transformer, num_features))

    if log_num_features:
        log_num_steps = []
        log_num_steps.append(("imputer", SimpleImputer(strategy="median")))
        if model_type == "linear":
            log_num_steps.extend([
                ("log1p", FunctionTransformer(np.log1p, feature_names_out="one-to-one")),
                ("scaler", StandardScaler()),
            ])
        log_num_transformer = Pipeline(log_num_steps) if log_num_steps else "passthrough"
        transformers.append(("log_num", log_num_transformer, log_num_features))

    if cat_features:
        cat_steps = []

        if model_type == "linear":
            cat_steps.append(("to_object", PandasObjectCaster()))
            cat_steps.append(("imputer", SimpleImputer(strategy="most_frequent")))
            cat_steps.append(("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)))
        elif model_type == "tree":
            cat_steps.append(("to_object", PandasObjectCaster()))
            cat_steps.append(("imputer", SimpleImputer(strategy="most_frequent")))
            cat_steps.append(("ordinal", OrdinalEncoder(
                categories="auto",
                handle_unknown="use_encoded_value",
                unknown_value=-1
            )))
        else:
            cat_steps.append(("caster", PandasCategoryCaster(fill_missing="missing")))

        transformers.append(("cat", Pipeline(cat_steps), cat_features))

    if bool_features:
        if model_type == "tree_modern":
            bool_steps = [
                ("to_float", FunctionTransformer(cast_to_float, feature_names_out="one-to-one")),
                ("imputer", SimpleImputer(strategy="constant", fill_value=0)),
            ]
        else:
            bool_steps = [
                ("to_int", FunctionTransformer(cast_to_int, feature_names_out="one-to-one")),
                ("imputer", SimpleImputer(strategy="constant", fill_value=0)),
            ]
        transformers.append(("bool", Pipeline(bool_steps), bool_features))

    if ordinal_features and ordinal_categories:
        ordinal_cast_step = ("to_string", PandasStringCaster())
        if model_type == "tree_modern":
            ordinal_steps = [
                ("to_object", PandasObjectCaster()),
                ("imputer", SimpleImputer(strategy="constant", fill_value="missing")),
                ("ordinal", OrdinalEncoder(
                    categories=ordinal_categories,
                    handle_unknown="use_encoded_value",
                    unknown_value=-1,
                )),
            ]
        else:
            ordinal_steps = [
                ("to_object", PandasObjectCaster()),
                ("imputer", SimpleImputer(strategy="constant", fill_value="missing")),
                ("ordinal", OrdinalEncoder(categories=ordinal_categories, handle_unknown="use_encoded_value", unknown_value=-1)),
            ]
        if model_type == "linear":
            ordinal_steps.append(("scaler", StandardScaler()))
        transformers.append(("ordinal", Pipeline(ordinal_steps), ordinal_features))
    if structural_features:
        structural_steps = []
        structural_steps.append(("imputer", KNNImputer(n_neighbors=5)))
        if model_type == "linear":
            structural_steps.append(("scaler", StandardScaler()))
        structural_transformer = Pipeline(structural_steps) if structural_steps else "passthrough"
        transformers.append(("structural", structural_transformer, structural_features))
    if target_encoded_features:
        for feature in target_encoded_features:
            te_pipe = Pipeline([("target_mean", SmoothedTargetEncoder(smoothing=5))])
            transformers.append((f"te_{feature}", te_pipe, [feature]))

    preprocessor = ColumnTransformer(
        transformers=transformers,
        remainder="drop",
        verbose_feature_names_out=True
    ).set_output(transform="pandas")

    return Pipeline(steps=[("preprocessing", preprocessor)])


class Model_pipeline(BaseEstimator, RegressorMixin):
    """
    A scikit-learn compatible wrapper that binds a dynamic preprocessor
    to a specific model estimator.
    """

    def __init__(self, config: dict, model_type: str = "tree", model=None, use_selection=False):
        self.config = config
        self.model_type = model_type
        self.model = model
        self.use_selection = use_selection
        self._build_pipeline()

    def _build_pipeline(self):
        """Constructs a flattened pipeline from the config and model."""
        preprocessor_pipe = get_pipeline(
            num_features=self.config.get("num_features", []),
            log_num_features=self.config.get("log_num_features", []),
            cat_features=self.config.get("cat_features", []),
            structural_features=self.config.get("structural_features", []),
            bool_features=self.config.get("bool_features", []),
            ordinal_features=self.config.get("ordinal_features", []),
            ordinal_categories=self.config.get("ordinal_categories", []),
            target_encoded_features=self.config.get("target_encoded_features", []),
            model_type=self.model_type
        )
        preprocessor_step = preprocessor_pipe.steps[0]
        steps = [
            preprocessor_step,
        ]
        if self.use_selection:
            selector = SelectFromModel(estimator=self.model, threshold="median")
            steps.append(("selection", selector))
        steps.append(("model", self.model))
        self.pipeline_ = Pipeline(steps=steps)

    def fit(self, X, y, **kwargs):
        """Fits the underlying preprocessing steps and the model."""
        self.pipeline_.fit(X, y, **kwargs)
        return self

    def predict(self, X):
        """Transforms the data and returns model predictions."""
        return self.pipeline_.predict(X)

    def get_score(self, X, y, scoring_func, scale_back=False, clip_max=None):
        """
        Calculates the score using the provided scoring_func.

        Parameters:
        - scoring_func: A callable like root_mean_squared_error
        - scale_back: If True, reverses log1p transformation via expm1
        - clip_max: Optional upper bound to prevent overflow during expm1
        """
        y_pred = self.predict(X)

        if scale_back:
            y_true_raw = np.expm1(y.copy())
            if clip_max is not None:
                y_pred = np.clip(y_pred, a_min=0, a_max=clip_max)
            else:
                y_pred = np.clip(y_pred, a_min=0, a_max=700)
            y_pred_raw = np.expm1(y_pred)
            return scoring_func(y_true_raw, y_pred_raw)

        return scoring_func(y, y_pred)
