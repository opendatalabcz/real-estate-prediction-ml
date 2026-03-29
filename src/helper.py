import time
import random
import json
import logging
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Dict, Any
from tqdm import tqdm
import os
import traceback
import re
import numpy as np
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder, StandardScaler, OrdinalEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error, mean_absolute_percentage_error
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sklearn.model_selection import GridSearchCV

from src.process import process_df


def remove_outliers(df: pd.DataFrame, th: int = 0.995):
    df_clean = df.copy()

    # 1. Create temporary Price per m2 feature
    # We use a small epsilon (1e-6) to avoid division by zero errors
    df_clean['price_per_m2'] = df_clean['price_total'] / (df_clean['usable_area_m2'] + 1e-6)

    thresholds = {
        'price_per_m2': th,
        # 'usable_area_m2': th  # Keep area to catch typos like 10,000m2 for an apartment
    }

    is_outlier = pd.Series(False, index=df_clean.index)

    for col, q in thresholds.items():
        if col in df_clean.columns:
            upper_limit = df_clean[col].quantile(q)
            lower_limit = df_clean[col].quantile(0.00001)

            is_outlier |= (df_clean[col] < lower_limit) | (df_clean[col] > upper_limit)

    # 2. Extract outliers and clean dataframe
    outliers = df_clean[is_outlier].copy()
    df_final = df_clean[~is_outlier].copy()

    # Drop the temporary helper column so it doesn't mess up your model training
    df_final = df_final.drop(columns=['price_per_m2'])

    print(f"Removed {len(outliers)} rows as outliers based on valuation and size.")
    return df_final, outliers


def plot_regression_enhanced(y_true_log, y_pred_log):

    y_true = np.expm1(y_true_log)
    y_pred = np.expm1(y_pred_log)

    r2 = r2_score(y_true, y_pred)
    mae = mean_absolute_error(y_true, y_pred)

    plt.figure(figsize=(10, 6))
    sns.scatterplot(x=y_true, y=y_pred, alpha=0.5, color='teal')

    max_val = max(y_true.max(), y_pred.max())
    plt.plot([0, max_val], [0, max_val], color='red', lw=2, linestyle='--')

    stats_text = f'R² Score: {r2:.3f}\nMAE: ${mae:,.0f}'
    plt.gca().text(0.05, 0.95, stats_text, transform=plt.gca().transAxes,
                   fontsize=12, verticalalignment='top', bbox=dict(boxstyle='round', facecolor='white', alpha=0.5))

    plt.title('Real vs Predicted (with Metrics)')
    plt.xlabel('Actual Price')
    plt.ylabel('Predicted Price')
    plt.show()


def plot_regression_enhanced_t(y_true_log, y_pred_log, train_log_mse):
    """
    Plots real vs predicted prices with Jensen's Inequality correction.

    Args:
        y_true_log: Actual log prices (Test/Val)
        y_pred_log: Predicted log prices (Test/Val)
        train_log_mse: The variance of residuals calculated from the TRAINING set
    """
    y_true = np.expm1(y_true_log)
    y_pred = np.expm1(y_pred_log)

    corrected_preds = np.expm1(y_pred_log + (train_log_mse / 2))

    r2_naive = r2_score(y_true, y_pred)
    mae_naive = mean_absolute_error(y_true, y_pred)
    r2_adj = r2_score(y_true, corrected_preds)
    mae_adj = mean_absolute_error(y_true, corrected_preds)

    plt.figure(figsize=(10, 6))

    sns.scatterplot(x=y_true, y=corrected_preds, alpha=0.5, color='teal')

    max_val = max(y_true.max(), corrected_preds.max())
    plt.plot([0, max_val], [0, max_val], color='red', lw=2, linestyle='--')

    # Stats box
    stats_text = (
        f'--- NAIVE ---\n'
        f'R²: {r2_naive:.5f} | MAE: ${mae_naive:,.0f}\n\n'
        f'--- CORRECTED ---\n'
        f'R²: {r2_adj:.5f} | MAE: ${mae_adj:,.0f}'
    )

    plt.gca().text(0.05, 0.95, stats_text, transform=plt.gca().transAxes,
                   fontsize=11, verticalalignment='top',
                   bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

    plt.title('Real vs Predicted (Variance Corrected)')
    plt.xlabel('Actual Price')
    plt.ylabel('Predicted Price')
    plt.show()


category_type_to_url = {
    0: "vse", 1: "prodej", 2: "pronajem", 3: "drazby"
}

category_main_to_url = {
    0: "vse", 1: "byt", 2: "dum", 3: "pozemek", 4: "komercni", 5: "ostatni"
}

category_locality_Region_id = {
    1: "Jihočeský",
    14: "Jihomoravský",
    3: "Karlovarský",
    6: "Královéhradecký",
    5: "Liberecký",
    12: "Moravskoslezský",
    8: "Olomoucký",
    7: "Pardubický",
    2: "Plzeňský",
    10: "Praha",
    11: "Středočeský",
    4: "Ústecký",
    13: "Vysočina",
    9: "Zlínský"
}

category_sub_to_url = {
    2: "1+kk", 3: "1+1", 4: "2+kk", 5: "2+1", 6: "3+kk", 7: "3+1", 8: "4+kk",
    9: "4+1", 10: "5+kk", 11: "5+1", 12: "6-a-vice", 16: "atypicky", 47: "pokoj",
    37: "rodinny", 39: "vila", 43: "chalupa", 33: "chata", 35: "pamatka",
    40: "na-klic", 44: "zemedelska-usedlost", 19: "bydleni", 18: "komercni",
    20: "pole", 22: "louka", 21: "les", 46: "rybnik", 48: "sady-vinice",
    23: "zahrada", 24: "ostatni-pozemky", 25: "kancelare", 26: "sklad",
    27: "vyrobni-prostor", 28: "obchodni-prostor", 29: "ubytovani",
    30: "restaurace", 31: "zemedelsky", 38: "cinzovni-dum", 49: "virtualni-kancelar",
    32: "ostatni-komercni-prostory", 34: "garaz", 52: "garazove-stani",
    50: "vinny-sklep", 51: "pudni-prostor", 53: "mobilni-domek", 36: "jine-nemovitosti"
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
