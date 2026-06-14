import re

import pandas as pd
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"


def _extract_rooms(cat: str) -> float:
    """Estimate number of rooms from category_sub."""
    mapping = {
        "1": 1.0, "1+kk": 1.0, "1+1": 2.0,
        "2": 2.0, "2+kk": 2.0, "2+1": 3.0,
        "3": 3.0, "3+kk": 3.0, "3+1": 4.0,
        "4": 4.0, "4+kk": 4.0, "4+1": 5.0,
        "5+": 5.5, "5+kk": 5.5, "5+1": 6.0,
        "6-a-vice": 6.0, "atypicky": 4.0,
    }
    return mapping.get(cat, float("nan"))


def _extract_neighborhood(street: str) -> str:
    if not isinstance(street, str) or not street.strip():
        return "unknown"
    parts = street.split(",")
    if len(parts) >= 2:
        hood = parts[1].strip()
        for sep in (" -", " –", "—"):
            if sep in hood:
                hood = hood.split(sep)[0].strip()
        return hood if hood else "unknown"
    return parts[0].strip()


def remove_price_outliers(df, column='price_total'):
    q1 = df[column].quantile(0.25)
    q3 = df[column].quantile(0.75)
    iqr = q3 - q1
    lower_bound = 0.009
    upper_bound = q3 + 1.5 * iqr
    return df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]


def process_df(df_original: pd.DataFrame) -> pd.DataFrame:
    df = df_original.copy()

    # 1. Drop raw/metadata columns
    cols_to_drop = [
        'currency', 'is_topped', 'updated', 'created_at',
        'energy_efficiency_rating', 'id', 'web_link',
        'meta_description', 'description', 'title', 'street',
    ]
    df = df.drop(columns=cols_to_drop, errors='ignore')

    # 4. Area cleaning
    df.loc[df['has_loggia'] == 0, 'loggia_area_m2'] = 0
    df.loc[df['has_cellar'] == 0, 'cellar_area_m2'] = 0

    if 'total_area_m2' in df.columns:
        df['total_area_m2_was_missing'] = df['total_area_m2'].isna().astype(int)
    else:
        df['total_area_m2_was_missing'] = 0

    if 'total_area_m2' in df.columns and 'usable_area_m2' in df.columns:
        df['total_area_m2'] = df['total_area_m2'].fillna(df['usable_area_m2'])
        df.loc[df['total_area_m2'] > 600, 'total_area_m2'] = df['usable_area_m2'] / 100
        excessive_area_mask = df['total_area_m2'] > (1.5 * df['usable_area_m2'])
        df.loc[excessive_area_mask, 'total_area_m2'] = 1.5 * df.loc[excessive_area_mask, 'usable_area_m2']

    # 5. Category mapping
    if 'category_sub' in df.columns:
        category_mapping = {
            '1+kk': '1', '1+1': '1+1', '2+kk': '2', '2+1': '2+1',
            '3+kk': '3', '3+1': '3+1', '4+kk': '4', '4+1': '4+1',
            '5+kk': '5+', '6-a-vice': '5+', 'atypicky': 'atypicky', '5+1': '5+'
        }
        df['category_sub'] = df['category_sub'].replace(category_mapping)

    # 6. Cast locality_region_id to string
    if 'locality_region_id' in df.columns:
        df['locality_region_id'] = df['locality_region_id'].astype('Int64').astype('string')

    # 7. Derived numeric features
    if "floor_number" in df.columns and "total_floors" in df.columns:
        ratio = df["floor_number"] / df["total_floors"]
        df["floor_ratio"] = ratio.clip(lower=0.0, upper=1.0)
        df["is_top_floor"] = (df["floor_number"] == df["total_floors"]).astype(int)
    else:
        df["floor_ratio"] = float("nan")
        df["is_top_floor"] = 0

    df["has_construction_year"] = df["construction_year"].notna().astype(int)

    if "construction_year" in df.columns:
        df["building_age"] = (2025 - df["construction_year"]).fillna(-1).clip(lower=-1)
    else:
        df["building_age"] = -1

    if "usable_area_m2" in df.columns and "category_sub" in df.columns:
        rooms = df["category_sub"].apply(_extract_rooms)
        df["area_per_room"] = (df["usable_area_m2"] / rooms).where(rooms > 0, float("nan"))
    else:
        df["area_per_room"] = float("nan")

    # 8. Interaction and location features
    if "locality_region_id" in df.columns and "category_sub" in df.columns:
        df["region_category"] = df["locality_region_id"].astype(str) + "_" + df["category_sub"].astype(str)

    if "locality_region_id" in df.columns and "building_condition" in df.columns:
        df["region_condition"] = df["locality_region_id"].astype(str) + "_" + df["building_condition"].astype(str)

    # 9. Type casting
    categorical_cols = [
        'category_sub', 'locality_region_id', 'is_furnished',
        'location_type', 'energy_class', 'ownership_type',
        'construction_type', 'building_condition',
    ]
    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].astype('category')

    return df


def get_pipeline_config() -> dict:
    return {
        "num_features": [
            "latitude", "longitude", "floor_ratio",
            "area_per_room", "building_age",
        ],
        "log_num_features": [
            "usable_area_m2", "total_area_m2", "loggia_area_m2", "cellar_area_m2",
        ],
        "bool_features": [
            "has_elevator", "has_terrace", "has_garage",
            "has_cellar", "has_loggia",
            "total_area_m2_was_missing", "has_construction_year",
            "is_top_floor",
        ],
        "cat_features": [
            "category_sub", "construction_type", "ownership_type",
            "is_furnished", "location_type",
        ],
        "structural_features": [
            "floor_number", "total_floors",
        ],
        "target_encoded_features": [
            "district_id", "construction_year",
            "region_category", "region_condition",
        ],
        "ordinal_features": [
            "energy_class", "building_condition", "locality_region_id",
        ],
        "ordinal_categories": [
            ["A", "B", "C", "D", "E", "F", "G", "missing"],
            [
                "Špatný", "Před rekonstrukcí", "Dobrý", "Velmi dobrý",
                "Po rekonstrukci", "V rekonstrukci", "Novostavba",
                "Ve výstavbě", "Projekt", "missing"
            ],
            [
                "10", "14", "11", "6", "1", "2", "5", "8",
                "9", "7", "3", "12", "4", "13"
            ],
        ]
    }


if __name__ == "__main__":
    df = pd.read_csv(DATA_DIR / 'apartments_raw_data.csv')
    df = process_df(df)
    df.to_csv(DATA_DIR / 'real_estate_processed.csv', index=False)
