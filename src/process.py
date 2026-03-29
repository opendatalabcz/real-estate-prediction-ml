import pandas as pd


def remove_price_outliers(df, column='price_total'):
    q1 = df[column].quantile(0.25)
    q3 = df[column].quantile(0.75)
    iqr = q3 - q1
    lower_bound = 0.009
    upper_bound = q3 + 1.5 * iqr

    return df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]


def process_df(df_original: pd.DataFrame) -> pd.DataFrame:
    cols_to_drop = [
        'construction_year', 'currency', 'is_topped', 'updated',
        'created_at', 'energy_efficiency_rating', 'municipality_id',
        'id', 'web_link', 'meta_description', 'description', 'title', 'street'
    ]
    df = df_original.drop(columns=cols_to_drop, errors='ignore')
    df.loc[df['has_loggia'] == 0, 'loggia_area_m2'] = 0
    df.loc[df['has_cellar'] == 0, 'cellar_area_m2'] = 0

    if 'total_area_m2' in df.columns and 'usable_area_m2' in df.columns:
        df['total_area_m2'] = df['total_area_m2'].fillna(df['usable_area_m2'])

    if 'category_sub' in df.columns:
        category_mapping = {
            '1+kk': '1',
            '1+1': '1+1',
            '2+kk': '2',
            '2+1': '2+1',
            '3+kk': '3',
            '3+1': '3+1',
            '4+kk': '4',
            '4+1': '4+1',
            '5+kk': '5+',
            '6-a-vice': '5+',
            'atypicky': '5+',
            '5+1': '5+'
        }
        df['category_sub'] = df['category_sub'].replace(category_mapping)

    categorical_cols = [
        'category_sub', 'locality_region_id', 'is_furnished',
        'location_type', 'energy_class', 'ownership_type',
        'construction_type', 'building_condition'
    ]
    for col in categorical_cols:
        if col in df.columns:
            df[col] = df[col].astype('category')

    return df


def get_pipeline_config() -> dict:
    """Returns the feature configurations required by the generalized pipeline."""
    return {
        "num_features": [
            "latitude",
            "longitude",
            "usable_area_m2",
            "total_area_m2",
            "loggia_area_m2",
            "cellar_area_m2",

        ],
        "bool_features": ["has_elevator", "has_terrace", "has_garage", "has_cellar", "has_loggia"],
        "cat_features": [
            "category_sub", "construction_type", "ownership_type",  "is_furnished"
        ],
        "structural_features": ["floor_number",
                                "total_floors",
                                "location_type",],
        "target_encoded_features": ["district_id"],

        "ordinal_features": ["energy_class", "building_condition", "locality_region_id"],

        "ordinal_categories": [
            ["A", "B", "C", "D", "E", "F", "G", "missing"],
            [
                "Špatný",
                "Před rekonstrukcí",
                "Dobrý",
                "Velmi dobrý",
                "Po rekonstrukci",
                "V rekonstrukci",
                "Novostavba",
                "Ve výstavbě",
                "Projekt",
                "missing"
            ],
            [
                10,  # : "Praha",              # ~150 000 Kč/m²
                14,  # : "Jihomoravský",       # ~105 000 Kč/m²
                11,  # : "Středočeský",        # ~88 000 Kč/m²
                6,  # "Královéhradecký",     # ~75 000 Kč/m²
                1,  # "Jihočeský",           # ~74 000 Kč/m²
                2,  # "Plzeňský",            # ~74 000 Kč/m²
                5,  # "Liberecký",           # ~73 000 Kč/m²
                8,  # "Olomoucký",           # ~72 000 Kč/m²
                9,  # "Zlínský",             # ~69 000 Kč/m²
                7,  # "Pardubický",          # ~65 000 Kč/m²
                3,  # "Karlovarský",         # ~60 000 Kč/m² (approx)
                12,  # "Moravskoslezský",    # ~55 000 Kč/m² (approx)
                4,  # "Ústecký",             # ~40 000 Kč/m² (approx, lowest)
                13,  # "Vysočina"            # ~?? similar lower-mid range
            ]
        ]
    }


if __name__ == "__main__":
    df = pd.read_csv('data/apartments_raw_data.csv')
    df = process_df(df)
    df.to_csv('data/real_estate_processed.csv', index=False)
