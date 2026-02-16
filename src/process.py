import pandas as pd


def process_df(df: pd.DataFrame) -> pd.DataFrame:
    cols_to_drop = [
        'construction_year', 'currency', 'is_topped', 'updated',
        'created_at', 'energy_efficiency_rating', 'municipality_id',
        'id', 'web_link', 'meta_description', 'description', 'title', 'street'
    ]
    df = df.drop(columns=cols_to_drop, errors='ignore')
    df = df[df['usable_area_m2'] <= 500].copy()

    category_mapping = {
        '1+kk': '1',
        '1+1': '2', '2+kk': '2',
        '2+1': '3', '3+kk': '3',
        '3+1': '4', '4+kk': '4',
        '4+1': '5+', '5+kk': '5+', '6-a-vice': '5+', 'atypicky': '5+', '5+1': '5+'
    }

    df['category_sub'] = df['category_sub'].replace(category_mapping)

    df['category_sub'] = df['category_sub'].astype('category')
    # df['district_id'] = df['district_id'].astype('category')
    df['locality_region_id'] = df['locality_region_id'].astype('category')
    df['is_furnished'] = df['is_furnished'].astype('category')
    df['location_type'] = df['location_type'].astype('category')
    df['energy_class'] = df['energy_class'].astype('category')
    df['ownership_type'] = df['ownership_type'].astype('category')
    df['construction_type'] = df['construction_type'].astype('category')
    df['building_condition'] = df['building_condition'].astype('category')

    return df


if __name__ == "__main__":
    df = pd.read_csv('data/apartments_raw_data.csv')
    df.info()
    df = process_df(df)
    df.to_csv('data/real_estate_processed.csv', index=False)
