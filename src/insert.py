import json
import re
import psycopg2
from psycopg2.extras import execute_values
from helper import category_sub_to_url
from datetime import datetime, timedelta

DB_CONFIG = {
    "dbname": "postgres",
    "user": "honzanovak",
    "password": "",
    "host": "localhost",
    "port": 5432
}


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def clean_int(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if not value:
        return None
    match = re.search(r"\d+", str(value))
    return int(match.group()) if match else None


def parse_floors(value):
    if not value:
        return None, None
    nums = re.findall(r"\d+", str(value))
    if len(nums) >= 2:
        return int(nums[0]), int(nums[1])
    if len(nums) == 1:
        return int(nums[0]), None
    return None, None


def parse_energy_class(value):
    if not value:
        return None
    match = re.search(r"Třída\s+([A-Z])", value)
    return match.group(1) if match else None


def insert_data(cursor, data):

    property_id = None
    property_id = data.get("recommendations_data", {}).get("hash_id")
    if not property_id:
        print("Skipping listing: missing property ID")
        return

    properties = {
        "id": property_id,
        "web_link": data.get("real_website_link"),
        "title": data.get("name", {}).get("value"),
        "description": data.get("text", {}).get("value"),
        "category_sub": category_sub_to_url[data.get("seo", {}).get("category_sub_cb")],
        "price_total": data.get("price_czk", {}).get("value_raw"),
        "currency": None,
        "street": data.get("locality", {}).get("value"),
        "district_id": data.get("locality_district_id"),
        "locality_region_id": data.get("recommendations_data", {}).get("locality_region_id"),
        "municipality_id": data.get("recommendations_data", {}).get("locality_municipality_id"),
        "latitude": data.get("map", {}).get("lat"),
        "longitude": data.get("map", {}).get("lon"),
        "meta_description": data.get("meta_description"),
        "updated": None,
        "is_topped": data.get("is_topped", False),

        # scalar specs
        "usable_area_m2": None,
        "total_area_m2": None,
        "loggia_area_m2": None,
        "cellar_area_m2": None,
        "floor_number": None,
        "total_floors": None,
        "construction_type": None,
        "building_condition": None,
        "ownership_type": None,
        "location_type": None,
        "construction_year": None,
        "energy_class": None,
        "energy_efficiency_rating": None,
        "has_elevator": bool(data.get("recommendations_data", {}).get("elevator", False)),
        "has_cellar": bool(data.get("recommendations_data", {}).get("cellar", False)),
        "has_terrace": bool(data.get("recommendations_data", {}).get("terrace", False)),
        "has_garage": bool(data.get("recommendations_data", {}).get("garage", False)),
        "has_loggia": bool(data.get("recommendations_data", {}).get("loggia", False)),
        "is_furnished": None,
    }

    features = []

    for item in data.get("items", []):
        name = item.get("name")
        value = item.get("value")
        itype = item.get("type")

        if itype == "set":
            for v in value or []:
                features.append((
                    property_id,
                    "set",
                    name,
                    v.get("value")
                ))
            continue

        if name == "Užitná ploch":
            properties["usable_area_m2"] = clean_int(value)

        elif name == "Celková plocha":
            properties["total_area_m2"] = clean_int(value)

        elif name == "Lodžie":
            properties["loggia_area_m2"] = clean_int(value)

        elif name == "Sklep":
            area = clean_int(value)
            if area:
                properties["cellar_area_m2"] = area

            properties["has_cellar"] = True

        elif name == "Podlaží":
            f, t = parse_floors(value)
            properties["floor_number"] = f
            properties["total_floors"] = t

        elif name == "Stavba":
            properties["construction_type"] = value

        elif name == "Stav objektu":
            properties["building_condition"] = value

        elif name == "Vlastnictví":
            properties["ownership_type"] = value

        elif name == "Umístění objektu":
            properties["location_type"] = value

        elif name == "Rok kolaudace":
            properties["construction_year"] = clean_int(value)

        elif name == "Energetická náročnost budovy":
            properties["energy_class"] = parse_energy_class(value)
            properties["energy_efficiency_rating"] = value

        elif name == "Výtah":
            properties["has_elevator"] = bool(value)

        elif name == "Vybavení":
            properties["is_furnished"] = value
        elif name == "Aktualizace":
            if value == "Dnes":
                value = datetime.now().strftime("%d.%m.%Y")
            if value == "Včera":
                value = (datetime.now() - timedelta(days=1)).strftime("%d.%m.%Y")
            properties["updated"] = value
        elif name == "Celková cena":
            properties["currency"] = item.get("currency")
    columns = list(properties.keys())
    values = list(properties.values())

    cursor.execute(
        f"""
        INSERT INTO properties ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(values))})
        ON CONFLICT (id) DO NOTHING;
        """,
        values
    )

    if features:
        execute_values(
            cursor,
            """
            INSERT INTO property_features (property_id, feature_type, name, value)
            VALUES %s
            """,
            features
        )
