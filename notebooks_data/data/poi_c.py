#!/usr/bin/env python3
"""
Convert Overpass Turbo GeoJSON export to a clean CSV for POI feature engineering.

Examples
--------
# Public transport export -> public_transport.csv
python data/poi_c.py \
    --input data/public_transport.geojson \
    --output data/public_transport.csv \
    --kind transport

# Grocery / retail export -> brand_stores.csv
python data/poi_c.py \
    --input data/brand_stores.geojson \
    --output data/brand_stores.csv \
    --kind grocery

# Optional region-center lookup for city-distance features
python data/poi_c.py \
    --input data/public_transport.geojson \
    --output data/public_transport.csv \
    --kind transport \
    --city-centers-output data/city_centers.csv

Output schema
-------------
Transport:
    osm_id, name, latitude, longitude, poi_kind, raw_main_tag

Grocery:
    osm_id, name, latitude, longitude, poi_kind, raw_main_tag
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN

EARTH_RADIUS_KM = 6371.0088


DEFAULT_CITY_CENTERS_PATH = Path("data/city_centers.csv")
REGION_CENTERS = {
    1: (48.9745, 14.4743),   # Jihocesky (Ceske Budejovice)
    2: (49.7384, 13.3736),   # Plzensky (Plzen)
    3: (50.2327, 12.8712),   # Karlovarsky (Karlovy Vary)
    4: (50.6607, 14.0328),   # Ustecky (Usti nad Labem)
    5: (50.7671, 15.0562),   # Liberecky (Liberec)
    6: (50.2104, 15.8252),   # Kralovehradecky (Hradec Kralove)
    7: (50.0343, 15.7704),   # Pardubicky (Pardubice)
    8: (49.5938, 17.2509),   # Olomoucky (Olomouc)
    9: (49.2265, 17.6625),   # Zlinsky (Zlin)
    10: (50.0880, 14.4208),  # Praha
     11: (50.0406, 14.5567),  # Stredocesky (polygon centroid)
    12: (49.8209, 18.2625),  # Moravskoslezsky (Ostrava)
    13: (49.3961, 15.5904),  # Vysocina (Jihlava)
    14: (49.1951, 16.6068),  # Jihomoravsky (Brno)
}


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to Overpass GeoJSON export")
    parser.add_argument("--output", required=True, help="Path to output CSV")
    parser.add_argument(
        "--kind",
        required=True,
        choices=["transport", "grocery", "auto"],
        help="POI family to classify",
    )
    parser.add_argument(
        "--city-centers-output",
        default=None,
        help="Optional path to also write the default locality-region center lookup CSV.",
    )
    parser.add_argument(
        "--cluster-radius",
        type=float,
        default=0,
        help="Spatial clustering radius in meters. Nearby POIs of the same kind within this "
             "distance are deduplicated (keeps one per cluster). 0 = disabled. "
             "Recommended: 30 for bus stops (merges dual-direction nodes).",
    )
    return parser.parse_args()


def load_geojson(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def extract_point(feature: dict[str, Any]) -> tuple[float | None, float | None]:
    geometry = feature.get("geometry") or {}
    geom_type = geometry.get("type")
    coords = geometry.get("coordinates")

    if geom_type == "Point" and isinstance(coords, list) and len(coords) >= 2:
        lon, lat = coords[0], coords[1]
        return lat, lon

    return None, None


def parse_osm_id(feature: dict[str, Any]) -> str:
    props = feature.get("properties") or {}
    if "@id" in props:
        return str(props["@id"])
    if "id" in feature:
        return str(feature["id"])
    return ""


def get_name(props: dict[str, Any]) -> str | None:
    for key in ["name", "official_name", "brand", "operator"]:
        value = props.get(key)
        if value not in [None, ""]:
            return str(value)
    return None


def classify_transport(props: dict[str, Any]) -> tuple[str, str]:
    highway = str(props.get("highway", "")).lower()
    railway = str(props.get("railway", "")).lower()
    public_transport = str(props.get("public_transport", "")).lower()
    station = str(props.get("station", "")).lower()
    subway = str(props.get("subway", "")).lower()
    tram = str(props.get("tram", "")).lower()
    bus = str(props.get("bus", "")).lower()
    train = str(props.get("train", "")).lower()

    # Strong metro signals
    if subway == "yes":
        return "metro", "subway=yes"
    if station in {"subway", "metro"}:
        return "metro", f"station={station}"
    if railway == "subway_entrance":
        return "metro", f"railway={railway}"

    # Tram
    if railway == "tram_stop" or tram == "yes":
        return "tram_stop", "tram=yes" if tram == "yes" else f"railway={railway}"

    # Train / rail
    if railway in {"station", "halt"} or train == "yes":
        return "train_station", "train=yes" if train == "yes" else f"railway={railway}"

    # Generic railway stop, often needs context
    if railway == "stop":
        return "train_stop", f"railway={railway}"

    # Bus
    if highway == "bus_stop" or bus == "yes":
        return "bus_stop", "bus=yes" if bus == "yes" else f"highway={highway}"

    # Public transport generic
    if public_transport in {"stop_position", "platform", "station"}:
        return "public_transport", f"public_transport={public_transport}"

    return "other_transport", "unclassified"


def classify_grocery(props: dict[str, Any]) -> tuple[str, str]:
    shop = str(props.get("shop", "")).lower()
    amenity = str(props.get("amenity", "")).lower()

    if shop == "supermarket":
        return "supermarket", f"shop={shop}"
    if shop == "convenience":
        return "convenience", f"shop={shop}"
    if shop == "grocery":
        return "grocery", f"shop={shop}"
    if amenity == "marketplace":
        return "marketplace", f"amenity={amenity}"

    if shop:
        return shop, f"shop={shop}"
    if amenity:
        return amenity, f"amenity={amenity}"
    return "other_retail", "unclassified"


def classify_auto(props: dict[str, Any]) -> tuple[str, str]:
    if any(k in props for k in ["highway", "railway", "public_transport", "station", "bus", "tram", "train"]):
        return classify_transport(props)
    if any(k in props for k in ["shop", "amenity"]):
        return classify_grocery(props)
    return "unknown", "unclassified"


def feature_to_row(feature: dict[str, Any], kind: str) -> dict[str, Any] | None:
    props = feature.get("properties") or {}
    lat, lon = extract_point(feature)

    if lat is None or lon is None:
        return None

    if kind == "transport":
        poi_kind, raw_main_tag = classify_transport(props)
    elif kind == "grocery":
        poi_kind, raw_main_tag = classify_grocery(props)
    else:
        poi_kind, raw_main_tag = classify_auto(props)

    return {
        "osm_id": parse_osm_id(feature),
        "name": get_name(props),
        "latitude": float(lat),
        "longitude": float(lon),
        "poi_kind": poi_kind,
        "raw_main_tag": raw_main_tag,
    }


def build_city_centers_table(
    region_centers: dict[int, tuple[float, float]] = REGION_CENTERS,
) -> pd.DataFrame:
    return (
        pd.DataFrame(
            [
                {
                    "locality_region_id": region_id,
                    "center_latitude": lat,
                    "center_longitude": lon,
                }
                for region_id, (lat, lon) in region_centers.items()
            ]
        )
        .sort_values("locality_region_id")
        .reset_index(drop=True)
    )


def write_city_centers_csv(path: str | Path = DEFAULT_CITY_CENTERS_PATH) -> pd.DataFrame:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    city_centers = build_city_centers_table()
    city_centers.to_csv(output_path, index=False)
    return city_centers


def _spatial_dedup(df: pd.DataFrame, radius_m: float) -> pd.DataFrame:
    """Cluster POIs of the same kind within `radius_m` meters and keep one per cluster.

    Uses DBSCAN with haversine distance. Each poi_kind group is clustered independently
    so that, e.g., a tram_stop and a bus_stop at the same location are not merged.
    """
    if radius_m <= 0:
        return df

    eps_rad = radius_m / 1000 / EARTH_RADIUS_KM
    cluster_col = "__cluster__"
    df = df.copy()
    df[cluster_col] = -1

    for kind in df["poi_kind"].unique():
        mask = df["poi_kind"] == kind
        kind_df = df.loc[mask]
        if len(kind_df) < 2:
            continue

        coords = np.radians(kind_df[["latitude", "longitude"]].to_numpy())
        clustering = DBSCAN(eps=eps_rad, min_samples=1, metric="haversine").fit(coords)
        df.loc[mask, cluster_col] = clustering.labels_

    # Keep the first row per (poi_kind, cluster) group
    n_before = len(df)
    df = df.drop_duplicates(subset=[cluster_col, "poi_kind"], keep="first").drop(columns=[cluster_col])
    n_after = len(df)

    print(f"  Spatial dedup ({radius_m}m radius): {n_before:,} -> {n_after:,} rows "
          f"({n_before - n_after:,} merged)")
    return df


def main() -> None:
    args = get_args()
    geojson = load_geojson(args.input)
    features = geojson.get("features", [])

    rows = []
    for feature in features:
        row = feature_to_row(feature, args.kind)
        if row is not None:
            rows.append(row)

    df = pd.DataFrame(rows)

    if df.empty:
        raise ValueError("No valid point features found in the input file.")

    # Basic cleaning
    df = df.dropna(subset=["latitude", "longitude"]).copy()
    df = df[df["latitude"].between(48.0, 51.5) & df["longitude"].between(12.0, 19.0)].copy()

    # Prefer unique OSM objects; fallback to coordinate dedup if osm_id missing
    if df["osm_id"].notna().any():
        df = df.drop_duplicates(subset=["osm_id"], keep="first")
    else:
        df = df.drop_duplicates(subset=["latitude", "longitude", "poi_kind"], keep="first")

    # Spatial dedup — merge dual-direction stops (e.g. bus stops ~10-30m apart)
    df = _spatial_dedup(df, radius_m=args.cluster_radius)

    # Save
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df = df.sort_values(["poi_kind", "latitude", "longitude"]).reset_index(drop=True)
    df.to_csv(output_path, index=False)

    print(f"Saved {len(df):,} rows to {output_path}")
    print(df["poi_kind"].value_counts(dropna=False).sort_index())

    if args.city_centers_output:
        city_centers = write_city_centers_csv(args.city_centers_output)
        print(f"Saved {len(city_centers):,} rows to {Path(args.city_centers_output)}")
        print(city_centers)


if __name__ == "__main__":
    main()
