"""
POI feature enhancement layer.

Adds 17 enhanced features on top of the 8 base location features:
  - Grocery split (supermarket vs convenience)       3 cols
  - KNN average distance (k=3)                       2 cols
  - Transport quality score                          1 col
  - Amenity diversity (unique types in 1km/2km)     2 cols
  - Gaussian kernel density (sigma=700m)             2 cols
  - Edge flags (has_X within 1km)                    2 cols  (shared with base)
Total: 12 new columns (excluding the 2 edge flags which replace basic counts)
"""

from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.neighbors import BallTree

EARTH_RADIUS_KM = 6371.0088


def _build_tree(df):
    coords = np.radians(df[["latitude", "longitude"]].to_numpy())
    return BallTree(coords, metric="haversine")


def _query_k(tree, lat, lng, k):
    pt = np.radians([[lat, lng]])
    dist, idx = tree.query(pt, k=k)
    return dist[0] * EARTH_RADIUS_KM, idx[0]


def _query_radius(tree, lat, lng, radius_km):
    pt = np.radians([[lat, lng]])
    r = radius_km / EARTH_RADIUS_KM
    count = tree.query_radius(pt, r=r, count_only=True)[0]
    return int(count)


def _query_radius_idx(tree, lat, lng, radius_km):
    pt = np.radians([[lat, lng]])
    r = radius_km / EARTH_RADIUS_KM
    return tree.query_radius(pt, r=r)[0]


def _haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return EARTH_RADIUS_KM * 2 * np.arcsin(np.sqrt(a))


class PoiHelper:
    """Cached BallTree POI engine. Builds trees once, then compute() per coordinate."""

    def __init__(self, transport_path, grocery_path, city_centers_path):
        self.transport = pd.read_csv(transport_path)
        self.grocery = pd.read_csv(grocery_path)
        self.city_centers = pd.read_csv(city_centers_path)

        # Build trees (cached, never rebuilt per call)
        self._t_transport = _build_tree(self.transport)
        self._t_grocery = _build_tree(self.grocery)

        # Subsets for split grocery
        sup = self.grocery[self.grocery["poi_kind"] == "supermarket"]
        conv = self.grocery[self.grocery["poi_kind"] == "convenience"]
        self._t_supermarket = _build_tree(sup) if len(sup) > 0 else None
        self._t_convenience = _build_tree(conv) if len(conv) > 0 else None

        # Cache metro/train subset
        mt = self.transport[
            self.transport["poi_kind"].str.contains("metro|train", case=False, na=False)
        ]
        self._t_metro_train = _build_tree(mt) if len(mt) > 0 else None

    # ------------------------------------------------------------------
    # Public API — returns dict of all 25 POI features
    # ------------------------------------------------------------------
    def compute(self, lat: float, lng: float, region_id: int) -> dict:
        f = {}

        # --- 1. Transport base (3) ---
        d1, _ = _query_k(self._t_transport, lat, lng, 1)
        f["transport_nearest_km"] = float(d1[0])
        f["transport_count_within_500m"] = _query_radius(self._t_transport, lat, lng, 0.5)
        f["transport_count_within_1000m"] = _query_radius(self._t_transport, lat, lng, 1.0)

        # --- 2. Metro / train nearest (1) ---
        if self._t_metro_train is not None:
            d, _ = _query_k(self._t_metro_train, lat, lng, 1)
            f["metro_train_nearest_km"] = float(d[0])
        else:
            f["metro_train_nearest_km"] = np.nan

        # --- 3. Grocery base (3) ---
        d1, _ = _query_k(self._t_grocery, lat, lng, 1)
        f["grocery_nearest_km"] = float(d1[0])
        f["grocery_count_within_500m"] = _query_radius(self._t_grocery, lat, lng, 0.5)
        f["grocery_count_within_1000m"] = _query_radius(self._t_grocery, lat, lng, 1.0)

        # --- 4. City centre distance (1) ---
        cc = self.city_centers[self.city_centers["locality_region_id"] == region_id]
        f["distance_to_city_center_km"] = (
            float(_haversine_km(lat, lng, cc.iloc[0]["center_latitude"], cc.iloc[0]["center_longitude"]))
            if len(cc) > 0 else np.nan
        )

        # ========== ENHANCED FEATURES ==========

        # --- 5. Grocery split: supermarket (3) ---
        if self._t_supermarket is not None:
            d, _ = _query_k(self._t_supermarket, lat, lng, 1)
            f["supermarket_nearest_km"] = float(d[0])
            f["supermarket_count_within_500m"] = _query_radius(self._t_supermarket, lat, lng, 0.5)
            f["supermarket_count_within_1000m"] = _query_radius(self._t_supermarket, lat, lng, 1.0)
        else:
            f["supermarket_nearest_km"] = f["supermarket_count_within_500m"] = f["supermarket_count_within_1000m"] = np.nan

        # --- 6. Grocery split: convenience (3) ---
        if self._t_convenience is not None:
            d, _ = _query_k(self._t_convenience, lat, lng, 1)
            f["convenience_nearest_km"] = float(d[0])
            f["convenience_count_within_500m"] = _query_radius(self._t_convenience, lat, lng, 0.5)
            f["convenience_count_within_1000m"] = _query_radius(self._t_convenience, lat, lng, 1.0)
        else:
            f["convenience_nearest_km"] = f["convenience_count_within_500m"] = f["convenience_count_within_1000m"] = np.nan

        # --- 7. KNN average distance (k=3) (2) ---
        d3, _ = _query_k(self._t_transport, lat, lng, 3)
        f["transport_avg_3_nearest_km"] = float(np.mean(d3))
        d3g, _ = _query_k(self._t_grocery, lat, lng, 3)
        f["grocery_avg_3_nearest_km"] = float(np.mean(d3g))

        # --- 8. Transport quality score (1) ---
        idx1k = _query_radius_idx(self._t_transport, lat, lng, 1.0)
        if len(idx1k) > 0:
            types = self.transport.iloc[idx1k]["poi_kind"].value_counts()
            score = (
                types.get("metro", 0) * 3
                + types.get("train_station", 0) * 3
                + types.get("train_stop", 0) * 2
                + types.get("tram_stop", 0) * 2
                + types.get("bus_stop", 0) * 1
                + types.get("public_transport", 0) * 1
            )
            f["transport_quality_score"] = int(score)
        else:
            f["transport_quality_score"] = 0

        # --- 9. Amenity diversity (2) ---
        for radius, key in [(1.0, "unique_poi_types_1000m"), (2.0, "unique_poi_types_2000m")]:
            idx = _query_radius_idx(self._t_transport, lat, lng, radius)
            f[key] = int(self.transport.iloc[idx]["poi_kind"].nunique()) if len(idx) > 0 else 0

        # --- 10. Gaussian kernel density sigma=700m (2) ---
        for prefix, tree, df_ref in [
            ("transport", self._t_transport, self.transport),
            ("grocery", self._t_grocery, self.grocery),
        ]:
            sigma = 0.7
            idx = _query_radius_idx(tree, lat, lng, sigma * 3)
            if len(idx) > 0:
                pts = df_ref.iloc[idx]
                d_rad = np.array([
                    _haversine_km(lat, lng, r["latitude"], r["longitude"])
                    for _, r in pts.iterrows()
                ])
                density = float(np.sum(np.exp(-(d_rad ** 2) / (2 * sigma ** 2))))
                f[f"{prefix}_density_sigma700m"] = density
            else:
                f[f"{prefix}_density_sigma700m"] = 0

        # --- 11. Edge flags (2) ---
        f["has_transport_1km"] = 1 if f["transport_count_within_1000m"] > 0 else 0
        f["has_grocery_1km"] = 1 if f["grocery_count_within_1000m"] > 0 else 0

        return f
