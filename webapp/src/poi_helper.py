"""
POI feature enhancement layer — top-5 forward-selected features only.

Returns 5 POI features matching report.tex (tab:poi_correlation):
  - transport_quality_score      continuous: 10/(1+d_metro) + 1/(1+d_tram)
  - city_center_travel_time_min  Haversine km to region city center
  - convenience_nearest_km        BallTree k=1 nearest convenience store
  - grocery_avg_3_nearest_km      BallTree k=3 avg distance to groceries
  - supermarket_nearest_km        BallTree k=1 nearest supermarket
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

        # Grocery subsets
        sup = self.grocery[self.grocery["poi_kind"] == "supermarket"]
        conv = self.grocery[self.grocery["poi_kind"] == "convenience"]
        self._t_supermarket = _build_tree(sup) if len(sup) > 0 else None
        self._t_convenience = _build_tree(conv) if len(conv) > 0 else None
        self._t_grocery = _build_tree(self.grocery)

        # Transport subsets for quality score
        metro = self.transport[self.transport["poi_kind"].str.lower() == "metro"]
        tram = self.transport[self.transport["poi_kind"].str.lower() == "tram_stop"]
        self._t_metro = _build_tree(metro) if len(metro) > 0 else None
        self._t_tram = _build_tree(tram) if len(tram) > 0 else None

    # ------------------------------------------------------------------
    # Public API — returns dict of top-5 POI features
    # ------------------------------------------------------------------
    def compute(self, lat: float, lng: float, region_id: int) -> dict:
        f = {}

        # --- 1. transport_quality_score — continuous per report.tex ---
        # Q = 10/(1+d_metro) + 1/(1+d_tram), bus weight=0
        score = 0.0
        if self._t_metro is not None:
            d, _ = _query_k(self._t_metro, lat, lng, 1)
            score += 10.0 / (1.0 + float(d[0]))
        if self._t_tram is not None:
            d, _ = _query_k(self._t_tram, lat, lng, 1)
            score += 1.0 / (1.0 + float(d[0]))
        f["transport_quality_score"] = score

        # --- 2. city_center_travel_time_min — Haversine km ---
        cc = self.city_centers[self.city_centers["locality_region_id"] == region_id]
        f["city_center_travel_time_min"] = (
            float(_haversine_km(lat, lng, cc.iloc[0]["center_latitude"], cc.iloc[0]["center_longitude"]))
            if len(cc) > 0 else 0.0
        )

        # --- 3. supermarket_nearest_km ---
        if self._t_supermarket is not None:
            d, _ = _query_k(self._t_supermarket, lat, lng, 1)
            f["supermarket_nearest_km"] = float(d[0])

        # --- 4. convenience_nearest_km ---
        if self._t_convenience is not None:
            d, _ = _query_k(self._t_convenience, lat, lng, 1)
            f["convenience_nearest_km"] = float(d[0])

        # --- 5. grocery_avg_3_nearest_km ---
        d3, _ = _query_k(self._t_grocery, lat, lng, 3)
        f["grocery_avg_3_nearest_km"] = float(np.mean(d3))

        return f
