"""
POI feature enhancement layer — top-5 forward-selected features only.

Returns 5 POI features matching report.tex (tab:poi_correlation):
  - transport_quality_score      continuous: 10/(1+d_metro) + 1/(1+d_tram)
  - city_center_travel_time_min  OSRM driving time (min), Haversine fallback
  - convenience_nearest_km        BallTree k=1 nearest convenience store
  - grocery_avg_3_nearest_km      BallTree k=3 avg distance to groceries
  - supermarket_nearest_km        BallTree k=1 nearest supermarket
"""

from pathlib import Path

import numpy as np
import pandas as pd
import requests
from sklearn.neighbors import BallTree

EARTH_RADIUS_KM = 6371.0088


def _build_tree(df):
    coords = np.radians(df[["latitude", "longitude"]].to_numpy())
    return BallTree(coords, metric="haversine")


def _query_k(tree, lat, lng, k):
    pt = np.radians([[lat, lng]])
    dist, idx = tree.query(pt, k=k)
    return dist[0] * EARTH_RADIUS_KM, idx[0]


def _haversine_km(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return EARTH_RADIUS_KM * 2 * np.arcsin(np.sqrt(a))


class PoiHelper:
    """Cached BallTree POI engine. Builds trees once, then compute() per coordinate."""

    OSRM_URL = "https://router.project-osrm.org/route/v1/driving"
    OSRM_TIMEOUT = 3

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

        self._osrm_session = requests.Session()

    def _travel_time_min(self, lat, lng, clat, clon):
        """OSRM driving time in minutes, Haversine fallback on failure."""
        try:
            url = f"{self.OSRM_URL}/{lng},{lat};{clon},{clat}?overview=false"
            resp = self._osrm_session.get(url, timeout=self.OSRM_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("code") == "Ok":
                    return data["routes"][0]["duration"] / 60.0
        except Exception:
            pass
        return float(_haversine_km(lat, lng, clat, clon)) * 0.8

    # ------------------------------------------------------------------
    # Public API — returns dict of top-5 POI features
    # ------------------------------------------------------------------
    def compute(self, lat: float, lng: float, region_id: int) -> dict:
        f = {}

        # --- 1. transport_quality_score — continuous per report.tex ---
        score = 0.0
        if self._t_metro is not None:
            d, _ = _query_k(self._t_metro, lat, lng, 1)
            score += 10.0 / (1.0 + float(d[0]))
        if self._t_tram is not None:
            d, _ = _query_k(self._t_tram, lat, lng, 1)
            score += 1.0 / (1.0 + float(d[0]))
        f["transport_quality_score"] = score

        # --- 2. city_center_travel_time_min — OSRM driving time (min) ---
        cc = self.city_centers[self.city_centers["locality_region_id"] == region_id]
        if len(cc) > 0:
            f["city_center_travel_time_min"] = self._travel_time_min(
                lat, lng,
                float(cc.iloc[0]["center_latitude"]),
                float(cc.iloc[0]["center_longitude"]),
            )
        else:
            f["city_center_travel_time_min"] = 0.0

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
