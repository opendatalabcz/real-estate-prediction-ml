"""
Compute driving travel times from each apartment to the nearest krajské město (regional city center)
using the OSRM (Open Source Routing Machine) API.

Input:
  - data/city_centers.csv (14 Czech city centers with lat/lon)
  - data/apartments_raw_data.csv (apartment listings with lat/lon)

Output:
  - data/city_center_travel_times_min.csv (travel time in minutes to nearest city center per listing)

Uses OSRM demo server (https://router.project-osrm.org/).
"""
import csv
import json
import time
import urllib.request
from pathlib import Path

OSRM_URL = "https://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}?overview=false"

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
APARTMENTS_CSV = DATA_DIR / "apartments_raw_data.csv"
CITY_CENTERS_CSV = DATA_DIR / "city_centers.csv"
OUTPUT_CSV = DATA_DIR / "city_center_travel_times.csv"


def load_apartments() -> list[dict]:
    with open(APARTMENTS_CSV, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def load_city_centers() -> list[dict]:
    with open(CITY_CENTERS_CSV, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Orthodromic distance in km (Haversine formula)."""
    from math import asin, cos, radians, sin, sqrt

    R = 6371.0088
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * R * asin(sqrt(a))


def query_osrm(lon1: float, lat1: float, lon2: float, lat2: float) -> float | None:
    """Query OSRM for driving time in minutes. Returns None on failure."""
    url = OSRM_URL.format(lon1=lon1, lat1=lat1, lon2=lon2, lat2=lat2)
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            if data.get("code") == "Ok" and data.get("routes"):
                return data["routes"][0]["duration"] / 60.0  # seconds -> minutes
    except Exception:
        pass
    return None


def main():
    apartments = load_apartments()
    city_centers = load_city_centers()

    print(f"Loaded {len(apartments)} apartments, {len(city_centers)} city centers")

    results = []
    for i, apt in enumerate(apartments):
        lat = float(apt["latitude"])
        lon = float(apt["longitude"])

        # Find nearest city center by Haversine distance (fast pre-filter)
        best_center = None
        best_dist = float("inf")
        for cc in city_centers:
            d = haversine_km(lat, lon, float(cc["latitude"]), float(cc["longitude"]))
            if d < best_dist:
                best_dist = d
                best_center = cc

        # Query OSRM for actual driving time
        travel_time = query_osrm(
            lon, lat,
            float(best_center["longitude"]),
            float(best_center["latitude"]),
        )

        # Fallback: estimate from Haversine distance (50 km/h average speed)
        if travel_time is None:
            travel_time = (best_dist / 50.0) * 60.0

        results.append({
            "listing_id": apt.get("id", i),
            "lat": apt["latitude"],
            "lon": apt["longitude"],
            "city_center": best_center["name"],
            "city_center_lat": best_center["latitude"],
            "city_center_lon": best_center["longitude"],
            "haversine_km": round(best_dist, 2),
            "travel_time_min": round(travel_time, 1),
        })

        if (i + 1) % 500 == 0:
            print(f"  Processed {i + 1}/{len(apartments)}...")
            time.sleep(0.5)  # rate-limit

    # Write output
    fieldnames = [
        "listing_id", "lat", "lon",
        "city_center", "city_center_lat", "city_center_lon",
        "haversine_km", "travel_time_min",
    ]
    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"Done. Wrote {len(results)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
