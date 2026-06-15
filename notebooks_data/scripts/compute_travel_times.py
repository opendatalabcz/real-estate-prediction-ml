"""
Compute driving travel times from each apartment to the nearest krajské město (regional city center)
using the OSRM (Open Source Routing Machine) API.

Rate limit: 400 requests per minute.

Input:
  - data/city_centers.csv (14 Czech city centers with locality_region_id, center_lat, center_lon)
  - data/apartments_raw_data.csv (apartment listings with id, latitude, longitude)

Output:
  - data/city_center_travel_times.csv (travel time in minutes to nearest city center per listing)
"""
import csv
import json
import time
import urllib.request
from pathlib import Path

OSRM_URL = "https://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}?overview=false"
REQUESTS_PER_MINUTE = 400
DELAY_BETWEEN = 60.0 / REQUESTS_PER_MINUTE  # seconds between requests

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
APARTMENTS_CSV = DATA_DIR / "apartments_raw_data.csv"
APARTMENTS_TEST_CSV = DATA_DIR / "apartments_raw_data_test.csv"
CITY_CENTERS_CSV = DATA_DIR / "city_centers.csv"
OUTPUT_CSV = DATA_DIR / "city_center_travel_times.csv"
RESUME_CSV = DATA_DIR / "city_center_travel_times_progress.csv"


def load_apartments(path=APARTMENTS_CSV):
    with open(path, encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def load_city_centers():
    with open(CITY_CENTERS_CSV, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def haversine_km(lat1, lon1, lat2, lon2):
    from math import asin, cos, radians, sin, sqrt
    r = 6371.0088
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * r * asin(sqrt(a))


def query_osrm(lon1, lat1, lon2, lat2):
    url = OSRM_URL.format(lon1=lon1, lat1=lat1, lon2=lon2, lat2=lat2)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "RealEstatePredictionML/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            if data.get("code") == "Ok" and data.get("routes"):
                return data["routes"][0]["duration"] / 60.0
    except Exception as e:
        pass
    return None


FIELDNAMES = [
    "listing_id", "lat", "lon",
    "city_center", "city_center_lat", "city_center_lon",
    "haversine_km", "travel_time_min",
]


def load_progress():
    done = {}
    # First check resume file (partial progress)
    if RESUME_CSV.exists():
        with open(RESUME_CSV, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                done[row["listing_id"]] = row
    # Also check output CSV for already-computed train data
    if OUTPUT_CSV.exists():
        with open(OUTPUT_CSV, encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                done[row["listing_id"]] = row
    return done


def save_progress(done):
    with open(RESUME_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(done.values())


def main():
    apartments = load_apartments(APARTMENTS_CSV)
    apartments_test = load_apartments(APARTMENTS_TEST_CSV)
    all_apartments = apartments + apartments_test
    city_centers = load_city_centers()
    print(f"Loaded {len(apartments)} train + {len(apartments_test)} test = {len(all_apartments)} apartments, {len(city_centers)} city centers")

    done = load_progress()
    print(f"Resuming: {len(done)} already computed")

    results = dict(done)

    if len(results) == len(all_apartments):
        print("All listings already computed, writing final output.")
    else:
        for i, apt in enumerate(all_apartments):
            listing_id = apt.get("id", str(i))
            if listing_id in results:
                continue

            lat = float(apt["latitude"])
            lon = float(apt["longitude"])

            best_center = None
            best_dist = float("inf")
            for cc in city_centers:
                d = haversine_km(lat, lon, float(cc["center_latitude"]), float(cc["center_longitude"]))
                if d < best_dist:
                    best_dist = d
                    best_center = cc

            travel_time = query_osrm(
                lon, lat,
                float(best_center["center_longitude"]),
                float(best_center["center_latitude"]),
            )

            if travel_time is None:
                travel_time = (best_dist / 50.0) * 60.0

            results[listing_id] = {
                "listing_id": listing_id,
                "lat": apt["latitude"],
                "lon": apt["longitude"],
                "city_center": best_center["locality_region_id"],
                "city_center_lat": best_center["center_latitude"],
                "city_center_lon": best_center["center_longitude"],
                "haversine_km": round(best_dist, 2),
                "travel_time_min": round(travel_time, 1),
            }

            completed = len(results)
            if completed % 50 == 0:
                print(f"  {completed}/{len(all_apartments)}")
                save_progress(results)

            time.sleep(DELAY_BETWEEN)

        save_progress(results)

    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for apt in all_apartments:
            listing_id = apt.get("id", "")
            if listing_id in results:
                writer.writerow(results[listing_id])

    RESUME_CSV.unlink(missing_ok=True)
    print(f"Done. Wrote {len(results)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
