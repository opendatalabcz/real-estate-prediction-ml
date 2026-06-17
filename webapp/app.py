import json
import os
from pathlib import Path

from flask import Flask, jsonify, render_template, request

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.predict_loader import PredictLoader
from config import DISTRICT_MAPPING_PATH, REGION_MAPPING_PATH

app = Flask(__name__)

predictor = PredictLoader()

districts_df = None
regions_df = None


def load_mappings():
    global districts_df, regions_df
    import pandas as pd
    districts_df = pd.read_csv(DISTRICT_MAPPING_PATH)
    regions_df = pd.read_csv(REGION_MAPPING_PATH)


load_mappings()


@app.route("/")
def index():
    districts = districts_df.to_dict(orient="records")
    regions = regions_df.to_dict(orient="records")
    return render_template("index.html", districts=districts, regions=regions)


@app.route("/predict", methods=["POST"])
def predict():
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided"}), 400

    required = [
        "latitude", "longitude", "usable_area_m2", "total_area_m2",
        "loggia_area_m2", "cellar_area_m2", "floor_number", "total_floors",
        "has_elevator", "has_terrace", "has_garage", "has_cellar", "has_loggia",
        "category_sub", "construction_type", "ownership_type", "is_furnished",
        "location_type", "energy_class", "building_condition",
        "locality_region_id", "district_id", "construction_year",
    ]
    missing = [col for col in required if col not in data]
    if missing:
        return jsonify({"error": f"Missing fields: {missing}"}), 400

    lat = float(data["latitude"])
    lng = float(data["longitude"])
    if not (48.0 <= lat <= 51.5) or not (12.0 <= lng <= 19.0):
        return jsonify({"error": "Souřadnice jsou mimo území České republiky"}), 400

    input_dict = {
        "category_sub": str(data["category_sub"]),
        "price_total": 0,
        "locality_region_id": int(data["locality_region_id"]),
        "district_id": int(data["district_id"]),
        "latitude": float(data["latitude"]),
        "longitude": float(data["longitude"]),
        "usable_area_m2": float(data["usable_area_m2"]),
        "total_area_m2": float(data["total_area_m2"]),
        "loggia_area_m2": float(data["loggia_area_m2"]),
        "cellar_area_m2": float(data["cellar_area_m2"]),
        "floor_number": float(data["floor_number"]) if data["floor_number"] is not None else None,
        "total_floors": float(data["total_floors"]) if data["total_floors"] is not None else None,
        "construction_type": str(data["construction_type"]),
        "building_condition": str(data["building_condition"]),
        "ownership_type": str(data["ownership_type"]),
        "location_type": str(data["location_type"]),
        "construction_year": float(data["construction_year"]) if data["construction_year"] is not None else None,
        "energy_class": str(data["energy_class"]) if data["energy_class"] else "missing",
        "has_elevator": bool(data["has_elevator"]),
        "has_garage": bool(data["has_garage"]),
        "has_cellar": bool(data["has_cellar"]),
        "has_terrace": bool(data["has_terrace"]),
        "has_loggia": bool(data["has_loggia"]),
        "is_furnished": str(data["is_furnished"]),
    }

    try:
        result = predictor.predict(input_dict)
        return jsonify({"success": True, **result})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/districts")
def api_districts():
    region = request.args.get("region")
    if region:
        filtered = districts_df[districts_df["locality_region_id"] == int(region)]
    else:
        filtered = districts_df
    return jsonify(filtered.to_dict(orient="records"))


@app.route("/about")
def about():
    return render_template("about.html")


@app.route("/api/regions")
def api_regions():
    return jsonify(regions_df.to_dict(orient="records"))


if __name__ == "__main__":
    debug_mode = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(debug=debug_mode, host="0.0.0.0", port=8080)
