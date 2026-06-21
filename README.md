# Real Estate Price Prediction ML

Prediction of apartment offer prices in the Czech Republic using machine learning.

This repository contains a complete end-to-end solution — from automated data collection and exploratory analysis through feature engineering and model training to a production web application with interactive frontend and prediction explainability.

The project is divided into two main parts:
1. **Research & Analysis** (`notebooks_data/`) — Jupyter notebooks, Python scripts, thesis report
2. **Web Application** (`webapp/`) — Flask REST API with Docker deployment

---

## 1. Research & Analysis (`notebooks_data/`)

This part focuses on designing and evaluating machine learning models for apartment price prediction in the Czech Republic. It covers the entire process from raw API data to an optimized XGBoost model, backed by a ~45-page LaTeX thesis report. The datasets comprise 13,184 apartment listings enriched with spatial POI data (71,447 public transport stops, 3,310 stores, 14 city centers).

| Directory | Contents |
|-----------|----------|
| `NBS/` | Jupyter notebooks (EDA, preprocessing, modeling, POI evaluation) + LaTeX thesis report |
| `data/` | Raw and processed datasets (apartments, POIs, transport, city centers, mappings) |
| `src/` | Research Python modules (scraping, preprocessing pipeline, feature engineering, model training, baseline experiments, calibration, POI workflow) |
| `scripts/` | Utility scripts (travel times, transport merging, model training, plot generation) |
| `models/` | Trained XGBoost pipeline artifacts (`.joblib`) |
| `artifacts/` | Experiment results and evaluations |

### Project Objectives

* **Data Acquisition & Preparation:** Automated scraping from real estate portals (API) → PostgreSQL ingestion → cleaning and validation → exploratory data analysis (`ydata_profiling`) → POI extraction from OpenStreetMap via Overpass Turbo.
* **Regression Model Design & Evaluation:** Baseline chain from global median through linear regression, ridge, and decision tree to XGBoost with 5-fold out-of-fold cross-validation. XGBoost selected as best performer (MedAPE 0.0942 OOF, R² 0.8876). Enriched with top-5 forward-selected POI features, further improving to MedAPE 0.0817 on test set.
* **Model Evaluation & Interpretation:** SHAP value decomposition, conformal prediction intervals (80% confidence, q = 0.1257), Duan's smearing factor for log-bias correction, piecewise band calibration across 10 price bands.
* **Results Presentation:** User-facing web interface with interactive map, prediction form, and SHAP waterfall chart for interpreting individual price estimates.

### Technologies Used

* **Data Processing:** Python, pandas, numpy, PostgreSQL, Jupyter Notebook, ydata_profiling
* **Machine Learning:** XGBoost, scikit-learn (Pipeline, ColumnTransformer, BallTree), joblib
* **Spatial Data & Visualization:** OSRM API, Overpass Turbo, GeoJSON, Matplotlib, LaTeX

---

## 2. Web Application (`webapp/`)

Flask 3.0 web application for apartment price prediction, deployed via Docker + Gunicorn. The REST API accepts 22 structured features on a `/predict` endpoint and returns a calibrated price, price per m², asymmetric 80% conformal confidence interval, and SHAP feature contributions as multipliers and percentage impact. The frontend uses Leaflet.js for an interactive map with point-in-polygon geofencing, Turf.js for spatial computation, and Plotly.js for SHAP waterfall chart visualization.

| Path | Purpose |
|------|---------|
| `app.py` | Flask application entry point |
| `config.py` | Application configuration |
| `src/` | POI feature computation, preprocessing pipeline, prediction loader |
| `templates/` | Jinja2 HTML templates |
| `static/` | CSS, JavaScript, map data, logos |
| `models/` | Trained model artifacts |
| `data/` | Reference data for runtime POI computation |
| `Dockerfile` | Container build definition |
| `docker-compose.yml` | Docker Compose orchestration |

### Prerequisites

* [Docker](https://docs.docker.com/get-docker/) (for containerized deployment)
* Python 3.12+ (for local development)

### File Structure

Trained models and reference data are included in the repository:

```text
webapp/
├── models/
│   ├── tuned_xgb_pipeline.joblib       # Trained XGBoost pipeline
│   ├── cleaning_params.joblib           # Removal policy parameters
│   └── smearing_factor.npy             # Duan's smearing correction factor
├── data/
│   ├── district_mapping.csv            # District ID → name mapping
│   ├── region_mapping.csv              # Region ID → name mapping
│   ├── brand_stores.csv                # Grocery store POI dataset
│   ├── public_transport.csv            # Public transport stops dataset
│   └── city_centers.csv                # Regional city center coordinates
└── static/
    └── data/
        └── okresy_boundaries_simplified_opt.geojson.gz  # District boundaries
```

### Running the Application

```bash
# Local development
cd webapp
pip install -r requirements.txt
python app.py

# Docker (production)
cd webapp
docker compose up -d
```

---

<img src="https://fit.cvut.cz/static/images/fit-cvut-logo-en.svg" alt="FIT CTU logo" height="200">

This software was developed with the support of the **Faculty of Information Technology, Czech Technical University in Prague**.

For more information, visit [fit.cvut.cz](https://fit.cvut.cz).

## License

See [LICENSE](LICENSE).
