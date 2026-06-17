# Real Estate Price Prediction ML

Prediction of apartment offer prices in the Czech Republic using machine learning.

<img src="https://fit.cvut.cz/static/images/fit-cvut-logo-en.svg" alt="FIT CTU logo" height="200">

This software was developed with the support of the **Faculty of Information Technology, Czech Technical University in Prague**.

For more information, visit [fit.cvut.cz](https://fit.cvut.cz).

## Project Structure

### `notebooks_data/` — Research & Analysis

| Directory | Contents |
|-----------|----------|
| `NBS/` | Jupyter notebooks (EDA, preprocessing, modeling) + LaTeX thesis report |
| `data/` | Raw and processed datasets (apartments, POIs, transport, city centers) |
| `src/` | Research Python scripts (scraping, preprocessing, feature engineering, model training) |
| `scripts/` | Utility scripts (travel times, transport merging, model training, plot generation) |
| `models/` | Trained XGBoost pipelines (.joblib) |
| `artifacts/` | Experiment results and evaluations |

### `webapp/` — Production Flask Web Application

Flask 3.0 web app for apartment price prediction, deployed via Docker + Gunicorn.

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

## Key Files

- `notebooks_data/NBS/report.tex` — Full thesis report (LaTeX, ~45 pages)
- `notebooks_data/NBS/preprocess.ipynb` — Data cleaning and preprocessing
- `notebooks_data/NBS/model.ipynb` — Main XGBoost model development
- `notebooks_data/src/pipe.py` — Scikit-learn preprocessing pipeline
- `notebooks_data/src/poi_models_workflow.py` — POI feature engineering workflow
- `webapp/app.py` — Web application API (`/predict` endpoint)
- `webapp/src/predict_loader.py` — Singleton model loader with SHAP explainer

## Setup

```bash
# Webapp
cd webapp
pip install -r requirements.txt
python app.py

# Docker
cd webapp
docker compose up -d
```

## License

See [LICENSE](LICENSE).
