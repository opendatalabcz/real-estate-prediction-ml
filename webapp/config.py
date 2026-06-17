from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent

MODEL_PATH = PROJECT_ROOT / "models" / "tuned_xgb_pipeline.joblib"
CLEANING_PARAMS_PATH = PROJECT_ROOT / "models" / "cleaning_params.joblib"

DISTRICT_MAPPING_PATH = PROJECT_ROOT / "data" / "district_mapping.csv"
REGION_MAPPING_PATH = PROJECT_ROOT / "data" / "region_mapping.csv"
BOUNDARIES_OPT_GZ_PATH = PROJECT_ROOT / "static" / "data" / "okresy_boundaries_simplified_opt.geojson.gz"

TEMPLATES_DIR = PROJECT_ROOT / "templates"
STATIC_DIR = PROJECT_ROOT / "static"
