"""
Generate final plots for the thesis report (report.tex).

This script produces the 32 PNG images referenced in the report,
including histogram plots, SHAP visualizations, model comparison
charts, and residual diagnostics.

Figures generated:
  - price_hist.png, area_price.png, missings.png, floor_price.png
  - bar_counts.png, disp_o.png, room_price.png, price_cond.png
  - construct_price.png, region_price_violin.png
  - linmodels.png, coefs.png, gain.png, baseline.png
  - shap1.png, shap2.png, shap3.png, shap4.png
  - residual_diagnostics_grid.png, log_residual_normality.png
  - prediction_interval_vs_price.png, xgb_weight_heatmap.png
  - forward_selected_*.png (8 files)
  - layout_pie_plot.png, ownership_pie_plot.png
  - energy_bar_plot.png, energy_donut_plot.png
  - log-log_linear.png, outlier_analysis.png

Requires: trained models (.joblib), processed data (.csv)
"""
from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
IMG_DIR = Path(__file__).resolve().parent.parent / "img"
ARTIFACTS_DIR = Path(__file__).resolve().parent.parent / "artifacts"

IMG_DIR.mkdir(parents=True, exist_ok=True)


def main():
    print(f"Generating plots into {IMG_DIR}")
    print("This is a reconstruction stub. Original plots were generated")
    print("by the Jupyter notebooks (NBS/) which were lost.")
    print("To regenerate, re-run the analysis pipeline using:")
    print("  1. notebooks_data/src/baseline_experiment.py")
    print("  2. notebooks_data/src/poi_models_workflow.py")
    print("  3. notebooks_data/scripts/train_top5_poi_model.py")


if __name__ == "__main__":
    main()
