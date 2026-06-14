from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.poi_models_workflow import (
    DEFAULT_FEATURE_VARIANTS,
    MINIMAL_FAST_VARIANTS,
    SELECTED_CLEANING_POLICY,
    add_uplift_vs_baseline,
    extract_locked_xgb_feature_importances,
    get_locked_xgb_artifacts,
    load_experiment_data,
    run_feature_variant_cv,
    run_final_test_comparison,
    run_reference_baseline_stack,
    run_robustness_checks,
    save_output_tables,
    select_best_variant_per_model,
    subgroup_summary,
)


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the POI / accessibility feature experiment end-to-end."
    )
    parser.add_argument(
        "--variant-set",
        choices=["default", "fast"],
        default="default",
        help="Choose the full experiment matrix or a smaller debugging set.",
    )
    parser.add_argument(
        "--output-dir",
        default="artifacts/poi_models",
        help="Directory where comparison tables will be written.",
    )
    return parser.parse_args()


def resolve_feature_variants(variant_set: str):
    if variant_set == "fast":
        return MINIMAL_FAST_VARIANTS
    return DEFAULT_FEATURE_VARIANTS


def main() -> None:
    args = get_args()
    output_dir = Path(args.output_dir)
    feature_variants = resolve_feature_variants(args.variant_set)

    pd.options.display.float_format = "{:,.4f}".format

    data = load_experiment_data()

    reference_table = run_reference_baseline_stack(
        data.train_fe,
        cleaning_policy=SELECTED_CLEANING_POLICY,
    )
    variant_cv_results, _ = run_feature_variant_cv(
        data.train_fe,
        feature_variants=feature_variants,
        cleaning_policy=SELECTED_CLEANING_POLICY,
    )
    variant_cv_uplift = add_uplift_vs_baseline(variant_cv_results)
    best_variant_per_model = select_best_variant_per_model(variant_cv_results)
    final_test_table = run_final_test_comparison(
        train_fe=data.train_fe,
        test_fe=data.test_fe,
        best_variant_per_model=best_variant_per_model,
        feature_variants=feature_variants,
        cleaning_policy=SELECTED_CLEANING_POLICY,
    )
    robustness_table = run_robustness_checks(
        data.train_fe,
        feature_variants=feature_variants,
    )

    saved_tables = save_output_tables(
        reference_table=reference_table,
        variant_cv_results=variant_cv_results,
        variant_cv_uplift=variant_cv_uplift,
        best_variant_per_model=best_variant_per_model,
        final_test_table=final_test_table,
        robustness_table=robustness_table,
        output_dir=output_dir,
    )

    locked_xgb = get_locked_xgb_artifacts(
        train_fe=data.train_fe,
        test_fe=data.test_fe,
        best_variant_per_model=best_variant_per_model,
        feature_variants=feature_variants,
        cleaning_policy=SELECTED_CLEANING_POLICY,
    )
    if locked_xgb is not None:
        output_dir.mkdir(parents=True, exist_ok=True)

        price_band_summary = subgroup_summary(
            locked_xgb["evaluation_df"],
            "price_band",
        )
        region_summary = subgroup_summary(
            locked_xgb["evaluation_df"],
            "locality_region_id",
        )
        feature_importance = extract_locked_xgb_feature_importances(
            train_fe=data.train_fe,
            locked_xgb_model=locked_xgb["model"],
            cleaning_policy=SELECTED_CLEANING_POLICY,
        )

        price_band_summary.to_csv(output_dir / "locked_xgb_price_band_summary.csv", index=False)
        region_summary.to_csv(output_dir / "locked_xgb_region_summary.csv", index=False)
        feature_importance.to_csv(output_dir / "locked_xgb_feature_importances.csv", index=False)

    print("POI validation report:")
    print(data.poi_validation_report.to_string())
    print()

    print("Best variant per model:")
    print(best_variant_per_model.to_string(index=False))
    print()

    print("Final holdout comparison:")
    print(final_test_table.to_string(index=False))
    print()

    print(f"Saved {len(saved_tables)} core tables to {output_dir.resolve()}")
    for name in saved_tables:
        print(output_dir / name)

    if locked_xgb is not None:
        print(output_dir / "locked_xgb_price_band_summary.csv")
        print(output_dir / "locked_xgb_region_summary.csv")
        print(output_dir / "locked_xgb_feature_importances.csv")


if __name__ == "__main__":
    main()
