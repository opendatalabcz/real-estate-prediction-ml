from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.baseline_workflow import (
    CV,
    DEFAULT_TARGET,
    TEST_PATH,
    TRAIN_PATH,
    build_dataset_card,
    build_diagnostics,
    build_final_test_table,
    build_model_catalog,
    fit_model_on_test,
    run_model_cv_comparison,
    save_output_tables,
    select_cleaning_policy,
)


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the structured apartment-price baseline end-to-end."
    )
    parser.add_argument(
        "--output-dir",
        default="artifacts/baseline",
        help="Directory where baseline output tables will be written.",
    )
    return parser.parse_args()


def main() -> None:
    args = get_args()
    output_dir = Path(args.output_dir)

    pd.options.display.float_format = "{:,.4f}".format

    dataset_card = build_dataset_card(TRAIN_PATH, TEST_PATH)
    model_catalog = build_model_catalog()
    train_raw = pd.read_csv(TRAIN_PATH)
    test_raw = pd.read_csv(TEST_PATH)

    policy_results, selected_policy = select_cleaning_policy(
        train_raw,
        target=DEFAULT_TARGET,
        cv=CV,
    )
    cv_results, locked_model_name, locked_builder = run_model_cv_comparison(
        train_raw,
        cleaning_policy=selected_policy,
        target=DEFAULT_TARGET,
        cv=CV,
    )

    locked_evaluation = fit_model_on_test(
        train_raw,
        test_raw,
        locked_builder,
        selected_policy,
        target=DEFAULT_TARGET,
    )
    final_test_table = build_final_test_table(
        model_name=locked_model_name,
        cleaning_policy=selected_policy,
        evaluation=locked_evaluation,
    )
    _, region_summary, category_summary, price_band_summary, worst_predictions = build_diagnostics(
        locked_evaluation.cleaned_test_df,
        locked_evaluation.y_test,
        locked_evaluation.predictions,
    )

    saved_tables = save_output_tables(
        dataset_card=dataset_card,
        policy_results=policy_results,
        cv_results=cv_results,
        final_test_table=final_test_table,
        region_summary=region_summary,
        category_summary=category_summary,
        price_band_summary=price_band_summary,
        worst_predictions=worst_predictions,
        output_dir=output_dir,
    )

    print("Dataset snapshot:")
    print(dataset_card.to_string(index=False))
    print()

    print("Model catalog:")
    print(model_catalog.to_string(index=False))
    print()

    print(f"Selected deployable cleaning policy: {selected_policy}")
    print(f"Locked final model from OOF CV: {locked_model_name}")
    print()

    print("Final holdout evaluation:")
    print(final_test_table.to_string(index=False))
    print()

    print(f"Saved {len(saved_tables)} tables to {output_dir.resolve()}")
    for name in saved_tables:
        print(output_dir / name)


if __name__ == "__main__":
    main()
