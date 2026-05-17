import argparse
import os
import time

import pandas as pd

from reader import (
    build_paths,
    load_schema,
    get_expected_columns,
    load_v1,
    load_v2,
    align_schema,
    load_all,
    run_query
)


# Benchmark module
#
# This script measures the execution time
# of the analytics query on:
# - v1
# - v2
# - combined dataset (v1 + v2)
#
# It supports:
# - small datasets
# - S / M / L experiments


BASE_DIR = os.path.dirname(os.path.dirname(__file__))

RESULTS_DIR = os.path.join(BASE_DIR, "results")


def benchmark_dataset(name, df):
    """
    Run the analytics query on one dataset
    and measure execution time.
    """

    start_time = time.time()

    run_query(df)

    execution_time = time.time() - start_time

    return {
        "dataset": name,
        "execution_time_sec": execution_time,
        "num_rows": len(df)
    }


def main():

    parser = argparse.ArgumentParser(
        description="Benchmark schema evolution datasets."
    )

    parser.add_argument(
        "--size",
        choices=["S", "M", "L"],
        default=None,
        help="Dataset size to benchmark: S, M, or L"
    )

    args = parser.parse_args()

    # Build dataset paths dynamically
    v1_path, v2_path, schema_path = build_paths(args.size)

    print("Loading datasets for benchmark...")
    print(f"v1 path: {v1_path}")
    print(f"v2 path: {v2_path}")

    # Load schema contract
    schema = load_schema(schema_path)

    expected_columns = get_expected_columns(schema)

    # Load datasets
    df_v1 = load_v1(v1_path)
    df_v2 = load_v2(v2_path)

    # Align schemas
    df_v1 = align_schema(df_v1, expected_columns)
    df_v2 = align_schema(df_v2, expected_columns)

    # Combine datasets
    df_all = load_all(df_v1, df_v2)

    # Run benchmarks
    results = [
        benchmark_dataset("v1", df_v1),
        benchmark_dataset("v2", df_v2),
        benchmark_dataset("v1+v2", df_all)
    ]

    # Convert results into DataFrame
    results_df = pd.DataFrame(results)

    print("\nBenchmark Results:")
    print(results_df)

    # Create results directory if needed
    os.makedirs(RESULTS_DIR, exist_ok=True)

    # Save benchmark results
    if args.size is not None:
        results_path = os.path.join(
            RESULTS_DIR,
            f"results_{args.size}.csv"
        )
    else:
        results_path = os.path.join(
            RESULTS_DIR,
            "results.csv"
        )

    results_df.to_csv(results_path, index=False)

    print(f"\nBenchmark results saved to: {results_path}")


if __name__ == "__main__":
    main()