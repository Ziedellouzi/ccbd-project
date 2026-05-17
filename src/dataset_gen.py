import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


# Final unified schema expected by the reader.
# v1 will miss "device_type", while v2 will contain it.
# The reader will later use this schema contract to align both versions.
SCHEMA_COLUMNS = {
    "ts": "datetime64[ns]",
    "user_id": "int64",
    "region": "object",
    "event_type": "object",
    "value": "float64",
    "device_type": "object"
}


# Dataset sizes used for the final experiments.
# These sizes are defined in number of rows for practical execution on the cluster.
DATASET_SIZES = {
    "S": 1_000_000,
    "M": 5_000_000,
    "L": 15_000_000
}


def generate_v1(n_rows: int, rng: np.random.Generator) -> pd.DataFrame:
    """
    Generate dataset version 1.

    v1 represents historical data before schema evolution.
    It contains the baseline schema:
    ts, user_id, region, event_type, value
    """

    return pd.DataFrame({
        # Random timestamps over January 2026
        "ts": pd.to_datetime("2026-01-01") + pd.to_timedelta(
            rng.integers(0, 60 * 24 * 30, n_rows), unit="m"
        ),

        # Skewed user activity:
        # a few users generate many events, many users generate few events.
        "user_id": rng.choice(
            np.arange(1, 1000),
            size=n_rows,
            p=rng.dirichlet(np.ones(999))
        ).astype("int64"),

        # Non-uniform region distribution
        "region": rng.choice(
            ["EU", "US", "ASIA"],
            size=n_rows,
            p=[0.5, 0.3, 0.2]
        ),

        # Funnel-like event distribution:
        # views are common, clicks are less common, purchases are rare.
        "event_type": rng.choice(
            ["view", "click", "purchase"],
            size=n_rows,
            p=[0.7, 0.25, 0.05]
        ),

        # Exponential distribution:
        # many small values and a few large values.
        "value": rng.exponential(scale=30, size=n_rows).astype("float64")
    })


def generate_v2(n_rows: int, rng: np.random.Generator) -> pd.DataFrame:
    """
    Generate dataset version 2.

    v2 represents newer data after schema evolution.
    It contains the same columns as v1 plus one additional nullable column:
    device_type
    """

    return pd.DataFrame({
        # Random timestamps over February 2026
        "ts": pd.to_datetime("2026-02-01") + pd.to_timedelta(
            rng.integers(0, 60 * 24 * 30, n_rows), unit="m"
        ),

        # Same user activity distribution as v1
        "user_id": rng.choice(
            np.arange(1, 1000),
            size=n_rows,
            p=rng.dirichlet(np.ones(999))
        ).astype("int64"),

        # Same region distribution as v1
        "region": rng.choice(
            ["EU", "US", "ASIA"],
            size=n_rows,
            p=[0.5, 0.3, 0.2]
        ),

        # Same event distribution as v1
        "event_type": rng.choice(
            ["view", "click", "purchase"],
            size=n_rows,
            p=[0.7, 0.25, 0.05]
        ),

        # Same value distribution as v1
        "value": rng.exponential(scale=30, size=n_rows).astype("float64"),

        # New column introduced in v2.
        # Mobile is more frequent than desktop to reflect modern usage patterns.
        "device_type": rng.choice(
            ["mobile", "desktop"],
            size=n_rows,
            p=[0.7, 0.3]
        )
    })


def write_parquet_parts(
    df: pd.DataFrame,
    output_dir: Path,
    files_per_version: int
) -> None:
    """
    Write a DataFrame into one or several Parquet files.

    Splitting the dataset into multiple files is useful for larger datasets
    and better reflects data lake storage layouts.
    """

    output_dir.mkdir(parents=True, exist_ok=True)

    n_rows = len(df)

    # Compute chunk size manually to avoid deprecated numpy behavior.
    chunk_size = (n_rows + files_per_version - 1) // files_per_version

    for i in range(files_per_version):
        start = i * chunk_size
        end = min(start + chunk_size, n_rows)

        chunk = df.iloc[start:end]

        if chunk.empty:
            continue

        output_path = output_dir / f"part-{i:05d}.parquet"
        chunk.to_parquet(output_path, index=False)


def write_schema_contract(output_path: Path, dataset_id: str) -> None:
    """
    Write the schema contract file.

    The schema contract describes the expected unified output schema.
    It is used by the reader to validate and align dataset versions.
    """

    output_path.parent.mkdir(parents=True, exist_ok=True)

    schema_contract = {
        "dataset_id": dataset_id,
        "description": "Unified schema contract for schema evolution variant.",
        "columns": SCHEMA_COLUMNS
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(schema_contract, f, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate v1 and v2 synthetic event datasets for schema evolution."
    )

    # Dataset name used in the storage layout
    parser.add_argument("--dataset-id", default="events")

    # Manual number of rows, used when --size is not provided
    parser.add_argument("--rows", type=int, default=10000)

    # Predefined dataset size: S, M, or L
    parser.add_argument(
        "--size",
        choices=["S", "M", "L"],
        default=None,
        help="Dataset size to generate: S, M, or L"
    )

    # Random seed for reproducibility
    parser.add_argument("--seed", type=int, default=42)

    # Number of Parquet files generated per version
    parser.add_argument("--files-per-version", type=int, default=1)

    # Root output directory
    parser.add_argument("--output-root", default="data")

    args = parser.parse_args()

    rng = np.random.default_rng(args.seed)

    # If --size is provided, use the predefined size.
    # Otherwise, use the custom number of rows from --rows.
    if args.size is not None:
        n_rows = DATASET_SIZES[args.size]
    else:
        n_rows = args.rows

    output_root = Path(args.output_root)

    # If S/M/L is used, create separated folders:
    # data/S/..., data/M/..., data/L/...
    if args.size is not None:
        output_root = output_root / args.size

    # Data lake-like layout
    curated_root = output_root / "curated" / args.dataset_id
    published_root = output_root / "published" / args.dataset_id

    print("Starting dataset generation...")
    print(f"Dataset ID: {args.dataset_id}")
    print(f"Rows per version: {n_rows}")
    print(f"Files per version: {args.files_per_version}")
    print(f"Output root: {output_root}")

    # Generate both dataset versions
    df_v1 = generate_v1(n_rows, rng)
    df_v2 = generate_v2(n_rows, rng)

    # Write datasets as Parquet parts
    write_parquet_parts(df_v1, curated_root / "v1", args.files_per_version)
    write_parquet_parts(df_v2, curated_root / "v2", args.files_per_version)

    # Write schema contract
    write_schema_contract(published_root / "schema.json", args.dataset_id)

    print("Dataset generation completed.")
    print(f"v1 path: {curated_root / 'v1'}")
    print(f"v2 path: {curated_root / 'v2'}")
    print(f"schema contract: {published_root / 'schema.json'}")


if __name__ == "__main__":
    main()