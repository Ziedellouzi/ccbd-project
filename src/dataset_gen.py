import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


BASE_SCHEMA_FIELDS = [
    {"name": "ts", "type": "timestamp[ns]", "required": True},
    {"name": "user_id", "type": "int64", "required": True},
    {"name": "region", "type": "string", "required": True},
    {"name": "event_type", "type": "string", "required": True},
    {"name": "value", "type": "double", "required": True},
    {"name": "device_type", "type": "string", "required": False},
]


def generate_v1(n_rows: int, rng: np.random.Generator) -> pd.DataFrame:
    """Generate version 1 of the dataset with the baseline schema."""
    return pd.DataFrame({
        "ts": pd.to_datetime("2026-01-01") + pd.to_timedelta(
            rng.integers(0, 60 * 24 * 30, n_rows), unit="m"
        ),
        "user_id": rng.choice(
            np.arange(1, 1000),
            size=n_rows,
            p=rng.dirichlet(np.ones(999))
        ).astype("int64"),
        "region": rng.choice(
            ["EU", "US", "ASIA"],
            size=n_rows,
            p=[0.5, 0.3, 0.2]
        ),
        "event_type": rng.choice(
            ["view", "click", "purchase"],
            size=n_rows,
            p=[0.7, 0.25, 0.05]
        ),
        "value": rng.exponential(scale=30, size=n_rows).astype("float64")
    })


def generate_v2(n_rows: int, rng: np.random.Generator) -> pd.DataFrame:
    """Generate version 2 of the dataset with one added nullable column."""
    df = pd.DataFrame({
        "ts": pd.to_datetime("2026-02-01") + pd.to_timedelta(
            rng.integers(0, 60 * 24 * 30, n_rows), unit="m"
        ),
        "user_id": rng.choice(
            np.arange(1, 1000),
            size=n_rows,
            p=rng.dirichlet(np.ones(999))
        ).astype("int64"),
        "region": rng.choice(
            ["EU", "US", "ASIA"],
            size=n_rows,
            p=[0.5, 0.3, 0.2]
        ),
        "event_type": rng.choice(
            ["view", "click", "purchase"],
            size=n_rows,
            p=[0.7, 0.25, 0.05]
        ),
        "value": rng.exponential(scale=30, size=n_rows).astype("float64"),
        "device_type": rng.choice(
            ["mobile", "desktop"],
            size=n_rows,
            p=[0.7, 0.3]
        )
    })

    return df


def write_parquet_parts(df: pd.DataFrame, output_dir: Path, files_per_version: int) -> None:
    """Write a dataframe into one or several Parquet files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    chunks = np.array_split(df, files_per_version)

    for i, chunk in enumerate(chunks):
        output_path = output_dir / f"part-{i:05d}.parquet"
        chunk.to_parquet(output_path, index=False)


def write_schema_contract(output_path: Path, dataset_id: str) -> None:
    """Write the expected unified schema contract."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    schema_contract = {
        "dataset_id": dataset_id,
        "description": "Unified schema contract for schema evolution variant.",
        "fields": BASE_SCHEMA_FIELDS
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(schema_contract, f, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate v1 and v2 synthetic event datasets for schema evolution."
    )

    parser.add_argument("--dataset-id", default="events")
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--files-per-version", type=int, default=1)
    parser.add_argument("--output-root", default="data")

    args = parser.parse_args()

    rng = np.random.default_rng(args.seed)

    output_root = Path(args.output_root)
    curated_root = output_root / "curated" / args.dataset_id
    published_root = output_root / "published" / args.dataset_id

    df_v1 = generate_v1(args.rows, rng)
    df_v2 = generate_v2(args.rows, rng)

    write_parquet_parts(df_v1, curated_root / "v1", args.files_per_version)
    write_parquet_parts(df_v2, curated_root / "v2", args.files_per_version)
    write_schema_contract(published_root / "schema.json", args.dataset_id)

    print("Dataset generation completed.")
    print(f"v1 path: {curated_root / 'v1'}")
    print(f"v2 path: {curated_root / 'v2'}")
    print(f"schema contract: {published_root / 'schema.json'}")


if __name__ == "__main__":
    main()
