import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


SCHEMA_FIELDS = [
    {"name": "ts", "type": "timestamp[ns]", "required": True},
    {"name": "user_id", "type": "int64", "required": True},
    {"name": "region", "type": "string", "required": True},
    {"name": "event_type", "type": "string", "required": True},
    {"name": "value", "type": "double", "required": True},
    {"name": "device_type", "type": "string", "required": False},
]


def generate_v1(n_rows: int, rng: np.random.Generator) -> pd.DataFrame:
    """Generate the baseline dataset schema."""
    return pd.DataFrame({
        "ts": pd.to_datetime("2026-01-01") + pd.to_timedelta(
            rng.integers(0, 60 * 24 * 30, n_rows),
            unit="m"
        ),
        "user_id": rng.integers(1, 1000, n_rows).astype("int64"),
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
    """Generate the evolved dataset schema."""
    return pd.DataFrame({
        "ts": pd.to_datetime("2026-02-01") + pd.to_timedelta(
            rng.integers(0, 60 * 24 * 30, n_rows),
            unit="m"
        ),
        "user_id": rng.integers(1, 1000, n_rows).astype("int64"),
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


def clear_parquet_files(output_dir: Path) -> None:
    if not output_dir.exists():
        return

    for file_path in output_dir.glob("*.parquet"):
        file_path.unlink()


def write_parquet_parts(
    df: pd.DataFrame,
    output_dir: Path,
    files_per_version: int
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    clear_parquet_files(output_dir)

    chunks = np.array_split(df, files_per_version)

    for index, chunk in enumerate(chunks):
        output_path = output_dir / f"part-{index:05d}.parquet"
        chunk.to_parquet(output_path, index=False)


def write_schema_contract(output_path: Path, dataset_id: str) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    schema_contract = {
        "dataset_id": dataset_id,
        "description": "Schema contract used by the schema evolution reader.",
        "fields": SCHEMA_FIELDS
    }

    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(schema_contract, file, indent=2)


def generate_datasets(
    rows_per_version: int,
    dataset_id: str = "events",
    output_root: str = "data",
    seed: int = 42,
    files_per_version: int = 1
) -> None:
    output_root_path = Path(output_root)

    curated_root = output_root_path / "curated" / dataset_id
    published_root = output_root_path / "published" / dataset_id

    rng = np.random.default_rng(seed)

    df_v1 = generate_v1(rows_per_version, rng)
    df_v2 = generate_v2(rows_per_version, rng)

    write_parquet_parts(
        df=df_v1,
        output_dir=curated_root / "v1",
        files_per_version=files_per_version
    )

    write_parquet_parts(
        df=df_v2,
        output_dir=curated_root / "v2",
        files_per_version=files_per_version
    )

    write_schema_contract(
        output_path=published_root / "schema.json",
        dataset_id=dataset_id
    )

    print("Dataset generation completed.")
    print(f"Rows per version: {rows_per_version}")
    print(f"v1 path: {curated_root / 'v1'}")
    print(f"v2 path: {curated_root / 'v2'}")
    print(f"schema contract: {published_root / 'schema.json'}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic datasets for the schema evolution project."
    )

    parser.add_argument("--dataset-id", default="events")
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--files-per-version", type=int, default=1)
    parser.add_argument("--output-root", default="data")

    args = parser.parse_args()

    generate_datasets(
        rows_per_version=args.rows,
        dataset_id=args.dataset_id,
        output_root=args.output_root,
        seed=args.seed,
        files_per_version=args.files_per_version
    )


if __name__ == "__main__":
    main()