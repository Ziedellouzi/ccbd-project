import argparse
import json
from pathlib import Path

import pandas as pd


# Reader module
#
# This script loads:
# - dataset version 1
# - dataset version 2
# - both versions together
#
# It also handles schema evolution by:
# - detecting missing columns
# - adding missing columns with NULL values
# - enforcing a unified schema contract


BASE_DIR = Path(__file__).resolve().parents[1]

DATASET_ID = "events"


def build_paths(size=None):
    """
    Build dataset paths dynamically.

    If size is provided (S/M/L),
    datasets are loaded from:
    data/S/...
    data/M/...
    data/L/...

    Otherwise, datasets are loaded from:
    data/...
    """

    data_root = BASE_DIR / "data"

    if size is not None:
        data_root = data_root / size

    v1_path = data_root / "curated" / DATASET_ID / "v1"
    v2_path = data_root / "curated" / DATASET_ID / "v2"

    schema_path = (
        data_root
        / "published"
        / DATASET_ID
        / "schema.json"
    )

    return v1_path, v2_path, schema_path


def load_schema(schema_path):
    """Load schema contract from schema.json."""

    with open(schema_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_expected_columns(schema):
    """Extract expected columns from schema contract."""

    return list(schema["columns"].keys())


def load_v1(v1_path):
    """Load dataset version 1."""

    return pd.read_parquet(v1_path)


def load_v2(v2_path):
    """Load dataset version 2."""

    return pd.read_parquet(v2_path)


def align_schema(df, expected_columns):
    """
    Align dataset schema with the schema contract.

    Missing columns are added with NULL values.
    Columns are reordered consistently.
    """

    for col in expected_columns:
        if col not in df.columns:
            df[col] = None

    return df[expected_columns]


def load_all(df_v1, df_v2):
    """Combine v1 and v2 into one unified dataset."""

    return pd.concat([df_v1, df_v2], ignore_index=True)


def run_query(df):
    """
    Run the fixed analytics query.

    The query:
    - filters EU events
    - filters a time range
    - groups by event_type
    - computes counts and average values
    """

    df_filtered = df[
        (df["region"] == "EU")
        & (df["ts"] >= "2026-01-01")
        & (df["ts"] <= "2026-03-01")
    ]

    return df_filtered.groupby("event_type").agg(
        count=("event_type", "count"),
        avg_value=("value", "mean")
    ).reset_index()


def main():

    parser = argparse.ArgumentParser(
        description="Read schema evolution datasets."
    )

    parser.add_argument(
        "--size",
        choices=["S", "M", "L"],
        default=None,
        help="Dataset size to load: S, M, or L"
    )

    args = parser.parse_args()

    # Build dataset paths dynamically
    v1_path, v2_path, schema_path = build_paths(args.size)

    print("Loading datasets...")
    print(f"v1 path: {v1_path}")
    print(f"v2 path: {v2_path}")
    print(f"schema path: {schema_path}")

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

    print("\nv1 shape:", df_v1.shape)
    print("v2 shape:", df_v2.shape)
    print("combined shape:", df_all.shape)

    print("\nUnified columns:")
    print(list(df_all.columns))

    print("\nQuery result:")
    print(run_query(df_all))


if __name__ == "__main__":
    main()