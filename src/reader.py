import argparse
import json
from pathlib import Path
from typing import Dict, List

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds


TYPE_MAPPING: Dict[str, pa.DataType] = {
    "timestamp[ns]": pa.timestamp("ns"),
    "int64": pa.int64(),
    "string": pa.string(),
    "double": pa.float64(),
}


def load_schema_contract(schema_path: Path) -> List[dict]:
    """Load the schema contract from a JSON file."""
    with open(schema_path, "r", encoding="utf-8") as f:
        contract = json.load(f)

    if "fields" not in contract:
        raise ValueError("Invalid schema contract: missing 'fields' key.")

    return contract["fields"]


def expected_arrow_schema(fields: List[dict]) -> pa.Schema:
    """Build the expected Arrow schema from the schema contract."""
    arrow_fields = []

    for field in fields:
        field_name = field["name"]
        field_type = field["type"]

        if field_type not in TYPE_MAPPING:
            raise ValueError(f"Unsupported type in schema contract: {field_type}")

        arrow_fields.append(pa.field(field_name, TYPE_MAPPING[field_type]))

    return pa.schema(arrow_fields)


def load_dataset_as_table(path: Path) -> pa.Table:
    """Load a Parquet dataset with pyarrow.dataset."""
    dataset = ds.dataset(path, format="parquet")
    return dataset.to_table()


def align_table_to_contract(table: pa.Table, fields: List[dict]) -> pa.Table:
    """
    Align dataset schema with the contract.

    Missing optional columns are added as null.
    Missing required columns raise an error.
    Existing columns are cast to the expected types.
    """
    columns = []

    for field in fields:
        name = field["name"]
        expected_type = TYPE_MAPPING[field["type"]]
        required = field.get("required", True)

        if name in table.column_names:
            column = table[name]

            if column.type != expected_type:
                column = column.cast(expected_type)

            columns.append(column)

        else:
            if required:
                raise ValueError(f"Missing required column: {name}")

            null_column = pa.array([None] * table.num_rows, type=expected_type)
            columns.append(null_column)

    return pa.Table.from_arrays(
        columns,
        schema=expected_arrow_schema(fields)
    )


def validate_table(table: pa.Table, fields: List[dict]) -> None:
    """Validate that a table matches the schema contract."""
    expected_schema = expected_arrow_schema(fields)

    if table.schema != expected_schema:
        raise ValueError(
            "Schema validation failed.\n"
            f"Expected: {expected_schema}\n"
            f"Actual: {table.schema}"
        )


def read_version(data_root: Path, dataset_id: str, version: str, fields: List[dict]) -> pa.Table:
    """Read and validate one dataset version."""
    version_path = data_root / "curated" / dataset_id / version

    table = load_dataset_as_table(version_path)
    table = align_table_to_contract(table, fields)
    validate_table(table, fields)

    return table


def read_mixed(data_root: Path, dataset_id: str, fields: List[dict]) -> pa.Table:
    """Read v1 and v2 together with a unified schema."""
    table_v1 = read_version(data_root, dataset_id, "v1", fields)
    table_v2 = read_version(data_root, dataset_id, "v2", fields)

    mixed_table = pa.concat_tables([table_v1, table_v2])
    validate_table(mixed_table, fields)

    return mixed_table


def fixed_analytics_query(table: pa.Table, region: str, start_ts: str, end_ts: str) -> pa.Table:
    """
    Run the fixed analytics query.

    Query:
    - filter by region;
    - filter by timestamp range;
    - group by event_type;
    - compute count and average value.
    """
    start_scalar = pa.scalar(np.datetime64(start_ts, "ns"), type=pa.timestamp("ns"))
    end_scalar = pa.scalar(np.datetime64(end_ts, "ns"), type=pa.timestamp("ns"))

    region_mask = pc.equal(table["region"], region)
    start_mask = pc.greater_equal(table["ts"], start_scalar)
    end_mask = pc.less(table["ts"], end_scalar)

    mask = pc.and_kleene(
        pc.and_kleene(region_mask, start_mask),
        end_mask
    )

    filtered = table.filter(mask)

    if filtered.num_rows == 0:
        return pa.table({
            "event_type": pa.array([], type=pa.string()),
            "value_count": pa.array([], type=pa.int64()),
            "value_mean": pa.array([], type=pa.float64())
        })

    return filtered.group_by("event_type").aggregate([
        ("value", "count"),
        ("value", "mean")
    ])


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read v1, v2, or mixed schema-evolved datasets."
    )

    parser.add_argument("--dataset-id", default="events")
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--mode", choices=["v1", "v2", "mixed"], required=True)
    parser.add_argument("--region", default="EU")
    parser.add_argument("--start-ts", default="2026-01-01")
    parser.add_argument("--end-ts", default="2026-03-01")

    args = parser.parse_args()

    data_root = Path(args.data_root)
    schema_path = data_root / "published" / args.dataset_id / "schema.json"

    fields = load_schema_contract(schema_path)

    if args.mode == "mixed":
        table = read_mixed(data_root, args.dataset_id, fields)
    else:
        table = read_version(data_root, args.dataset_id, args.mode, fields)

    result = fixed_analytics_query(
        table=table,
        region=args.region,
        start_ts=args.start_ts,
        end_ts=args.end_ts
    )

    print(f"Mode: {args.mode}")
    print(f"Rows loaded: {table.num_rows}")
    print("Schema:")
    print(table.schema)
    print("\nQuery result:")
    print(result.to_pandas())


if __name__ == "__main__":
    main()
