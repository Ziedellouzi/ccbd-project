import argparse
import json
from typing import Dict, List

import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as fs


MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "ccbd-data"


TYPE_MAPPING: Dict[str, pa.DataType] = {
    "timestamp[ns]": pa.timestamp("ns"),
    "int64": pa.int64(),
    "string": pa.string(),
    "double": pa.float64(),
}


def create_s3_filesystem():
    return fs.S3FileSystem(
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        endpoint_override=MINIO_ENDPOINT,
        scheme="http",
        region="us-east-1",
    )


def load_schema_contract(s3_filesystem, dataset_id: str) -> List[dict]:
    schema_path = f"{BUCKET_NAME}/published/{dataset_id}/schema.json"

    with s3_filesystem.open_input_file(schema_path) as file:
        contract = json.loads(file.read().decode("utf-8"))

    if "fields" not in contract:
        raise ValueError("Invalid schema contract: missing 'fields' key.")

    return contract["fields"]


def expected_arrow_schema(fields: List[dict]) -> pa.Schema:
    arrow_fields = []

    for field in fields:
        field_type = field["type"]

        if field_type not in TYPE_MAPPING:
            raise ValueError(f"Unsupported type in schema contract: {field_type}")

        arrow_fields.append(
            pa.field(field["name"], TYPE_MAPPING[field_type])
        )

    return pa.schema(arrow_fields)


def load_dataset_as_table(
    s3_filesystem,
    dataset_id: str,
    version: str
) -> pa.Table:
    dataset_path = f"{BUCKET_NAME}/curated/{dataset_id}/{version}"

    dataset = ds.dataset(
        dataset_path,
        format="parquet",
        filesystem=s3_filesystem
    )

    return dataset.to_table()


def align_table_to_contract(
    table: pa.Table,
    fields: List[dict]
) -> pa.Table:
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
            continue

        if required:
            raise ValueError(f"Missing required column: {name}")

        # Optional columns added in newer schemas become null for older data.
        columns.append(
            pa.array([None] * table.num_rows, type=expected_type)
        )

    return pa.Table.from_arrays(
        columns,
        schema=expected_arrow_schema(fields)
    )


def validate_table(table: pa.Table, fields: List[dict]) -> None:
    expected_schema = expected_arrow_schema(fields)

    if table.schema != expected_schema:
        raise ValueError(
            "Schema validation failed.\n"
            f"Expected: {expected_schema}\n"
            f"Actual: {table.schema}"
        )


def read_version(
    s3_filesystem,
    dataset_id: str,
    version: str,
    fields: List[dict]
) -> pa.Table:
    table = load_dataset_as_table(s3_filesystem, dataset_id, version)
    table = align_table_to_contract(table, fields)

    validate_table(table, fields)

    return table


def read_mixed(
    s3_filesystem,
    dataset_id: str,
    fields: List[dict]
) -> pa.Table:
    table_v1 = read_version(s3_filesystem, dataset_id, "v1", fields)
    table_v2 = read_version(s3_filesystem, dataset_id, "v2", fields)

    mixed_table = pa.concat_tables([table_v1, table_v2])
    validate_table(mixed_table, fields)

    return mixed_table


def fixed_analytics_query(
    table: pa.Table,
    region: str,
    start_ts: str,
    end_ts: str
) -> pa.Table:
    start_np = np.datetime64(start_ts)
    end_np = np.datetime64(end_ts)

    mask = (
        (table["region"].to_numpy() == region)
        & (table["ts"].to_numpy() >= start_np)
        & (table["ts"].to_numpy() < end_np)
    )

    filtered = table.filter(pa.array(mask))

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
        description="Read schema-evolved datasets from MinIO."
    )

    parser.add_argument("--dataset-id", default="events")
    parser.add_argument("--mode", choices=["v1", "v2", "mixed"], required=True)
    parser.add_argument("--region", default="EU")
    parser.add_argument("--start-ts", default="2026-01-01")
    parser.add_argument("--end-ts", default="2026-03-01")

    args = parser.parse_args()

    s3_filesystem = create_s3_filesystem()
    fields = load_schema_contract(s3_filesystem, args.dataset_id)

    if args.mode == "mixed":
        table = read_mixed(s3_filesystem, args.dataset_id, fields)
    else:
        table = read_version(
            s3_filesystem,
            args.dataset_id,
            args.mode,
            fields
        )

    result = fixed_analytics_query(
        table=table,
        region=args.region,
        start_ts=args.start_ts,
        end_ts=args.end_ts
    )

    print("Storage: MinIO / S3")
    print(f"Bucket: {BUCKET_NAME}")
    print(f"Mode: {args.mode}")
    print(f"Rows loaded: {table.num_rows}")

    print("\nSchema:")
    print(table.schema)

    print("\nQuery result:")
    print(result.to_pandas())


if __name__ == "__main__":
    main()