import csv
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, Tuple

import boto3


MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "ccbd-data"

DATASET_ID = "events"
RESULTS_DIR = Path("results")

DATASET_SIZES = {
    "S": 10_000,
    "M": 100_000,
    "L": 1_000_000,
}


def create_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def create_bucket_if_missing(s3_client) -> None:
    response = s3_client.list_buckets()
    bucket_names = [bucket["Name"] for bucket in response["Buckets"]]

    if BUCKET_NAME not in bucket_names:
        s3_client.create_bucket(Bucket=BUCKET_NAME)


def clear_prefix(s3_client, prefix: str) -> None:
    while True:
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=prefix,
        )

        objects = [
            {"Key": obj["Key"]}
            for obj in response.get("Contents", [])
        ]

        if objects:
            s3_client.delete_objects(
                Bucket=BUCKET_NAME,
                Delete={"Objects": objects},
            )

        if not response.get("IsTruncated"):
            break


def clear_dataset_from_bucket(s3_client) -> None:
    clear_prefix(s3_client, f"curated/{DATASET_ID}/")
    clear_prefix(s3_client, f"published/{DATASET_ID}/")


def run_command(command: list[str]) -> Tuple[float, str]:
    start = time.perf_counter()

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
    )

    elapsed = time.perf_counter() - start

    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise RuntimeError(f"Command failed: {' '.join(command)}")

    return elapsed, result.stdout


def directory_size(path: Path) -> int:
    if not path.exists():
        return 0

    return sum(
        file.stat().st_size
        for file in path.rglob("*")
        if file.is_file()
    )


def list_prefix(s3_client, prefix: str) -> Tuple[int, int, float]:
    start = time.perf_counter()

    object_count = 0
    total_bytes = 0
    continuation_token = None

    while True:
        request = {
            "Bucket": BUCKET_NAME,
            "Prefix": prefix,
        }

        if continuation_token:
            request["ContinuationToken"] = continuation_token

        response = s3_client.list_objects_v2(**request)

        for obj in response.get("Contents", []):
            if not obj["Key"].endswith("/"):
                object_count += 1
                total_bytes += obj["Size"]

        if not response.get("IsTruncated"):
            break

        continuation_token = response.get("NextContinuationToken")

    elapsed = time.perf_counter() - start

    return object_count, total_bytes, elapsed


def safe_throughput_mb_s(total_bytes: int, elapsed_s: float) -> float:
    if elapsed_s == 0:
        return 0.0

    return (total_bytes / 1_000_000) / elapsed_s


def mode_prefix(mode: str) -> str:
    if mode == "v1":
        return f"curated/{DATASET_ID}/v1/"

    if mode == "v2":
        return f"curated/{DATASET_ID}/v2/"

    return f"curated/{DATASET_ID}/"


def parse_rows_loaded(output: str) -> int:
    match = re.search(r"Rows loaded:\s*(\d+)", output)

    if match:
        return int(match.group(1))

    return -1


def benchmark_size(
    size_label: str,
    rows_per_version: int,
    s3_client
) -> list[Dict[str, object]]:
    local_root = Path("data_bench") / size_label
    download_root = Path("downloads_bench") / size_label

    print(f"\n=== Benchmarking {size_label} ({rows_per_version} rows/version) ===")

    # Each script keeps one responsibility. The benchmark only runs and times them.
    print("1. Generating data...")
    run_command([
        sys.executable,
        "src/dataset_gen.py",
        "--rows",
        str(rows_per_version),
        "--output-root",
        str(local_root),
    ])

    print("2. Clearing old objects from MinIO...")
    clear_dataset_from_bucket(s3_client)

    print("3. Uploading data...")
    upload_bytes = directory_size(local_root)

    upload_time_s, _ = run_command([
        sys.executable,
        "src/upload.py",
        "--local-root",
        str(local_root),
    ])

    upload_throughput = safe_throughput_mb_s(
        upload_bytes,
        upload_time_s,
    )

    print("4. Downloading data...")
    download_time_s, _ = run_command([
        sys.executable,
        "src/download.py",
        "--output-root",
        str(download_root),
    ])

    download_bytes = directory_size(download_root)

    download_throughput = safe_throughput_mb_s(
        download_bytes,
        download_time_s,
    )

    rows = []

    for mode in ["v1", "v2", "mixed"]:
        print(f"5. Reading with reader_s3.py --mode {mode}")

        object_count, total_stored_bytes, listing_time_s = list_prefix(
            s3_client,
            mode_prefix(mode),
        )

        query_time_s, reader_output = run_command([
            sys.executable,
            "src/reader_s3.py",
            "--mode",
            mode,
        ])

        rows.append({
            "dataset_size_label": size_label,
            "rows_per_version": rows_per_version,
            "mode": mode,
            "upload_bytes": upload_bytes,
            "upload_time_s": upload_time_s,
            "upload_throughput_mb_s": upload_throughput,
            "download_bytes": download_bytes,
            "download_time_s": download_time_s,
            "download_throughput_mb_s": download_throughput,
            "listing_time_s": listing_time_s,
            "object_count": object_count,
            "total_stored_bytes": total_stored_bytes,
            "query_time_s": query_time_s,
            "rows_loaded": parse_rows_loaded(reader_output),
        })

    return rows


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    s3_client = create_s3_client()
    create_bucket_if_missing(s3_client)

    all_rows = []

    for size_label, rows_per_version in DATASET_SIZES.items():
        all_rows.extend(
            benchmark_size(
                size_label=size_label,
                rows_per_version=rows_per_version,
                s3_client=s3_client,
            )
        )

    output_path = RESULTS_DIR / "results.csv"

    with open(output_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=all_rows[0].keys())
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nBenchmark completed: {output_path}")


if __name__ == "__main__":
    main()