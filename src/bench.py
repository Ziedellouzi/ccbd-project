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
# Si ccbd-data n’existe pas, elle le crée.
    if BUCKET_NAME not in bucket_names:
        s3_client.create_bucket(Bucket=BUCKET_NAME)

# Cette fonction supprime tous les objets qui commencent par un certain prefix
def clear_prefix(s3_client, prefix: str) -> None:
    while True:
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=prefix,
        )
# Construire la liste des objets à supprimer
        objects = [
            {"Key": obj["Key"]}
            for obj in response.get("Contents", [])
        ]

        if objects:
            s3_client.delete_objects(
                Bucket=BUCKET_NAME,
                Delete={"Objects": objects},
            )
# Si la liste n’est pas tronquée (Il reste encore des objets à lister) , on a fini, on sort de la boucle.
        if not response.get("IsTruncated"):
            break


def clear_dataset_from_bucket(s3_client) -> None:
    clear_prefix(s3_client, f"curated/{DATASET_ID}/")
    clear_prefix(s3_client, f"published/{DATASET_ID}/")

# run_command() = lance une commande + mesure son temps + récupère son output
def run_command(command: list[str]) -> Tuple[float, str]:
    # Elle sert à mesurer le temps d’exécution de la commande.
    start = time.perf_counter()
# permet à un script Python de lancer un autre programme ou un autre script.
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
    )
# Cette ligne calcule le temps passé.
    elapsed = time.perf_counter() - start

    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise RuntimeError(f"Command failed: {' '.join(command)}")

    return elapsed, result.stdout
    
# Combien de bytes allons-nous uploader vers MinIO ?
# Elle additionne la taille de tous les fichiers.
# Donc si :
# v1 parquet = 200 KB 
# v2 parquet = 230 KB
# schema.json = 1 KB
# alors :
# directory_size = 431 KB
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
# Nombre d’objets sous le prefix, Taille totale des objets listé, Temps nécessaire pour faire le listing 
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

# Elle exécute tout le benchmark pour une taille donnée
def benchmark_size(
    size_label: str,
    rows_per_version: int,
    s3_client
) -> list[Dict[str, object]]:
    local_root = Path("data_bench") / size_label
    download_root = Path("downloads_bench") / size_label

    print(f"\n=== Benchmarking {size_label} ({rows_per_version} rows/version) ===")
# Étape 1 — Générer les données
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
# Étape 2 — Nettoyer MinIO
    print("2. Clearing old objects from MinIO...")
    clear_dataset_from_bucket(s3_client)
# Étape 3 — Upload vers MinIO
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
# Étape 4 — Download depuis MinIO
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
# Étape 5 — Lire v1, v2 et mixed
    for mode in ["v1", "v2", "mixed"]:
        print(f"5. Reading with reader_s3.py --mode {mode}")
# Listing dans MinIO
        object_count, total_stored_bytes, listing_time_s = list_prefix(
            s3_client,
            mode_prefix(mode),
        )
# Lancer le reader
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
            # Extraire le nombre de lignes
            "rows_loaded": parse_rows_loaded(reader_output),
        })

    return rows


def main() -> None:
    # Créer le dossier results
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
# Cette partie écrit les résultats dans un fichier CSV
    with open(output_path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=all_rows[0].keys())
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nBenchmark completed: {output_path}")


if __name__ == "__main__":
    main()
