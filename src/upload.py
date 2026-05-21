import argparse
from pathlib import Path

import boto3


MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "ccbd-data"


def create_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def upload_directory(s3_client, local_root: Path) -> None:
    if not local_root.exists():
        raise FileNotFoundError(f"Directory not found: {local_root}")

    files = [path for path in local_root.rglob("*") if path.is_file()]

    if not files:
        raise ValueError(f"No files found in: {local_root}")

    for file_path in files:
        # Keep curated/... and published/... as object keys in MinIO.
        object_key = file_path.relative_to(local_root.parent).as_posix()

        print(f"Uploading: {object_key}")

        s3_client.upload_file(
            Filename=str(file_path),
            Bucket=BUCKET_NAME,
            Key=object_key,
        )

    print(f"Uploaded {len(files)} file(s) from {local_root}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Upload generated dataset files to MinIO."
    )

    parser.add_argument(
        "--local-root",
        default="data",
        help="Local root containing curated/ and published/ folders."
    )

    args = parser.parse_args()

    local_root = Path(args.local_root)
    s3_client = create_s3_client()

    upload_directory(s3_client, local_root / "curated")
    upload_directory(s3_client, local_root / "published")

    print("Upload completed.")


if __name__ == "__main__":
    main()