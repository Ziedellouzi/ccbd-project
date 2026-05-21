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


def download_prefix(
    s3_client,
    prefix: str,
    output_root: Path
) -> None:
    response = s3_client.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=prefix,
    )

    if "Contents" not in response:
        raise ValueError(f"No objects found under prefix: {prefix}")

    for obj in response["Contents"]:
        object_key = obj["Key"]

        if object_key.endswith("/"):
            continue

        # Recreate the object-storage path under the local output folder.
        local_path = output_root / object_key
        local_path.parent.mkdir(parents=True, exist_ok=True)

        print(f"Downloading: {object_key} -> {local_path}")

        s3_client.download_file(
            Bucket=BUCKET_NAME,
            Key=object_key,
            Filename=str(local_path),
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download dataset files from MinIO."
    )

    parser.add_argument(
        "--output-root",
        default="downloads",
        help="Local directory where downloaded objects are stored."
    )

    args = parser.parse_args()

    s3_client = create_s3_client()
    output_root = Path(args.output_root)

    download_prefix(s3_client, "curated/", output_root)
    download_prefix(s3_client, "published/", output_root)

    print("Download completed.")


if __name__ == "__main__":
    main()