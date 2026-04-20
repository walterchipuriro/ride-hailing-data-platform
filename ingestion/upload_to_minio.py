#!/usr/bin/env python3

import argparse
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload a local file to MinIO raw ride events path."
    )
    parser.add_argument(
        "--file",
        required=True,
        help="Path to the local file to upload.",
    )
    return parser.parse_args()


def load_config() -> dict:
    load_dotenv()

    return {
        "endpoint_url": os.getenv("MINIO_ENDPOINT"),
        "access_key": os.getenv("MINIO_ROOT_USER"),
        "secret_key": os.getenv("MINIO_ROOT_PASSWORD"),
        "bucket_name": os.getenv("MINIO_BUCKET"),
    }


def create_s3_client(config: dict):
    return boto3.client(
        "s3",
        endpoint_url=config["endpoint_url"],
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket_exists(s3_client, bucket_name: str) -> None:
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except ClientError as exc:
        raise RuntimeError(
            f"Bucket '{bucket_name}' does not exist or is not accessible."
        ) from exc


def upload_file(s3_client, bucket_name: str, local_file: Path) -> str:
    object_key = f"raw/ride_events/{local_file.name}"
    s3_client.upload_file(str(local_file), bucket_name, object_key)
    return object_key


def main() -> None:
    args = parse_args()
    local_file = Path(args.file)

    if not local_file.exists():
        raise FileNotFoundError(f"File not found: {local_file}")

    config = load_config()
    s3_client = create_s3_client(config)

    ensure_bucket_exists(s3_client, config["bucket_name"])
    object_key = upload_file(s3_client, config["bucket_name"], local_file)

    print(f"Uploaded file: {local_file}")
    print(f"Bucket: {config['bucket_name']}")
    print(f"Object path: {object_key}")


if __name__ == "__main__":
    main()
