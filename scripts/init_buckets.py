#!/usr/bin/env python3
"""
Initialize MinIO bucket structure for the AI Data Lakehouse.

Creates the lakehouse bucket with the following structure:
  s3://lakehouse/bronze/files/         - Raw unstructured file storage
  s3://lakehouse/silver/ravendb_landing/ - RavenDB OLAP ETL Landing Zone
  s3://lakehouse/gold/vectors/         - Vector Parquet Storage
  s3://lakehouse/warehouse/            - Iceberg Warehouse Location

Usage:
  pip install boto3
  python init_buckets.py
"""

import boto3
from botocore.client import Config
import time
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# MinIO Configuration
MINIO_ENDPOINT = os.getenv('MINIO_URL', 'http://localhost:9100')
MINIO_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', 'admin')
MINIO_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'password')
BUCKET_NAME = "lakehouse"

# Folder structure to create (S3 uses "prefix" objects)
PREFIXES = [
    "bronze/files/",
    "silver/ravendb_landing/orders/",
    "gold/vectors/",
    "gold/milvus_import/",
    "warehouse/",
]


def create_s3_client():
    """Create a boto3 S3 client configured for MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def wait_for_minio(client, max_retries=30, delay=2):
    """Wait for MinIO to be available."""
    print("Waiting for MinIO to be ready...")
    for i in range(max_retries):
        try:
            client.list_buckets()
            print("✓ MinIO is ready!")
            return True
        except Exception as e:
            print(f"  Attempt {i+1}/{max_retries}: MinIO not ready yet...")
            time.sleep(delay)
    raise Exception("MinIO did not become ready in time")


def create_bucket(client, bucket_name):
    """Create the main bucket if it doesn't exist."""
    try:
        client.head_bucket(Bucket=bucket_name)
        print(f"✓ Bucket '{bucket_name}' already exists")
    except client.exceptions.ClientError:
        client.create_bucket(Bucket=bucket_name)
        print(f"✓ Created bucket '{bucket_name}'")


def create_prefix(client, bucket_name, prefix):
    """Create a folder prefix by uploading an empty object."""
    # S3 doesn't have real folders, but we create a placeholder
    client.put_object(
        Bucket=bucket_name,
        Key=prefix,
        Body=b"",
    )
    print(f"  ✓ Created prefix: s3://{bucket_name}/{prefix}")


def main():
    print("=" * 60)
    print("AI Data Lakehouse - MinIO Bucket Initialization")
    print("=" * 60)
    
    # Create client and wait for MinIO
    client = create_s3_client()
    wait_for_minio(client)
    
    # Create main bucket
    print(f"\nCreating bucket structure...")
    create_bucket(client, BUCKET_NAME)
    
    # Create folder prefixes
    for prefix in PREFIXES:
        create_prefix(client, BUCKET_NAME, prefix)
    
    print("\n" + "=" * 60)
    print("✓ Bucket initialization complete!")
    print("=" * 60)
    console_port = os.getenv('MINIO_CONSOLE_PORT', '9101')
    print(f"\nMinIO Console: http://localhost:{console_port}")
    print(f"  Username: {MINIO_ACCESS_KEY}")
    print(f"  Password: {MINIO_SECRET_KEY}")
    print(f"\nS3 Endpoint: {MINIO_ENDPOINT}")
    print(f"Bucket: s3://{BUCKET_NAME}/")


if __name__ == "__main__":
    main()
