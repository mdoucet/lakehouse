#!/usr/bin/env python3
"""
RavenDB to Parquet Sync for Community Edition.

This script provides an alternative to RavenDB Enterprise's OLAP ETL feature.
It reads documents from RavenDB, flattens them, and writes Parquet files to MinIO.

The output mimics what RavenDB OLAP ETL would produce:
  s3://lakehouse/silver/ravendb_landing/orders/{date}/data.parquet

Usage:
  pip install ravendb pyarrow boto3 pandas
  python ravendb_sync.py
"""

import os
import io
import time
from datetime import datetime
from typing import List, Dict, Any

import boto3
from botocore.client import Config
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

try:
    from ravendb import DocumentStore
except ImportError:
    print("Installing ravendb package...")
    import subprocess
    subprocess.check_call(["pip", "install", "ravendb"])
    from ravendb import DocumentStore

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Configuration
RAVENDB_URL = os.getenv('RAVENDB_URL', 'http://localhost:8080')
DATABASE_NAME = "Northwind"

MINIO_ENDPOINT = os.getenv('MINIO_URL', 'http://localhost:9100')
MINIO_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', 'admin')
MINIO_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'password')
BUCKET_NAME = "lakehouse"
LANDING_ZONE_PREFIX = "silver/ravendb_landing/orders"

BATCH_SIZE = 100


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


def flatten_order(doc: Dict[str, Any], doc_id: str) -> Dict[str, Any]:
    """
    Flatten a RavenDB order document for columnar storage.
    
    This mimics the RavenDB OLAP ETL script from the lakehouse.md spec:
    ```javascript
    loadToOrders(partitionBy(this.OrderDate), {
        OrderId: id(this),
        CustomerId: this.CustomerId,
        OrderDate: this.OrderDate,
        TotalAmount: this.Lines.reduce(...),
        Status: this.Status
    });
    ```
    """
    # Calculate total if not pre-computed
    total_amount = doc.get("TotalAmount", 0)
    if not total_amount and "Lines" in doc:
        total_amount = sum(
            line.get("Price", 0) * line.get("Quantity", 0)
            for line in doc.get("Lines", [])
        )
    
    # Parse order date for partitioning
    order_date_str = doc.get("OrderDate", datetime.now().isoformat())
    if isinstance(order_date_str, str):
        try:
            order_date = datetime.fromisoformat(order_date_str.replace("Z", "+00:00"))
        except ValueError:
            order_date = datetime.now()
    else:
        order_date = order_date_str
    
    return {
        "OrderId": doc_id,
        "CustomerId": doc.get("CustomerId", ""),
        "OrderDate": order_date,
        "TotalAmount": round(total_amount, 2),
        "Status": doc.get("Status", "Unknown"),
        "ShipCity": doc.get("ShipTo", {}).get("City", ""),
        "ShipCountry": doc.get("ShipTo", {}).get("Country", ""),
        "LineCount": len(doc.get("Lines", [])),
        # Metadata for tracking
        "SyncedAt": datetime.now(),
    }


def fetch_all_orders(store: DocumentStore) -> List[Dict[str, Any]]:
    """Fetch all orders from RavenDB."""
    orders = []
    
    with store.open_session() as session:
        # Stream all documents from Orders collection
        query = session.query_collection("Orders")
        
        for doc in query:
            # Get document ID from metadata
            doc_id = session.advanced.get_document_id(doc)
            flattened = flatten_order(doc.__dict__, doc_id)
            orders.append(flattened)
    
    return orders


def group_by_date(orders: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Group orders by date for partitioned storage."""
    grouped = {}
    for order in orders:
        date_key = order["OrderDate"].strftime("%Y-%m-%d")
        if date_key not in grouped:
            grouped[date_key] = []
        grouped[date_key].append(order)
    return grouped


def write_parquet_to_minio(
    s3_client,
    orders: List[Dict[str, Any]],
    date_partition: str
):
    """Write orders to a Parquet file in MinIO."""
    # Convert to DataFrame
    df = pd.DataFrame(orders)
    
    # Round timestamps to milliseconds for Spark compatibility
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.floor('ms')
    
    # Define explicit schema with timestamp(ms) instead of timestamp(ns)
    schema_fields = []
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            # Use millisecond timestamps for Spark compatibility
            schema_fields.append(pa.field(col, pa.timestamp('ms')))
        elif pd.api.types.is_integer_dtype(df[col]):
            schema_fields.append(pa.field(col, pa.int64()))
        elif pd.api.types.is_float_dtype(df[col]):
            schema_fields.append(pa.field(col, pa.float64()))
        else:
            schema_fields.append(pa.field(col, pa.string()))
    
    schema = pa.schema(schema_fields)
    
    # Convert to PyArrow Table with explicit schema
    table = pa.Table.from_pandas(df, schema=schema)
    
    # Write to in-memory buffer
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression='snappy')
    buffer.seek(0)
    
    # Upload to MinIO
    key = f"{LANDING_ZONE_PREFIX}/{date_partition}/data.parquet"
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )
    
    return key


def main():
    print("=" * 60)
    print("RavenDB → Parquet Sync (Community Edition)")
    print("=" * 60)
    
    # Initialize RavenDB store
    print("\nConnecting to RavenDB...")
    store = DocumentStore(urls=[RAVENDB_URL], database=DATABASE_NAME)
    store.initialize()
    
    # Initialize MinIO client
    print("Connecting to MinIO...")
    s3_client = create_s3_client()
    
    # Fetch all orders
    print("\nFetching orders from RavenDB...")
    orders = fetch_all_orders(store)
    print(f"  ✓ Found {len(orders)} orders")
    
    if not orders:
        print("\n⚠ No orders found. Run seed_ravendb.py first.")
        return
    
    # Group by date for partitioned storage
    print("\nGrouping orders by date...")
    grouped = group_by_date(orders)
    print(f"  ✓ {len(grouped)} date partitions")
    
    # Write Parquet files
    print("\nWriting Parquet files to MinIO...")
    files_written = []
    for date_partition, date_orders in grouped.items():
        key = write_parquet_to_minio(s3_client, date_orders, date_partition)
        files_written.append(key)
        print(f"  ✓ s3://{BUCKET_NAME}/{key} ({len(date_orders)} orders)")
    
    print("\n" + "=" * 60)
    print("✓ Sync complete!")
    print("=" * 60)
    print(f"\nTotal orders synced: {len(orders)}")
    print(f"Parquet files created: {len(files_written)}")
    print(f"\nLanding zone: s3://{BUCKET_NAME}/{LANDING_ZONE_PREFIX}/")
    print("\nNext step: Run bridge_ravendb.py to merge into Iceberg")


if __name__ == "__main__":
    main()
