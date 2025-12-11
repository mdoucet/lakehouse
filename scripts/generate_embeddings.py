#!/usr/bin/env python3
"""
Generate Vector Embeddings from Order Data.

This script reads order data from Iceberg, generates embeddings using a local
sentence-transformer model, and writes the vectors to both:
1. Iceberg gold layer (source of truth)
2. Parquet files for Milvus bulk import

For desktop demo, we use the lightweight 'all-MiniLM-L6-v2' model (384 dimensions).

Prerequisites:
  pip install sentence-transformers pandas pyarrow boto3

Usage (standalone, not in Spark):
  python generate_embeddings.py
"""

import os
import io
from typing import List, Dict, Any

import boto3
from botocore.client import Config
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Configuration
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET_NAME = "lakehouse"

# Embedding model config
MODEL_NAME = "all-MiniLM-L6-v2"  # 384 dimensions, fast
EMBEDDING_DIM = 384

# Paths
ORDERS_LANDING = "silver/ravendb_landing/orders"
VECTORS_OUTPUT = "gold/vectors/orders"
MILVUS_IMPORT = "gold/milvus_import"


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


def load_embedding_model():
    """Load the sentence-transformer model."""
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        print("Installing sentence-transformers...")
        import subprocess
        subprocess.check_call(["pip", "install", "sentence-transformers"])
        from sentence_transformers import SentenceTransformer
    
    print(f"Loading embedding model: {MODEL_NAME}")
    model = SentenceTransformer(MODEL_NAME)
    print(f"  ✓ Model loaded (dimension: {EMBEDDING_DIM})")
    return model


def read_orders_from_landing(s3_client) -> pd.DataFrame:
    """Read order data from the landing zone Parquet files."""
    print("\nReading orders from landing zone...")
    
    # List all Parquet files
    response = s3_client.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=ORDERS_LANDING
    )
    
    parquet_files = [
        obj['Key'] for obj in response.get('Contents', [])
        if obj['Key'].endswith('.parquet')
    ]
    
    if not parquet_files:
        raise ValueError(f"No Parquet files found in {ORDERS_LANDING}/")
    
    print(f"  ✓ Found {len(parquet_files)} Parquet files")
    
    # Read and concatenate all files
    dfs = []
    for key in parquet_files:
        obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        dfs.append(df)
    
    df_all = pd.concat(dfs, ignore_index=True)
    print(f"  ✓ Loaded {len(df_all)} orders")
    return df_all


def create_text_for_embedding(row: pd.Series) -> str:
    """
    Create a text representation of an order for embedding.
    
    Combines multiple fields to create a semantic representation.
    """
    parts = [
        f"Order {row.get('OrderId', 'unknown')}",
        f"Customer: {row.get('CustomerId', 'unknown')}",
        f"Status: {row.get('Status', 'unknown')}",
        f"Amount: ${row.get('TotalAmount', 0):.2f}",
        f"Ship to: {row.get('ShipCity', 'unknown')}, {row.get('ShipCountry', 'unknown')}",
        f"Items: {row.get('LineCount', 0)} line items",
    ]
    return ". ".join(parts)


def generate_embeddings(model, df: pd.DataFrame) -> pd.DataFrame:
    """Generate embeddings for all orders."""
    print("\nGenerating embeddings...")
    
    # Create text representations
    texts = df.apply(create_text_for_embedding, axis=1).tolist()
    
    # Generate embeddings in batches
    batch_size = 32
    all_embeddings = []
    
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        embeddings = model.encode(batch, show_progress_bar=False)
        all_embeddings.extend(embeddings)
        
        if (i + batch_size) % 100 == 0 or i + batch_size >= len(texts):
            print(f"  ✓ Processed {min(i + batch_size, len(texts))}/{len(texts)} orders")
    
    # Create output DataFrame
    df_vectors = pd.DataFrame({
        'order_id': df['OrderId'],
        'embedding': [emb.tolist() for emb in all_embeddings],
        'text': texts,  # Keep text for reference
    })
    
    print(f"  ✓ Generated {len(df_vectors)} embeddings")
    return df_vectors


def write_vectors_to_minio(s3_client, df_vectors: pd.DataFrame):
    """Write vector data to MinIO for Iceberg and Milvus."""
    print("\nWriting vectors to MinIO...")
    
    # Prepare data for Milvus (needs flat vector format)
    # Milvus expects: id (INT64), vector (FLOAT_VECTOR)
    df_milvus = pd.DataFrame({
        'id': range(len(df_vectors)),  # Milvus needs integer IDs
        'order_id': df_vectors['order_id'],
        'vector': df_vectors['embedding'],
    })
    
    # Write to Parquet for Milvus bulk import
    buffer = io.BytesIO()
    
    # Convert embedding list to proper format for Parquet
    # Milvus bulk import expects a specific format
    df_export = df_milvus.copy()
    
    table = pa.Table.from_pandas(df_export)
    pq.write_table(table, buffer)
    buffer.seek(0)
    
    milvus_key = f"{MILVUS_IMPORT}/vectors.parquet"
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=milvus_key,
        Body=buffer.getvalue()
    )
    print(f"  ✓ Wrote s3://{BUCKET_NAME}/{milvus_key}")
    
    # Also write to gold/vectors for Iceberg consumption
    buffer2 = io.BytesIO()
    pq.write_table(table, buffer2)
    buffer2.seek(0)
    
    gold_key = f"{VECTORS_OUTPUT}/vectors.parquet"
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=gold_key,
        Body=buffer2.getvalue()
    )
    print(f"  ✓ Wrote s3://{BUCKET_NAME}/{gold_key}")
    
    return milvus_key


def main():
    print("=" * 60)
    print("Vector Embedding Generation")
    print("=" * 60)
    
    # Initialize clients
    s3_client = create_s3_client()
    model = load_embedding_model()
    
    # Read orders
    df_orders = read_orders_from_landing(s3_client)
    
    # Generate embeddings
    df_vectors = generate_embeddings(model, df_orders)
    
    # Write to MinIO
    milvus_path = write_vectors_to_minio(s3_client, df_vectors)
    
    print("\n" + "=" * 60)
    print("✓ Embedding generation complete!")
    print("=" * 60)
    print(f"\nOrders processed: {len(df_vectors)}")
    print(f"Embedding dimension: {EMBEDDING_DIM}")
    print(f"Model: {MODEL_NAME}")
    print(f"\nMilvus import file: s3://{BUCKET_NAME}/{milvus_path}")
    print("\nNext step: Run milvus_bulk_load.py to index in Milvus")


if __name__ == "__main__":
    main()
