#!/usr/bin/env python3
"""
Tiled Server for Scientific Data Serving.

This script configures and runs a Tiled server to serve HDF5/NeXus files
from the lakehouse. Tiled is a data access service developed by Bluesky
for large-scale scientific user facilities.

Tiled provides:
- RESTful API for data access
- Streaming of large datasets
- Web-based data browser
- Python client for programmatic access

Usage:
  # Start server with local files
  python serve_tiled.py --data-dir /path/to/hdf5/files

  # Start server with S3 backend
  python serve_tiled.py --s3

  # Start server using metadata catalog from Iceberg
  python serve_tiled.py --catalog /path/to/hdf5_metadata.parquet

Requirements:
  pip install tiled[all] h5py pandas pyarrow boto3
  
Documentation:
  https://blueskyproject.io/tiled/
  https://github.com/bluesky/tiled
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional

# Tiled imports
try:
    from tiled.server.app import build_app
    from tiled.adapters.mapping import MapAdapter
    from tiled.adapters.hdf5 import HDF5Adapter
    import uvicorn
except ImportError as e:
    print("Error: Tiled is not installed.")
    print("Install with: pip install 'tiled[all]'")
    print(f"Import error: {e}")
    sys.exit(1)

import pandas as pd
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Default configuration
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000


def path_to_uri(file_path: str) -> str:
    """Convert a file path to a file:// URI."""
    from pathlib import Path
    abs_path = Path(file_path).resolve()
    return f"file://{abs_path}"


def create_hdf5_adapter(file_path: str) -> HDF5Adapter:
    """Create an HDF5 adapter for a single file."""
    return HDF5Adapter.from_uris(path_to_uri(file_path))


def create_local_catalog(data_dir: Path) -> MapAdapter:
    """
    Create a Tiled catalog from local HDF5 files.
    
    Scans the directory for HDF5/NeXus files and creates adapters.
    """
    hdf5_extensions = {'.h5', '.hdf5', '.nxs', '.nx5'}
    
    adapters = {}
    
    for file_path in data_dir.rglob('*'):
        if file_path.is_file():
            suffixes = ''.join(file_path.suffixes).lower()
            if any(suffixes.endswith(ext) for ext in hdf5_extensions):
                # Create a safe key name
                key = file_path.stem.replace('.', '_').replace('-', '_')
                
                # Ensure unique keys
                base_key = key
                counter = 1
                while key in adapters:
                    key = f"{base_key}_{counter}"
                    counter += 1
                
                try:
                    file_uri = path_to_uri(str(file_path))
                    adapters[key] = HDF5Adapter.from_uris(file_uri)
                    print(f"  ✓ Loaded: {key} -> {file_path}")
                except Exception as e:
                    print(f"  ⚠ Failed to load {file_path}: {e}")
    
    print(f"\n✓ Created catalog with {len(adapters)} entries")
    return MapAdapter(adapters)


def create_catalog_from_parquet(parquet_path: Path) -> MapAdapter:
    """
    Create a Tiled catalog from HDF5 metadata stored in Parquet.
    
    This uses the metadata extracted by ingest_hdf5.py to build
    the serving catalog.
    """
    df = pd.read_parquet(parquet_path)
    
    adapters = {}
    
    for _, row in df.iterrows():
        file_path = row.get('tiled_uri') or row.get('file_path')
        
        # Ensure we have a proper file:// URI
        if file_path.startswith('file://'):
            file_uri = file_path
        elif file_path.startswith('s3://'):
            file_uri = file_path  # S3 URIs are handled differently
        else:
            file_uri = path_to_uri(file_path)
        
        # Create safe key name
        key = Path(row['file_name']).stem.replace('.', '_').replace('-', '_')
        
        # Ensure unique keys
        base_key = key
        counter = 1
        while key in adapters:
            key = f"{base_key}_{counter}"
            counter += 1
        
        try:
            # Add metadata from the catalog
            metadata = {
                'file_name': row.get('file_name', ''),
                'title': row.get('title', ''),
                'instrument': row.get('instrument_name', ''),
                'sample': row.get('sample_name', ''),
                'start_time': row.get('start_time', ''),
                'file_size_bytes': row.get('file_size_bytes', 0),
            }
            
            adapters[key] = HDF5Adapter.from_uris(
                file_uri,
                metadata=metadata
            )
            print(f"  ✓ Loaded: {key}")
        except Exception as e:
            print(f"  ⚠ Failed to load {file_path}: {e}")
    
    print(f"\n✓ Created catalog with {len(adapters)} entries from Parquet")
    return MapAdapter(adapters)


def create_s3_catalog() -> MapAdapter:
    """
    Create a Tiled catalog from HDF5 files stored in MinIO/S3.
    
    Lists files in s3://lakehouse/bronze/hdf5/ and creates adapters.
    """
    import boto3
    from botocore.client import Config
    
    minio_endpoint = os.getenv('MINIO_URL', 'http://localhost:9100')
    access_key = os.getenv('AWS_ACCESS_KEY_ID', 'admin')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'password')
    bucket_name = 'lakehouse'
    prefix = 'bronze/hdf5/'
    
    client = boto3.client(
        's3',
        endpoint_url=minio_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1',
    )
    
    adapters = {}
    
    try:
        paginator = client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                file_name = key.split('/')[-1]
                
                # Skip non-HDF5 files
                suffixes = ''.join(Path(file_name).suffixes).lower()
                if not any(suffixes.endswith(ext) for ext in {'.h5', '.hdf5', '.nxs'}):
                    continue
                
                # Create adapter key
                adapter_key = Path(file_name).stem.replace('.', '_').replace('-', '_')
                
                # For S3, we need to use fsspec
                s3_uri = f"s3://{bucket_name}/{key}"
                
                try:
                    # Configure S3 filesystem
                    import s3fs
                    fs = s3fs.S3FileSystem(
                        key=access_key,
                        secret=secret_key,
                        client_kwargs={'endpoint_url': minio_endpoint}
                    )
                    
                    adapters[adapter_key] = HDF5Adapter.from_uris(
                        s3_uri,
                        storage_options={
                            'key': access_key,
                            'secret': secret_key,
                            'client_kwargs': {'endpoint_url': minio_endpoint}
                        }
                    )
                    print(f"  ✓ Loaded: {adapter_key} -> {s3_uri}")
                except Exception as e:
                    print(f"  ⚠ Failed to load {s3_uri}: {e}")
                    
    except Exception as e:
        print(f"Error listing S3 bucket: {e}")
    
    print(f"\n✓ Created catalog with {len(adapters)} entries from S3")
    return MapAdapter(adapters)


def create_demo_catalog() -> MapAdapter:
    """
    Create a demo catalog with sample data for testing.
    
    This is useful for testing the Tiled setup without real HDF5 files.
    """
    import numpy as np
    from tiled.adapters.array import ArrayAdapter
    from tiled.adapters.dataframe import DataFrameAdapter
    
    # Create sample array data
    sample_array = np.random.rand(100, 100)
    sample_df = pd.DataFrame({
        'time': pd.date_range('2024-01-01', periods=100, freq='1min'),
        'intensity': np.random.rand(100) * 1000,
        'wavelength': np.linspace(0.5, 10.0, 100),
    })
    
    adapters = {
        'demo_2d_array': ArrayAdapter.from_array(
            sample_array,
            metadata={'description': 'Demo 2D array data'}
        ),
        'demo_timeseries': DataFrameAdapter.from_pandas(
            sample_df,
            metadata={'description': 'Demo time series data'}
        ),
    }
    
    print("✓ Created demo catalog with sample data")
    return MapAdapter(adapters)


def main():
    """Main entry point for the Tiled server."""
    parser = argparse.ArgumentParser(
        description='Start Tiled server for serving HDF5/NeXus data'
    )
    
    source_group = parser.add_mutually_exclusive_group()
    source_group.add_argument(
        '--data-dir', '-d',
        type=str,
        help='Directory containing HDF5 files to serve'
    )
    source_group.add_argument(
        '--catalog', '-c',
        type=str,
        help='Path to Parquet metadata catalog (from ingest_hdf5.py)'
    )
    source_group.add_argument(
        '--s3',
        action='store_true',
        help='Serve files from MinIO S3 bronze layer'
    )
    source_group.add_argument(
        '--demo',
        action='store_true',
        help='Start with demo/sample data'
    )
    
    parser.add_argument(
        '--host',
        default=DEFAULT_HOST,
        help=f'Host to bind to (default: {DEFAULT_HOST})'
    )
    parser.add_argument(
        '--port', '-p',
        type=int,
        default=DEFAULT_PORT,
        help=f'Port to bind to (default: {DEFAULT_PORT})'
    )
    parser.add_argument(
        '--reload',
        action='store_true',
        help='Enable auto-reload for development'
    )
    parser.add_argument(
        '--public',
        action='store_true',
        help='Allow public access (no authentication)'
    )
    
    args = parser.parse_args()
    
    print("="*60)
    print("TILED DATA SERVER")
    print("="*60)
    
    # Create catalog based on source
    if args.data_dir:
        data_dir = Path(args.data_dir).expanduser()
        if not data_dir.exists():
            print(f"Error: Directory not found: {data_dir}")
            return 1
        print(f"Loading HDF5 files from: {data_dir}")
        catalog = create_local_catalog(data_dir)
        
    elif args.catalog:
        catalog_path = Path(args.catalog).expanduser()
        if not catalog_path.exists():
            print(f"Error: Catalog file not found: {catalog_path}")
            return 1
        print(f"Loading catalog from: {catalog_path}")
        catalog = create_catalog_from_parquet(catalog_path)
        
    elif args.s3:
        print("Loading HDF5 files from MinIO S3...")
        catalog = create_s3_catalog()
        
    elif args.demo:
        print("Creating demo catalog with sample data...")
        catalog = create_demo_catalog()
        
    else:
        print("No data source specified. Use --help for options.")
        print("\nQuick start examples:")
        print("  python serve_tiled.py --demo")
        print("  python serve_tiled.py --data-dir ~/data/expt11/reduced")
        print("  python serve_tiled.py --catalog ./output/hdf5_metadata.parquet")
        return 1
    
    # Build and run the Tiled app
    print(f"\nStarting Tiled server on http://{args.host}:{args.port}")
    print(f"Web UI available at: http://{args.host}:{args.port}/ui")
    print(f"API available at:    http://{args.host}:{args.port}/api/v1")
    print("\nPress Ctrl+C to stop the server")
    print("="*60 + "\n")
    
    # Configure authentication for public access
    from tiled.config import Authentication
    authentication = Authentication(allow_anonymous_access=True)
    
    # Build the Tiled app with public access
    app = build_app(catalog, authentication=authentication)
    
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        reload=args.reload,
    )
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
