#!/usr/bin/env python3
"""
HDF5 Scientific Data Ingestion Script for the AI Data Lakehouse.

This script processes HDF5/NeXus files (commonly used in neutron scattering
experiments) and:
  1. Extracts metadata from the HDF5 file structure
  2. Writes metadata to Parquet format for Iceberg cataloging
  3. Stores file location info for serving via Tiled

The metadata includes information needed to locate and serve the original
data file through Tiled (https://blueskyproject.io/tiled/).

Usage:
  python ingest_hdf5.py /path/to/file.nxs.h5
  python ingest_hdf5.py /path/to/directory  # Process all HDF5 files

Requirements:
  pip install h5py pandas pyarrow boto3 python-dotenv
"""

import argparse
import os
import sys
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import h5py
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.client import Config
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MinIO/S3 Configuration
MINIO_ENDPOINT = os.getenv('MINIO_URL', 'http://localhost:9100')
MINIO_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', 'admin')
MINIO_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'password')
BUCKET_NAME = "lakehouse"

# HDF5 file extensions to recognize
HDF5_EXTENSIONS = {'.h5', '.hdf5', '.nxs', '.nx5', '.nxs.h5'}


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


def is_hdf5_file(path: Path) -> bool:
    """Check if a file is an HDF5 file based on extension."""
    suffixes = ''.join(path.suffixes).lower()
    return any(suffixes.endswith(ext) for ext in HDF5_EXTENSIONS)


def extract_nexus_metadata(h5file: h5py.File) -> Dict[str, Any]:
    """
    Extract NeXus-specific metadata from neutron scattering files.
    
    NeXus files follow a hierarchical structure with NXentry, NXinstrument,
    NXsample, etc. groups.
    """
    metadata = {}
    
    def safe_decode(value):
        """Safely decode bytes to string, flattening arrays."""
        import numpy as np
        if value is None:
            return None
        if isinstance(value, bytes):
            return value.decode('utf-8', errors='replace')
        elif isinstance(value, np.ndarray):
            # Flatten numpy arrays to a single string
            if value.size == 0:
                return ''
            flat = value.flatten()
            if flat.dtype.kind in ('S', 'U', 'O'):  # String types
                decoded = [safe_decode(v) for v in flat]
                return ', '.join(str(d) for d in decoded if d)
            else:
                return str(flat[0]) if flat.size == 1 else str(flat.tolist())
        elif isinstance(value, (list, tuple)):
            decoded = [safe_decode(v) for v in value]
            return ', '.join(str(d) for d in decoded if d)
        elif isinstance(value, (int, float)):
            return value
        return str(value)
    
    def get_attr(obj, name, default=None):
        """Safely get an attribute from an HDF5 object."""
        try:
            val = obj.attrs.get(name, default)
            return safe_decode(val) if val is not None else default
        except Exception:
            return default
    
    def get_dataset_value(group, name, default=None):
        """Safely get a dataset value."""
        try:
            if name in group:
                ds = group[name]
                if isinstance(ds, h5py.Dataset):
                    val = ds[()]
                    return safe_decode(val)
            return default
        except Exception:
            return default
    
    # Find NXentry groups (main data entries)
    for entry_name in h5file.keys():
        entry = h5file[entry_name]
        if not isinstance(entry, h5py.Group):
            continue
            
        nx_class = get_attr(entry, 'NX_class')
        if nx_class == 'NXentry' or entry_name.startswith('entry'):
            # Entry-level metadata
            metadata['entry_name'] = entry_name
            metadata['title'] = get_dataset_value(entry, 'title', '')
            metadata['experiment_identifier'] = get_dataset_value(entry, 'experiment_identifier', '')
            metadata['start_time'] = get_dataset_value(entry, 'start_time', '')
            metadata['end_time'] = get_dataset_value(entry, 'end_time', '')
            metadata['duration'] = get_dataset_value(entry, 'duration', 0)
            metadata['run_number'] = get_dataset_value(entry, 'run_number', '')
            
            # Instrument metadata
            if 'instrument' in entry or any('instrument' in k.lower() for k in entry.keys()):
                inst_key = next((k for k in entry.keys() if 'instrument' in k.lower()), 'instrument')
                if inst_key in entry:
                    inst = entry[inst_key]
                    metadata['instrument_name'] = get_dataset_value(inst, 'name', '')
                    metadata['instrument_type'] = get_attr(inst, 'NX_class', '')
                    
                    # Look for specific instrument components
                    for comp_name in inst.keys():
                        comp = inst[comp_name]
                        if isinstance(comp, h5py.Group):
                            comp_class = get_attr(comp, 'NX_class', '')
                            if comp_class == 'NXsource':
                                metadata['source_name'] = get_dataset_value(comp, 'name', '')
                                metadata['source_type'] = get_dataset_value(comp, 'type', '')
            
            # Sample metadata
            if 'sample' in entry:
                sample = entry['sample']
                metadata['sample_name'] = get_dataset_value(sample, 'name', '')
                metadata['sample_description'] = get_dataset_value(sample, 'description', '')
            
            # User metadata
            if 'user' in entry:
                user = entry['user']
                metadata['user_name'] = get_dataset_value(user, 'name', '')
                metadata['user_facility'] = get_dataset_value(user, 'facility_user_id', '')
            
            break  # Only process first entry
    
    return metadata


def extract_hdf5_metadata(file_path: Path) -> Dict[str, Any]:
    """
    Extract comprehensive metadata from an HDF5 file.
    
    Returns a dictionary containing:
    - File information (path, size, timestamps)
    - HDF5 structure information (groups, datasets)
    - NeXus-specific metadata if applicable
    - Data array shapes and types
    """
    metadata = {
        # File information
        'file_path': str(file_path.absolute()),
        'file_name': file_path.name,
        'file_size_bytes': file_path.stat().st_size,
        'file_modified_time': datetime.fromtimestamp(file_path.stat().st_mtime).isoformat(),
        'file_created_time': datetime.fromtimestamp(file_path.stat().st_ctime).isoformat(),
        'ingestion_time': datetime.now().isoformat(),
        
        # Tiled serving information
        'tiled_uri': f"file://{file_path.absolute()}",
        'tiled_adapter': 'hdf5',
        'content_type': 'application/x-hdf5',
    }
    
    try:
        with h5py.File(file_path, 'r') as f:
            # Basic HDF5 structure info
            metadata['hdf5_driver'] = f.driver
            metadata['hdf5_libver'] = str(f.libver)
            
            # Count groups and datasets
            group_count = 0
            dataset_count = 0
            dataset_info = []
            
            def visitor(name, obj):
                nonlocal group_count, dataset_count
                if isinstance(obj, h5py.Group):
                    group_count += 1
                elif isinstance(obj, h5py.Dataset):
                    dataset_count += 1
                    # Collect info about significant datasets
                    if obj.size > 100:  # Only track non-trivial datasets
                        ds_info = {
                            'path': name,
                            'shape': list(obj.shape),
                            'dtype': str(obj.dtype),
                            'size': obj.size,
                        }
                        dataset_info.append(ds_info)
            
            f.visititems(visitor)
            
            metadata['group_count'] = group_count
            metadata['dataset_count'] = dataset_count
            metadata['significant_datasets'] = json.dumps(dataset_info[:20])  # Limit to top 20
            
            # Extract NeXus-specific metadata
            nexus_metadata = extract_nexus_metadata(f)
            metadata.update(nexus_metadata)
            
    except Exception as e:
        metadata['extraction_error'] = str(e)
        print(f"⚠ Warning: Error extracting metadata from {file_path}: {e}")
    
    return metadata


def create_parquet_schema() -> pa.Schema:
    """Define the PyArrow schema for HDF5 metadata."""
    return pa.schema([
        ('file_path', pa.string()),
        ('file_name', pa.string()),
        ('file_size_bytes', pa.int64()),
        ('file_modified_time', pa.string()),
        ('file_created_time', pa.string()),
        ('ingestion_time', pa.string()),
        ('tiled_uri', pa.string()),
        ('tiled_adapter', pa.string()),
        ('content_type', pa.string()),
        ('hdf5_driver', pa.string()),
        ('hdf5_libver', pa.string()),
        ('group_count', pa.int32()),
        ('dataset_count', pa.int32()),
        ('significant_datasets', pa.string()),  # JSON array
        ('entry_name', pa.string()),
        ('title', pa.string()),
        ('experiment_identifier', pa.string()),
        ('start_time', pa.string()),
        ('end_time', pa.string()),
        ('duration', pa.float64()),
        ('run_number', pa.string()),
        ('instrument_name', pa.string()),
        ('instrument_type', pa.string()),
        ('source_name', pa.string()),
        ('source_type', pa.string()),
        ('sample_name', pa.string()),
        ('sample_description', pa.string()),
        ('user_name', pa.string()),
        ('user_facility', pa.string()),
        ('extraction_error', pa.string()),
    ])


def metadata_to_dataframe(metadata_list: List[Dict[str, Any]]) -> pd.DataFrame:
    """Convert list of metadata dictionaries to a DataFrame."""
    # Ensure all expected columns exist
    schema_fields = [
        'file_path', 'file_name', 'file_size_bytes', 'file_modified_time',
        'file_created_time', 'ingestion_time', 'tiled_uri', 'tiled_adapter',
        'content_type', 'hdf5_driver', 'hdf5_libver', 'group_count',
        'dataset_count', 'significant_datasets', 'entry_name', 'title',
        'experiment_identifier', 'start_time', 'end_time', 'duration',
        'run_number', 'instrument_name', 'instrument_type', 'source_name',
        'source_type', 'sample_name', 'sample_description', 'user_name',
        'user_facility', 'extraction_error'
    ]
    
    for record in metadata_list:
        for field in schema_fields:
            if field not in record:
                record[field] = None
    
    return pd.DataFrame(metadata_list)


def save_parquet_local(df: pd.DataFrame, output_path: Path) -> Path:
    """Save DataFrame to local Parquet file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pandas(df),
        output_path,
        compression='snappy'
    )
    print(f"✓ Saved metadata to {output_path}")
    return output_path


def upload_to_s3(local_path: Path, s3_key: str) -> str:
    """Upload a file to MinIO/S3."""
    client = create_s3_client()
    
    client.upload_file(
        str(local_path),
        BUCKET_NAME,
        s3_key
    )
    
    s3_uri = f"s3://{BUCKET_NAME}/{s3_key}"
    print(f"✓ Uploaded to {s3_uri}")
    return s3_uri


def upload_hdf5_to_bronze(file_path: Path) -> str:
    """
    Upload the original HDF5 file to the bronze layer for serving.
    
    Returns the S3 URI of the uploaded file.
    """
    client = create_s3_client()
    
    # Preserve directory structure under bronze/hdf5/
    s3_key = f"bronze/hdf5/{file_path.name}"
    
    client.upload_file(
        str(file_path),
        BUCKET_NAME,
        s3_key
    )
    
    s3_uri = f"s3://{BUCKET_NAME}/{s3_key}"
    print(f"✓ Uploaded HDF5 file to {s3_uri}")
    return s3_uri


def process_files(paths: List[str], upload_hdf5: bool = False) -> pd.DataFrame:
    """
    Process one or more HDF5 files or directories.
    
    Args:
        paths: List of file paths or directory paths
        upload_hdf5: Whether to upload original HDF5 files to S3
        
    Returns:
        DataFrame with all extracted metadata
    """
    all_metadata = []
    
    for path_str in paths:
        path = Path(path_str).expanduser()
        
        if path.is_file():
            if is_hdf5_file(path):
                print(f"Processing: {path}")
                metadata = extract_hdf5_metadata(path)
                
                if upload_hdf5:
                    s3_uri = upload_hdf5_to_bronze(path)
                    metadata['s3_uri'] = s3_uri
                    metadata['tiled_uri'] = s3_uri  # Update to S3 URI for Tiled
                
                all_metadata.append(metadata)
            else:
                print(f"⚠ Skipping non-HDF5 file: {path}")
                
        elif path.is_dir():
            print(f"Scanning directory: {path}")
            for file_path in path.rglob('*'):
                if file_path.is_file() and is_hdf5_file(file_path):
                    print(f"  Processing: {file_path}")
                    metadata = extract_hdf5_metadata(file_path)
                    
                    if upload_hdf5:
                        s3_uri = upload_hdf5_to_bronze(file_path)
                        metadata['s3_uri'] = s3_uri
                        metadata['tiled_uri'] = s3_uri
                    
                    all_metadata.append(metadata)
        else:
            print(f"⚠ Path not found: {path}")
    
    if not all_metadata:
        print("⚠ No HDF5 files found to process")
        return pd.DataFrame()
    
    print(f"\n✓ Processed {len(all_metadata)} HDF5 file(s)")
    return metadata_to_dataframe(all_metadata)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Ingest HDF5/NeXus files into the AI Data Lakehouse'
    )
    parser.add_argument(
        'paths',
        nargs='+',
        help='HDF5 file(s) or directory(ies) to process'
    )
    parser.add_argument(
        '--output', '-o',
        default='./output/hdf5_metadata.parquet',
        help='Output path for Parquet file (default: ./output/hdf5_metadata.parquet)'
    )
    parser.add_argument(
        '--upload-metadata',
        action='store_true',
        help='Upload Parquet metadata to MinIO (silver layer)'
    )
    parser.add_argument(
        '--upload-hdf5',
        action='store_true',
        help='Upload original HDF5 files to MinIO (bronze layer)'
    )
    parser.add_argument(
        '--generate-tiled-config',
        action='store_true',
        help='Generate a Tiled configuration file for serving'
    )
    
    args = parser.parse_args()
    
    # Process files
    df = process_files(args.paths, upload_hdf5=args.upload_hdf5)
    
    if df.empty:
        print("No files processed. Exiting.")
        return 1
    
    # Save Parquet locally
    output_path = Path(args.output)
    save_parquet_local(df, output_path)
    
    # Upload to S3 if requested
    if args.upload_metadata:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        s3_key = f"silver/hdf5_metadata/hdf5_metadata_{timestamp}.parquet"
        upload_to_s3(output_path, s3_key)
    
    # Generate Tiled config if requested
    if args.generate_tiled_config:
        generate_tiled_catalog(df, output_path.parent / 'tiled_catalog.yml')
    
    # Print summary
    print("\n" + "="*60)
    print("INGESTION SUMMARY")
    print("="*60)
    print(f"Files processed:    {len(df)}")
    print(f"Total size:         {df['file_size_bytes'].sum() / 1024 / 1024:.2f} MB")
    print(f"Metadata saved to:  {output_path}")
    if args.upload_metadata:
        print(f"Metadata S3 path:   s3://{BUCKET_NAME}/{s3_key}")
    print("="*60)
    
    return 0


def generate_tiled_catalog(df: pd.DataFrame, output_path: Path):
    """Generate a Tiled catalog configuration from processed metadata."""
    catalog_entries = []
    
    for _, row in df.iterrows():
        entry = {
            'path': row['file_name'].replace('.', '_').replace('-', '_'),
            'tree': 'tiled.adapters.hdf5:HDF5Adapter.from_uri',
            'args': {
                'filepath': row['tiled_uri'],
            },
            'metadata': {
                'title': row.get('title', ''),
                'instrument': row.get('instrument_name', ''),
                'sample': row.get('sample_name', ''),
                'start_time': row.get('start_time', ''),
            }
        }
        catalog_entries.append(entry)
    
    # Write YAML config
    import yaml
    config = {
        'trees': catalog_entries
    }
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    
    print(f"✓ Generated Tiled catalog config: {output_path}")


if __name__ == '__main__':
    sys.exit(main())
