#!/usr/bin/env python3
"""
Configure Dremio with Nessie/Iceberg data source.

This script automates the Dremio setup after first-run configuration.
You must first create an admin account via the Dremio UI at http://localhost:9047

Usage:
    python scripts/setup_dremio.py --username admin --password your_password
"""

import argparse
import json
import os
import sys
import time

import requests
from dotenv import load_dotenv

load_dotenv()

# Configuration from environment
DREMIO_HOST = os.getenv("DREMIO_HOST", "localhost")
DREMIO_PORT = os.getenv("DREMIO_UI_PORT", "9047")
DREMIO_URL = f"http://{DREMIO_HOST}:{DREMIO_PORT}"

# Nessie/MinIO configuration (for Dremio to connect internally via Docker network)
NESSIE_INTERNAL_URL = "http://nessie:19120/api/v2"
MINIO_INTERNAL_ENDPOINT = "minio:9000"
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password")


def authenticate(username: str, password: str) -> str:
    """Authenticate with Dremio and return auth token."""
    print(f"Authenticating with Dremio at {DREMIO_URL}...")
    
    response = requests.post(
        f"{DREMIO_URL}/apiv2/login",
        headers={"Content-Type": "application/json"},
        json={"userName": username, "password": password}
    )
    
    if response.status_code != 200:
        print(f"Authentication failed: {response.status_code}")
        print(response.text)
        sys.exit(1)
    
    token = response.json().get("token")
    print("✓ Authentication successful")
    return token


def get_headers(token: str) -> dict:
    """Return headers with authentication token."""
    return {
        "Authorization": f"_dremio{token}",
        "Content-Type": "application/json"
    }


def check_source_exists(token: str, source_name: str) -> bool:
    """Check if a data source already exists."""
    response = requests.get(
        f"{DREMIO_URL}/api/v3/catalog/by-path/{source_name}",
        headers=get_headers(token)
    )
    return response.status_code == 200


def create_nessie_source(token: str, source_name: str = "lakehouse") -> bool:
    """Create a Nessie data source connected to MinIO."""
    
    if check_source_exists(token, source_name):
        print(f"✓ Data source '{source_name}' already exists")
        return True
    
    print(f"Creating Nessie data source '{source_name}'...")
    
    # Nessie source configuration for Dremio
    source_config = {
        "entityType": "source",
        "name": source_name,
        "type": "NESSIE",
        "config": {
            "nessieEndpoint": NESSIE_INTERNAL_URL,
            "nessieAuthType": "NONE",
            "credentialType": "ACCESS_KEY",
            "awsAccessKey": AWS_ACCESS_KEY,
            "awsAccessSecret": AWS_SECRET_KEY,
            "awsRootPath": "/warehouse",
            "secure": False,
            "propertyList": [
                {"name": "fs.s3a.path.style.access", "value": "true"},
                {"name": "fs.s3a.endpoint", "value": MINIO_INTERNAL_ENDPOINT},
                {"name": "dremio.s3.compat", "value": "true"}
            ]
        }
    }
    
    response = requests.post(
        f"{DREMIO_URL}/api/v3/catalog",
        headers=get_headers(token),
        json=source_config
    )
    
    if response.status_code in [200, 201]:
        print(f"✓ Created Nessie data source '{source_name}'")
        return True
    else:
        print(f"Failed to create data source: {response.status_code}")
        print(response.text)
        return False


def refresh_source(token: str, source_name: str) -> bool:
    """Refresh metadata for a data source."""
    print(f"Refreshing metadata for '{source_name}'...")
    
    # First get the source ID
    response = requests.get(
        f"{DREMIO_URL}/api/v3/catalog/by-path/{source_name}",
        headers=get_headers(token)
    )
    
    if response.status_code != 200:
        print(f"Source not found: {response.status_code}")
        return False
    
    source_id = response.json().get("id")
    
    # Trigger refresh
    response = requests.post(
        f"{DREMIO_URL}/api/v3/catalog/{source_id}/refresh",
        headers=get_headers(token)
    )
    
    if response.status_code in [200, 204]:
        print(f"✓ Metadata refresh triggered for '{source_name}'")
        return True
    else:
        print(f"Refresh failed: {response.status_code}")
        return False


def run_query(token: str, sql: str) -> dict:
    """Run a SQL query in Dremio."""
    print(f"Running query: {sql[:50]}...")
    
    # Submit job
    response = requests.post(
        f"{DREMIO_URL}/api/v3/sql",
        headers=get_headers(token),
        json={"sql": sql}
    )
    
    if response.status_code != 200:
        print(f"Query submission failed: {response.status_code}")
        print(response.text)
        return {}
    
    job = response.json()
    job_id = job.get("id")
    
    # Poll for job completion
    for _ in range(30):
        response = requests.get(
            f"{DREMIO_URL}/api/v3/job/{job_id}",
            headers=get_headers(token)
        )
        
        if response.status_code != 200:
            break
            
        status = response.json()
        job_state = status.get("jobState")
        
        if job_state == "COMPLETED":
            print("✓ Query completed")
            return status
        elif job_state in ["FAILED", "CANCELED"]:
            print(f"Query {job_state}")
            return status
        
        time.sleep(1)
    
    return {}


def test_connection(token: str, source_name: str) -> bool:
    """Test the connection by running a simple query."""
    print("\nTesting connection with sample query...")
    
    # Try to query the orders table (path: source.namespace.table)
    result = run_query(
        token, 
        f"SELECT COUNT(*) as total FROM {source_name}.structured_data.orders"
    )
    
    if result.get("jobState") == "COMPLETED":
        row_count = result.get("rowCount", 0)
        print(f"✓ Found {row_count} rows in orders table")
        return True
    else:
        print("Query did not complete successfully")
        print("This is normal if the Iceberg table hasn't been created yet.")
        print("Run the bridge_ravendb.py script first to create Iceberg tables.")
        return False


def main():
    parser = argparse.ArgumentParser(description="Configure Dremio with Nessie/Iceberg")
    parser.add_argument("--username", "-u", default=None, help="Dremio username (or set DREMIO_USER in .env)")
    parser.add_argument("--password", "-p", default=None, help="Dremio password (or set DREMIO_PASSWORD in .env)")
    parser.add_argument("--source-name", "-s", default="lakehouse", help="Data source name")
    parser.add_argument("--test-only", action="store_true", help="Only test connection")
    parser.add_argument("--refresh", "-r", action="store_true", help="Only refresh metadata (useful after Spark bridge jobs)")
    args = parser.parse_args()
    
    # Get credentials from args or environment
    username = args.username or os.getenv("DREMIO_USER", "admin")
    password = args.password or os.getenv("DREMIO_PASSWORD")
    
    if not password:
        print("Error: Dremio password required. Either:")
        print("  - Pass via --password / -p argument")
        print("  - Set DREMIO_PASSWORD in .env file")
        sys.exit(1)
    
    # Authenticate
    token = authenticate(username, password)
    
    if args.test_only:
        test_connection(token, args.source_name)
        return
    
    if args.refresh:
        # Just refresh metadata and exit
        refresh_source(token, args.source_name)
        print("\n✓ Metadata refresh triggered. Tables should now be visible in Dremio.")
        return
    
    # Create data source
    if create_nessie_source(token, args.source_name):
        # Give it a moment to initialize
        time.sleep(2)
        
        # Refresh metadata
        refresh_source(token, args.source_name)
        
        # Wait for refresh
        time.sleep(3)
        
        # Test connection
        test_connection(token, args.source_name)
    
    print("\n" + "="*50)
    print("Dremio Setup Complete!")
    print("="*50)
    print(f"\nDremio UI: {DREMIO_URL}")
    print(f"Data Source: {args.source_name}")
    print("\nYou can now query Iceberg tables via:")
    print(f"  - Dremio UI SQL Runner")
    print(f"  - ODBC/JDBC on port 31010")
    print(f"  - Arrow Flight on port 32010")


if __name__ == "__main__":
    main()
