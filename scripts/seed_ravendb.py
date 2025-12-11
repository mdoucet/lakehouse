#!/usr/bin/env python3
"""
Seed RavenDB with sample order documents for the AI Data Lakehouse demo.

Creates a database called 'Northwind' with an 'Orders' collection containing
sample order documents that demonstrate the RavenDB -> Iceberg pipeline.

Usage:
  pip install pyravendb
  python seed_ravendb.py
"""

import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
import json

# Try to import ravendb-python client
try:
    from ravendb import DocumentStore
except ImportError:
    print("Installing ravendb package...")
    import subprocess
    subprocess.check_call(["pip", "install", "ravendb"])
    from ravendb import DocumentStore

# RavenDB Configuration
RAVENDB_URL = "http://localhost:8080"
DATABASE_NAME = "Northwind"
NUM_ORDERS = 500

# Sample data for generating realistic orders
CUSTOMERS = [f"customers/{i}-A" for i in range(1, 51)]
PRODUCTS = [
    {"name": "Widget Pro", "price": 29.99},
    {"name": "Gadget Plus", "price": 49.99},
    {"name": "Super Component", "price": 15.99},
    {"name": "Mega Module", "price": 89.99},
    {"name": "Ultra Kit", "price": 199.99},
    {"name": "Basic Pack", "price": 9.99},
    {"name": "Premium Bundle", "price": 299.99},
    {"name": "Starter Set", "price": 39.99},
    {"name": "Advanced Tool", "price": 79.99},
    {"name": "Essential Item", "price": 19.99},
]
STATUSES = ["Pending", "Processing", "Shipped", "Delivered", "Cancelled"]
STATUS_WEIGHTS = [0.1, 0.15, 0.25, 0.45, 0.05]


def generate_order_lines() -> List[Dict[str, Any]]:
    """Generate random order line items."""
    num_lines = random.randint(1, 5)
    lines = []
    for _ in range(num_lines):
        product = random.choice(PRODUCTS)
        lines.append({
            "ProductName": product["name"],
            "Price": product["price"],
            "Quantity": random.randint(1, 10),
        })
    return lines


def generate_order(order_num: int) -> Dict[str, Any]:
    """Generate a single order document."""
    # Random date in the last 2 years
    days_ago = random.randint(0, 730)
    order_date = datetime.now() - timedelta(days=days_ago)
    
    lines = generate_order_lines()
    total = sum(line["Price"] * line["Quantity"] for line in lines)
    
    # Status weighted toward completed orders for older orders
    if days_ago > 30:
        status = random.choices(STATUSES, weights=STATUS_WEIGHTS)[0]
    else:
        status = random.choices(STATUSES, weights=[0.3, 0.3, 0.2, 0.15, 0.05])[0]
    
    return {
        "@metadata": {
            "@collection": "Orders"
        },
        "CustomerId": random.choice(CUSTOMERS),
        "OrderDate": order_date.isoformat(),
        "RequiredDate": (order_date + timedelta(days=random.randint(3, 14))).isoformat(),
        "ShippedDate": (order_date + timedelta(days=random.randint(1, 7))).isoformat() if status in ["Shipped", "Delivered"] else None,
        "Status": status,
        "Lines": lines,
        "TotalAmount": round(total, 2),
        "ShipTo": {
            "City": random.choice(["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Seattle", "Denver", "Boston"]),
            "Country": "USA"
        },
        "Notes": f"Order generated for demo purposes. Batch {order_num // 100 + 1}."
    }


def wait_for_ravendb(max_retries=30, delay=2):
    """Wait for RavenDB to be available."""
    print("Waiting for RavenDB to be ready...")
    import urllib.request
    import urllib.error
    
    for i in range(max_retries):
        try:
            urllib.request.urlopen(f"{RAVENDB_URL}/databases", timeout=5)
            print("✓ RavenDB is ready!")
            return True
        except (urllib.error.URLError, urllib.error.HTTPError):
            print(f"  Attempt {i+1}/{max_retries}: RavenDB not ready yet...")
            time.sleep(delay)
    raise Exception("RavenDB did not become ready in time")


def create_database(store: DocumentStore):
    """Create the database if it doesn't exist."""
    from ravendb.serverwide.operations.common import CreateDatabaseOperation
    from ravendb.serverwide import DatabaseRecord
    
    try:
        # Check if database exists
        store.maintenance.server.send(CreateDatabaseOperation(DatabaseRecord(DATABASE_NAME)))
        print(f"✓ Created database '{DATABASE_NAME}'")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"✓ Database '{DATABASE_NAME}' already exists")
        else:
            raise


def main():
    print("=" * 60)
    print("AI Data Lakehouse - RavenDB Seed Data")
    print("=" * 60)
    
    wait_for_ravendb()
    
    # Initialize DocumentStore
    store = DocumentStore(urls=[RAVENDB_URL], database=DATABASE_NAME)
    store.initialize()
    
    # Create database
    create_database(store)
    
    print(f"\nGenerating {NUM_ORDERS} sample orders...")
    
    # Bulk insert orders
    with store.bulk_insert() as bulk_insert:
        for i in range(NUM_ORDERS):
            order = generate_order(i)
            order_id = f"orders/{i+1:04d}-A"
            bulk_insert.store(order, order_id)
            
            if (i + 1) % 100 == 0:
                print(f"  ✓ Inserted {i + 1} orders...")
    
    print(f"\n✓ Successfully inserted {NUM_ORDERS} orders into RavenDB")
    
    # Print sample order
    with store.open_session() as session:
        sample = session.load("orders/0001-A")
        print("\n" + "-" * 40)
        print("Sample Order (orders/0001-A):")
        print("-" * 40)
        print(json.dumps(sample, indent=2, default=str))
    
    print("\n" + "=" * 60)
    print("✓ RavenDB seeding complete!")
    print("=" * 60)
    print(f"\nRavenDB Studio: {RAVENDB_URL}")
    print(f"Database: {DATABASE_NAME}")
    print(f"Collection: Orders ({NUM_ORDERS} documents)")


if __name__ == "__main__":
    main()
