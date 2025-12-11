#!/usr/bin/env python3
"""
File Inventory Script: Register unstructured files in Iceberg.

This script scans the bronze/files/ bucket and creates an Iceberg registry table
so the AI can query file metadata without scanning S3 every time.

Uses Nessie branching for safe "Write-Audit-Publish" pattern.

Run this inside the Spark container (see bridge_ravendb.py for full command).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, input_file_name, current_timestamp, 
    regexp_extract, lit
)


def create_spark_session():
    """Create Spark session with Iceberg and Nessie configuration."""
    return SparkSession.builder \
        .appName("File_Inventory") \
        .getOrCreate()


def create_namespace_if_not_exists(spark):
    """Create the unstructured namespace if it doesn't exist."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.unstructured")
    print("✓ Namespace 'nessie.unstructured' ready")


def create_file_registry_table(spark):
    """Create the file registry Iceberg table."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.unstructured.file_registry (
            file_path STRING,
            file_name STRING,
            file_extension STRING,
            size_bytes LONG,
            ingested_at TIMESTAMP
        ) USING iceberg
    """)
    print("✓ Table 'nessie.unstructured.file_registry' ready")


def scan_bronze_files(spark):
    """
    Scan the bronze/files/ bucket for all files.
    
    Uses Spark's binaryFile format for efficient metadata-only listing.
    """
    bronze_path = "s3a://lakehouse/bronze/files/"
    
    try:
        # Read file metadata (not content) using binaryFile format
        df = spark.read.format("binaryFile") \
            .option("pathGlobFilter", "*") \
            .option("recursiveFileLookup", "true") \
            .load(bronze_path)
        
        # Extract useful metadata
        df_files = df.select(
            col("path").alias("file_path"),
            regexp_extract(col("path"), r"[^/]+$", 0).alias("file_name"),
            regexp_extract(col("path"), r"\.([^.]+)$", 1).alias("file_extension"),
            col("length").alias("size_bytes"),
            current_timestamp().alias("ingested_at")
        )
        
        count = df_files.count()
        print(f"✓ Found {count} files in bronze layer")
        return df_files
        
    except Exception as e:
        if "Path does not exist" in str(e) or "No such file" in str(e):
            print("⚠ No files found in bronze/files/")
            print("  Creating empty registry. Add files and re-run to index them.")
            return None
        raise


def use_write_audit_publish(spark, df_files):
    """
    Use Nessie branching for safe ingestion (Write-Audit-Publish pattern).
    
    1. Create a feature branch
    2. Write data to the branch
    3. Merge back to main (atomic commit)
    """
    print("\nUsing Write-Audit-Publish pattern with Nessie branches...")
    
    # Step 1: Create ingest branch
    try:
        spark.sql("CREATE BRANCH IF NOT EXISTS ingest_files IN nessie FROM main")
        print("  ✓ Created branch 'ingest_files'")
    except Exception as e:
        if "already exists" in str(e).lower():
            print("  ✓ Branch 'ingest_files' already exists")
        else:
            raise
    
    # Step 2: Switch to ingest branch and write
    spark.conf.set("spark.sql.catalog.nessie.ref", "ingest_files")
    print("  ✓ Switched to branch 'ingest_files'")
    
    # Append new files to registry
    df_files.writeTo("nessie.unstructured.file_registry").append()
    print("  ✓ Wrote files to registry on branch")
    
    # Step 3: Switch back to main and merge
    spark.conf.set("spark.sql.catalog.nessie.ref", "main")
    spark.sql("MERGE BRANCH ingest_files INTO main IN nessie")
    print("  ✓ Merged 'ingest_files' into 'main'")
    
    # Cleanup: drop the feature branch
    spark.sql("DROP BRANCH IF EXISTS ingest_files IN nessie")
    print("  ✓ Cleaned up 'ingest_files' branch")


def simple_append(spark, df_files):
    """Simple append without branching (for demo simplicity)."""
    df_files.writeTo("nessie.unstructured.file_registry").append()
    print("✓ Appended files to registry")


def show_registry_stats(spark):
    """Display file registry statistics."""
    print("\n" + "-" * 40)
    print("File Registry Statistics:")
    print("-" * 40)
    
    # Total files and size
    stats = spark.sql("""
        SELECT 
            COUNT(*) as total_files,
            SUM(size_bytes) as total_bytes,
            COUNT(DISTINCT file_extension) as unique_extensions
        FROM nessie.unstructured.file_registry
    """).collect()[0]
    
    print(f"Total files: {stats['total_files']}")
    print(f"Total size: {stats['total_bytes'] or 0:,} bytes")
    print(f"Unique extensions: {stats['unique_extensions']}")
    
    # Files by extension
    print("\nFiles by extension:")
    spark.sql("""
        SELECT 
            COALESCE(file_extension, 'no-ext') as extension,
            COUNT(*) as count,
            SUM(size_bytes) as total_bytes
        FROM nessie.unstructured.file_registry
        GROUP BY file_extension
        ORDER BY count DESC
        LIMIT 10
    """).show()


def main():
    print("=" * 60)
    print("File Inventory - Register Files in Iceberg")
    print("=" * 60)
    
    # Create Spark session
    print("\nInitializing Spark session...")
    spark = create_spark_session()
    
    # Setup namespace and table
    print("\nSetting up Iceberg table...")
    create_namespace_if_not_exists(spark)
    create_file_registry_table(spark)
    
    # Scan bronze files
    print("\nScanning bronze layer for files...")
    df_files = scan_bronze_files(spark)
    
    if df_files is not None and df_files.count() > 0:
        # Use simple append for demo (change to use_write_audit_publish for prod)
        print("\nRegistering files in Iceberg...")
        simple_append(spark, df_files)
        
        # Show stats
        show_registry_stats(spark)
    else:
        print("\n⚠ No files to register. Add files to bronze/files/ and re-run.")
    
    print("\n" + "=" * 60)
    print("✓ File inventory complete!")
    print("=" * 60)
    print("\nFile metadata available in: nessie.unstructured.file_registry")
    
    spark.stop()


if __name__ == "__main__":
    main()
