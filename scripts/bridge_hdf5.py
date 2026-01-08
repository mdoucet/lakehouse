#!/usr/bin/env python3
"""
Bridge Job: Register HDF5 Metadata in Iceberg Catalog.

This script reads the HDF5 metadata Parquet files from the silver layer
and registers them in an Iceberg table managed by Nessie.

This enables SQL queries over the HDF5 file catalog, e.g.:
  SELECT * FROM nessie.scientific_data.hdf5_catalog 
  WHERE instrument_name = 'REF_L' 
  ORDER BY start_time DESC

Run this inside the Spark container:
  docker exec -it spark-iceberg /opt/spark/bin/spark-submit \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions \
    --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
    --conf spark.sql.catalog.nessie.uri=http://nessie:19120/api/v1 \
    --conf spark.sql.catalog.nessie.ref=main \
    --conf spark.sql.catalog.nessie.warehouse=s3a://lakehouse/warehouse \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=admin \
    --conf spark.hadoop.fs.s3a.secret.key=password \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    /opt/spark/scripts/bridge_hdf5.py

Or use the helper script:
  ./run_spark.sh scripts/bridge_hdf5.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp


def create_spark_session():
    """Create Spark session with Iceberg and Nessie configuration."""
    return SparkSession.builder \
        .appName("HDF5_Catalog_Bridge") \
        .getOrCreate()


def create_namespace_if_not_exists(spark):
    """Create the scientific_data namespace if it doesn't exist."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.scientific_data")
    print("✓ Namespace 'nessie.scientific_data' ready")


def create_hdf5_catalog_table(spark):
    """Create the Iceberg table for HDF5 metadata if it doesn't exist."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.scientific_data.hdf5_catalog (
            file_path STRING,
            file_name STRING,
            file_size_bytes LONG,
            file_modified_time STRING,
            file_created_time STRING,
            ingestion_time TIMESTAMP,
            tiled_uri STRING,
            tiled_adapter STRING,
            content_type STRING,
            hdf5_driver STRING,
            hdf5_libver STRING,
            group_count INT,
            dataset_count INT,
            significant_datasets STRING,
            entry_name STRING,
            title STRING,
            experiment_identifier STRING,
            start_time STRING,
            end_time STRING,
            duration DOUBLE,
            run_number STRING,
            instrument_name STRING,
            instrument_type STRING,
            source_name STRING,
            source_type STRING,
            sample_name STRING,
            sample_description STRING,
            user_name STRING,
            user_facility STRING,
            extraction_error STRING,
            s3_uri STRING,
            cataloged_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (instrument_name)
    """)
    print("✓ Table 'nessie.scientific_data.hdf5_catalog' ready")


def read_hdf5_metadata(spark):
    """Read HDF5 metadata Parquet files from the silver layer."""
    metadata_path = "s3a://lakehouse/silver/hdf5_metadata/*.parquet"
    
    try:
        df = spark.read.parquet(metadata_path)
        count = df.count()
        print(f"✓ Read {count} HDF5 metadata records from silver layer")
        return df
    except Exception as e:
        print(f"⚠ Error reading HDF5 metadata: {e}")
        print("  Make sure ingest_hdf5.py has been run with --upload-metadata first.")
        return None


def merge_into_iceberg(spark, df_source):
    """Merge source data into Iceberg table using UPSERT logic."""
    from pyspark.sql.functions import lit, when
    from pyspark.sql.types import LongType, IntegerType, DoubleType, StringType
    
    # Add cataloged_at timestamp
    df_prepared = df_source.withColumn("cataloged_at", current_timestamp())
    
    # Cast columns to expected types to handle type mismatches
    type_casts = {
        "file_size_bytes": LongType(),
        "group_count": IntegerType(),
        "dataset_count": IntegerType(),
        "duration": DoubleType(),
    }
    
    for col_name, col_type in type_casts.items():
        if col_name in df_prepared.columns:
            df_prepared = df_prepared.withColumn(col_name, col(col_name).cast(col_type))
    
    # Convert string timestamps
    if "ingestion_time" in df_prepared.columns:
        df_prepared = df_prepared.withColumn("ingestion_time", to_timestamp(col("ingestion_time")))
    
    # Ensure all string columns are actually strings (not arrays)
    string_columns = [
        "file_path", "file_name", "file_modified_time", "file_created_time",
        "tiled_uri", "tiled_adapter", "content_type", "hdf5_driver", "hdf5_libver",
        "significant_datasets", "entry_name", "title", "experiment_identifier",
        "start_time", "end_time", "run_number", "instrument_name", "instrument_type",
        "source_name", "source_type", "sample_name", "sample_description",
        "user_name", "user_facility", "extraction_error", "s3_uri"
    ]
    
    for col_name in string_columns:
        if col_name in df_prepared.columns:
            df_prepared = df_prepared.withColumn(col_name, col(col_name).cast(StringType()))
    
    # Fill missing columns with nulls
    expected_columns = [
        "file_path", "file_name", "file_size_bytes", "file_modified_time",
        "file_created_time", "ingestion_time", "tiled_uri", "tiled_adapter",
        "content_type", "hdf5_driver", "hdf5_libver", "group_count",
        "dataset_count", "significant_datasets", "entry_name", "title",
        "experiment_identifier", "start_time", "end_time", "duration",
        "run_number", "instrument_name", "instrument_type", "source_name",
        "source_type", "sample_name", "sample_description", "user_name",
        "user_facility", "extraction_error", "s3_uri", "cataloged_at"
    ]
    
    for col_name in expected_columns:
        if col_name not in df_prepared.columns:
            df_prepared = df_prepared.withColumn(col_name, lit(None))
    
    # Select only expected columns in order
    df_prepared = df_prepared.select(*expected_columns)
    
    # Register source as temp view
    df_prepared.createOrReplaceTempView("hdf5_source")
    
    # Perform MERGE (upsert) based on file_path
    merge_sql = """
        MERGE INTO nessie.scientific_data.hdf5_catalog AS target
        USING hdf5_source AS source
        ON target.file_path = source.file_path
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """
    
    spark.sql(merge_sql)
    
    # Get final count
    final_count = spark.sql("SELECT COUNT(*) FROM nessie.scientific_data.hdf5_catalog").collect()[0][0]
    print(f"✓ Merged data into Iceberg table. Total records: {final_count}")


def show_catalog_summary(spark):
    """Display a summary of the HDF5 catalog."""
    print("\n" + "="*60)
    print("HDF5 CATALOG SUMMARY")
    print("="*60)
    
    # Count by instrument
    print("\nFiles by Instrument:")
    spark.sql("""
        SELECT 
            COALESCE(instrument_name, 'Unknown') as instrument,
            COUNT(*) as file_count,
            SUM(file_size_bytes) / 1024 / 1024 as total_size_mb
        FROM nessie.scientific_data.hdf5_catalog
        GROUP BY instrument_name
        ORDER BY file_count DESC
    """).show(truncate=False)
    
    # Recent files
    print("\nRecent Files:")
    spark.sql("""
        SELECT 
            file_name,
            instrument_name,
            title,
            cataloged_at
        FROM nessie.scientific_data.hdf5_catalog
        ORDER BY cataloged_at DESC
        LIMIT 5
    """).show(truncate=False)
    
    print("="*60)


def main():
    """Main entry point."""
    print("="*60)
    print("HDF5 CATALOG BRIDGE - Registering in Nessie/Iceberg")
    print("="*60 + "\n")
    
    # Create Spark session
    spark = create_spark_session()
    print("✓ Spark session created")
    
    # Create namespace and table
    create_namespace_if_not_exists(spark)
    create_hdf5_catalog_table(spark)
    
    # Read metadata from silver layer
    df_metadata = read_hdf5_metadata(spark)
    
    if df_metadata is None or df_metadata.count() == 0:
        print("\n⚠ No HDF5 metadata found in silver layer.")
        print("  Run ingest_hdf5.py with --upload-metadata first:")
        print("  python scripts/ingest_hdf5.py /path/to/files --upload-metadata")
        spark.stop()
        return 1
    
    # Merge into Iceberg
    merge_into_iceberg(spark, df_metadata)
    
    # Show summary
    show_catalog_summary(spark)
    
    # Stop Spark
    spark.stop()
    print("\n✓ Bridge job completed successfully")
    
    return 0


if __name__ == "__main__":
    exit(main())
