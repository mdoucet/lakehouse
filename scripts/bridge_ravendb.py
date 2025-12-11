#!/usr/bin/env python3
"""
Bridge Job: Merge RavenDB Landing Zone into Iceberg Table.

This script reads the raw Parquet files written by ravendb_sync.py (or RavenDB OLAP ETL)
and merges them into a managed Iceberg table using UPSERT logic.

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
    /opt/spark/scripts/bridge_ravendb.py
"""

from pyspark.sql import SparkSession


def create_spark_session():
    """Create Spark session with Iceberg and Nessie configuration."""
    return SparkSession.builder \
        .appName("RavenDB_Bridge") \
        .getOrCreate()


def create_namespace_if_not_exists(spark):
    """Create the structured_data namespace if it doesn't exist."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.structured_data")
    print("✓ Namespace 'nessie.structured_data' ready")


def create_orders_table(spark):
    """Create the Iceberg orders table if it doesn't exist."""
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.structured_data.orders (
            OrderId STRING,
            CustomerId STRING,
            OrderDate TIMESTAMP,
            TotalAmount DOUBLE,
            Status STRING,
            ShipCity STRING,
            ShipCountry STRING,
            LineCount INT,
            SyncedAt TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(OrderDate))
    """)
    print("✓ Table 'nessie.structured_data.orders' ready")


def read_landing_zone(spark):
    """Read raw Parquet files from the RavenDB landing zone."""
    landing_path = "s3a://lakehouse/silver/ravendb_landing/orders/*/*.parquet"
    
    try:
        df = spark.read.parquet(landing_path)
        count = df.count()
        print(f"✓ Read {count} records from landing zone")
        return df
    except Exception as e:
        print(f"⚠ Error reading landing zone: {e}")
        print("  Make sure ravendb_sync.py has been run first.")
        return None


def merge_into_iceberg(spark, df_source):
    """Merge source data into Iceberg table using UPSERT logic."""
    # Register source as temp view
    df_source.createOrReplaceTempView("source_updates")
    
    # Perform MERGE (upsert)
    merge_sql = """
        MERGE INTO nessie.structured_data.orders AS target
        USING source_updates AS source
        ON target.OrderId = source.OrderId
        WHEN MATCHED THEN 
            UPDATE SET 
                CustomerId = source.CustomerId,
                OrderDate = source.OrderDate,
                TotalAmount = source.TotalAmount,
                Status = source.Status,
                ShipCity = source.ShipCity,
                ShipCountry = source.ShipCountry,
                LineCount = source.LineCount,
                SyncedAt = source.SyncedAt
        WHEN NOT MATCHED THEN 
            INSERT (OrderId, CustomerId, OrderDate, TotalAmount, Status, 
                    ShipCity, ShipCountry, LineCount, SyncedAt)
            VALUES (source.OrderId, source.CustomerId, source.OrderDate, 
                    source.TotalAmount, source.Status, source.ShipCity, 
                    source.ShipCountry, source.LineCount, source.SyncedAt)
    """
    
    spark.sql(merge_sql)
    print("✓ MERGE completed successfully")


def show_table_stats(spark):
    """Display table statistics after merge."""
    print("\n" + "-" * 40)
    print("Table Statistics:")
    print("-" * 40)
    
    # Row count
    count = spark.sql("SELECT COUNT(*) as cnt FROM nessie.structured_data.orders").collect()[0]["cnt"]
    print(f"Total rows: {count}")
    
    # Status distribution
    print("\nStatus distribution:")
    spark.sql("""
        SELECT Status, COUNT(*) as count 
        FROM nessie.structured_data.orders 
        GROUP BY Status 
        ORDER BY count DESC
    """).show()
    
    # Recent snapshots (Iceberg history)
    print("Recent snapshots (time travel history):")
    spark.sql("""
        SELECT snapshot_id, committed_at, operation 
        FROM nessie.structured_data.orders.snapshots 
        ORDER BY committed_at DESC 
        LIMIT 5
    """).show(truncate=False)


def main():
    print("=" * 60)
    print("RavenDB Bridge Job - Merge to Iceberg")
    print("=" * 60)
    
    # Create Spark session
    print("\nInitializing Spark session...")
    spark = create_spark_session()
    
    # Setup namespace and table
    print("\nSetting up Iceberg table...")
    create_namespace_if_not_exists(spark)
    create_orders_table(spark)
    
    # Read from landing zone
    print("\nReading from landing zone...")
    df_source = read_landing_zone(spark)
    
    if df_source is None:
        spark.stop()
        return
    
    # Perform merge
    print("\nMerging into Iceberg table...")
    merge_into_iceberg(spark, df_source)
    
    # Show stats
    show_table_stats(spark)
    
    print("\n" + "=" * 60)
    print("✓ Bridge job complete!")
    print("=" * 60)
    print("\nData is now available in: nessie.structured_data.orders")
    print("Query with: SELECT * FROM nessie.structured_data.orders LIMIT 10")
    
    spark.stop()


if __name__ == "__main__":
    main()
