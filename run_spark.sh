#!/bin/bash
# Helper script to run Spark jobs with all required configurations

SPARK_PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0,org.apache.hadoop:hadoop-aws:3.3.4"

SPARK_CONF=(
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions
  --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog
  --conf spark.sql.catalog.nessie.uri=http://nessie:19120/api/v1
  --conf spark.sql.catalog.nessie.ref=main
  --conf spark.sql.catalog.nessie.warehouse=s3a://lakehouse/warehouse
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000
  --conf spark.hadoop.fs.s3a.access.key=admin
  --conf spark.hadoop.fs.s3a.secret.key=password
  --conf spark.hadoop.fs.s3a.path.style.access=true
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
)

if [ -z "$1" ]; then
  echo "Usage: $0 <script_name.py>"
  echo "Example: $0 bridge_ravendb.py"
  exit 1
fi

SCRIPT_PATH="/opt/spark/scripts/$1"

echo "Running Spark job: $1"
echo "================================"

docker exec -it spark-iceberg /opt/spark/bin/spark-submit \
  --packages "$SPARK_PACKAGES" \
  "${SPARK_CONF[@]}" \
  "$SCRIPT_PATH"
