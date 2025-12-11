# AI Data Lakehouse Demo

A complete demonstration of an AI Data Lakehouse running on Docker, featuring:

- **RavenDB** - Document database (source system)
- **MinIO** - S3-compatible object storage
- **Apache Iceberg** - Open table format with time travel
- **Project Nessie** - Git-like versioning for data
- **Milvus** - Vector database for AI/semantic search
- **Apache Spark** - Compute engine

## Architecture

```
RavenDB (Documents) ──────────────────────────────────────┐
         │                                                │
         ▼                                                ▼
    OLAP ETL or                                     MinIO (S3)
    Custom Sync                                    ┌─────────────┐
         │                                         │ bronze/     │ ← Raw files
         ▼                                         │ silver/     │ ← Landing zone
┌─────────────────┐                                │ gold/       │ ← Vectors
│  Landing Zone   │──────────────────────────────▶│ warehouse/  │ ← Iceberg
│  (Parquet)      │                                └─────────────┘
└─────────────────┘                                       │
         │                                                │
         ▼                                                ▼
┌─────────────────┐      ┌─────────────────┐     ┌─────────────────┐
│  Apache Spark   │─────▶│ Apache Iceberg  │────▶│ Project Nessie  │
│  (Compute)      │      │ (Table Format)  │     │ (Catalog)       │
└─────────────────┘      └─────────────────┘     └─────────────────┘
         │
         ▼
┌─────────────────┐      ┌─────────────────┐
│  Embeddings     │─────▶│     Milvus      │
│  (Vectors)      │      │ (Vector Search) │
└─────────────────┘      └─────────────────┘
```

## Prerequisites

- **Docker Desktop** 4.x or later
- **16 GB RAM** (minimum 12 GB)
- **20 GB free disk space**
- **Python 3.9+** (for running scripts locally)

## Quick Start

### 1. Start All Services

```bash
cd /path/to/lakehouse
docker-compose up -d
```

Wait for all services to be healthy:

```bash
docker-compose ps
```

### 2. Initialize the Storage Layer

```bash
pip install boto3
python scripts/init_buckets.py
```

### 3. Seed RavenDB with Sample Data

```bash
pip install ravendb
python scripts/seed_ravendb.py
```

### 4. Sync RavenDB to Parquet

```bash
pip install pandas pyarrow
python scripts/ravendb_sync.py
```

### 5. Bridge to Iceberg (via Spark)

```bash
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
```

### 6. Generate Embeddings

```bash
pip install sentence-transformers
python scripts/generate_embeddings.py
```

### 7. Load Vectors into Milvus

```bash
pip install pymilvus
python scripts/milvus_bulk_load.py
```

### 8. Run the Demo Notebook

```bash
pip install jupyter
jupyter notebook notebooks/demo.ipynb
```

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| RavenDB Studio | http://localhost:8080 | - |
| MinIO Console | http://localhost:9001 | admin / password |
| Nessie API | http://localhost:19120 | - |
| Milvus | localhost:19530 | - |
| Spark UI | http://localhost:4040 | - |

## Project Structure

```
lakehouse/
├── docker-compose.yml       # All services
├── scripts/
│   ├── init_buckets.py      # Create MinIO bucket structure
│   ├── seed_ravendb.py      # Populate RavenDB with sample data
│   ├── ravendb_sync.py      # Sync RavenDB → Parquet (Community Ed.)
│   ├── bridge_ravendb.py    # Merge landing zone → Iceberg
│   ├── inventory_files.py   # Register unstructured files
│   ├── generate_embeddings.py # Create vector embeddings
│   └── milvus_bulk_load.py  # Load vectors into Milvus
├── notebooks/
│   └── demo.ipynb           # Interactive demonstration
├── data/                    # Sample files (mounted to Spark)
└── docs/
    └── lakehouse.md         # Original specification
```

## Data Flow

1. **RavenDB** stores operational documents (Orders)
2. **ravendb_sync.py** exports documents to Parquet in MinIO
3. **bridge_ravendb.py** (Spark) merges Parquet into Iceberg tables
4. **generate_embeddings.py** creates vector embeddings
5. **milvus_bulk_load.py** indexes vectors in Milvus
6. **Application** queries Milvus → Iceberg → MinIO

## RavenDB Enterprise (Optional)

If you have a RavenDB Enterprise license, you can use the native OLAP ETL instead of `ravendb_sync.py`:

1. Open RavenDB Studio (http://localhost:8080)
2. Go to Settings → Ongoing Tasks → Add Task → OLAP ETL
3. Configure destination:
   - Type: S3
   - Bucket: `lakehouse`
   - Path: `silver/ravendb_landing/orders/`
   - Endpoint: `http://minio:9000`
   - Access Key: `admin`
   - Secret Key: `password`

## Troubleshooting

### Services not starting
```bash
docker-compose logs <service-name>
```

### Milvus memory issues
Reduce Milvus memory in `docker-compose.yml`:
```yaml
milvus:
  mem_limit: 1g
```

### Spark package download slow
The Spark packages are cached in a Docker volume. First run may be slow.

### MinIO connection refused
Wait for MinIO to be healthy before running scripts:
```bash
docker-compose exec minio curl -f http://localhost:9000/minio/health/live
```

## Cleanup

```bash
# Stop all services
docker-compose down

# Remove all data (volumes)
docker-compose down -v
```

## Next Steps

- Add OpenMetadata for data governance
- Configure RavenDB Enterprise OLAP ETL
- Implement incremental sync (CDC)
- Add Trino/Presto for SQL queries
- Deploy to Kubernetes
