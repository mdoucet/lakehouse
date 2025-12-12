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

**Note for Apple Silicon (M1/M2/M3) users:** This demo uses Milvus v2.4.0 which has improved ARM64 support. All services are tested and working on Apple Silicon Macs.

## Environment Configuration

All service endpoints, ports, and credentials are configured via a `.env` file. This makes it easy to customize the setup without editing multiple files.

The `.env` file contains configuration for:
- **RavenDB** - Database URL and ports
- **MinIO** - S3-compatible storage endpoints and credentials
- **Nessie** - Iceberg catalog endpoint
- **Milvus** - Vector database connection details
- **Spark** - Compute engine ports
- **AWS/S3** - Credentials for MinIO access

**Important:** The `.env` file is loaded automatically by both:
- **docker-compose** - When starting services
- **Python scripts** - Via `python-dotenv` package

All scripts use `os.getenv()` to read configuration, with sensible defaults if variables are not set.

### Customizing Configuration

To customize ports or endpoints, simply edit the `.env` file before starting services:

```bash
# Example: Change MinIO ports if 9100/9101 are already in use
MINIO_PORT=9200
MINIO_CONSOLE_PORT=9201
```

Then start services with the new configuration:

```bash
docker-compose up -d
```

## Quick Start

### 1. Set Up Python Environment (Recommended)

Create and activate a Python virtual environment to manage dependencies:

```bash
cd /path/to/lakehouse

# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# On Linux/macOS:
source venv/bin/activate

# On Windows:
# venv\Scripts\activate
```

Install all Python dependencies at once:

```bash
pip install -r requirements.txt
```

This installs all required packages including `python-dotenv` for environment variable support.

**Note:** Keep the virtual environment activated for all subsequent Python commands.

### 2. Start All Services

The `.env` file is automatically loaded by docker-compose:

```bash
docker-compose up -d
```

Wait for all services to be healthy:

```bash
docker-compose ps
```

### 3. Initialize the Storage Layer

```bash
python scripts/init_buckets.py
```

The script will automatically read configuration from `.env`.

### 4. Seed RavenDB with Sample Data

```bash
python scripts/seed_ravendb.py
```

### 5. Sync RavenDB to Parquet

```bash
python scripts/ravendb_sync.py
```

### 6. Bridge to Iceberg (via Spark)

Use the helper script to simplify Spark job execution:

```bash
./run_spark.sh bridge_ravendb.py
```

Or run manually with full configuration:

```bash
docker exec -it spark-iceberg /opt/spark/bin/spark-submit \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions \
  --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
  --conf spark.sql.catalog.nessie.uri=http://nessie:19120/api/v1 \
  --conf spark.sql.catalog.nessie.ref=main \
  --conf spark.sql.catalog.nessie.warehouse=s3a://lakehouse/warehouse \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9100 \
  --conf spark.hadoop.fs.s3a.access.key=admin \
  --conf spark.hadoop.fs.s3a.secret.key=password \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  /opt/spark/scripts/bridge_ravendb.py
```

### 7. Generate Embeddings

```bash
python scripts/generate_embeddings.py
```

### 8. Load Vectors into Milvus

```bash
python scripts/milvus_bulk_load.py
```

### 9. Run the Demo Notebook

```bash
jupyter notebook notebooks/demo.ipynb
```

## Service URLs

The following URLs use the default ports from `.env`. If you've customized the ports, adjust accordingly:

| Service | URL | Credentials | Environment Variable |
|---------|-----|-------------|---------------------|
| RavenDB Studio | http://localhost:8080 | - | `RAVENDB_PORT` |
| MinIO Console | http://localhost:9101 | admin / password | `MINIO_CONSOLE_PORT` |
| MinIO S3 API | http://localhost:9100 | admin / password | `MINIO_PORT` |
| Nessie API | http://localhost:19120 | - | `NESSIE_PORT` |
| Milvus | localhost:19530 | - | `MILVUS_PORT` |
| Spark UI | http://localhost:4040 | - | `SPARK_UI_PORT` |

## Project Structure

```
lakehouse/
├── docker-compose.yml       # All services
├── run_spark.sh             # Helper script for Spark jobs
├── requirements.txt         # Python dependencies
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

### Milvus not starting (ARM64/Apple Silicon)
The demo uses Milvus v2.4.0 which has better ARM64 support. If you experience issues:
```bash
docker-compose logs milvus
```

To use an older version or troubleshoot, check the Milvus logs for etcd-related errors.

### Milvus memory issues
Reduce Milvus memory in `docker-compose.yml`:
```yaml
milvus:
  mem_limit: 1g
```

### Parquet schema compatibility issues
The `ravendb_sync.py` script writes Parquet files with millisecond-precision timestamps for Spark compatibility. If you encounter schema errors, ensure you're running the latest version of the sync script.

### Spark package download slow
The Spark packages are cached in a Docker volume. First run may be slow.

### MinIO connection refused
Wait for MinIO to be healthy before running scripts:
```bash
docker-compose exec minio curl -f http://localhost:9000/minio/health/live
```

### Python dependencies
Install all dependencies at once:
```bash
pip install -r requirements.txt
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
