# Spark Data Processor

A cloud-based distributed data processing web application with Spark/PySpark integration for ML jobs, performance benchmarking, and scalability analysis.

## Features

- **File Upload**: Support for CSV, JSON, TXT, and PDF files up to 100MB
- **Cloud Storage**: S3 integration with local filesystem fallback
- **Distributed Processing**: Databricks cluster management via REST API
- **ML Tasks**:
  - Descriptive Statistics (rows, columns, null %, unique counts, min/max/mean)
  - Linear Regression (R2, RMSE, MAE)
  - Logistic Regression (accuracy, precision, recall, F1)
  - K-Means Clustering (inertia, silhouette score)
  - FP-Growth (frequent patterns, association rules)
  - Time Series Analysis (temporal aggregations)
- **Performance Benchmarking**: Run jobs on 1, 2, 4, 8 workers with speedup/efficiency metrics
- **Job Monitoring**: Real-time status, logs, and progress tracking

## Project Structure

```
├── backend/                 # FastAPI backend
│   ├── app/
│   │   ├── main.py         # API endpoints
│   │   ├── models.py       # Pydantic models
│   │   ├── config.py       # Configuration
│   │   ├── storage.py      # Storage backends (Local/S3)
│   │   ├── spark_jobs.py   # Spark job implementations
│   │   ├── job_manager.py  # Job orchestration
│   │   └── databricks_service.py  # Databricks REST API
│   └── requirements.txt
├── frontend/               # React frontend
│   ├── src/
│   │   ├── App.jsx        # Main application
│   │   └── index.css      # Tailwind styles
│   └── package.json
├── scripts/                # Utility scripts
│   ├── download_sample_data.py  # Dataset downloader
│   ├── run_local.sh       # Local development
│   └── databricks_deploy.py     # Databricks deployment
├── infrastructure/         # IaC templates
│   └── terraform/
│       ├── main.tf        # AWS resources
│       └── databricks.tf  # Databricks config
├── notebooks/             # Databricks notebooks
│   └── spark_ml_jobs.py   # PySpark ML implementations
└── docs/
    └── performance_report.md  # Sample performance report
```

## Quick Start

### Local Development

1. **Install Dependencies**

```bash
# Backend
cd backend
pip install -r requirements.txt

# Frontend
cd frontend
npm install
```

2. **Set Environment Variables**

```bash
# For local development (storage fallback)
export STORAGE_BACKEND=local

# For S3 storage
export STORAGE_BACKEND=s3
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1
export S3_BUCKET=your-bucket-name

# For Databricks (optional)
export DATABRICKS_HOST=your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=your-personal-access-token
```

3. **Run the Application**

```bash
# Start backend (port 8000)
cd backend
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# Start frontend (port 5000)
cd frontend
npm run dev
```

4. **Access the Application**

- Frontend: http://localhost:5000
- API Docs: http://localhost:8000/docs

### Download Sample Datasets

```bash
# Download all standard datasets
python scripts/download_sample_data.py

# Download specific dataset
python scripts/download_sample_data.py -d iris

# Generate large synthetic dataset
python scripts/download_sample_data.py -d synthetic -r 100000
```

## Cloud Deployment

### AWS S3 Setup

1. Create an S3 bucket for data storage
2. Configure IAM credentials with S3 access
3. Set environment variables

### Databricks Setup

1. Create a Databricks workspace
2. Generate a personal access token
3. Upload notebooks to workspace:

```bash
python scripts/databricks_deploy.py check
python scripts/databricks_deploy.py upload \
    --local-path notebooks/spark_ml_jobs.py \
    --workspace-path /Shared/spark-data-processor/ml_jobs
```

4. Create a cluster:

```bash
python scripts/databricks_deploy.py create-cluster \
    --cluster-name spark-processor \
    --num-workers 4
```

### Terraform Deployment

```bash
cd infrastructure/terraform

# Initialize
terraform init

# Plan
terraform plan -var="aws_region=us-east-1"

# Apply
terraform apply
```

## API Reference

### Upload File
```
POST /api/files/upload
Content-Type: multipart/form-data
```

### Create Job
```
POST /api/jobs
Content-Type: application/json

{
  "file_id": "uuid",
  "tasks": ["descriptive_stats", "linear_regression", "kmeans"],
  "worker_counts": [1, 2, 4, 8]
}
```

### Get Job Status
```
GET /api/jobs/{job_id}
```

### Get Performance Metrics
```
GET /api/jobs/{job_id}/metrics
```

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| STORAGE_BACKEND | Storage type (local/s3) | local |
| MAX_UPLOAD_SIZE_MB | Maximum file size | 100 |
| AWS_ACCESS_KEY_ID | AWS access key | - |
| AWS_SECRET_ACCESS_KEY | AWS secret key | - |
| S3_BUCKET | S3 bucket name | - |
| DATABRICKS_HOST | Databricks workspace URL | - |
| DATABRICKS_TOKEN | Databricks access token | - |

## Performance Testing

1. Generate a large dataset:
```bash
python scripts/download_sample_data.py -d synthetic -r 500000
```

2. Upload the dataset via the UI

3. Select all ML tasks and worker configurations

4. Review the performance metrics and charts

See [Performance Report](docs/performance_report.md) for sample results.

## License

MIT License
