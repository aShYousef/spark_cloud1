# Spark Data Processor

## Overview
A cloud-based distributed data processing web application with Spark/PySpark integration for ML jobs, performance benchmarking, and scalability analysis.

## Project Architecture
- **Backend**: FastAPI (Python 3.11) running on port 8000
- **Frontend**: React 18 with Vite, Tailwind CSS, Recharts running on port 5000
- **Database**: PostgreSQL for persistent file/job storage
- **Storage**: Local filesystem with S3 fallback support
- **ML Engine**: PySpark simulation with Databricks REST API integration

## Key Files
- `backend/app/main.py` - FastAPI application and endpoints
- `backend/app/spark_jobs.py` - ML task implementations with PDF support
- `backend/app/job_manager.py` - Job orchestration
- `backend/app/database.py` - PostgreSQL persistence layer
- `frontend/src/App.jsx` - React main component
- `scripts/` - Utility scripts for deployment and data download
- `notebooks/` - Databricks PySpark notebooks for production
- `infrastructure/terraform/` - AWS and Databricks IaC templates

## Running the Application
The application runs via a combined workflow (`bash start.sh`) that:
1. Starts the FastAPI backend on port 8000
2. Starts the React frontend on port 5000 (exposed to user)

## Environment Variables
- `DATABASE_URL` - PostgreSQL connection string (auto-configured)
- `STORAGE_BACKEND` - Storage type (local/s3), defaults to "local"
- `DATABRICKS_HOST` - Databricks workspace URL (optional)
- `DATABRICKS_TOKEN` - Databricks access token (optional)
- `AWS_*` - AWS credentials for S3 storage (optional)

## Features Implemented
1. File upload with validation (CSV, JSON, TXT, PDF with text extraction)
2. ML tasks: descriptive stats, linear/logistic regression, K-means, FP-Growth, time series
3. Performance benchmarking with 1/2/4/8 workers (speedup/efficiency)
4. Job monitoring with real-time logs
5. Results visualization with charts
6. PostgreSQL persistence for files and jobs
7. Terraform templates for AWS/Databricks deployment

## Recent Changes
- Added PostgreSQL database for persistent storage
- Added PDF text extraction support
- Improved documentation with architecture explanation
- Added deployment scripts and sample datasets
