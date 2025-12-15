Spark Data Processor

A cloud-based distributed data processing and machine learning platform built using Apache Spark / PySpark, FastAPI, and React.
The system enables users to upload datasets, run parallel and distributed analytics, execute multiple machine learning jobs, and evaluate scalability and performance on cloud infrastructure.


---

📌 Project Requirements Coverage

This project satisfies all required specifications:

Cloud-based data processing service

Distributed and parallel processing using Apache Spark / PySpark

Dataset upload and cloud storage

Descriptive statistics (multiple metrics)

Multiple machine learning jobs using Spark MLlib

Execution on 1, 2, 4, and 8 workers

Performance measurement (execution time, speedup, efficiency)

Results visualization and download

User-friendly web interface



---

🏗️ Architecture Overview

The system follows a three-layer cloud architecture:

1. Frontend (React + Vite)

Dataset upload

Task selection

Job monitoring

Visualization of performance metrics



2. Backend API (FastAPI)

Handles file uploads

Stores datasets in cloud/local storage

Submits Spark jobs

Tracks job status and execution metrics



3. Distributed Processing Layer

Apache Spark / PySpark

Executed on Databricks clusters

Scales across multiple workers





---

✨ Features

🔹 Data Upload

Supports CSV, JSON, and Parquet files

Files stored in cloud storage (AWS S3 or local storage for development)


🔹 Descriptive Statistics

At least four statistics are computed and stored, including:

Number of rows

Number of columns

Data types

Null / missing value percentages

Min / Max / Mean / Std (numeric columns)

Unique value counts


🔹 Machine Learning Jobs (Spark MLlib)

The following five ML jobs are implemented:

1. Descriptive Statistics


2. Linear Regression


3. Logistic Regression


4. K-Means Clustering


5. FP-Growth (Frequent Pattern Mining)



Each job:

Runs in a distributed Spark environment

Outputs results to cloud storage

Displays results in the UI



---

📈 Scalability and Performance Evaluation

To evaluate scalability, each ML job is executed using different cluster sizes:

1 worker

2 workers

4 workers

8 workers


For each configuration:

Execution time is recorded

Speedup and efficiency are computed


Metrics:

Speedup    = T1 / Tp
Efficiency = Speedup / p

Where:

T1 = execution time with 1 worker

Tp = execution time with p workers


Performance results are:

Displayed in the web interface

Stored in cloud storage for reporting



---

📂 Project Structure

spark_cloud1/
│
├── backend/
│   ├── app/
│   │   ├── main.py          # FastAPI application
│   │   ├── models.py        # Database models
│   │   ├── schemas.py       # API schemas
│   │   └── services/
│   │       └── spark_jobs.py
│   └── requirements.txt
│
├── frontend/
│   ├── src/
│   │   ├── App.jsx
│   │   ├── main.jsx
│   │   └── index.css
│   ├── vite.config.js
│   └── package.json
│
├── notebooks/
│   └── spark_ml_jobs.py     # PySpark ML jobs (Databricks)
│
├── infrastructure/
│   └── terraform/
│       ├── main.tf
│       └── databricks.tf
│
├── scripts/
│   ├── databricks_deploy.py
│   ├── download_sample_data.py
│   └── run_local.sh
│
└── README.md


---

🚀 Quick Start (Local Development)

1️⃣ Install Dependencies

cd backend
pip install -r requirements.txt

cd ../frontend
npm install


---

2️⃣ Run the Application

bash scripts/run_local.sh


---

3️⃣ Access the Application

Frontend UI:
👉 http://localhost:5000

Backend API:
👉 http://localhost:8000

API Documentation:
👉 http://localhost:8000/docs



---

☁️ Cloud Deployment

AWS Setup

S3 bucket for dataset and result storage

IAM roles for Spark access


Databricks Setup

Spark clusters configured with 1–8 workers

PySpark notebook uploaded automatically

Jobs executed remotely via Databricks REST API



---

🔌 API Reference

Upload Dataset

POST /api/files/upload

Create Spark Job

POST /api/jobs

Get Job Status

GET /api/jobs/{job_id}

Get Performance Metrics

GET /api/jobs/{job_id}/metrics


---

⚙️ Configuration

Variable	Description	Default

STORAGE_BACKEND	local / s3	local
AWS_REGION	AWS region	us-east-1
DATABRICKS_HOST	Databricks workspace URL	—
DATABRICKS_TOKEN	Databricks token	—



---

🎓 Academic Notes

Large datasets from UCI Machine Learning Repository are used

The project demonstrates parallel processing, distributed ML, and scalability

Designed for academic evaluation and cloud computing coursework
Conclusion

This project demonstrates how distributed Spark-based analytics and machine learning can be delivered as a cloud service with performance evaluation and scalability analysis.
