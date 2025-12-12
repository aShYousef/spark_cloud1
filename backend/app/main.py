import os
import uuid
import asyncio
from datetime import datetime
from typing import List, Optional
from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .config import settings
from .models import (
    Job, JobConfig, JobResult, JobStatus, MLTaskType,
    FileUploadResponse, PerformanceMetrics
)
from .storage import get_storage
from .job_manager import job_manager
from .databricks_service import databricks_service
from .database import db_store

app = FastAPI(
    title=settings.APP_NAME,
    description="Cloud-based distributed data processing with Spark/PySpark",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

storage = get_storage()

@app.get("/api/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "storage_backend": settings.STORAGE_BACKEND,
        "databricks_configured": databricks_service.is_configured()
    }

@app.post("/api/files/upload", response_model=FileUploadResponse)
async def upload_file(file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")
    
    ext = file.filename.split('.')[-1].lower()
    if ext not in settings.ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"File type '{ext}' not allowed. Allowed types: {settings.ALLOWED_EXTENSIONS}"
        )
    
    content = await file.read()
    file_size = len(content)
    
    max_size = settings.MAX_UPLOAD_SIZE_MB * 1024 * 1024
    if file_size > max_size:
        raise HTTPException(
            status_code=400,
            detail=f"File too large. Maximum size: {settings.MAX_UPLOAD_SIZE_MB}MB"
        )
    
    storage_path = await storage.save_file(content, file.filename, "uploads")
    file_id = storage_path.split('/')[-1].split('.')[0]
    
    db_store.save_file(file_id, file.filename, ext, file_size, storage_path)
    
    file_info = FileUploadResponse(
        file_id=file_id,
        filename=file.filename,
        file_size=file_size,
        file_type=ext,
        storage_path=storage_path,
        upload_time=datetime.utcnow()
    )
    
    return file_info

@app.get("/api/files")
async def list_files():
    return db_store.list_files()

@app.get("/api/files/{file_id}")
async def get_file_info(file_id: str):
    file_data = db_store.get_file(file_id)
    if not file_data:
        raise HTTPException(status_code=404, detail="File not found")
    return file_data

@app.get("/api/files/download/{file_path:path}")
async def download_file(file_path: str):
    try:
        full_path = os.path.join("storage", file_path)
        if os.path.exists(full_path):
            return FileResponse(full_path)
        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/files/preview/{file_id}")
async def preview_file(file_id: str, rows: int = 10):
    file_info = db_store.get_file(file_id)
    if not file_info:
        raise HTTPException(status_code=404, detail="File not found")
    try:
        content = await storage.get_file(file_info["storage_path"])
        
        if file_info["file_type"] == "csv":
            import pandas as pd
            from io import StringIO
            df = pd.read_csv(StringIO(content.decode('utf-8')), nrows=rows)
            return {
                "columns": df.columns.tolist(),
                "data": df.to_dict(orient="records"),
                "total_columns": len(df.columns),
                "preview_rows": len(df)
            }
        elif file_info["file_type"] == "json":
            import json
            data = json.loads(content.decode('utf-8'))
            if isinstance(data, list):
                return {"data": data[:rows], "total_records": len(data)}
            return {"data": data}
        else:
            lines = content.decode('utf-8').split('\n')[:rows]
            return {"lines": lines}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/jobs", response_model=Job)
async def create_job(config: JobConfig, background_tasks: BackgroundTasks):
    file_info = db_store.get_file(config.file_id)
    if not file_info:
        raise HTTPException(status_code=404, detail="File not found")
    
    job = await job_manager.create_job(
        file_id=config.file_id,
        filename=file_info["filename"],
        config=config
    )
    
    background_tasks.add_task(
        job_manager.run_job,
        job.job_id,
        file_info["storage_path"]
    )
    
    return job

@app.get("/api/jobs", response_model=List[Job])
async def list_jobs():
    return job_manager.list_jobs()

@app.get("/api/jobs/{job_id}", response_model=Job)
async def get_job(job_id: str):
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job

@app.get("/api/jobs/{job_id}/logs")
async def get_job_logs(job_id: str):
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"logs": job.logs}

@app.get("/api/jobs/{job_id}/results")
async def get_job_results(job_id: str):
    job = job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"results": job.results}

@app.get("/api/jobs/{job_id}/metrics", response_model=List[PerformanceMetrics])
async def get_job_metrics(job_id: str):
    metrics = job_manager.compute_performance_metrics(job_id)
    if not metrics:
        raise HTTPException(status_code=404, detail="No metrics available")
    return metrics

@app.get("/api/tasks")
async def get_available_tasks():
    return {
        "tasks": [
            {
                "id": MLTaskType.DESCRIPTIVE_STATS.value,
                "name": "Descriptive Statistics",
                "description": "Compute rows, columns, null %, unique counts, min/max/mean"
            },
            {
                "id": MLTaskType.LINEAR_REGRESSION.value,
                "name": "Linear Regression",
                "description": "Train linear regression model with R2, RMSE, MAE metrics"
            },
            {
                "id": MLTaskType.LOGISTIC_REGRESSION.value,
                "name": "Logistic Regression",
                "description": "Binary classification with accuracy, precision, recall, F1"
            },
            {
                "id": MLTaskType.KMEANS.value,
                "name": "K-Means Clustering",
                "description": "Unsupervised clustering with inertia and silhouette metrics"
            },
            {
                "id": MLTaskType.FPGROWTH.value,
                "name": "FP-Growth",
                "description": "Frequent pattern mining and association rules"
            },
            {
                "id": MLTaskType.TIME_SERIES.value,
                "name": "Time Series Analysis",
                "description": "Temporal aggregations and pattern analysis"
            }
        ]
    }

@app.get("/api/cluster/status")
async def get_cluster_status():
    if databricks_service.is_configured():
        clusters = await databricks_service.list_clusters()
        return {"configured": True, "clusters": clusters}
    return {"configured": False, "message": "Databricks not configured. Using local simulation."}

if os.path.exists("static"):
    app.mount("/", StaticFiles(directory="static", html=True), name="static")
