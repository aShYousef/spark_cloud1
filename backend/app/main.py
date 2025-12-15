import os
from datetime import datetime
from typing import List

from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .config import settings
from .models import (
    Job, JobConfig, JobStatus, MLTaskType,
    FileUploadResponse, PerformanceMetrics
)
from .storage import get_storage
from .job_manager import job_manager
from .databricks_service import databricks_service
from .database import db_store

# =========================
# App initialization
# =========================
app = FastAPI(
    title=settings.APP_NAME,
    description="Cloud-based distributed data processing with Spark/PySpark",
    version="1.0.0"
)

# =========================
# Middleware
# =========================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

storage = get_storage()

# =========================
# Root & Health
# =========================
@app.get("/")
async def root():
    return {
        "message": "Spark Cloud API is running",
        "docs": "/docs",
        "health": "/api/health"
    }

@app.get("/api/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "storage_backend": settings.STORAGE_BACKEND,
        "databricks_configured": databricks_service.is_configured()
    }

# =========================
# File APIs
# =========================
@app.post("/api/files/upload", response_model=FileUploadResponse)
async def upload_file(file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="No filename provided")

    ext = file.filename.split(".")[-1].lower()
    if ext not in settings.ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"File type '{ext}' not allowed"
        )

    content = await file.read()
    file_size = len(content)

    if file_size > settings.MAX_UPLOAD_SIZE_MB * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large")

    storage_path = await storage.save_file(content, file.filename, "uploads")
    file_id = storage_path.split("/")[-1].split(".")[0]

    db_store.save_file(file_id, file.filename, ext, file_size, storage_path)

    return FileUploadResponse(
        file_id=file_id,
        filename=file.filename,
        file_size=file_size,
        file_type=ext,
        storage_path=storage_path,
        upload_time=datetime.utcnow()
    )

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
    full_path = os.path.join("storage", file_path)
    if not os.path.exists(full_path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(full_path)

# =========================
# Jobs APIs
# =========================
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

# =========================
# Tasks & Cluster
# =========================
@app.get("/api/tasks")
async def get_available_tasks():
    return {"tasks": [task.value for task in MLTaskType]}

@app.get("/api/cluster/status")
async def get_cluster_status():
    if databricks_service.is_configured():
        return {
            "configured": True,
            "clusters": await databricks_service.list_clusters()
        }
    return {
        "configured": False,
        "message": "Databricks not configured"
    }

# =========================
# Static files (optional)
# =========================
if os.path.exists("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")
