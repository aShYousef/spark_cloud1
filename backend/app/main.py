import os
import uvicorn  # ✅ تم الإضافة: ضروري لتشغيل السيرفر
from datetime import datetime
from typing import List

from fastapi import (
    FastAPI,
    File,
    UploadFile,
    HTTPException,
    BackgroundTasks
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .config import settings
from .models import (
    Job,
    JobConfig,
    JobStatus,
    MLTaskType,
    FileUploadResponse,
    PerformanceMetrics
)
from .storage import get_storage
from .job_manager import job_manager
from .databricks_service import databricks_service
from .database import db_store

# =====================================================
# Application initialization
# =====================================================
app = FastAPI(
    title=settings.APP_NAME,
    description=(
        "Cloud-based distributed data processing platform "
        "using Apache Spark / PySpark for large-scale ML analytics"
    ),
    version="1.0.0"
)

# =====================================================
# Middleware (CORS)
# =====================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # For development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

storage = get_storage()

# =====================================================
# Root & Health
# =====================================================
@app.get("/")
async def root():
    return {
        "service": settings.APP_NAME,
        "status": "running",
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

# =====================================================
# File APIs
# =====================================================
@app.post("/api/files/upload", response_model=FileUploadResponse)
async def upload_file(file: UploadFile = File(...)):
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided")

    ext = file.filename.split(".")[-1].lower()
    if ext not in settings.ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"File type '{ext}' not supported"
        )

    content = await file.read()
    size_bytes = len(content)

    if size_bytes > settings.MAX_UPLOAD_SIZE_MB * 1024 * 1024:
        raise HTTPException(
            status_code=400,
            detail="File exceeds maximum allowed size"
        )

    storage_path = await storage.save_file(
        content=content,
        filename=file.filename,
        folder="uploads"
    )

    file_id = os.path.basename(storage_path).split(".")[0]

    db_store.save_file(
        file_id=file_id,
        filename=file.filename,
        file_type=ext,
        file_size=size_bytes,
        storage_path=storage_path
    )

    return FileUploadResponse(
        file_id=file_id,
        filename=file.filename,
        file_size=size_bytes,
        file_type=ext,
        storage_path=storage_path,
        upload_time=datetime.utcnow()
    )

@app.get("/api/files")
async def list_files():
    return db_store.list_files()

@app.get("/api/files/{file_id}")
async def get_file(file_id: str):
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

# =====================================================
# Job APIs
# =====================================================
@app.post("/api/jobs", response_model=Job)
async def create_job(
    config: JobConfig,
    background_tasks: BackgroundTasks
):
    file_info = db_store.get_file(config.file_id)
    if not file_info:
        raise HTTPException(status_code=404, detail="Uploaded file not found")

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

@app.get(
    "/api/jobs/{job_id}/metrics",
    response_model=List[PerformanceMetrics]
)
async def get_job_metrics(job_id: str):
    metrics = job_manager.compute_performance_metrics(job_id)
    if not metrics:
        raise HTTPException(
            status_code=404,
            detail="No performance metrics available"
        )
    return metrics

# =====================================================
# Metadata APIs
# =====================================================
@app.get("/api/tasks")
async def list_available_tasks():
    return {
        "tasks": [
            {
                "id": task.name,
                "value": task.value
            }
            for task in MLTaskType
        ]
    }

@app.get("/api/cluster/status")
async def cluster_status():
    if not databricks_service.is_configured():
        return {
            "configured": False,
            "message": "Databricks is not configured"
        }

    clusters = await databricks_service.list_clusters()
    return {
        "configured": True,
        "clusters": clusters
    }

# =====================================================
# Static files (Frontend build)
# =====================================================
if os.path.exists("static"):
    app.mount(
        "/static",
        StaticFiles(directory="static"),
        name="static"
    )

# =====================================================
# ✅ Main Execution Entry Point (تم الإضافة)
# =====================================================
if __name__ == "__main__":
    # Render assigns a port dynamically in the PORT environment variable
    port = int(os.environ.get("PORT", 8000))
    # Host must be 0.0.0.0 to be accessible outside the container
    uvicorn.run(app, host="0.0.0.0", port=port)

