import os
from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy import (
    create_engine,
    Column,
    String,
    Integer,
    DateTime,
    JSON,
    Float
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# =====================================================
# Database initialization
# =====================================================
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        future=True
    )
    SessionLocal = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine
    )
else:
    engine = None
    SessionLocal = None

Base = declarative_base()

# =====================================================
# Database Models
# =====================================================
class FileRecord(Base):
    """
    Stores metadata for uploaded datasets.
    """
    __tablename__ = "files"

    file_id = Column(String(36), primary_key=True, index=True)
    filename = Column(String(255), nullable=False)
    file_type = Column(String(20), nullable=False)
    file_size = Column(Integer, nullable=False)
    storage_path = Column(String(512), nullable=False)

    rows = Column(Integer, nullable=True)
    columns = Column(Integer, nullable=True)

    upload_time = Column(DateTime, default=datetime.utcnow)


class JobRecord(Base):
    """
    Stores Spark job executions and performance metrics.
    """
    __tablename__ = "jobs"

    job_id = Column(String(36), primary_key=True, index=True)
    file_id = Column(String(36), nullable=False)
    filename = Column(String(255), nullable=False)

    # Job configuration (tasks, workers, options)
    config = Column(JSON, nullable=False)

    # Job lifecycle
    status = Column(String(20), default="pending")
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)

    # Spark execution results
    results = Column(JSON, default=lambda: [])
    logs = Column(JSON, default=lambda: [])

    # Performance analysis
    execution_times = Column(JSON, default=lambda: [])
    speedups = Column(JSON, default=lambda: [])
    efficiencies = Column(JSON, default=lambda: [])

    cluster_id = Column(String(100), nullable=True)

# =====================================================
# Helpers
# =====================================================
def init_db() -> bool:
    """
    Create database tables if a database is configured.
    """
    if engine:
        Base.metadata.create_all(bind=engine)
        return True
    return False


def get_db():
    """
    Dependency for FastAPI routes.
    """
    if SessionLocal:
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()
    else:
        yield None

# =====================================================
# Database Store (DB + In-memory fallback)
# =====================================================
class DatabaseStore:
    """
    Unified storage layer.
    Uses a real database if DATABASE_URL is provided,
    otherwise falls back to in-memory storage.
    """

    def __init__(self):
        self.use_db = init_db()
        self._files_cache: Dict[str, Dict[str, Any]] = {}
        self._jobs_cache: Dict[str, Dict[str, Any]] = {}

    # -------------------------
    # Files
    # -------------------------
    def save_file(
        self,
        file_id: str,
        filename: str,
        file_type: str,
        file_size: int,
        storage_path: str,
        rows: Optional[int] = None,
        columns: Optional[int] = None
    ) -> Dict[str, Any]:

        data = {
            "file_id": file_id,
            "filename": filename,
            "file_type": file_type,
            "file_size": file_size,
            "storage_path": storage_path,
            "rows": rows,
            "columns": columns,
            "upload_time": datetime.utcnow().isoformat()
        }

        if self.use_db and SessionLocal:
            db = SessionLocal()
            try:
                db.add(FileRecord(**{
                    "file_id": file_id,
                    "filename": filename,
                    "file_type": file_type,
                    "file_size": file_size,
                    "storage_path": storage_path,
                    "rows": rows,
                    "columns": columns
                }))
                db.commit()
            except Exception as e:
                db.rollback()
                print(f"[DB] save_file error: {e}")
            finally:
                db.close()

        self._files_cache[file_id] = data
        return data

    def get_file(self, file_id: str) -> Optional[Dict[str, Any]]:
        if file_id in self._files_cache:
            return self._files_cache[file_id]

        if self.use_db and SessionLocal:
            db = SessionLocal()
            try:
                r = db.query(FileRecord).filter_by(file_id=file_id).first()
                if r:
                    data = {
                        "file_id": r.file_id,
                        "filename": r.filename,
                        "file_type": r.file_type,
                        "file_size": r.file_size,
                        "storage_path": r.storage_path,
                        "rows": r.rows,
                        "columns": r.columns,
                        "upload_time": r.upload_time.isoformat()
                    }
                    self._files_cache[file_id] = data
                    return data
            finally:
                db.close()
        return None

    def list_files(self) -> List[Dict[str, Any]]:
        if self.use_db and SessionLocal:
            db = SessionLocal()
            try:
                for r in db.query(FileRecord).all():
                    self._files_cache[r.file_id] = {
                        "file_id": r.file_id,
                        "filename": r.filename,
                        "file_type": r.file_type,
                        "file_size": r.file_size,
                        "storage_path": r.storage_path,
                        "rows": r.rows,
                        "columns": r.columns,
                        "upload_time": r.upload_time.isoformat()
                    }
            finally:
                db.close()

        return list(self._files_cache.values())

    # -------------------------
    # Jobs
    # -------------------------
    def save_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        job_id = job["job_id"]

        if self.use_db and SessionLocal:
            db = SessionLocal()
            try:
                record = db.query(JobRecord).filter_by(job_id=job_id).first()
                if record:
                    for k, v in job.items():
                        if hasattr(record, k):
                            setattr(record, k, v)
                else:
                    db.add(JobRecord(**job))
                db.commit()
            except Exception as e:
                db.rollback()
                print(f"[DB] save_job error: {e}")
            finally:
                db.close()

        self._jobs_cache[job_id] = job
        return job

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        if job_id in self._jobs_cache:
            return self._jobs_cache[job_id]

        if self.use_db and SessionLocal:
            db = SessionLocal()
            try:
                r = db.query(JobRecord).filter_by(job_id=job_id).first()
                if r:
                    job = {
                        "job_id": r.job_id,
                        "file_id": r.file_id,
                        "filename": r.filename,
                        "config": r.config,
                        "status": r.status,
                        "created_at": r.created_at.isoformat(),
                        "started_at": r.started_at.isoformat() if r.started_at else None,
                        "completed_at": r.completed_at.isoformat() if r.completed_at else None,
                        "results": r.results or [],
                        "logs": r.logs or [],
                        "execution_times": r.execution_times or [],
                        "speedups": r.speedups or [],
                        "efficiencies": r.efficiencies or [],
                        "cluster_id": r.cluster_id
                    }
                    self._jobs_cache[job_id] = job
                    return job
            finally:
                db.close()
        return None

    def list_jobs(self) -> List[Dict[str, Any]]:
        if self.use_db and SessionLocal:
            db = SessionLocal()
            try:
                for r in db.query(JobRecord).order_by(JobRecord.created_at.desc()).all():
                    self._jobs_cache[r.job_id] = {
                        "job_id": r.job_id,
                        "file_id": r.file_id,
                        "filename": r.filename,
                        "config": r.config,
                        "status": r.status,
                        "created_at": r.created_at.isoformat(),
                        "started_at": r.started_at.isoformat() if r.started_at else None,
                        "completed_at": r.completed_at.isoformat() if r.completed_at else None,
                        "results": r.results or [],
                        "logs": r.logs or [],
                        "execution_times": r.execution_times or [],
                        "speedups": r.speedups or [],
                        "efficiencies": r.efficiencies or [],
                        "cluster_id": r.cluster_id
                    }
            finally:
                db.close()

        return list(self._jobs_cache.values())


# Singleton
db_store = DatabaseStore()
