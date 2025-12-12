import os
import json
from datetime import datetime
from typing import Optional, List, Dict, Any
from sqlalchemy import create_engine, Column, String, Integer, Float, DateTime, Text, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
else:
    engine = None
    SessionLocal = None

Base = declarative_base()

class FileRecord(Base):
    __tablename__ = "files"
    
    file_id = Column(String(36), primary_key=True)
    filename = Column(String(255), nullable=False)
    file_type = Column(String(10), nullable=False)
    file_size = Column(Integer, nullable=False)
    storage_path = Column(String(512), nullable=False)
    upload_time = Column(DateTime, default=datetime.utcnow)

class JobRecord(Base):
    __tablename__ = "jobs"
    
    job_id = Column(String(36), primary_key=True)
    file_id = Column(String(36), nullable=False)
    filename = Column(String(255), nullable=False)
    config = Column(JSON, nullable=False)
    status = Column(String(20), nullable=False, default="pending")
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)
    results = Column(JSON, default=list)
    logs = Column(JSON, default=list)
    cluster_id = Column(String(100), nullable=True)

def init_db():
    """Initialize database tables."""
    if engine:
        Base.metadata.create_all(bind=engine)
        return True
    return False

def get_db():
    """Get database session."""
    if SessionLocal:
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()
    else:
        yield None

class DatabaseStore:
    """Database-backed storage for files and jobs with in-memory fallback."""
    
    def __init__(self):
        self.use_db = init_db()
        self._files_cache: Dict[str, Dict] = {}
        self._jobs_cache: Dict[str, Dict] = {}
    
    def save_file(self, file_id: str, filename: str, file_type: str, 
                  file_size: int, storage_path: str) -> Dict:
        file_data = {
            "file_id": file_id,
            "filename": filename,
            "file_type": file_type,
            "file_size": file_size,
            "storage_path": storage_path,
            "upload_time": datetime.utcnow().isoformat()
        }
        
        if self.use_db and SessionLocal:
            db = SessionLocal()
            try:
                record = FileRecord(
                    file_id=file_id,
                    filename=filename,
                    file_type=file_type,
                    file_size=file_size,
                    storage_path=storage_path
                )
                db.add(record)
                db.commit()
            except Exception as e:
                db.rollback()
                print(f"DB error saving file: {e}")
            finally:
                db.close()
        
        self._files_cache[file_id] = file_data
        return file_data
    
    def get_file(self, file_id: str) -> Optional[Dict]:
        if file_id in self._files_cache:
            return self._files_cache[file_id]
        
        if self.use_db and SessionLocal:
            db = SessionLocal()
            try:
                record = db.query(FileRecord).filter(FileRecord.file_id == file_id).first()
                if record:
                    file_data = {
                        "file_id": record.file_id,
                        "filename": record.filename,
                        "file_type": record.file_type,
                        "file_size": record.file_size,
                        "storage_path": record.storage_path,
                        "upload_time": record.upload_time.isoformat() if record.upload_time else None
                    }
                    self._files_cache[file_id] = file_data
                    return file_data
            finally:
                db.close()
        
        return None
    
    def list_files(self) -> List[Dict]:
        if self.use_db and SessionLocal:
            db = SessionLocal()
            try:
                records = db.query(FileRecord).all()
                for record in records:
                    self._files_cache[record.file_id] = {
                        "file_id": record.file_id,
                        "filename": record.filename,
                        "file_type": record.file_type,
                        "file_size": record.file_size,
                        "storage_path": record.storage_path,
                        "upload_time": record.upload_time.isoformat() if record.upload_time else None
                    }
            finally:
                db.close()
        
        return list(self._files_cache.values())
    
    def save_job(self, job_data: Dict) -> Dict:
        job_id = job_data["job_id"]
        
        if self.use_db and SessionLocal:
            db = SessionLocal()
            try:
                existing = db.query(JobRecord).filter(JobRecord.job_id == job_id).first()
                if existing:
                    existing.status = job_data.get("status", existing.status)
                    existing.started_at = job_data.get("started_at")
                    existing.completed_at = job_data.get("completed_at")
                    existing.results = job_data.get("results", [])
                    existing.logs = job_data.get("logs", [])
                    existing.cluster_id = job_data.get("cluster_id")
                else:
                    record = JobRecord(
                        job_id=job_id,
                        file_id=job_data["file_id"],
                        filename=job_data["filename"],
                        config=job_data.get("config", {}),
                        status=job_data.get("status", "pending"),
                        results=job_data.get("results", []),
                        logs=job_data.get("logs", [])
                    )
                    db.add(record)
                db.commit()
            except Exception as e:
                db.rollback()
                print(f"DB error saving job: {e}")
            finally:
                db.close()
        
        self._jobs_cache[job_id] = job_data
        return job_data
    
    def get_job(self, job_id: str) -> Optional[Dict]:
        if job_id in self._jobs_cache:
            return self._jobs_cache[job_id]
        
        if self.use_db and SessionLocal:
            db = SessionLocal()
            try:
                record = db.query(JobRecord).filter(JobRecord.job_id == job_id).first()
                if record:
                    job_data = {
                        "job_id": record.job_id,
                        "file_id": record.file_id,
                        "filename": record.filename,
                        "config": record.config,
                        "status": record.status,
                        "created_at": record.created_at.isoformat() if record.created_at else None,
                        "started_at": record.started_at.isoformat() if record.started_at else None,
                        "completed_at": record.completed_at.isoformat() if record.completed_at else None,
                        "results": record.results or [],
                        "logs": record.logs or [],
                        "cluster_id": record.cluster_id
                    }
                    self._jobs_cache[job_id] = job_data
                    return job_data
            finally:
                db.close()
        
        return None
    
    def list_jobs(self) -> List[Dict]:
        if self.use_db and SessionLocal:
            db = SessionLocal()
            try:
                records = db.query(JobRecord).order_by(JobRecord.created_at.desc()).all()
                for record in records:
                    self._jobs_cache[record.job_id] = {
                        "job_id": record.job_id,
                        "file_id": record.file_id,
                        "filename": record.filename,
                        "config": record.config,
                        "status": record.status,
                        "created_at": record.created_at.isoformat() if record.created_at else None,
                        "started_at": record.started_at.isoformat() if record.started_at else None,
                        "completed_at": record.completed_at.isoformat() if record.completed_at else None,
                        "results": record.results or [],
                        "logs": record.logs or [],
                        "cluster_id": record.cluster_id
                    }
            finally:
                db.close()
        
        return list(self._jobs_cache.values())

db_store = DatabaseStore()
