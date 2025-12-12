from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from enum import Enum
from datetime import datetime

class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class MLTaskType(str, Enum):
    LINEAR_REGRESSION = "linear_regression"
    LOGISTIC_REGRESSION = "logistic_regression"
    KMEANS = "kmeans"
    FPGROWTH = "fpgrowth"
    TIME_SERIES = "time_series"
    DESCRIPTIVE_STATS = "descriptive_stats"

class FileUploadResponse(BaseModel):
    file_id: str
    filename: str
    file_size: int
    file_type: str
    storage_path: str
    upload_time: datetime

class JobConfig(BaseModel):
    file_id: str
    tasks: List[MLTaskType]
    worker_counts: List[int] = Field(default=[1, 2, 4, 8])
    target_column: Optional[str] = None
    feature_columns: Optional[List[str]] = None
    cluster_column: Optional[str] = None
    timestamp_column: Optional[str] = None
    num_clusters: int = 3
    min_support: float = 0.1
    min_confidence: float = 0.5

class JobResult(BaseModel):
    job_id: str
    task_type: MLTaskType
    worker_count: int
    execution_time_seconds: float
    status: JobStatus
    metrics: Dict[str, Any] = {}
    output_path: Optional[str] = None
    error_message: Optional[str] = None

class Job(BaseModel):
    job_id: str
    file_id: str
    filename: str
    config: JobConfig
    status: JobStatus
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    results: List[JobResult] = []
    logs: List[str] = []
    cluster_id: Optional[str] = None

class PerformanceMetrics(BaseModel):
    task_type: str
    worker_counts: List[int]
    execution_times: List[float]
    speedups: List[float]
    efficiencies: List[float]

class DescriptiveStats(BaseModel):
    num_rows: int
    num_columns: int
    column_stats: Dict[str, Dict[str, Any]]

class ClusterStatus(BaseModel):
    cluster_id: str
    state: str
    num_workers: int
    spark_version: str
    driver_node_type: str
    worker_node_type: str
