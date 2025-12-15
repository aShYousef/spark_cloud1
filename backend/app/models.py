from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from enum import Enum
from datetime import datetime


# =====================================================
# Job & Task Enums
# =====================================================
class JobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class MLTaskType(str, Enum):
    # Descriptive analytics
    DESCRIPTIVE_STATS = "descriptive_stats"

    # Machine learning tasks (Spark MLlib)
    LINEAR_REGRESSION = "linear_regression"
    LOGISTIC_REGRESSION = "logistic_regression"
    KMEANS = "kmeans"
    FPGROWTH = "fpgrowth"
    TIME_SERIES = "time_series"


# =====================================================
# File Models
# =====================================================
class FileUploadResponse(BaseModel):
    file_id: str
    filename: str
    file_size: int
    file_type: str
    storage_path: str
    upload_time: datetime


# =====================================================
# Job Configuration
# =====================================================
class JobConfig(BaseModel):
    """
    Configuration sent by the user when submitting a job.
    """
    file_id: str

    # Selected tasks
    tasks: List[MLTaskType]

    # Distributed execution setup
    worker_counts: List[int] = Field(default=[1, 2, 4, 8])

    # ML-specific parameters
    target_column: Optional[str] = None
    feature_columns: Optional[List[str]] = None
    cluster_column: Optional[str] = None
    timestamp_column: Optional[str] = None

    # Algorithm parameters
    num_clusters: int = 3
    min_support: float = 0.1
    min_confidence: float = 0.5


# =====================================================
# Job Execution Result
# =====================================================
class JobResult(BaseModel):
    """
    Result of a single task executed with a specific worker count.
    """
    job_id: str
    task_type: MLTaskType
    worker_count: int

    execution_time_seconds: float
    status: JobStatus

    # Output metrics (model coefficients, accuracy, stats, etc.)
    metrics: Dict[str, Any] = {}

    # Path to saved results in cloud storage
    output_path: Optional[str] = None

    error_message: Optional[str] = None


# =====================================================
# Job Metadata
# =====================================================
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

    # Used when running on Databricks / cluster manager
    cluster_id: Optional[str] = None


# =====================================================
# Performance & Scalability Metrics
# =====================================================
class PerformanceMetrics(BaseModel):
    """
    Used to analyze scalability and speedup.
    """
    task_type: str
    worker_counts: List[int]
    execution_times: List[float]

    # Speedup = T1 / Tp
    speedups: List[float]

    # Efficiency = Speedup / p
    efficiencies: List[float]


# =====================================================
# Descriptive Statistics Output
# =====================================================
class DescriptiveStats(BaseModel):
    """
    Minimum required descriptive statistics for the project.
    """
    num_rows: int
    num_columns: int

    # Per-column statistics:
    # min, max, mean, null_percentage, unique_count, etc.
    column_stats: Dict[str, Dict[str, Any]]


# =====================================================
# Cluster Monitoring
# =====================================================
class ClusterStatus(BaseModel):
    cluster_id: str
    state: str
    num_workers: int
    spark_version: str
    driver_node_type: str
    worker_node_type: str
