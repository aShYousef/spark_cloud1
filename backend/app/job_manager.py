import uuid
import asyncio
from datetime import datetime
from typing import Dict, List, Optional

from .models import (
    Job,
    JobConfig,
    JobResult,
    JobStatus,
    MLTaskType,
    PerformanceMetrics
)

# ✅ التعديل الأول: تغيير الاسم في الاستيراد
from .spark_jobs import (
    run_descriptive_statistics,
    run_linear_regression,
    run_logistic_regression,
    run_kmeans,
    run_fpgrowth,
    run_time_series
)

from .database import db_store
from .databricks_service import databricks_service

# =====================================================
# In-memory job registry (DB persistence handled separately)
# =====================================================
_jobs: Dict[str, Job] = {}

# =====================================================
# Job Manager
# =====================================================
class JobManager:
    """
    Orchestrates distributed Spark jobs and records performance metrics.
    """

    def __init__(self):
        self.jobs = _jobs

    # -------------------------------------------------
    # Job lifecycle
    # -------------------------------------------------
    async def create_job(
        self,
        file_id: str,
        filename: str,
        config: JobConfig
    ) -> Job:
        job_id = str(uuid.uuid4())

        job = Job(
            job_id=job_id,
            file_id=file_id,
            filename=filename,
            config=config,
            status=JobStatus.PENDING,
            created_at=datetime.utcnow(),
            results=[],
            logs=[]
        )

        self.jobs[job_id] = job

        db_store.save_job({
            "job_id": job_id,
            "file_id": file_id,
            "filename": filename,
            "config": config.dict(),
            "status": JobStatus.PENDING,
            "created_at": job.created_at,
            "results": [],
            "logs": []
        })

        return job

    async def run_job(self, job_id: str, file_path: str) -> Job:
        job = self.jobs.get(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")

        job.status = JobStatus.RUNNING
        job.started_at = datetime.utcnow()
        job.logs.append(self._log("Job started"))

        try:
            for workers in job.config.worker_counts:
                job.logs.append(self._log(f"Running with {workers} workers"))

                for task in job.config.tasks:
                    job.logs.append(self._log(f"Starting task: {task.value}"))

                    result = await self._execute_task(
                        task=task,
                        file_path=file_path,
                        workers=workers,
                        config=job.config
                    )

                    job.results.append(result)

                    if result.status == JobStatus.COMPLETED:
                        job.logs.append(
                            self._log(
                                f"{task.value} completed in "
                                f"{result.execution_time_seconds:.2f}s "
                                f"({workers} workers)"
                            )
                        )
                    else:
                        job.logs.append(
                            self._log(
                                f"{task.value} failed: {result.error_message}"
                            )
                        )

            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.utcnow()
            job.logs.append(self._log("Job completed successfully"))

        except Exception as e:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.logs.append(self._log(f"Job failed: {str(e)}"))

        # Persist final job state
        db_store.save_job({
            "job_id": job.job_id,
            "file_id": job.file_id,
            "filename": job.filename,
            "config": job.config.dict(),
            "status": job.status,
            "started_at": job.started_at,
            "completed_at": job.completed_at,
            "results": [r.dict() for r in job.results],
            "logs": job.logs
        })

        return job

    # -------------------------------------------------
    # Task execution
    # -------------------------------------------------
    async def _execute_task(
        self,
        task: MLTaskType,
        file_path: str,
        workers: int,
        config: JobConfig
    ) -> JobResult:

        start = datetime.utcnow()

        try:
            if task == MLTaskType.DESCRIPTIVE_STATS:
                # ✅ التعديل الثاني: استخدام الاسم الجديد للدالة
                metrics = run_descriptive_statistics(file_path, workers)

            elif task == MLTaskType.LINEAR_REGRESSION:
                metrics = run_linear_regression(
                    file_path,
                    workers,
                    config.target_column,
                    config.feature_columns
                )

            elif task == MLTaskType.LOGISTIC_REGRESSION:
                metrics = run_logistic_regression(
                    file_path,
                    workers,
                    config.target_column,
                    config.feature_columns
                )

            elif task == MLTaskType.KMEANS:
                metrics = run_kmeans(
                    file_path,
                    workers,
                    config.num_clusters,
                    config.feature_columns
                )

            elif task == MLTaskType.FPGROWTH:
                metrics = run_fpgrowth(
                    file_path,
                    workers,
                    config.min_support,
                    config.min_confidence
                )

            elif task == MLTaskType.TIME_SERIES:
                metrics = run_time_series(
                    file_path,
                    workers,
                    config.timestamp_column
                )

            else:
                raise ValueError("Unsupported task")

            elapsed = (datetime.utcnow() - start).total_seconds()

            return JobResult(
                job_id=str(uuid.uuid4()),
                task_type=task,
                worker_count=workers,
                execution_time_seconds=elapsed,
                status=JobStatus.COMPLETED,
                metrics=metrics
            )

        except Exception as e:
            return JobResult(
                job_id=str(uuid.uuid4()),
                task_type=task,
                worker_count=workers,
                execution_time_seconds=0.0,
                status=JobStatus.FAILED,
                error_message=str(e)
            )

    # -------------------------------------------------
    # Utilities
    # -------------------------------------------------
    def compute_performance_metrics(
        self,
        job_id: str
    ) -> List[PerformanceMetrics]:
        job = self.jobs.get(job_id)
        if not job:
            return []

        grouped: Dict[str, Dict[int, float]] = {}

        for r in job.results:
            if r.status != JobStatus.COMPLETED:
                continue

            key = r.task_type.value
            grouped.setdefault(key, {})[r.worker_count] = r.execution_time_seconds

        metrics: List[PerformanceMetrics] = []

        for task, values in grouped.items():
            workers = sorted(values.keys())
            times = [values[w] for w in workers]

            base = times[0]
            speedups = [base / t for t in times]
            efficiencies = [s / w for s, w in zip(speedups, workers)]

            metrics.append(
                PerformanceMetrics(
                    task_type=task,
                    worker_counts=workers,
                    execution_times=times,
                    speedups=speedups,
                    efficiencies=efficiencies
                )
            )

        return metrics

    def get_job(self, job_id: str) -> Optional[Job]:
        return self.jobs.get(job_id)

    def list_jobs(self) -> List[Job]:
        return list(self.jobs.values())

    @staticmethod
    def _log(msg: str) -> str:
        return f"[{datetime.utcnow().isoformat()}] {msg}"


# Singleton
job_manager = JobManager()

