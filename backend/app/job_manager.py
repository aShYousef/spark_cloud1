import asyncio
import uuid
from datetime import datetime
from typing import Dict, List, Optional
from .models import Job, JobConfig, JobResult, JobStatus, MLTaskType, PerformanceMetrics
from .spark_jobs import spark_simulator
from .databricks_service import databricks_service

jobs_db: Dict[str, Job] = {}

class JobManager:
    def __init__(self):
        self.jobs = jobs_db
    
    async def create_job(self, file_id: str, filename: str, config: JobConfig) -> Job:
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
        return job
    
    async def run_job(self, job_id: str, file_path: str) -> Job:
        job = self.jobs.get(job_id)
        if not job:
            raise ValueError(f"Job {job_id} not found")
        
        job.status = JobStatus.RUNNING
        job.started_at = datetime.utcnow()
        job.logs.append(f"[{datetime.utcnow().isoformat()}] Job started")
        
        try:
            for worker_count in job.config.worker_counts:
                job.logs.append(f"[{datetime.utcnow().isoformat()}] Running tasks with {worker_count} workers")
                
                for task in job.config.tasks:
                    job.logs.append(f"[{datetime.utcnow().isoformat()}] Starting task: {task.value}")
                    
                    result = await self._run_task(
                        task=task,
                        file_path=file_path,
                        num_workers=worker_count,
                        config=job.config
                    )
                    
                    job.results.append(result)
                    
                    if result.status == JobStatus.COMPLETED:
                        job.logs.append(
                            f"[{datetime.utcnow().isoformat()}] Task {task.value} completed "
                            f"in {result.execution_time_seconds:.2f}s with {worker_count} workers"
                        )
                    else:
                        job.logs.append(
                            f"[{datetime.utcnow().isoformat()}] Task {task.value} failed: {result.error_message}"
                        )
            
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.utcnow()
            job.logs.append(f"[{datetime.utcnow().isoformat()}] Job completed successfully")
            
        except Exception as e:
            job.status = JobStatus.FAILED
            job.completed_at = datetime.utcnow()
            job.logs.append(f"[{datetime.utcnow().isoformat()}] Job failed: {str(e)}")
        
        return job
    
    async def _run_task(self, task: MLTaskType, file_path: str, 
                        num_workers: int, config: JobConfig) -> JobResult:
        if task == MLTaskType.DESCRIPTIVE_STATS:
            return await spark_simulator.compute_descriptive_stats(file_path, num_workers)
        
        elif task == MLTaskType.LINEAR_REGRESSION:
            return await spark_simulator.run_linear_regression(
                file_path, num_workers,
                config.target_column or "",
                config.feature_columns or []
            )
        
        elif task == MLTaskType.LOGISTIC_REGRESSION:
            return await spark_simulator.run_logistic_regression(
                file_path, num_workers,
                config.target_column or "",
                config.feature_columns or []
            )
        
        elif task == MLTaskType.KMEANS:
            return await spark_simulator.run_kmeans(
                file_path, num_workers,
                config.num_clusters,
                config.feature_columns
            )
        
        elif task == MLTaskType.FPGROWTH:
            return await spark_simulator.run_fpgrowth(
                file_path, num_workers,
                config.min_support,
                config.min_confidence
            )
        
        elif task == MLTaskType.TIME_SERIES:
            return await spark_simulator.run_time_series(
                file_path, num_workers,
                config.timestamp_column
            )
        
        else:
            return JobResult(
                job_id=str(uuid.uuid4()),
                task_type=task,
                worker_count=num_workers,
                execution_time_seconds=0,
                status=JobStatus.FAILED,
                error_message=f"Unknown task type: {task}"
            )
    
    def get_job(self, job_id: str) -> Optional[Job]:
        return self.jobs.get(job_id)
    
    def list_jobs(self) -> List[Job]:
        return list(self.jobs.values())
    
    def compute_performance_metrics(self, job_id: str) -> List[PerformanceMetrics]:
        job = self.jobs.get(job_id)
        if not job:
            return []
        
        task_results: Dict[str, Dict[int, float]] = {}
        
        for result in job.results:
            if result.status == JobStatus.COMPLETED:
                task_type = result.task_type.value
                if task_type not in task_results:
                    task_results[task_type] = {}
                task_results[task_type][result.worker_count] = result.execution_time_seconds
        
        metrics = []
        for task_type, times in task_results.items():
            worker_counts = sorted(times.keys())
            execution_times = [times[w] for w in worker_counts]
            
            if not execution_times:
                continue
            
            base_time = execution_times[0]
            speedups = [base_time / t if t > 0 else 0 for t in execution_times]
            efficiencies = [s / w if w > 0 else 0 for s, w in zip(speedups, worker_counts)]
            
            metrics.append(PerformanceMetrics(
                task_type=task_type,
                worker_counts=worker_counts,
                execution_times=execution_times,
                speedups=speedups,
                efficiencies=efficiencies
            ))
        
        return metrics

job_manager = JobManager()
