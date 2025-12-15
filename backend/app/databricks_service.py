import httpx
import time
import uuid
import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime

from .config import settings
from .models import ClusterStatus

# =====================================================
# Databricks Service
# =====================================================
class DatabricksService:
    """
    Manages Spark clusters and job execution on Databricks.

    - Supports real Databricks clusters
    - Falls back to MOCK mode if Databricks is not configured
    - Designed for benchmarking with 1/2/4/8 workers
    """

    def __init__(self):
        self.host = settings.DATABRICKS_HOST
        self.token = settings.DATABRICKS_TOKEN

        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        } if self.token else {}

        self.base_url = f"https://{self.host}/api/2.0" if self.host else None

    # -------------------------------------------------
    # Configuration
    # -------------------------------------------------
    def is_configured(self) -> bool:
        """
        Returns True if Databricks credentials are provided.
        """
        return bool(self.host and self.token)

    # -------------------------------------------------
    # Cluster Management
    # -------------------------------------------------
    async def create_cluster(
        self,
        num_workers: int,
        cluster_name: Optional[str] = None
    ) -> str:
        """
        Create a Spark cluster with a specific number of workers.
        Used to benchmark scalability.
        """
        if not self.is_configured():
            # MOCK cluster (for Render / local dev)
            return f"mock-cluster-{num_workers}-{uuid.uuid4().hex[:6]}"

        cluster_name = (
            cluster_name
            or f"spark-ml-{num_workers}w-{uuid.uuid4().hex[:6]}"
        )

        cluster_config = {
            "cluster_name": cluster_name,
            "spark_version": settings.DATABRICKS_CLUSTER_SPEC["spark_version"],
            "node_type_id": settings.DATABRICKS_CLUSTER_SPEC["node_type_id"],
            "driver_node_type_id": settings.DATABRICKS_CLUSTER_SPEC["driver_node_type_id"],
            "num_workers": num_workers,
            "autotermination_minutes": 30,
            "spark_conf": {
                "spark.speculation": "true",
                "spark.sql.adaptive.enabled": "true"
            }
        }

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                f"{self.base_url}/clusters/create",
                headers=self.headers,
                json=cluster_config
            )
            r.raise_for_status()
            return r.json()["cluster_id"]

    async def start_cluster(self, cluster_id: str) -> bool:
        if not self.is_configured():
            return True

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                f"{self.base_url}/clusters/start",
                headers=self.headers,
                json={"cluster_id": cluster_id}
            )
            return r.status_code == 200

    async def stop_cluster(self, cluster_id: str) -> bool:
        if not self.is_configured():
            return True

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                f"{self.base_url}/clusters/delete",
                headers=self.headers,
                json={"cluster_id": cluster_id}
            )
            return r.status_code == 200

    async def get_cluster_status(self, cluster_id: str) -> ClusterStatus:
        """
        Returns detailed cluster status.
        """
        if not self.is_configured():
            return ClusterStatus(
                cluster_id=cluster_id,
                state="RUNNING",
                num_workers=int(cluster_id.split("-")[2]) if "mock" in cluster_id else 4,
                spark_version="mock",
                driver_node_type="local",
                worker_node_type="local"
            )

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(
                f"{self.base_url}/clusters/get",
                headers=self.headers,
                params={"cluster_id": cluster_id}
            )
            r.raise_for_status()
            data = r.json()

            return ClusterStatus(
                cluster_id=cluster_id,
                state=data.get("state", "UNKNOWN"),
                num_workers=data.get("num_workers", 0),
                spark_version=data.get("spark_version", ""),
                driver_node_type=data.get("driver_node_type_id", ""),
                worker_node_type=data.get("node_type_id", "")
            )

    async def wait_for_cluster_running(
        self,
        cluster_id: str,
        timeout: int = 600
    ) -> bool:
        """
        Waits until cluster is RUNNING.
        """
        if not self.is_configured():
            return True

        start = time.time()
        while time.time() - start < timeout:
            status = await self.get_cluster_status(cluster_id)
            if status.state == "RUNNING":
                return True
            if status.state in ["ERROR", "TERMINATED"]:
                return False
            await asyncio.sleep(10)
        return False

    # -------------------------------------------------
    # Job Submission
    # -------------------------------------------------
    async def submit_job(
        self,
        cluster_id: str,
        notebook_path: str,
        parameters: Dict[str, Any]
    ) -> str:
        """
        Submit a Spark ML job (Notebook-based).
        """
        if not self.is_configured():
            return f"mock-run-{uuid.uuid4().hex[:6]}"

        run_config = {
            "run_name": f"spark-ml-job-{uuid.uuid4().hex[:6]}",
            "existing_cluster_id": cluster_id,
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": {
                    k: str(v) for k, v in parameters.items()
                }
            }
        }

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                f"{self.base_url}/jobs/runs/submit",
                headers=self.headers,
                json=run_config
            )
            r.raise_for_status()
            return r.json()["run_id"]

    async def get_run_status(self, run_id: str) -> Dict[str, Any]:
        if not self.is_configured():
            return {
                "state": {
                    "life_cycle_state": "TERMINATED",
                    "result_state": "SUCCESS"
                },
                "start_time": int(time.time() * 1000),
                "end_time": int(time.time() * 1000) + 60000
            }

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(
                f"{self.base_url}/jobs/runs/get",
                headers=self.headers,
                params={"run_id": run_id}
            )
            r.raise_for_status()
            return r.json()

    async def wait_for_run_completion(
        self,
        run_id: str,
        timeout: int = 3600
    ) -> Dict[str, Any]:
        """
        Waits for Spark job completion and returns final status.
        """
        if not self.is_configured():
            return await self.get_run_status(run_id)

        start = time.time()
        while time.time() - start < timeout:
            status = await self.get_run_status(run_id)
            life_cycle = status.get("state", {}).get("life_cycle_state")

            if life_cycle in ["TERMINATED", "INTERNAL_ERROR", "SKIPPED"]:
                return status

            await asyncio.sleep(15)

        return {
            "state": {
                "life_cycle_state": "TIMEOUT",
                "result_state": "FAILED"
            }
        }

    # -------------------------------------------------
    # Utilities
    # -------------------------------------------------
    async def list_clusters(self) -> List[ClusterStatus]:
        if not self.is_configured():
            return []

        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(
                f"{self.base_url}/clusters/list",
                headers=self.headers
            )
            r.raise_for_status()

            return [
                ClusterStatus(
                    cluster_id=c["cluster_id"],
                    state=c["state"],
                    num_workers=c.get("num_workers", 0),
                    spark_version=c.get("spark_version", ""),
                    driver_node_type=c.get("driver_node_type_id", ""),
                    worker_node_type=c.get("node_type_id", "")
                )
                for c in r.json().get("clusters", [])
            ]


# Singleton
databricks_service = DatabricksService()
