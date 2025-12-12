import httpx
import time
import uuid
from typing import Optional, Dict, Any, List
from datetime import datetime
from .config import settings
from .models import ClusterStatus, JobStatus

class DatabricksService:
    def __init__(self):
        self.host = settings.DATABRICKS_HOST
        self.token = settings.DATABRICKS_TOKEN
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        self.base_url = f"https://{self.host}/api/2.0" if self.host else None
    
    def is_configured(self) -> bool:
        return bool(self.host and self.token)
    
    async def create_cluster(self, num_workers: int, cluster_name: Optional[str] = None) -> str:
        if not self.is_configured():
            return f"mock-cluster-{uuid.uuid4().hex[:8]}"
        
        cluster_name = cluster_name or f"spark-processor-{num_workers}w-{uuid.uuid4().hex[:8]}"
        
        cluster_config = {
            "cluster_name": cluster_name,
            "spark_version": settings.DATABRICKS_CLUSTER_SPEC["spark_version"],
            "node_type_id": settings.DATABRICKS_CLUSTER_SPEC["node_type_id"],
            "driver_node_type_id": settings.DATABRICKS_CLUSTER_SPEC["driver_node_type_id"],
            "num_workers": num_workers,
            "autotermination_minutes": 30,
            "spark_conf": {
                "spark.speculation": "true"
            }
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/clusters/create",
                headers=self.headers,
                json=cluster_config
            )
            response.raise_for_status()
            return response.json()["cluster_id"]
    
    async def start_cluster(self, cluster_id: str) -> bool:
        if not self.is_configured():
            return True
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/clusters/start",
                headers=self.headers,
                json={"cluster_id": cluster_id}
            )
            return response.status_code == 200
    
    async def stop_cluster(self, cluster_id: str) -> bool:
        if not self.is_configured():
            return True
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/clusters/delete",
                headers=self.headers,
                json={"cluster_id": cluster_id}
            )
            return response.status_code == 200
    
    async def get_cluster_status(self, cluster_id: str) -> ClusterStatus:
        if not self.is_configured():
            return ClusterStatus(
                cluster_id=cluster_id,
                state="RUNNING",
                num_workers=4,
                spark_version="13.3.x-scala2.12",
                driver_node_type="i3.xlarge",
                worker_node_type="i3.xlarge"
            )
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/clusters/get",
                headers=self.headers,
                params={"cluster_id": cluster_id}
            )
            response.raise_for_status()
            data = response.json()
            
            return ClusterStatus(
                cluster_id=cluster_id,
                state=data.get("state", "UNKNOWN"),
                num_workers=data.get("num_workers", 0),
                spark_version=data.get("spark_version", ""),
                driver_node_type=data.get("driver_node_type_id", ""),
                worker_node_type=data.get("node_type_id", "")
            )
    
    async def wait_for_cluster_running(self, cluster_id: str, timeout: int = 600) -> bool:
        if not self.is_configured():
            return True
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            status = await self.get_cluster_status(cluster_id)
            if status.state == "RUNNING":
                return True
            elif status.state in ["TERMINATED", "ERROR", "UNKNOWN"]:
                return False
            await asyncio.sleep(10)
        return False
    
    async def submit_job(self, cluster_id: str, notebook_path: str, parameters: Dict[str, Any]) -> str:
        if not self.is_configured():
            return f"mock-run-{uuid.uuid4().hex[:8]}"
        
        run_config = {
            "run_name": f"spark-job-{uuid.uuid4().hex[:8]}",
            "existing_cluster_id": cluster_id,
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": {str(k): str(v) for k, v in parameters.items()}
            }
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/jobs/runs/submit",
                headers=self.headers,
                json=run_config
            )
            response.raise_for_status()
            return response.json()["run_id"]
    
    async def get_run_status(self, run_id: str) -> Dict[str, Any]:
        if not self.is_configured():
            return {
                "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
                "start_time": int(time.time() * 1000),
                "end_time": int(time.time() * 1000) + 60000
            }
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/jobs/runs/get",
                headers=self.headers,
                params={"run_id": run_id}
            )
            response.raise_for_status()
            return response.json()
    
    async def wait_for_run_completion(self, run_id: str, timeout: int = 3600) -> Dict[str, Any]:
        if not self.is_configured():
            return await self.get_run_status(run_id)
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            status = await self.get_run_status(run_id)
            life_cycle_state = status.get("state", {}).get("life_cycle_state", "")
            
            if life_cycle_state in ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]:
                return status
            
            await asyncio.sleep(15)
        
        return {"state": {"life_cycle_state": "TIMEOUT", "result_state": "FAILED"}}
    
    async def list_clusters(self) -> List[ClusterStatus]:
        if not self.is_configured():
            return []
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.base_url}/clusters/list",
                headers=self.headers
            )
            response.raise_for_status()
            clusters = response.json().get("clusters", [])
            
            return [
                ClusterStatus(
                    cluster_id=c["cluster_id"],
                    state=c["state"],
                    num_workers=c.get("num_workers", 0),
                    spark_version=c.get("spark_version", ""),
                    driver_node_type=c.get("driver_node_type_id", ""),
                    worker_node_type=c.get("node_type_id", "")
                )
                for c in clusters
            ]

import asyncio
databricks_service = DatabricksService()
