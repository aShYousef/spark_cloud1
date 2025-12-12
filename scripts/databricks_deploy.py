#!/usr/bin/env python3
"""
Databricks deployment script for Spark jobs.
This script uploads notebooks and configures jobs on Databricks.
"""

import os
import json
import base64
import requests
from typing import Dict, Any, Optional

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

def get_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }

def upload_notebook(local_path: str, workspace_path: str) -> bool:
    """Upload a notebook to Databricks workspace."""
    if not os.path.exists(local_path):
        print(f"File not found: {local_path}")
        return False
    
    with open(local_path, 'r') as f:
        content = f.read()
    
    encoded_content = base64.b64encode(content.encode()).decode()
    
    url = f"https://{DATABRICKS_HOST}/api/2.0/workspace/import"
    payload = {
        "path": workspace_path,
        "format": "SOURCE",
        "language": "PYTHON",
        "content": encoded_content,
        "overwrite": True
    }
    
    response = requests.post(url, headers=get_headers(), json=payload)
    
    if response.status_code == 200:
        print(f"Uploaded: {local_path} -> {workspace_path}")
        return True
    else:
        print(f"Failed to upload {local_path}: {response.text}")
        return False

def create_cluster(cluster_name: str, num_workers: int = 2) -> Optional[str]:
    """Create a new Databricks cluster."""
    url = f"https://{DATABRICKS_HOST}/api/2.0/clusters/create"
    
    payload = {
        "cluster_name": cluster_name,
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "driver_node_type_id": "i3.xlarge",
        "num_workers": num_workers,
        "autotermination_minutes": 30,
        "spark_conf": {
            "spark.speculation": "true",
            "spark.databricks.delta.preview.enabled": "true"
        },
        "spark_env_vars": {
            "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
        }
    }
    
    response = requests.post(url, headers=get_headers(), json=payload)
    
    if response.status_code == 200:
        cluster_id = response.json()["cluster_id"]
        print(f"Created cluster: {cluster_id}")
        return cluster_id
    else:
        print(f"Failed to create cluster: {response.text}")
        return None

def create_job(job_name: str, notebook_path: str, cluster_id: str) -> Optional[str]:
    """Create a new Databricks job."""
    url = f"https://{DATABRICKS_HOST}/api/2.1/jobs/create"
    
    payload = {
        "name": job_name,
        "tasks": [
            {
                "task_key": "main_task",
                "existing_cluster_id": cluster_id,
                "notebook_task": {
                    "notebook_path": notebook_path
                }
            }
        ],
        "max_concurrent_runs": 1
    }
    
    response = requests.post(url, headers=get_headers(), json=payload)
    
    if response.status_code == 200:
        job_id = response.json()["job_id"]
        print(f"Created job: {job_id}")
        return job_id
    else:
        print(f"Failed to create job: {response.text}")
        return None

def run_job(job_id: str, parameters: Dict[str, Any] = None) -> Optional[str]:
    """Run a Databricks job."""
    url = f"https://{DATABRICKS_HOST}/api/2.1/jobs/run-now"
    
    payload = {"job_id": job_id}
    if parameters:
        payload["notebook_params"] = {str(k): str(v) for k, v in parameters.items()}
    
    response = requests.post(url, headers=get_headers(), json=payload)
    
    if response.status_code == 200:
        run_id = response.json()["run_id"]
        print(f"Started run: {run_id}")
        return run_id
    else:
        print(f"Failed to run job: {response.text}")
        return None

def check_configuration() -> bool:
    """Check if Databricks is properly configured."""
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        print("Error: DATABRICKS_HOST and DATABRICKS_TOKEN environment variables must be set")
        print("")
        print("Set these environment variables:")
        print("  export DATABRICKS_HOST=<your-workspace>.cloud.databricks.com")
        print("  export DATABRICKS_TOKEN=<your-personal-access-token>")
        return False
    return True

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Databricks deployment utility")
    parser.add_argument("action", choices=["check", "upload", "create-cluster", "create-job", "run-job"])
    parser.add_argument("--local-path", help="Local file path for upload")
    parser.add_argument("--workspace-path", help="Databricks workspace path")
    parser.add_argument("--cluster-name", help="Cluster name")
    parser.add_argument("--num-workers", type=int, default=2, help="Number of workers")
    parser.add_argument("--cluster-id", help="Cluster ID")
    parser.add_argument("--job-name", help="Job name")
    parser.add_argument("--job-id", help="Job ID")
    parser.add_argument("--notebook-path", help="Notebook path in workspace")
    
    args = parser.parse_args()
    
    if args.action == "check":
        if check_configuration():
            print("Databricks configuration is valid!")
            url = f"https://{DATABRICKS_HOST}/api/2.0/clusters/list"
            response = requests.get(url, headers=get_headers())
            if response.status_code == 200:
                clusters = response.json().get("clusters", [])
                print(f"Found {len(clusters)} clusters")
            else:
                print(f"API call failed: {response.status_code}")
    
    elif args.action == "upload":
        if not check_configuration():
            return
        if not args.local_path or not args.workspace_path:
            print("--local-path and --workspace-path required")
            return
        upload_notebook(args.local_path, args.workspace_path)
    
    elif args.action == "create-cluster":
        if not check_configuration():
            return
        if not args.cluster_name:
            print("--cluster-name required")
            return
        create_cluster(args.cluster_name, args.num_workers)
    
    elif args.action == "create-job":
        if not check_configuration():
            return
        if not all([args.job_name, args.notebook_path, args.cluster_id]):
            print("--job-name, --notebook-path, and --cluster-id required")
            return
        create_job(args.job_name, args.notebook_path, args.cluster_id)
    
    elif args.action == "run-job":
        if not check_configuration():
            return
        if not args.job_id:
            print("--job-id required")
            return
        run_job(args.job_id)

if __name__ == "__main__":
    main()
