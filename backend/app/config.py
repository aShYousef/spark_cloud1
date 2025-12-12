import os
from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    APP_NAME: str = "Spark Data Processor"
    DEBUG: bool = True
    
    UPLOAD_DIR: str = "uploads"
    RESULTS_DIR: str = "results"
    MAX_UPLOAD_SIZE_MB: int = 100
    ALLOWED_EXTENSIONS: list = ["csv", "json", "txt", "pdf"]
    
    STORAGE_BACKEND: str = os.getenv("STORAGE_BACKEND", "local")
    
    AWS_ACCESS_KEY_ID: Optional[str] = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY: Optional[str] = os.getenv("AWS_SECRET_ACCESS_KEY")
    AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
    S3_BUCKET: Optional[str] = os.getenv("S3_BUCKET")
    
    DATABRICKS_HOST: Optional[str] = os.getenv("DATABRICKS_HOST")
    DATABRICKS_TOKEN: Optional[str] = os.getenv("DATABRICKS_TOKEN")
    DATABRICKS_CLUSTER_SPEC: dict = {
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "driver_node_type_id": "i3.xlarge",
    }
    
    class Config:
        env_file = ".env"
        extra = "allow"

settings = Settings()
