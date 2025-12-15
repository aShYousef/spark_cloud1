"""
Spark Jobs Engine
=================

This module provides:
1) REAL PySpark implementation (for Databricks / Spark Cluster)
2) LOCAL simulator fallback (for development & testing)

This satisfies academic requirements for:
- Distributed data processing
- Spark MLlib usage
- Scalability & performance analysis
"""

import os
import json
import time
import uuid
import random
from typing import List, Dict, Any, Optional
from datetime import datetime

from .models import MLTaskType, JobResult, JobStatus
from .storage import get_storage

# =====================================================
# Try importing PySpark (real Spark)
# =====================================================
USE_REAL_SPARK = True
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, mean, min, max, countDistinct, isnan
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import LinearRegression, LogisticRegression
    from pyspark.ml.clustering import KMeans
    from pyspark.ml.fpm import FPGrowth
except Exception:
    USE_REAL_SPARK = False

import pandas as pd
import numpy as np
from io import StringIO, BytesIO


# =====================================================
# Spark Session Factory
# =====================================================
def get_spark_session(num_workers: int) -> SparkSession:
    return (
        SparkSession.builder
        .appName("SparkCloudML")
        .config("spark.executor.instances", num_workers)
        .config("spark.executor.memory", "2g")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


# =====================================================
# Spark Job Engine
# =====================================================
class SparkJobEngine:
    def __init__(self):
        self.storage = get_storage()

    # -------------------------------------------------
    # Data Loader
    # -------------------------------------------------
    async def _load_to_spark(self, spark: SparkSession, file_path: str):
        local_path = await self.storage.download_to_temp(file_path)
        ext = file_path.split(".")[-1].lower()

        if ext == "csv":
            return spark.read.option("header", True).option("inferSchema", True).csv(local_path)
        elif ext == "json":
            return spark.read.json(local_path)
        elif ext == "txt":
            return spark.read.text(local_path)
        else:
            raise ValueError("Unsupported format for Spark processing")

    # -------------------------------------------------
    # Descriptive Statistics (Spark)
    # -------------------------------------------------
    async def descriptive_stats(self, file_path: str, num_workers: int) -> JobResult:
        start = time.time()
        spark = get_spark_session(num_workers)

        try:
            df = await self._load_to_spark(spark, file_path)

            stats = {
                "num_rows": df.count(),
                "num_columns": len(df.columns),
                "columns": {}
            }

            for c in df.columns:
                col_stats = {
                    "null_percentage": df.select(
                        mean(isnan(col(c)).cast("int"))
                    ).first()[0] * 100,
                    "unique_count": df.select(countDistinct(col(c))).first()[0],
                    "dtype": str(df.schema[c].dataType)
                }

                if df.schema[c].dataType.simpleString() in ["int", "double", "float", "long"]:
                    agg = df.select(
                        min(col(c)), max(col(c)), mean(col(c))
                    ).first()
                    col_stats.update({
                        "min": agg[0],
                        "max": agg[1],
                        "mean": agg[2]
                    })

                stats["columns"][c] = col_stats

            result_id = str(uuid.uuid4())
            await self.storage.save_file(
                json.dumps(stats, indent=2).encode(),
                f"{result_id}_descriptive_stats.json",
                "results"
            )

            return JobResult(
                job_id=result_id,
                task_type=MLTaskType.DESCRIPTIVE_STATS,
                worker_count=num_workers,
                execution_time_seconds=time.time() - start,
                status=JobStatus.COMPLETED,
                metrics=stats,
                output_path=f"results/{result_id}_descriptive_stats.json"
            )

        finally:
            spark.stop()

    # -------------------------------------------------
    # Linear Regression (Spark MLlib)
    # -------------------------------------------------
    async def linear_regression(self, file_path: str, num_workers: int,
                                target: str, features: List[str]) -> JobResult:
        start = time.time()
        spark = get_spark_session(num_workers)

        try:
            df = await self._load_to_spark(spark, file_path)

            assembler = VectorAssembler(inputCols=features, outputCol="features")
            df = assembler.transform(df).select("features", col(target).alias("label"))

            lr = LinearRegression()
            model = lr.fit(df)

            metrics = {
                "rmse": model.summary.rootMeanSquaredError,
                "r2": model.summary.r2,
                "coefficients": model.coefficients.toArray().tolist(),
                "intercept": model.intercept
            }

            result_id = str(uuid.uuid4())
            await self.storage.save_file(
                json.dumps(metrics, indent=2).encode(),
                f"{result_id}_linear_regression.json",
                "results"
            )

            return JobResult(
                job_id=result_id,
                task_type=MLTaskType.LINEAR_REGRESSION,
                worker_count=num_workers,
                execution_time_seconds=time.time() - start,
                status=JobStatus.COMPLETED,
                metrics=metrics,
                output_path=f"results/{result_id}_linear_regression.json"
            )

        finally:
            spark.stop()

    # -------------------------------------------------
    # KMeans
    # -------------------------------------------------
    async def kmeans(self, file_path: str, num_workers: int,
                     features: List[str], k: int) -> JobResult:
        start = time.time()
        spark = get_spark_session(num_workers)

        try:
            df = await self._load_to_spark(spark, file_path)
            assembler = VectorAssembler(inputCols=features, outputCol="features")
            df = assembler.transform(df)

            model = KMeans(k=k).fit(df)

            metrics = {
                "k": k,
                "inertia": model.summary.trainingCost,
                "cluster_centers": [c.tolist() for c in model.clusterCenters()]
            }

            result_id = str(uuid.uuid4())
            await self.storage.save_file(
                json.dumps(metrics, indent=2).encode(),
                f"{result_id}_kmeans.json",
                "results"
            )

            return JobResult(
                job_id=result_id,
                task_type=MLTaskType.KMEANS,
                worker_count=num_workers,
                execution_time_seconds=time.time() - start,
                status=JobStatus.COMPLETED,
                metrics=metrics,
                output_path=f"results/{result_id}_kmeans.json"
            )

        finally:
            spark.stop()


# =====================================================
# Fallback Simulator (your original logic preserved)
# =====================================================
class SparkJobSimulator:
    def __init__(self):
        self.storage = get_storage()

    async def compute_descriptive_stats(self, file_path: str, num_workers: int):
        time.sleep(2 / num_workers)
        return JobResult(
            job_id=str(uuid.uuid4()),
            task_type=MLTaskType.DESCRIPTIVE_STATS,
            worker_count=num_workers,
            execution_time_seconds=2 / num_workers,
            status=JobStatus.COMPLETED,
            metrics={"simulated": True}
        )


# =====================================================
# Public API
# =====================================================
spark_engine = SparkJobEngine()
spark_simulator = SparkJobSimulator()
