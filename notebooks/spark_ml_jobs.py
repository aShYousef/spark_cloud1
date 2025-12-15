# Databricks notebook source
# MAGIC %md
# MAGIC # Spark ML Jobs Notebook
# MAGIC Distributed Machine Learning & Analytics using PySpark on Databricks

# COMMAND ----------

# =========================
# Setup & Imports
# =========================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.fpm import FPGrowth
from pyspark.ml.evaluation import (
    RegressionEvaluator,
    BinaryClassificationEvaluator,
    ClusteringEvaluator
)
import json
import time

spark = SparkSession.builder.getOrCreate()

# =========================
# Widgets (Parameters)
# =========================

dbutils.widgets.text("input_path", "", "Input file path")
dbutils.widgets.text("output_path", "", "Output path")
dbutils.widgets.text("task_type", "descriptive_stats", "Task type")
dbutils.widgets.text("target_column", "", "Target column")
dbutils.widgets.text("feature_columns", "", "Feature columns (comma-separated)")
dbutils.widgets.text("num_clusters", "3", "KMeans clusters")
dbutils.widgets.text("min_support", "0.1", "FP-Growth min support")
dbutils.widgets.text("min_confidence", "0.5", "FP-Growth min confidence")

input_path = dbutils.widgets.get("input_path")
output_path = dbutils.widgets.get("output_path")
task_type = dbutils.widgets.get("task_type")
target_column = dbutils.widgets.get("target_column")
feature_columns = (
    dbutils.widgets.get("feature_columns").split(",")
    if dbutils.widgets.get("feature_columns") else []
)
num_clusters = int(dbutils.widgets.get("num_clusters"))
min_support = float(dbutils.widgets.get("min_support"))
min_confidence = float(dbutils.widgets.get("min_confidence"))

num_workers = spark.sparkContext.defaultParallelism

# =========================
# Load Data
# =========================

start_time = time.time()

if input_path.endswith(".csv"):
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
elif input_path.endswith(".json"):
    df = spark.read.json(input_path)
elif input_path.endswith(".parquet"):
    df = spark.read.parquet(input_path)
else:
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

df.cache()

# =========================
# Descriptive Statistics
# =========================

def descriptive_stats(df):
    stats = {
        "rows": df.count(),
        "columns": len(df.columns),
        "schema": {},
    }

    for c in df.columns:
        dtype = str(df.schema[c].dataType)
        col_stats = {
            "dtype": dtype,
            "null_count": df.filter(col(c).isNull()).count(),
            "unique_values": df.select(c).distinct().count(),
        }

        if "Int" in dtype or "Double" in dtype or "Long" in dtype:
            summary = df.select(c).summary("min", "max", "mean").collect()
            col_stats["min"] = summary[0][1]
            col_stats["max"] = summary[1][1]
            col_stats["mean"] = summary[2][1]

        stats["schema"][c] = col_stats

    return stats

# =========================
# Linear Regression
# =========================

def linear_regression_job(df):
    numeric_cols = [
        f.name for f in df.schema.fields
        if "Int" in str(f.dataType) or "Double" in str(f.dataType)
    ]

    target = target_column or numeric_cols[-1]
    features = feature_columns or numeric_cols[:-1]

    df_clean = df.select([target] + features).na.drop()

    assembler = VectorAssembler(inputCols=features, outputCol="features")
    data = assembler.transform(df_clean)

    train, test = data.randomSplit([0.8, 0.2], seed=42)

    model = LinearRegression(
        labelCol=target,
        featuresCol="features"
    ).fit(train)

    preds = model.transform(test)

    evaluator = RegressionEvaluator(labelCol=target, predictionCol="prediction")

    return {
        "rmse": evaluator.setMetricName("rmse").evaluate(preds),
        "r2": evaluator.setMetricName("r2").evaluate(preds),
        "coefficients": model.coefficients.tolist(),
        "intercept": model.intercept
    }

# =========================
# Logistic Regression
# =========================

def logistic_regression_job(df):
    numeric_cols = [
        f.name for f in df.schema.fields
        if "Int" in str(f.dataType) or "Double" in str(f.dataType)
    ]

    target = target_column or numeric_cols[-1]
    features = feature_columns or numeric_cols[:-1]

    df_bin = df.select([target] + features).na.drop()
    median = df_bin.approxQuantile(target, [0.5], 0.01)[0]

    df_bin = df_bin.withColumn(
        "label", when(col(target) > median, 1.0).otherwise(0.0)
    )

    assembler = VectorAssembler(inputCols=features, outputCol="features")
    data = assembler.transform(df_bin)

    train, test = data.randomSplit([0.8, 0.2], seed=42)

    model = LogisticRegression(labelCol="label").fit(train)
    preds = model.transform(test)

    evaluator = BinaryClassificationEvaluator(labelCol="label")

    return {
        "auc": evaluator.evaluate(preds),
        "coefficients": model.coefficients.tolist(),
        "intercept": model.intercept
    }

# =========================
# KMeans
# =========================

def kmeans_job(df):
    numeric_cols = [
        f.name for f in df.schema.fields
        if "Int" in str(f.dataType) or "Double" in str(f.dataType)
    ][:5]

    df_clean = df.select(numeric_cols).na.drop()

    assembler = VectorAssembler(inputCols=numeric_cols, outputCol="features")
    data = assembler.transform(df_clean)

    model = KMeans(k=num_clusters, seed=42).fit(data)
    preds = model.transform(data)

    evaluator = ClusteringEvaluator()

    return {
        "clusters": num_clusters,
        "silhouette": evaluator.evaluate(preds),
        "centroids": [c.tolist() for c in model.clusterCenters()]
    }

# =========================
# FP-Growth
# =========================

def fpgrowth_job(df):
    df_items = df.select(
        array(*[col(c).cast("string") for c in df.columns[:10]]).alias("items")
    )

    model = FPGrowth(
        itemsCol="items",
        minSupport=min_support,
        minConfidence=min_confidence
    ).fit(df_items)

    return {
        "frequent_itemsets": model.freqItemsets.count(),
        "association_rules": model.associationRules.count()
    }

# =========================
# Run Task
# =========================

if task_type == "descriptive_stats":
    results = descriptive_stats(df)
elif task_type == "linear_regression":
    results = linear_regression_job(df)
elif task_type == "logistic_regression":
    results = logistic_regression_job(df)
elif task_type == "kmeans":
    results = kmeans_job(df)
elif task_type == "fpgrowth":
    results = fpgrowth_job(df)
else:
    results = {"error": "Unknown task"}

# =========================
# Performance Metrics
# =========================

execution_time = time.time() - start_time

results["execution_time_seconds"] = execution_time
results["num_workers"] = num_workers

# =========================
# Save Results
# =========================

if output_path:
    dbutils.fs.put(
        output_path,
        json.dumps(results, indent=2),
        overwrite=True
    )

print(json.dumps(results, indent=2))
