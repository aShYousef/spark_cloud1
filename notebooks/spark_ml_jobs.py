# Databricks notebook source
# MAGIC %md
# MAGIC # Spark ML Jobs Notebook
# MAGIC This notebook contains PySpark implementations of the ML tasks
# MAGIC for running on Databricks clusters.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.fpm import FPGrowth
from pyspark.ml.evaluation import RegressionEvaluator, BinaryClassificationEvaluator, ClusteringEvaluator
import json
import time

# Get parameters
dbutils.widgets.text("input_path", "", "Input file path")
dbutils.widgets.text("output_path", "", "Output path for results")
dbutils.widgets.text("task_type", "descriptive_stats", "Task type")
dbutils.widgets.text("target_column", "", "Target column for regression/classification")
dbutils.widgets.text("feature_columns", "", "Feature columns (comma-separated)")
dbutils.widgets.text("num_clusters", "3", "Number of clusters for KMeans")
dbutils.widgets.text("min_support", "0.1", "Minimum support for FPGrowth")
dbutils.widgets.text("min_confidence", "0.5", "Minimum confidence for FPGrowth")

input_path = dbutils.widgets.get("input_path")
output_path = dbutils.widgets.get("output_path")
task_type = dbutils.widgets.get("task_type")
target_column = dbutils.widgets.get("target_column")
feature_columns = dbutils.widgets.get("feature_columns").split(",") if dbutils.widgets.get("feature_columns") else []
num_clusters = int(dbutils.widgets.get("num_clusters"))
min_support = float(dbutils.widgets.get("min_support"))
min_confidence = float(dbutils.widgets.get("min_confidence"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

start_time = time.time()

# Detect file format and load
if input_path.endswith(".csv"):
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
elif input_path.endswith(".json"):
    df = spark.read.json(input_path)
elif input_path.endswith(".parquet"):
    df = spark.read.parquet(input_path)
else:
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

df.cache()
print(f"Loaded {df.count()} rows with {len(df.columns)} columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Descriptive Statistics

# COMMAND ----------

def compute_descriptive_stats(df):
    """Compute comprehensive descriptive statistics."""
    stats = {
        "num_rows": df.count(),
        "num_columns": len(df.columns),
        "columns": {}
    }
    
    for col_name in df.columns:
        col_type = str(df.schema[col_name].dataType)
        col_stats = {
            "dtype": col_type,
            "null_count": df.filter(col(col_name).isNull()).count(),
            "null_percentage": (df.filter(col(col_name).isNull()).count() / stats["num_rows"]) * 100,
            "unique_count": df.select(col_name).distinct().count()
        }
        
        # Numeric column stats
        if "Int" in col_type or "Double" in col_type or "Float" in col_type or "Long" in col_type:
            summary = df.select(col_name).summary("min", "max", "mean", "stddev").collect()
            col_stats["min"] = float(summary[0][1]) if summary[0][1] else None
            col_stats["max"] = float(summary[1][1]) if summary[1][1] else None
            col_stats["mean"] = float(summary[2][1]) if summary[2][1] else None
            col_stats["std"] = float(summary[3][1]) if summary[3][1] else None
        
        stats["columns"][col_name] = col_stats
    
    return stats

if task_type == "descriptive_stats":
    results = compute_descriptive_stats(df)
    print(json.dumps(results, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Linear Regression

# COMMAND ----------

def run_linear_regression(df, target_col, feature_cols):
    """Train and evaluate linear regression model."""
    # Get numeric columns if not specified
    if not feature_cols:
        numeric_cols = [f.name for f in df.schema.fields 
                       if str(f.dataType) in ["IntegerType", "DoubleType", "FloatType", "LongType"]]
        if target_col:
            feature_cols = [c for c in numeric_cols if c != target_col]
        else:
            feature_cols = numeric_cols[:-1]
            target_col = numeric_cols[-1]
    
    # Prepare data
    df_clean = df.select([target_col] + feature_cols).na.drop()
    
    # Assemble features
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_assembled = assembler.transform(df_clean)
    
    # Scale features
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)
    
    # Split data
    train_df, test_df = df_scaled.randomSplit([0.8, 0.2], seed=42)
    
    # Train model
    lr = LinearRegression(featuresCol="scaledFeatures", labelCol=target_col)
    model = lr.fit(train_df)
    
    # Evaluate
    predictions = model.transform(test_df)
    evaluator_rmse = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="rmse")
    evaluator_r2 = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="r2")
    evaluator_mae = RegressionEvaluator(labelCol=target_col, predictionCol="prediction", metricName="mae")
    
    return {
        "r2_score": evaluator_r2.evaluate(predictions),
        "rmse": evaluator_rmse.evaluate(predictions),
        "mae": evaluator_mae.evaluate(predictions),
        "coefficients": dict(zip(feature_cols, model.coefficients.toArray().tolist())),
        "intercept": model.intercept,
        "target_column": target_col,
        "feature_columns": feature_cols,
        "num_samples": df_clean.count()
    }

if task_type == "linear_regression":
    results = run_linear_regression(df, target_column, feature_columns)
    print(json.dumps(results, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logistic Regression

# COMMAND ----------

def run_logistic_regression(df, target_col, feature_cols):
    """Train and evaluate logistic regression model."""
    # Get numeric columns if not specified
    if not feature_cols:
        numeric_cols = [f.name for f in df.schema.fields 
                       if str(f.dataType) in ["IntegerType", "DoubleType", "FloatType", "LongType"]]
        if target_col:
            feature_cols = [c for c in numeric_cols if c != target_col]
        else:
            feature_cols = numeric_cols[:-1]
            target_col = numeric_cols[-1]
    
    # Prepare data
    df_clean = df.select([target_col] + feature_cols).na.drop()
    
    # Convert target to binary
    median_val = df_clean.approxQuantile(target_col, [0.5], 0.01)[0]
    df_binary = df_clean.withColumn("label", when(col(target_col) > median_val, 1.0).otherwise(0.0))
    
    # Assemble features
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_assembled = assembler.transform(df_binary)
    
    # Split data
    train_df, test_df = df_assembled.randomSplit([0.8, 0.2], seed=42)
    
    # Train model
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=100)
    model = lr.fit(train_df)
    
    # Evaluate
    predictions = model.transform(test_df)
    evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")
    
    # Calculate metrics
    tp = predictions.filter((col("prediction") == 1) & (col("label") == 1)).count()
    fp = predictions.filter((col("prediction") == 1) & (col("label") == 0)).count()
    fn = predictions.filter((col("prediction") == 0) & (col("label") == 1)).count()
    tn = predictions.filter((col("prediction") == 0) & (col("label") == 0)).count()
    
    accuracy = (tp + tn) / (tp + tn + fp + fn) if (tp + tn + fp + fn) > 0 else 0
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
    
    return {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "auc": evaluator.evaluate(predictions),
        "coefficients": dict(zip(feature_cols, model.coefficients.toArray().tolist())),
        "intercept": model.intercept,
        "target_column": target_col,
        "feature_columns": feature_cols,
        "num_samples": df_clean.count()
    }

if task_type == "logistic_regression":
    results = run_logistic_regression(df, target_column, feature_columns)
    print(json.dumps(results, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## K-Means Clustering

# COMMAND ----------

def run_kmeans(df, num_clusters, feature_cols):
    """Train and evaluate K-Means clustering model."""
    # Get numeric columns if not specified
    if not feature_cols:
        numeric_cols = [f.name for f in df.schema.fields 
                       if str(f.dataType) in ["IntegerType", "DoubleType", "FloatType", "LongType"]]
        feature_cols = numeric_cols[:5]  # Use first 5 numeric columns
    
    # Prepare data
    df_clean = df.select(feature_cols).na.drop()
    
    # Assemble features
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_assembled = assembler.transform(df_clean)
    
    # Scale features
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
    scaler_model = scaler.fit(df_assembled)
    df_scaled = scaler_model.transform(df_assembled)
    
    # Train model
    kmeans = KMeans(featuresCol="scaledFeatures", k=num_clusters, seed=42)
    model = kmeans.fit(df_scaled)
    
    # Evaluate
    predictions = model.transform(df_scaled)
    evaluator = ClusteringEvaluator(featuresCol="scaledFeatures", predictionCol="prediction")
    silhouette = evaluator.evaluate(predictions)
    
    # Get cluster sizes
    cluster_sizes = predictions.groupBy("prediction").count().collect()
    
    return {
        "num_clusters": num_clusters,
        "inertia": model.summary.trainingCost,
        "silhouette_score": silhouette,
        "cluster_sizes": {str(row["prediction"]): row["count"] for row in cluster_sizes},
        "feature_columns": feature_cols,
        "num_samples": df_clean.count(),
        "centroids": [c.tolist() for c in model.clusterCenters()]
    }

if task_type == "kmeans":
    results = run_kmeans(df, num_clusters, feature_columns)
    print(json.dumps(results, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## FP-Growth (Frequent Pattern Mining)

# COMMAND ----------

def run_fpgrowth(df, min_support, min_confidence):
    """Run FP-Growth algorithm for frequent pattern mining."""
    # Convert data to transaction format
    columns = df.columns[:10]  # Use first 10 columns
    
    # Create items from column values
    def create_items(row):
        items = []
        for col in columns:
            val = row[col]
            if val is not None:
                items.append(f"{col}_{str(val)[:20]}")
        return items
    
    from pyspark.sql.types import ArrayType, StringType
    create_items_udf = udf(create_items, ArrayType(StringType()))
    
    df_items = df.withColumn("items", create_items_udf(struct([col(c) for c in columns])))
    
    # Run FP-Growth
    fpGrowth = FPGrowth(itemsCol="items", minSupport=min_support, minConfidence=min_confidence)
    model = fpGrowth.fit(df_items)
    
    # Get frequent itemsets and rules
    freq_itemsets = model.freqItemsets.collect()
    assoc_rules = model.associationRules.collect()
    
    return {
        "num_transactions": df.count(),
        "min_support": min_support,
        "min_confidence": min_confidence,
        "num_frequent_itemsets": len(freq_itemsets),
        "num_association_rules": len(assoc_rules),
        "frequent_itemsets": [{"items": row["items"], "freq": row["freq"]} for row in freq_itemsets[:15]],
        "association_rules": [
            {
                "antecedent": row["antecedent"],
                "consequent": row["consequent"],
                "confidence": row["confidence"],
                "lift": row["lift"]
            }
            for row in assoc_rules[:10]
        ]
    }

if task_type == "fpgrowth":
    results = run_fpgrowth(df, min_support, min_confidence)
    print(json.dumps(results, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------

execution_time = time.time() - start_time

if output_path:
    results["execution_time_seconds"] = execution_time
    results_json = json.dumps(results, indent=2)
    
    # Save to DBFS
    dbutils.fs.put(output_path, results_json, overwrite=True)
    print(f"Results saved to: {output_path}")

print(f"\nExecution time: {execution_time:.2f} seconds")
