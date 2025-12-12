import json
import time
import random
import uuid
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
from datetime import datetime
from .models import MLTaskType, DescriptiveStats, JobResult, JobStatus
from .storage import get_storage

class SparkJobSimulator:
    def __init__(self):
        self.storage = get_storage()
    
    async def load_data(self, file_path: str) -> pd.DataFrame:
        content = await self.storage.get_file(file_path)
        ext = file_path.split('.')[-1].lower()
        
        if ext == 'csv':
            from io import StringIO
            return pd.read_csv(StringIO(content.decode('utf-8')))
        elif ext == 'json':
            return pd.read_json(content.decode('utf-8'))
        elif ext == 'txt':
            from io import StringIO
            return pd.read_csv(StringIO(content.decode('utf-8')), sep='\t')
        else:
            raise ValueError(f"Unsupported file type: {ext}")
    
    async def compute_descriptive_stats(self, file_path: str, num_workers: int) -> JobResult:
        start_time = time.time()
        
        try:
            df = await self.load_data(file_path)
            
            base_time = 2.0 + len(df) * 0.0001
            simulated_time = base_time / (num_workers ** 0.7) + random.uniform(0.1, 0.5)
            time.sleep(min(simulated_time, 3))
            
            stats = {
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": {}
            }
            
            for col in df.columns:
                col_stats = {
                    "null_count": int(df[col].isna().sum()),
                    "null_percentage": float(df[col].isna().mean() * 100),
                    "unique_count": int(df[col].nunique()),
                    "dtype": str(df[col].dtype)
                }
                
                if pd.api.types.is_numeric_dtype(df[col]):
                    col_stats.update({
                        "min": float(df[col].min()) if not df[col].isna().all() else None,
                        "max": float(df[col].max()) if not df[col].isna().all() else None,
                        "mean": float(df[col].mean()) if not df[col].isna().all() else None,
                        "std": float(df[col].std()) if not df[col].isna().all() else None,
                        "median": float(df[col].median()) if not df[col].isna().all() else None
                    })
                
                stats["columns"][col] = col_stats
            
            result_id = str(uuid.uuid4())
            result_path = f"results/{result_id}_descriptive_stats.json"
            await self.storage.save_file(
                json.dumps(stats, indent=2).encode(),
                f"{result_id}_descriptive_stats.json",
                "results"
            )
            
            execution_time = time.time() - start_time
            
            return JobResult(
                job_id=result_id,
                task_type=MLTaskType.DESCRIPTIVE_STATS,
                worker_count=num_workers,
                execution_time_seconds=execution_time,
                status=JobStatus.COMPLETED,
                metrics=stats,
                output_path=result_path
            )
            
        except Exception as e:
            return JobResult(
                job_id=str(uuid.uuid4()),
                task_type=MLTaskType.DESCRIPTIVE_STATS,
                worker_count=num_workers,
                execution_time_seconds=time.time() - start_time,
                status=JobStatus.FAILED,
                error_message=str(e)
            )
    
    async def run_linear_regression(self, file_path: str, num_workers: int, 
                                    target_col: str, feature_cols: List[str]) -> JobResult:
        start_time = time.time()
        
        try:
            df = await self.load_data(file_path)
            
            base_time = 5.0 + len(df) * 0.0005
            simulated_time = base_time / (num_workers ** 0.75) + random.uniform(0.2, 0.8)
            time.sleep(min(simulated_time, 5))
            
            if target_col not in df.columns:
                numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
                if len(numeric_cols) >= 2:
                    target_col = numeric_cols[-1]
                    feature_cols = numeric_cols[:-1][:5]
                else:
                    raise ValueError("Not enough numeric columns for regression")
            
            X = df[feature_cols].fillna(0).values
            y = df[target_col].fillna(0).values
            
            X_mean = X.mean(axis=0)
            X_std = X.std(axis=0) + 1e-8
            X_norm = (X - X_mean) / X_std
            
            X_b = np.c_[np.ones((len(X_norm), 1)), X_norm]
            theta = np.linalg.lstsq(X_b, y, rcond=None)[0]
            
            y_pred = X_b.dot(theta)
            ss_res = np.sum((y - y_pred) ** 2)
            ss_tot = np.sum((y - y.mean()) ** 2)
            r2 = 1 - (ss_res / (ss_tot + 1e-8))
            rmse = np.sqrt(np.mean((y - y_pred) ** 2))
            mae = np.mean(np.abs(y - y_pred))
            
            metrics = {
                "r2_score": float(r2),
                "rmse": float(rmse),
                "mae": float(mae),
                "coefficients": {col: float(c) for col, c in zip(feature_cols, theta[1:])},
                "intercept": float(theta[0]),
                "target_column": target_col,
                "feature_columns": feature_cols,
                "num_samples": len(df)
            }
            
            result_id = str(uuid.uuid4())
            result_path = f"results/{result_id}_linear_regression.json"
            await self.storage.save_file(
                json.dumps(metrics, indent=2).encode(),
                f"{result_id}_linear_regression.json",
                "results"
            )
            
            return JobResult(
                job_id=result_id,
                task_type=MLTaskType.LINEAR_REGRESSION,
                worker_count=num_workers,
                execution_time_seconds=time.time() - start_time,
                status=JobStatus.COMPLETED,
                metrics=metrics,
                output_path=result_path
            )
            
        except Exception as e:
            return JobResult(
                job_id=str(uuid.uuid4()),
                task_type=MLTaskType.LINEAR_REGRESSION,
                worker_count=num_workers,
                execution_time_seconds=time.time() - start_time,
                status=JobStatus.FAILED,
                error_message=str(e)
            )
    
    async def run_logistic_regression(self, file_path: str, num_workers: int,
                                      target_col: str, feature_cols: List[str]) -> JobResult:
        start_time = time.time()
        
        try:
            df = await self.load_data(file_path)
            
            base_time = 6.0 + len(df) * 0.0006
            simulated_time = base_time / (num_workers ** 0.72) + random.uniform(0.3, 1.0)
            time.sleep(min(simulated_time, 5))
            
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            
            if target_col and target_col in df.columns:
                pass
            elif len(numeric_cols) >= 2:
                target_col = numeric_cols[-1]
            else:
                raise ValueError("No suitable target column found")
            
            if not feature_cols:
                feature_cols = [c for c in numeric_cols if c != target_col][:5]
            
            X = df[feature_cols].fillna(0).values
            y = df[target_col].fillna(0).values
            
            y_median = np.median(y)
            y_binary = (y > y_median).astype(int)
            
            def sigmoid(z):
                return 1 / (1 + np.exp(-np.clip(z, -500, 500)))
            
            X_mean = X.mean(axis=0)
            X_std = X.std(axis=0) + 1e-8
            X_norm = (X - X_mean) / X_std
            
            weights = np.zeros(X_norm.shape[1])
            bias = 0
            lr = 0.1
            
            for _ in range(100):
                z = np.dot(X_norm, weights) + bias
                predictions = sigmoid(z)
                dw = np.dot(X_norm.T, (predictions - y_binary)) / len(y_binary)
                db = np.mean(predictions - y_binary)
                weights -= lr * dw
                bias -= lr * db
            
            final_preds = (sigmoid(np.dot(X_norm, weights) + bias) > 0.5).astype(int)
            accuracy = np.mean(final_preds == y_binary)
            
            tp = np.sum((final_preds == 1) & (y_binary == 1))
            fp = np.sum((final_preds == 1) & (y_binary == 0))
            fn = np.sum((final_preds == 0) & (y_binary == 1))
            
            precision = tp / (tp + fp + 1e-8)
            recall = tp / (tp + fn + 1e-8)
            f1 = 2 * precision * recall / (precision + recall + 1e-8)
            
            metrics = {
                "accuracy": float(accuracy),
                "precision": float(precision),
                "recall": float(recall),
                "f1_score": float(f1),
                "coefficients": {col: float(w) for col, w in zip(feature_cols, weights)},
                "intercept": float(bias),
                "target_column": target_col,
                "feature_columns": feature_cols,
                "num_samples": len(df),
                "class_distribution": {"0": int(np.sum(y_binary == 0)), "1": int(np.sum(y_binary == 1))}
            }
            
            result_id = str(uuid.uuid4())
            result_path = f"results/{result_id}_logistic_regression.json"
            await self.storage.save_file(
                json.dumps(metrics, indent=2).encode(),
                f"{result_id}_logistic_regression.json",
                "results"
            )
            
            return JobResult(
                job_id=result_id,
                task_type=MLTaskType.LOGISTIC_REGRESSION,
                worker_count=num_workers,
                execution_time_seconds=time.time() - start_time,
                status=JobStatus.COMPLETED,
                metrics=metrics,
                output_path=result_path
            )
            
        except Exception as e:
            return JobResult(
                job_id=str(uuid.uuid4()),
                task_type=MLTaskType.LOGISTIC_REGRESSION,
                worker_count=num_workers,
                execution_time_seconds=time.time() - start_time,
                status=JobStatus.FAILED,
                error_message=str(e)
            )
    
    async def run_kmeans(self, file_path: str, num_workers: int, 
                         num_clusters: int = 3, feature_cols: Optional[List[str]] = None) -> JobResult:
        start_time = time.time()
        
        try:
            df = await self.load_data(file_path)
            
            base_time = 4.0 + len(df) * 0.0004
            simulated_time = base_time / (num_workers ** 0.8) + random.uniform(0.2, 0.6)
            time.sleep(min(simulated_time, 5))
            
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            if feature_cols:
                feature_cols = [c for c in feature_cols if c in numeric_cols]
            if not feature_cols:
                feature_cols = numeric_cols[:5]
            
            if len(feature_cols) < 2:
                raise ValueError("Need at least 2 numeric columns for clustering")
            
            X = df[feature_cols].fillna(0).values
            
            X_mean = X.mean(axis=0)
            X_std = X.std(axis=0) + 1e-8
            X_norm = (X - X_mean) / X_std
            
            np.random.seed(42)
            idx = np.random.choice(len(X_norm), min(num_clusters, len(X_norm)), replace=False)
            centroids = X_norm[idx].copy()
            
            for _ in range(50):
                distances = np.sqrt(((X_norm[:, np.newaxis] - centroids) ** 2).sum(axis=2))
                labels = distances.argmin(axis=1)
                
                new_centroids = np.array([
                    X_norm[labels == k].mean(axis=0) if np.sum(labels == k) > 0 else centroids[k]
                    for k in range(num_clusters)
                ])
                
                if np.allclose(centroids, new_centroids):
                    break
                centroids = new_centroids
            
            distances = np.sqrt(((X_norm[:, np.newaxis] - centroids) ** 2).sum(axis=2))
            labels = distances.argmin(axis=1)
            
            inertia = sum(np.sum((X_norm[labels == k] - centroids[k]) ** 2) 
                         for k in range(num_clusters))
            
            cluster_sizes = {str(k): int(np.sum(labels == k)) for k in range(num_clusters)}
            
            overall_mean = X_norm.mean(axis=0)
            between_ss = sum(np.sum(labels == k) * np.sum((centroids[k] - overall_mean) ** 2)
                           for k in range(num_clusters))
            total_ss = np.sum((X_norm - overall_mean) ** 2)
            silhouette_approx = between_ss / (total_ss + 1e-8)
            
            metrics = {
                "num_clusters": num_clusters,
                "inertia": float(inertia),
                "silhouette_score_approx": float(silhouette_approx),
                "cluster_sizes": cluster_sizes,
                "feature_columns": feature_cols,
                "num_samples": len(df),
                "centroids": centroids.tolist()
            }
            
            result_id = str(uuid.uuid4())
            result_path = f"results/{result_id}_kmeans.json"
            await self.storage.save_file(
                json.dumps(metrics, indent=2).encode(),
                f"{result_id}_kmeans.json",
                "results"
            )
            
            return JobResult(
                job_id=result_id,
                task_type=MLTaskType.KMEANS,
                worker_count=num_workers,
                execution_time_seconds=time.time() - start_time,
                status=JobStatus.COMPLETED,
                metrics=metrics,
                output_path=result_path
            )
            
        except Exception as e:
            return JobResult(
                job_id=str(uuid.uuid4()),
                task_type=MLTaskType.KMEANS,
                worker_count=num_workers,
                execution_time_seconds=time.time() - start_time,
                status=JobStatus.FAILED,
                error_message=str(e)
            )
    
    async def run_fpgrowth(self, file_path: str, num_workers: int,
                           min_support: float = 0.1, min_confidence: float = 0.5) -> JobResult:
        start_time = time.time()
        
        try:
            df = await self.load_data(file_path)
            
            base_time = 7.0 + len(df) * 0.0008
            simulated_time = base_time / (num_workers ** 0.65) + random.uniform(0.4, 1.2)
            time.sleep(min(simulated_time, 5))
            
            columns = df.columns.tolist()[:10]
            
            transactions = []
            for idx, row in df.head(1000).iterrows():
                items = set()
                for col in columns:
                    val = row[col]
                    if pd.notna(val):
                        if pd.api.types.is_numeric_dtype(df[col]):
                            median = df[col].median()
                            items.add(f"{col}_{'high' if val > median else 'low'}")
                        else:
                            items.add(f"{col}_{str(val)[:20]}")
                if items:
                    transactions.append(items)
            
            if not transactions:
                raise ValueError("No valid transactions could be created from the data")
            
            item_counts = {}
            for trans in transactions:
                for item in trans:
                    item_counts[item] = item_counts.get(item, 0) + 1
            
            n_trans = len(transactions)
            min_count = int(min_support * n_trans)
            frequent_items = {item: count for item, count in item_counts.items() 
                            if count >= min_count}
            
            frequent_itemsets = [{"items": [item], "support": count / n_trans} 
                                for item, count in sorted(frequent_items.items(), 
                                                         key=lambda x: -x[1])[:20]]
            
            pair_counts = {}
            for trans in transactions:
                freq_in_trans = [item for item in trans if item in frequent_items]
                for i, item1 in enumerate(freq_in_trans):
                    for item2 in freq_in_trans[i+1:]:
                        pair = tuple(sorted([item1, item2]))
                        pair_counts[pair] = pair_counts.get(pair, 0) + 1
            
            for pair, count in sorted(pair_counts.items(), key=lambda x: -x[1])[:10]:
                if count >= min_count:
                    frequent_itemsets.append({
                        "items": list(pair),
                        "support": count / n_trans
                    })
            
            association_rules = []
            for itemset in frequent_itemsets:
                if len(itemset["items"]) == 2:
                    item1, item2 = itemset["items"]
                    sup_both = itemset["support"]
                    sup1 = frequent_items.get(item1, 0) / n_trans
                    sup2 = frequent_items.get(item2, 0) / n_trans
                    
                    if sup1 > 0:
                        conf1 = sup_both / sup1
                        if conf1 >= min_confidence:
                            association_rules.append({
                                "antecedent": [item1],
                                "consequent": [item2],
                                "confidence": conf1,
                                "support": sup_both,
                                "lift": conf1 / (sup2 + 1e-8)
                            })
                    
                    if sup2 > 0:
                        conf2 = sup_both / sup2
                        if conf2 >= min_confidence:
                            association_rules.append({
                                "antecedent": [item2],
                                "consequent": [item1],
                                "confidence": conf2,
                                "support": sup_both,
                                "lift": conf2 / (sup1 + 1e-8)
                            })
            
            association_rules = sorted(association_rules, key=lambda x: -x["confidence"])[:20]
            
            metrics = {
                "num_transactions": n_trans,
                "min_support": min_support,
                "min_confidence": min_confidence,
                "num_frequent_itemsets": len(frequent_itemsets),
                "num_association_rules": len(association_rules),
                "frequent_itemsets": frequent_itemsets[:15],
                "association_rules": association_rules[:10]
            }
            
            result_id = str(uuid.uuid4())
            result_path = f"results/{result_id}_fpgrowth.json"
            await self.storage.save_file(
                json.dumps(metrics, indent=2).encode(),
                f"{result_id}_fpgrowth.json",
                "results"
            )
            
            return JobResult(
                job_id=result_id,
                task_type=MLTaskType.FPGROWTH,
                worker_count=num_workers,
                execution_time_seconds=time.time() - start_time,
                status=JobStatus.COMPLETED,
                metrics=metrics,
                output_path=result_path
            )
            
        except Exception as e:
            return JobResult(
                job_id=str(uuid.uuid4()),
                task_type=MLTaskType.FPGROWTH,
                worker_count=num_workers,
                execution_time_seconds=time.time() - start_time,
                status=JobStatus.FAILED,
                error_message=str(e)
            )
    
    async def run_time_series(self, file_path: str, num_workers: int,
                              timestamp_col: Optional[str] = None) -> JobResult:
        start_time = time.time()
        
        try:
            df = await self.load_data(file_path)
            
            base_time = 3.0 + len(df) * 0.0003
            simulated_time = base_time / (num_workers ** 0.78) + random.uniform(0.1, 0.4)
            time.sleep(min(simulated_time, 5))
            
            date_cols = []
            for col in df.columns:
                try:
                    pd.to_datetime(df[col].head(10))
                    date_cols.append(col)
                except:
                    pass
            
            if timestamp_col and timestamp_col in df.columns:
                pass
            elif date_cols:
                timestamp_col = date_cols[0]
            else:
                df['_synthetic_timestamp'] = pd.date_range(
                    start='2020-01-01', periods=len(df), freq='H'
                )
                timestamp_col = '_synthetic_timestamp'
            
            df[timestamp_col] = pd.to_datetime(df[timestamp_col])
            
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()[:3]
            
            aggregations = {}
            
            if numeric_cols:
                for col in numeric_cols:
                    aggregations[col] = {
                        "overall_mean": float(df[col].mean()),
                        "overall_std": float(df[col].std()),
                        "overall_min": float(df[col].min()),
                        "overall_max": float(df[col].max())
                    }
            
            df['_hour'] = df[timestamp_col].dt.hour
            df['_dayofweek'] = df[timestamp_col].dt.dayofweek
            df['_month'] = df[timestamp_col].dt.month
            
            hourly_pattern = {}
            daily_pattern = {}
            monthly_pattern = {}
            
            if numeric_cols:
                value_col = numeric_cols[0]
                hourly_agg = df.groupby('_hour')[value_col].mean()
                daily_agg = df.groupby('_dayofweek')[value_col].mean()
                monthly_agg = df.groupby('_month')[value_col].mean()
                
                hourly_pattern = {str(k): float(v) for k, v in hourly_agg.items()}
                daily_pattern = {str(k): float(v) for k, v in daily_agg.items()}
                monthly_pattern = {str(k): float(v) for k, v in monthly_agg.items()}
            
            time_range = {
                "start": str(df[timestamp_col].min()),
                "end": str(df[timestamp_col].max()),
                "duration_days": float((df[timestamp_col].max() - df[timestamp_col].min()).days)
            }
            
            metrics = {
                "timestamp_column": timestamp_col,
                "numeric_columns_analyzed": numeric_cols,
                "num_records": len(df),
                "time_range": time_range,
                "column_aggregations": aggregations,
                "hourly_pattern": hourly_pattern,
                "daily_pattern": daily_pattern,
                "monthly_pattern": monthly_pattern
            }
            
            result_id = str(uuid.uuid4())
            result_path = f"results/{result_id}_time_series.json"
            await self.storage.save_file(
                json.dumps(metrics, indent=2).encode(),
                f"{result_id}_time_series.json",
                "results"
            )
            
            return JobResult(
                job_id=result_id,
                task_type=MLTaskType.TIME_SERIES,
                worker_count=num_workers,
                execution_time_seconds=time.time() - start_time,
                status=JobStatus.COMPLETED,
                metrics=metrics,
                output_path=result_path
            )
            
        except Exception as e:
            return JobResult(
                job_id=str(uuid.uuid4()),
                task_type=MLTaskType.TIME_SERIES,
                worker_count=num_workers,
                execution_time_seconds=time.time() - start_time,
                status=JobStatus.FAILED,
                error_message=str(e)
            )

spark_simulator = SparkJobSimulator()
