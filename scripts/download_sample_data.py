#!/usr/bin/env python3
"""
Sample dataset download script for performance testing.
Downloads popular datasets from UCI ML Repository and other sources.
"""

import os
import urllib.request
import zipfile
import gzip
import shutil

DATASETS = {
    "iris": {
        "url": "https://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data",
        "filename": "iris.csv",
        "description": "Iris flower dataset (150 samples, 5 columns)"
    },
    "wine": {
        "url": "https://archive.ics.uci.edu/ml/machine-learning-databases/wine/wine.data",
        "filename": "wine.csv",
        "description": "Wine dataset (178 samples, 14 columns)"
    },
    "adult": {
        "url": "https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data",
        "filename": "adult.csv",
        "description": "Adult Census dataset (32,561 samples, 15 columns)"
    },
    "breast_cancer": {
        "url": "https://archive.ics.uci.edu/ml/machine-learning-databases/breast-cancer-wisconsin/wdbc.data",
        "filename": "breast_cancer.csv",
        "description": "Breast Cancer Wisconsin dataset (569 samples, 32 columns)"
    }
}

def download_dataset(name, output_dir="sample_data"):
    """Download a specific dataset."""
    if name not in DATASETS:
        print(f"Unknown dataset: {name}")
        print(f"Available datasets: {list(DATASETS.keys())}")
        return None
    
    os.makedirs(output_dir, exist_ok=True)
    
    dataset = DATASETS[name]
    output_path = os.path.join(output_dir, dataset["filename"])
    
    print(f"Downloading {name}: {dataset['description']}")
    print(f"URL: {dataset['url']}")
    
    try:
        urllib.request.urlretrieve(dataset["url"], output_path)
        print(f"Saved to: {output_path}")
        return output_path
    except Exception as e:
        print(f"Error downloading {name}: {e}")
        return None

def generate_large_dataset(rows=100000, output_dir="sample_data"):
    """Generate a large synthetic dataset for performance testing."""
    import random
    import csv
    from datetime import datetime, timedelta
    
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"synthetic_{rows}.csv")
    
    print(f"Generating synthetic dataset with {rows} rows...")
    
    categories = ["A", "B", "C", "D", "E"]
    start_date = datetime(2020, 1, 1)
    
    with open(output_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["id", "timestamp", "category", "value1", "value2", "value3", "value4", "target"])
        
        for i in range(rows):
            timestamp = start_date + timedelta(hours=i)
            row = [
                i,
                timestamp.isoformat(),
                random.choice(categories),
                round(random.gauss(50, 15), 2),
                round(random.gauss(100, 30), 2),
                round(random.uniform(0, 1), 4),
                random.randint(1, 100),
                1 if random.random() > 0.5 else 0
            ]
            writer.writerow(row)
    
    file_size = os.path.getsize(output_path) / (1024 * 1024)
    print(f"Generated: {output_path} ({file_size:.2f} MB)")
    return output_path

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Download sample datasets for Spark Data Processor")
    parser.add_argument("--dataset", "-d", choices=list(DATASETS.keys()) + ["all", "synthetic"],
                       help="Dataset to download")
    parser.add_argument("--rows", "-r", type=int, default=100000,
                       help="Number of rows for synthetic dataset")
    parser.add_argument("--output", "-o", default="sample_data",
                       help="Output directory")
    parser.add_argument("--list", "-l", action="store_true",
                       help="List available datasets")
    
    args = parser.parse_args()
    
    if args.list:
        print("Available datasets:")
        for name, info in DATASETS.items():
            print(f"  {name}: {info['description']}")
        print("  synthetic: Generate large synthetic dataset")
        return
    
    if args.dataset == "all":
        for name in DATASETS:
            download_dataset(name, args.output)
    elif args.dataset == "synthetic":
        generate_large_dataset(args.rows, args.output)
    elif args.dataset:
        download_dataset(args.dataset, args.output)
    else:
        print("Downloading all standard datasets and generating synthetic data...")
        for name in DATASETS:
            download_dataset(name, args.output)
        generate_large_dataset(args.rows, args.output)

if __name__ == "__main__":
    main()
