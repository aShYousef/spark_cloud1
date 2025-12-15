#!/bin/bash
echo "Starting Spark Data Processor backend..."

cd backend || exit 1

uvicorn app.main:app \
  --host 0.0.0.0 \
  --port 8000 \
  --reload
