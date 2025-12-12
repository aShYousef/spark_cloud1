#!/bin/bash
mkdir -p storage/uploads storage/results
export STORAGE_BACKEND=local
cd backend && uvicorn app.main:app --host 0.0.0.0 --port 8000 &
cd frontend && npm run dev
