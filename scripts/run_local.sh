#!/bin/bash
# Local development script for Spark Data Processor

echo "Starting Spark Data Processor in local mode..."

# Set environment variables for local development
export STORAGE_BACKEND=local
export DEBUG=true

# Create necessary directories
mkdir -p storage/uploads storage/results

# Install Python dependencies
echo "Installing Python dependencies..."
cd backend
pip install -r requirements.txt

# Start the backend server
echo "Starting FastAPI backend on port 8000..."
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload &
BACKEND_PID=$!

# Install and start frontend
echo "Installing frontend dependencies..."
cd ../frontend
npm install

echo "Starting React frontend on port 5000..."
npm run dev &
FRONTEND_PID=$!

echo ""
echo "======================================"
echo "Spark Data Processor is running!"
echo "Frontend: http://localhost:5000"
echo "Backend API: http://localhost:8000"
echo "API Docs: http://localhost:8000/docs"
echo "======================================"
echo ""
echo "Press Ctrl+C to stop all services"

# Wait for interrupt
trap "kill $BACKEND_PID $FRONTEND_PID 2>/dev/null" EXIT
wait
