#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

if [ ! -d "venv" ]; then
    echo "Virtual environment not found. Please run ./setup.sh first."
    exit 1
fi

source venv/bin/activate

export AIRFLOW_HOME=$(pwd)

echo "Starting Airflow Standalone..."
airflow standalone
