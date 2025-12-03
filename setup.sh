#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

echo "Creating virtual environment..."
python3 -m venv venv

echo "Activating virtual environment..."
source venv/bin/activate

echo "Installing dependencies..."
pip install -r requirements.txt

echo "Setup complete!"
echo "You can now run './start.sh' to start Airflow."
