#!/bin/sh

echo "Running process_data.py..."
python /app/src/process_data.py || { echo "process_data.py failed"; exit 1; }

echo "Running run_queries.py..."
python /app/src/run_queries.py || { echo "run_queries.py failed"; exit 1; }

echo "Running data_vizualization.py..."
python /app/src/data_vizualization.py || { echo "data_vizualization.py failed"; exit 1; }

echo "All scripts executed successfully!"
