# Use an official Python image
FROM python:3.11

COPY requirements.txt .
RUN pip install -r requirements.txt

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Set the working directory inside the container
WORKDIR /app

# Copy the application files
COPY src/ /app/src/
COPY sql/ /app/sql/
COPY requirements.txt /app/
COPY .env /app/

# Copy the service account file into the container
COPY data-project-452300-e2c341ffd483.json /app/data-project-452300-e2c341ffd483.json


COPY ddl.sql /app/ddl.sql
COPY trips.csv /app/trips.csv

# Set environment variable inside the container
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/data-project-452300-e2c341ffd483.json"


# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Default command to run both scripts
CMD ["bash", "-c", "python /app/src/process_data.py && python /app/src/run_queries.py && python /app/src/data_vizualization.py"]
