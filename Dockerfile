# Use an official Python image
FROM python:3.11

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Set the working directory inside the container
WORKDIR /app

# Copy requirements first to optimize caching
COPY requirements.txt /app/

# Install dependencies
RUN python -m pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r /app/requirements.txt

# Install Matplotlib dependencies for rendering inside Docker
RUN apt-get update && apt-get install -y python3-tk

# Copy application files
COPY src/ /app/src/
COPY sql/ /app/sql/
COPY ddl.sql /app/ddl.sql
COPY trips.csv /app/trips.csv

# Copy optional environment file
COPY .env /app/.env

# Copy the service account file
COPY data-project-452300-e2c341ffd483.json /app/data-project-452300-e2c341ffd483.json

# Set environment variable inside the container
ENV GOOGLE_APPLICATION_CREDENTIALS="/app/data-project-452300-e2c341ffd483.json"

# Copy entrypoint script for correct execution
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# Default command to run all scripts in sequence
CMD ["/app/entrypoint.sh"]
