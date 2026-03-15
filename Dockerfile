# Social Data Bridge - CPU Base Image
# Used for: parse, lingua, postgres_ingest, postgres_ml profiles
FROM python:3.13-slim

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    zstd \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY social_data_bridge/ ./social_data_bridge/
COPY config/ ./config/

# Create directories
RUN mkdir -p /data/dumps /data/extracted /data/parsed /data/output /data/database

# Default command (override per service)
CMD ["python", "-m", "social_data_bridge.orchestrators.parse"]
