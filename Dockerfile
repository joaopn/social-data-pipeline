# Reddit Data Tools - CPU Base Image
# Used for: parse, ml_cpu, db_pg profiles
FROM python:3.11-slim

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
COPY reddit_data_tools/ ./reddit_data_tools/
COPY config/ ./config/

# Create directories
RUN mkdir -p /data/dumps /data/extracted /data/csv /data/output /data/database

# Default command (override per service)
CMD ["python", "-m", "reddit_data_tools.orchestrators.parse"]
