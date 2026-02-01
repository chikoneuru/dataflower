# DataFlower Serverless Workflow Orchestration
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY experiments/ ./experiments/
COPY configs/ ./configs/
COPY run_all_experiments.py .

# Create directories for results and checkpoints
RUN mkdir -p results checkpoints data

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["python", "run_all_experiments.py", "--config", "configs/experiment_config.yaml"]
