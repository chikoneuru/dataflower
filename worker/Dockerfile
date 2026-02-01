FROM python:3.11-slim

# Install system dependencies required by functions
# - tesseract-ocr: for text extraction (recognizer__extract)
# - iputils-ping: for network connectivity checks
RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# Set working directory inside the container
WORKDIR /app

# Copy everything from the root (parent of worker/)
COPY worker/ /app/
COPY provider/ /app/provider
# Ensure Python can import packages from /app
ENV PYTHONPATH=/app

# Copy TensorFlow cache if it exists (prevents timeout during build)
COPY .tmp/python_pkgs_whls /tmp/python_pkgs_whls

# Install all cached packages (dependency order preserved by pip download)
RUN pip install --no-cache-dir /tmp/python_pkgs_whls/*.whl

# Install lightweight worker deps only
# RUN pip install --no-cache-dir --default-timeout=120 -r /app/requirements.txt
RUN pip install --no-cache-dir \
    --find-links /tmp/python_pkgs_whls/ \
    --default-timeout=120 \
    -r /app/requirements.txt

# Run the FastAPI worker app using the correct module path (file is /app/worker.py)
CMD ["uvicorn", "worker:app", "--reload", "--host", "0.0.0.0", "--port", "5000"]