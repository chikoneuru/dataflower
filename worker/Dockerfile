FROM python:3.10-slim

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

# Install lightweight worker deps only
RUN pip install --no-cache-dir --default-timeout=120 -r /app/requirements.txt

# Run the FastAPI worker app using the correct module path
CMD ["uvicorn", "worker.worker:app", "--reload", "--host", "0.0.0.0", "--port", "5000"]