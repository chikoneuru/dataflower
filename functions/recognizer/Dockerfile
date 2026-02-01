FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Copy TensorFlow cache if it exists (prevents timeout during build)
COPY .tmp/tensorflow_cache /tmp/tensorflow_cache
COPY .tmp/python_pkgs_whls /tmp/python_pkgs_whls

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir \
    --find-links /tmp/tensorflow_cache/ \
    --find-links /tmp/python_pkgs_whls/ \
    -r requirements.txt

# Copy all recognizer functions and assets
COPY . /app/functions/recognizer

# Make functions importable as `functions.recognizer.<fn>.main`
ENV PYTHONPATH="/app"

CMD ["python", "-c", "print('Recognizer function container ready')"]