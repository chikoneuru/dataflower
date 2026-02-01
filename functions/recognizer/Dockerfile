FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

# Copy all recognizer functions and assets
COPY . /app/functions/recognizer

# Make functions importable as `functions.recognizer.<fn>.main`
ENV PYTHONPATH="/app"

CMD ["python", "-c", "print('Recognizer functions container ready')"]