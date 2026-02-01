FROM python:3.10-slim

WORKDIR /app

# Install pip dependencies first
COPY functions/recognizer/recognizer__mosaic/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Don't copy main.py - we'll mount it as a volume
