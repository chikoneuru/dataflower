FROM python:3.11-slim

WORKDIR /app

# Install pip dependencies first
COPY functions/recognizer/recognizer__translate/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the shared input retrieval service
COPY functions/recognizer/shared/input_retrieval_service.py /app/input_retrieval_service.py

# Don't copy main.py - we'll mount it as a volume
