FROM python:3.11-slim

WORKDIR /app

# Install pip dependencies first
COPY functions/recognizer/recognizer__translate/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the shared input retrieval service
COPY functions/recognizer/shared/input_retrieval_service.py /app/input_retrieval_service.py

# Copy the container API
COPY functions/shared/container_api.py /app/container_api.py

# Copy the main.py file
COPY functions/recognizer/recognizer__translate/main.py /app/main.py
