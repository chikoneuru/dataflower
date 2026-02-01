FROM python:3.11-slim
WORKDIR /app

# Install dependencies
COPY functions/recognizer/recognizer__upload/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the container API
COPY functions/shared/container_api.py /app/container_api.py

# Copy the main.py file
COPY functions/recognizer/recognizer__upload/main.py /app/main.py