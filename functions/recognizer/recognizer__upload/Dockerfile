FROM python:3.11-slim
WORKDIR /app

# Install dependencies
COPY functions/recognizer/recognizer__upload/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Don't copy main.py - it will be mounted as volume
# The main.py will be mounted at runtime by docker-compose