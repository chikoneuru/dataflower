FROM python:3.11-slim

WORKDIR /app

RUN apt-get clean
RUN apt-get update
RUN apt-get install -y tesseract-ocr

COPY functions/recognizer/recognizer__mosaic/requirements.txt /app/requirements.txt
COPY .tmp/python_pkgs_whls /tmp/python_pkgs_whls

RUN pip install --no-cache-dir \
    --find-links /tmp/python_pkgs_whls/ \
    -r requirements.txt
    
# Copy the shared input retrieval service
COPY functions/recognizer/shared/input_retrieval_service.py /app/input_retrieval_service.py

# Copy the container API
COPY functions/shared/container_api.py /app/container_api.py

# Copy the main.py file
COPY functions/recognizer/recognizer__mosaic/main.py /app/main.py
