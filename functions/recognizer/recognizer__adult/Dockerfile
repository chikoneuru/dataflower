FROM python:3.11-slim

WORKDIR /app

# Copy TensorFlow cache if it exists (prevents timeout during build)
COPY .tmp/tensorflow_cache /tmp/tensorflow_cache
COPY .tmp/python_pkgs_whls /tmp/python_pkgs_whls

# Copy only the model file (static data)
COPY functions/recognizer/recognizer__adult/resnet50_final_adult.h5 /app/resnet50_final_adult.h5

# Copy the shared input retrieval service
COPY functions/recognizer/shared/input_retrieval_service.py /app/input_retrieval_service.py

# Copy the container API
COPY functions/shared/container_api.py /app/container_api.py

# Copy the main.py file
COPY functions/recognizer/recognizer__adult/main.py /app/main.py

# Install other dependencies (will use cache if available)
COPY functions/recognizer/recognizer__adult/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir /tmp/tensorflow_cache/*.whl

RUN pip install --no-cache-dir \
    --find-links /tmp/python_pkgs_whls/ \
    -r requirements.txt