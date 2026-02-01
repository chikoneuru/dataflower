FROM python:3.11-slim

WORKDIR /app

# Copy TensorFlow cache if it exists (prevents timeout during build)
COPY .tmp/tensorflow_cache /tmp/tensorflow_cache
COPY .tmp/python_pkgs_whls /tmp/python_pkgs_whls

# Copy only the model file (static data)
COPY functions/recognizer/recognizer__violence/resnet50_final_violence.h5 /app/resnet50_final_violence.h5

# Copy the shared input retrieval service
COPY functions/recognizer/shared/input_retrieval_service.py /app/input_retrieval_service.py

# Install other dependencies (will use cache if available)
COPY functions/recognizer/recognizer__violence/requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir /tmp/tensorflow_cache/*.whl

RUN pip install --no-cache-dir \
    --find-links /tmp/python_pkgs_whls/ \
    -r requirements.txt


# Don't copy main.py - we'll mount it as a volume
