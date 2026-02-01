FROM python:3.12-slim

WORKDIR /app

# Copy TensorFlow cache if it exists (prevents timeout during build)
COPY .tmp/tensorflow_cache /tmp/tensorflow_cache

# Install all cached packages (dependency order preserved by pip download)
RUN pip install --no-cache-dir /tmp/tensorflow_cache/*.whl

# Install other dependencies (will use cache if available)
COPY functions/recognizer/recognizer__violence/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir \
    --find-links /tmp/tensorflow_cache/ \
    -r requirements.txt

# Copy only the model file (static data)
COPY functions/recognizer/recognizer__violence/resnet50_final_violence.h5 /app/resnet50_final_violence.h5

# Don't copy main.py - we'll mount it as a volume
