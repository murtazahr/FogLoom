FROM python:3.8

WORKDIR /app

# Install System Dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl gcc libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python Dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the event handler script
COPY docker_image_event_handler.py .

ENTRYPOINT ["python", "docker_image_event_handler.py"]