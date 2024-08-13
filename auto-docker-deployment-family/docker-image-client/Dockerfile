FROM python:3.8

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libssl-dev curl software-properties-common apt-transport-https && \
    rm -rf /var/lib/apt/lists/*

# Install Sawtooth CLI
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 8AA7AF1F1091A5FD && \
    apt-add-repository 'deb [arch=amd64] http://repo.sawtooth.me/ubuntu/chime/stable bionic universe' && \
    apt-get update && apt-get install -y sawtooth

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Docker CLI
RUN apt-get update && \
    apt-get install -y --no-install-recommends docker.io && \
    rm -rf /var/lib/apt/lists/*

# Copy the client script
COPY docker_image_client.py .

# Copy tar file
COPY temp-anomaly-detection.tar .

# Generate Sawtooth keys
RUN sawtooth keygen

ENV SAWTOOTH_PRIVATE_KEY /root/.sawtooth/keys/root.priv

ENTRYPOINT ["python", "docker_image_client.py"]