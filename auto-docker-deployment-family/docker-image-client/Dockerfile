FROM ubuntu:18.04

# Install Python 3.8, Sawtooth and system dependencies
RUN apt-get update && apt-get install -y software-properties-common \
    && apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 8AA7AF1F1091A5FD \
    && add-apt-repository 'deb [arch=amd64] http://repo.sawtooth.me/ubuntu/chime/stable bionic universe' \
    && apt-get update  \
    && apt-get install -y sawtooth \
    && add-apt-repository ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install -y python3.8 python3.8-venv python3.8-dev python3-pip \
    gcc libssl-dev curl apt-transport-https \
    && rm -rf /var/lib/apt/lists/*

# Set Python 3.8 as the default python version
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.8 1 \
    && update-alternatives --set python3 /usr/bin/python3.8 \
    && ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /app

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