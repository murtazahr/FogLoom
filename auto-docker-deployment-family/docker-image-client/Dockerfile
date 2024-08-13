FROM hyperledger/sawtooth-shell:chime

RUN apt-get update && apt-get install -y software-properties-common python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

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