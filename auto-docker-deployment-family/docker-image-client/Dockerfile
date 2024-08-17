# Start with the Docker-in-Docker image
FROM docker:dind AS dind-base

# Now, use the Sawtooth shell image as the main base
FROM hyperledger/sawtooth-shell:chime

# Copy Docker binaries from dind-base
COPY --from=dind-base /usr/local/bin/docker /usr/local/bin/
COPY --from=dind-base /usr/local/bin/dockerd /usr/local/bin/

# Install necessary packages
RUN apt-get update && apt-get install -y \
    wget curl \
    iptables \
    && rm -rf /var/lib/apt/lists/*

# Install Miniconda
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh \
    && bash miniconda.sh -b -p /opt/conda \
    && rm miniconda.sh

# Add conda to path
ENV PATH="/opt/conda/bin:${PATH}"

# Create Conda environment with Python 3.8
RUN conda create -n myenv python=3.8 -y

WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install Python dependencies in the Conda environment
RUN conda run -n myenv pip install --no-cache-dir -r requirements.txt

# Copy the client script
COPY docker_image_client.py .

# Copy tar file
COPY temp-anomaly-detection.tar .

# Generate Sawtooth keys
RUN sawtooth keygen # root keys
RUN sawtooth keygen client # user keys

ENV PRIVATE_KEY_FILE /root/.sawtooth/keys/client.priv

# Set up the environment activation in .bashrc
RUN echo "source activate myenv" >> ~/.bashrc

# Copy the entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Use the entrypoint script
ENTRYPOINT ["docker-entrypoint.sh"]

# Keep the container running
CMD ["tail", "-f", "/dev/null"]