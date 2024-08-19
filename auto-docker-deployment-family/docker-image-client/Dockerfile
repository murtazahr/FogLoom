# Now, use the Sawtooth shell image as the main base
FROM hyperledger/sawtooth-shell:chime as sawtooth-shell

# Generate Sawtooth keys
RUN sawtooth keygen # root keys
RUN sawtooth keygen client # user keys

# Start with the Docker-in-Docker image
FROM docker:dind

# Copy Docker binaries from dind-base
COPY --from=sawtooth-shell /root/.sawtooth /root/.sawtooth

# Install necessary packages
RUN apk add --no-cache \
    wget \
    curl \
    iptables \
    bash \
    libstdc++ \
    glib \
    ca-certificates

# Install Miniconda
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh && \
    bash miniconda.sh -b -p /opt/conda && \
    rm miniconda.sh

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

ENV PRIVATE_KEY_FILE /root/.sawtooth/keys/client.priv

# Set up the environment activation in .bashrc
RUN echo "source /opt/conda/etc/profile.d/conda.sh && conda activate myenv" >> ~/.bashrc

# Copy the entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Use the entrypoint script
ENTRYPOINT ["docker-entrypoint.sh"]

# Keep the container running
CMD ["tail", "-f", "/dev/null"]