# Now, use the Sawtooth shell image as the main base
FROM hyperledger/sawtooth-shell:chime as sawtooth-shell

# Generate Sawtooth keys
RUN sawtooth keygen # root keys
RUN sawtooth keygen client # user keys

# Start with the Docker-in-Docker image
FROM docker:dind

# Copy Docker binaries from dind-base
COPY --from=sawtooth-shell /root/.sawtooth /root/.sawtooth

# Install dependencies
RUN apk add --no-cache \
    build-base \
    libffi-dev \
    openssl-dev \
    bzip2-dev \
    zlib-dev \
    xz-dev \
    readline-dev \
    sqlite-dev \
    tk-dev \
    wget

# Download and install Python 3.8
RUN wget https://www.python.org/ftp/python/3.8.16/Python-3.8.16.tgz \
    && tar xzf Python-3.8.16.tgz \
    && cd Python-3.8.16 \
    && ./configure --enable-optimizations \
    && make altinstall \
    && cd .. \
    && rm -rf Python-3.8.16 Python-3.8.16.tgz

# Set up symbolic links
RUN ln -s /usr/local/bin/python3.8 /usr/local/bin/python3 \
    && ln -s /usr/local/bin/pip3.8 /usr/local/bin/pip3

# Verify installation
RUN python3 --version && pip3 --version

# Clean up
RUN apk del build-base

WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install Python dependencies in the Conda environment
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy the client script
COPY docker_image_client.py .

# Copy tar file
COPY temp-anomaly-detection.tar .

ENV PRIVATE_KEY_FILE /root/.sawtooth/keys/client.priv

# Copy the entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Use the entrypoint script
ENTRYPOINT ["docker-entrypoint.sh"]

# Keep the container running
CMD ["tail", "-f", "/dev/null"]