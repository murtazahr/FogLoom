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
RUN apk add --no-cache bash wget ca-certificates

# Install dependencies
# Install Miniforge
RUN mkdir -p ~/miniconda3 && \
    wget https://repo.anaconda.com/miniconda/Miniconda3-py39_24.5.0-0-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh && \
    bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3 && \
    rm -rf ~/miniconda3/miniconda.sh

# Add conda to path
RUN ~/miniconda3/bin/conda init bash && \
    ~/miniconda3/bin/conda init zsh && exec $SHELL

# Create a new conda environment with Python 3.8
RUN conda create -n myenv python=3.8 -y && \
    conda clean -afy

WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install packages from requirements.txt
RUN /bin/bash -c "source activate myenv && \
    pip install -r requirements.txt && \
    conda clean -afy"

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