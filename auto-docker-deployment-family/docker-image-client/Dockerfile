FROM hyperledger/sawtooth-shell:chime

# Install necessary packages
RUN apt-get update && apt-get install -y \
    wget curl \
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

# Install Docker CLI using the official Docker repository
# Install Docker CLI using the official installation script
RUN curl -fsSL https://get.docker.com -o get-docker.sh \
    && sh get-docker.sh \
    && rm get-docker.sh

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

# Use a simple entrypoint that keeps the container running
CMD ["tail", "-f", "/dev/null"]