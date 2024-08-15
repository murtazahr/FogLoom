FROM hyperledger/sawtooth-shell:chime

# Install necessary packages
RUN apt-get update && apt-get install -y \
    wget \
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
RUN apt-get update && apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg-agent \
    software-properties-common \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add - \
    && add-apt-repository \
       "deb [arch=amd64] https://download.docker.com/linux/debian \
       $(lsb_release -cs) \
       stable" \
    && apt-get update \
    && apt-get install -y docker-ce-cli \
    && rm -rf /var/lib/apt/lists/*

# Copy the client script
COPY docker_image_client.py .

# Copy tar file
COPY temp-anomaly-detection.tar .

# Generate Sawtooth keys
RUN sawtooth keygen

ENV SAWTOOTH_PRIVATE_KEY /root/.sawtooth/keys/root.priv

# Set up the environment activation in .bashrc
RUN echo "source activate myenv" >> ~/.bashrc

# Use a simple entrypoint that keeps the container running
CMD ["tail", "-f", "/dev/null"]