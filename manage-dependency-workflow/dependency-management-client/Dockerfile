# Now, use the Sawtooth shell image as the main base
FROM hyperledger/sawtooth-shell:chime as sawtooth-shell

# Generate Sawtooth keys
RUN sawtooth keygen # root keys
RUN sawtooth keygen client # user keys

FROM python:3.8

# Copy Sawtooth keys from shell
COPY --from=sawtooth-shell /root/.sawtooth /root/.sawtooth

# Install Docker
RUN apt-get update && apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    nano \
    lsb-release

# Verify installations
RUN python --version

WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install packages from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the client script
COPY dependency_management_client.py .

ENV SAWTOOTH_PRIVATE_KEY /root/.sawtooth/keys/client.priv

# Keep the container running
CMD ["tail", "-f", "/dev/null"]