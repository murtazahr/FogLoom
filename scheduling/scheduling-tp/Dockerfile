# Now, use the Sawtooth shell image as the main base
FROM hyperledger/sawtooth-shell:chime AS sawtooth-shell

# Generate Sawtooth keys
RUN sawtooth keygen # root keys
RUN sawtooth keygen client # user keys

FROM python:3.8

# Copy Sawtooth keys from shell
COPY --from=sawtooth-shell /root/.sawtooth /root/.sawtooth

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV SAWTOOTH_PRIVATE_KEY /root/.sawtooth/keys/client.priv

# Copy the transaction processor script
COPY scheduling_tp.py .
COPY scheduler.py .
COPY blockchain_task_status_updater.py .

CMD ["python", "scheduling_tp.py"]