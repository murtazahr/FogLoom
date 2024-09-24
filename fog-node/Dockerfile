# Now, use the Sawtooth shell image as the main base
FROM hyperledger/sawtooth-shell:chime AS sawtooth-shell

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
    procps \
    gnupg \
    lsb-release
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
RUN echo \
  "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian \
  $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
RUN apt-get update && apt-get install -y docker-ce docker-ce-cli containerd.io

# Verify installations
RUN python --version
RUN docker --version

WORKDIR /app

# Install Python Dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV SAWTOOTH_PRIVATE_KEY /root/.sawtooth/keys/client.priv

# Copy the event handler scripts
COPY event_handlers/* ./event_handlers/

# Copy scheduler scripts
COPY scheduled/* ./scheduled/

# Copy services
COPY services/* ./services/

# Copy the task executor script
COPY task_executor ./task_executor

# Copy the entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Use the entrypoint script
ENTRYPOINT ["docker-entrypoint.sh"]
CMD sh -c "for file in event_handlers/*.py; do python \"\$file\" & done; for file in scheduled/*.py; do python \"\$file\" & done; for file in task_executor/*.py; do python \"\$file\" & done; for file in services/*.py; do python \"\$file\" & done; wait"

