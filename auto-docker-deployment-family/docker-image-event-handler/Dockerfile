FROM python:3.8

WORKDIR /app

# Install System Dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl gcc libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Docker
RUN apt-get update && apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
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

# Install Python Dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the event handler script
COPY docker_image_event_handler.py .

ENTRYPOINT ["python", "docker_image_event_handler.py"]