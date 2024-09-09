FROM python:3.8

# Install Docker
RUN apt-get update && apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

# Verify installations
RUN python --version

WORKDIR /app

# Copy the requirements file
COPY requirements.txt .

# Install packages from requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the client script
COPY scheduler.py .
COPY app_requirements.json .
COPY dependency_graph.json .
COPY iot_data.json .

# Keep the container running
CMD ["tail", "-f", "/dev/null"]