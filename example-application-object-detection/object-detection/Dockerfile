# Build stage
FROM python:3.9-slim AS builder

# Set the working directory
WORKDIR /app

# Copy only the requirements file first to leverage Docker cache
COPY requirements.txt .

# Install build dependencies and Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge -y --auto-remove build-essential \
    && rm -rf /var/lib/apt/lists/*

# Runtime stage
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy installed packages from builder stage
COPY --from=builder /usr/local/lib/python3.9/site-packages /usr/local/lib/python3.9/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Install only runtime dependencies
RUN apt-get update && rm -rf /var/lib/apt/lists/*

# Copy only necessary files
COPY process.py .
COPY yolov8n.pt .

# Make port 12345 available to the world outside this container
EXPOSE 12345

# Run process.py when the container launches
CMD ["python", "process.py"]