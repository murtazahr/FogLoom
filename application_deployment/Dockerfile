FROM python:3.9-slim-buster

  # Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python

  # Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
build-essential pkg-config \
&& rm -rf /var/lib/apt/lists/*

WORKDIR /app

  # Copy requirements file
COPY requirements.txt .

  # Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

  # Copy project files
COPY . .

  # Compile protobuf
RUN python -m grpc_tools.protoc -I. --python_out=. deployment_tp/payload.proto

  # Command to run the application
CMD ["python", "-m", "deployment_tp"]