FROM python:3.8

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libssl-dev && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the transaction processor script
COPY scheduling_tp.py .
COPY scheduler.py .

CMD ["python", "scheduling_tp.py"]