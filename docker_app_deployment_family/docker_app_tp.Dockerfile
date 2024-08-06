FROM python:3.8-slim-buster

RUN apt-get update \
 && apt-get install -y \
    pkg-config \
    build-essential \
    libzmq3-dev \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

RUN pip install \
    sawtooth-sdk==1.2.5 \
    cbor==1.0.0 \
    colorlog \
    protobuf==3.20.0

WORKDIR /app

COPY docker_app_tp.py .

CMD ["python3", "docker_app_tp.py"]