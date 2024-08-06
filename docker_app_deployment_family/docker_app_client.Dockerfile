FROM hyperledger/sawtooth-shell:chime

RUN apt-get update \
 && apt-get install -y \
    pkg-config \
    build-essential \
    libzmq3-dev \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y python3-pip

COPY docker_app_client.py /usr/local/bin/docker_app
RUN chmod +x /usr/local/bin/docker_app

RUN pip3 install cbor sawtooth-sdk