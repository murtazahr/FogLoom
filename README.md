# Fogbus v3 - A Hyperledger Sawtooth Based Fog Computing Framework

## Description

Fogbus v3 is a decentralized fog computing framework that offers Byzantine Fault Tolerance with automated docker deployment, pluggable scheduling algorithms, and smart context switching between fog and cloud. This framework is designed to be secure, maintain privacy, support auto-deployment, and handle dynamic scaling and scheduling.

## Features

- Secure and privacy-preserving architecture
- Byzantine Fault Tolerance
- Automated Docker deployment
- Dynamic scaling and scheduling
- Pluggable scheduling algorithms
- Smart context switching between fog and cloud
- Integration with Hyperledger Sawtooth for on-chain communication

## Prerequisites

The framework has been extensively tested on Ubuntu machines but should support macOS and Windows. For the development environment, you'll need:

- Docker
- Docker Compose
- Python 3.8

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/murtazahr/Fogbusv3-Transaction-Processors.git
   cd Fogbusv3-Transaction-Processors
   ```

2. Give execution access to the deployment script:
   ```
   chmod +x deploy-network.sh
   ```

3. Run the deployment script:
   ```
   ./deploy-network.sh
   ```

This will clean up the environment and bring up a test network with 5 Sawtooth nodes and a test application (available in the `example-application` directory).

## Usage

Once the Sawtooth network is running, you can execute the following steps:

1. Access the client container:
   ```
   docker exec -it sawtooth-docker-image-client bash
   ```

2. Run the test application:
   ```
   python docker_image_client.py temp-anomaly-detection.tar
   ```

This will invoke the auto-docker-deployment smart contract and run the Docker application on all 5 peer nodes in the network.

Note: Further functionality for scheduling, scaling, and processing data is currently under development.

## Hyperledger Sawtooth Integration

Fogbus v3 creates a Hyperledger Sawtooth network among all the nodes in the system. All on-chain communication takes place via this network, which uses ZeroMQ (ZMQ) behind the scenes.

## Current Status and Future Work

The framework currently runs using Docker Compose on a single machine. Active development is ongoing to support industry applications and extend functionality.

## Contributing

As this project is in active development, contribution guidelines will be provided in future updates. Stay tuned for more information on how you can contribute to Fogbus v3.

## License

[License information to be added]

## Acknowledgments

[Any acknowledgments or credits to be added]

---

For more information or to report issues, please visit the [GitHub repository](https://github.com/murtazahr/Fogbusv3-Transaction-Processors).
