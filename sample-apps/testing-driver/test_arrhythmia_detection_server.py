import json
import asyncio
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SERVER_HOST = '127.0.0.1'
SERVER_PORT = 12345


async def send_data_to_server(data):
    reader, writer = await asyncio.open_connection(SERVER_HOST, SERVER_PORT)

    logger.info(f"Sending data to server...")
    writer.write(json.dumps(data).encode())
    await writer.drain()

    logger.info(f"Waiting for response...")
    response = await reader.read()

    writer.close()
    await writer.wait_closed()

    return json.loads(response.decode())


async def main():
    # Read input JSON file
    input_file = 'input_payload.json'
    with open(input_file, 'r') as f:
        input_data = json.load(f)

    logger.info(f"Read input data from {input_file}")

    # Send data to server and get response
    response = await send_data_to_server(input_data)

    # Write response to output JSON file
    output_file = 'output_result.json'
    with open(output_file, 'w') as f:
        json.dump(response, f, indent=2)

    logger.info(f"Wrote output data to {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
