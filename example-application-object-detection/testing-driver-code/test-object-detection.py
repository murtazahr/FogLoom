import asyncio
import json
import base64
import sys
import os


async def send_image_and_process(host1, port1, host2, port2, image_path):
    try:
        # Stage 1: Object Detection
        with open(image_path, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode('utf-8')

        payload = {
            "data": [encoded_string]
        }

        # Connect to the first server (object detection)
        reader1, writer1 = await asyncio.open_connection(host1, port1)
        print(f"Connected to object detection server at {host1}:{port1}")

        # Send the JSON payload
        writer1.write(json.dumps(payload).encode())
        await writer1.drain()

        print("Sent image to object detection server, waiting for response...")

        # Read the response
        data1 = await reader1.read()
        response1 = json.loads(data1.decode())

        writer1.close()
        await writer1.wait_closed()

        print("Received response from object detection server")

        # Stage 2: Bounding Box Drawing
        # Connect to the second server (bounding box drawing)
        reader2, writer2 = await asyncio.open_connection(host2, port2)
        print(f"Connected to bounding box server at {host2}:{port2}")

        # Send the response from the first server to the second server
        writer2.write(json.dumps(response1).encode())
        await writer2.drain()

        print("Sent detection results to bounding box server, waiting for response...")

        # Read the response
        data2 = await reader2.read()
        response2 = json.loads(data2.decode())

        writer2.close()
        await writer2.wait_closed()

        print("Received response from bounding box server")

        # Process and save the images
        output_images = response2.get('data', [])
        for i, img_base64 in enumerate(output_images):
            img_data = base64.b64decode(img_base64)
            filename = f"output_image_{i + 1}.jpg"
            with open(filename, "wb") as f:
                f.write(img_data)
            print(f"Saved {filename}")

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response: {e}")
    except ConnectionResetError:
        print("Connection was reset by the server. This might indicate a server-side error.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: python client.py <host1> <port1> <host2> <port2> <image_path>")
        sys.exit(1)

    host1, port1 = sys.argv[1], int(sys.argv[2])
    host2, port2 = sys.argv[3], int(sys.argv[4])
    image_path = sys.argv[5]

    asyncio.run(send_image_and_process(host1, port1, host2, port2, image_path))
