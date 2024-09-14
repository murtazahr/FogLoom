import json
import cv2
import numpy as np
import base64
from ultralytics import YOLO
import asyncio
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Load the YOLO model globally
model = YOLO('yolov8n.pt')  # Load the nano model


def process_image(base64_image):
    # Decode base64 image
    image_data = base64.b64decode(base64_image)
    nparr = np.frombuffer(image_data, np.uint8)
    image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

    if image is None:
        logger.error("Unable to decode image")
        return None

    # Perform object detection
    results = model(image)

    # Process results
    detections = []
    for r in results:
        boxes = r.boxes
        for box in boxes:
            x1, y1, x2, y2 = box.xyxy[0]
            confidence = box.conf[0]
            class_id = box.cls[0]
            class_name = model.names[int(class_id)]

            detections.append({
                "class": class_name,
                "confidence": float(confidence),
                "bbox": [float(x1), float(y1), float(x2), float(y2)]
            })

    return detections


async def read_json(reader):
    buffer = b""
    while True:
        chunk = await reader.read(4096)  # Read in chunks
        if not chunk:
            raise ValueError("Connection closed before receiving a valid JSON object.")
        buffer += chunk
        try:
            return json.loads(buffer.decode())
        except json.JSONDecodeError:
            # If it's not yet a valid JSON, continue reading
            continue


async def handle_client(reader, writer):
    try:
        logger.info("New client connected")
        input_data = await read_json(reader)

        logger.debug(f"Received JSON data of length: {len(json.dumps(input_data))} bytes")

        if 'data' not in input_data or not isinstance(input_data['data'], list):
            raise ValueError("'data' field (as a list) is required in the JSON input")

        results = []
        for base64_image in input_data['data']:
            result = {'original_image': base64_image, 'detections': process_image(base64_image)}
            if result is not None:
                results.append(result)

        output = {"data": results}
        output_json = json.dumps(output)
        writer.write(output_json.encode())
        await writer.drain()
        logger.info("Processed request successfully")
    except ValueError as ve:
        logger.error(str(ve))
        error_msg = json.dumps({"error": str(ve)})
        writer.write(error_msg.encode())
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        error_msg = json.dumps({"error": f"An internal error occurred: {str(e)}"})
        writer.write(error_msg.encode())
    finally:
        await writer.drain()
        writer.close()
        logger.info("Connection closed")


async def main():
    server = await asyncio.start_server(handle_client, '0.0.0.0', 12345)

    addr = server.sockets[0].getsockname()
    logger.info(f'Serving on {addr}')

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
