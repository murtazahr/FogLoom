import json
import base64
import cv2
import numpy as np
import asyncio
import logging

logging.basicConfig(level=logging.DEBUG)  # Change to DEBUG for more detailed logs
logger = logging.getLogger(__name__)


def draw_boxes(image, detections):
    for det in detections:
        bbox = det['bbox']
        x1, y1, x2, y2 = map(int, bbox)
        class_name = det['class']
        confidence = det['confidence']

        # Draw bounding box
        cv2.rectangle(image, (x1, y1), (x2, y2), (0, 255, 0), 2)

        # Prepare label
        label = f"{class_name}: {confidence:.2f}"

        # Get text size
        (text_width, text_height), _ = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 1)

        # Draw filled rectangle for text background
        cv2.rectangle(image, (x1, y1 - text_height - 5), (x1 + text_width, y1), (0, 255, 0), -1)

        # Put text on the image
        cv2.putText(image, label, (x1, y1 - 5), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 0), 1)

    return image


def process_results(results):
    output_images = []

    for result in results:
        try:
            # Check if 'original_image' and 'detections' keys exist
            if 'original_image' not in result or 'detections' not in result:
                logger.error(f"Missing 'original_image' or 'detections' in result: {result}")
                continue

            # Decode base64 image
            image_data = base64.b64decode(result['original_image'])
            nparr = np.frombuffer(image_data, np.uint8)
            image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

            if image is None:
                logger.error("Failed to decode image")
                continue

            # Draw bounding boxes
            image_with_boxes = draw_boxes(image, result['detections'])

            # Encode the image with bounding boxes to base64
            _, buffer = cv2.imencode('.jpg', image_with_boxes)
            output_base64 = base64.b64encode(buffer).decode('utf-8')

            output_images.append(output_base64)
        except Exception as e:
            logger.error(f"Error processing result: {e}")
            logger.error(f"Problematic result: {result}")

    return output_images


async def read_json(reader):
    buffer = b""
    while True:
        chunk = await reader.read(8192)  # Read in 8KB chunks
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

        if 'data' not in input_data or not isinstance(input_data['data'], list):
            raise ValueError("'data' field (as a list) is required in the JSON input")

        output_images = process_results(input_data['data'])

        output = {"data": output_images}
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
