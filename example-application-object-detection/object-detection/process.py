import json
import cv2
import numpy as np
import base64
import asyncio
import logging
import onnx
import traceback

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load the YOLO model globally
model_path = "yolov8n.onnx"
try:
    net = cv2.dnn.readNet(model_path)
    logger.info(f"Successfully loaded model from {model_path}")
except Exception as e:
    logger.error(f"Failed to load model: {str(e)}")
    raise

# Extract class names from ONNX model
try:
    onnx_model = onnx.load(model_path)
    metadata = onnx_model.metadata_props
    for prop in metadata:
        if prop.key == 'names':
            class_names = eval(prop.value)
            logger.info(f"Successfully extracted {len(class_names)} class names from model metadata")
            break
    else:
        raise ValueError("Class names not found in ONNX model metadata")
except Exception as e:
    logger.error(f"Failed to extract class names: {str(e)}")
    raise


def process_image(base64_image):
    try:
        # Decode base64 image
        image_data = base64.b64decode(base64_image)
        nparr = np.frombuffer(image_data, np.uint8)
        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if image is None:
            logger.error("Unable to decode image")
            return None

        logger.debug(f"Successfully decoded image, shape: {image.shape}")

        # Prepare image for inference
        input_height, input_width = 640, 640
        blob = cv2.dnn.blobFromImage(image, 1 / 255.0, (input_width, input_height), swapRB=True, crop=False)
        net.setInput(blob)

        # Perform inference
        outputs = net.forward(net.getUnconnectedOutLayersNames())[0]
        logger.debug(f"Model output shape: {outputs.shape}")

        # Process results
        outputs = outputs.transpose((1, 2, 0))
        rows = outputs.shape[1]
        boxes = []
        scores = []
        class_ids = []

        image_height, image_width = image.shape[:2]
        x_factor = image_width / input_width
        y_factor = image_height / input_height

        for i in range(rows):
            classes_scores = outputs[4:, i, 0]
            max_score = np.amax(classes_scores)
            if max_score >= 0.5:
                class_id = np.argmax(classes_scores)
                x, y, w, h = outputs[0:4, i, 0]
                left = int((x - 0.5 * w) * x_factor)
                top = int((y - 0.5 * h) * y_factor)
                width = int(w * x_factor)
                height = int(h * y_factor)
                box = np.array([left, top, width, height])
                boxes.append(box)
                scores.append(max_score)
                class_ids.append(class_id)

        # Apply NMS
        indices = cv2.dnn.NMSBoxes(boxes, scores, 0.1, 0.5)

        detections = []
        for i in indices:
            box = boxes[i]
            left, top, width, height = box
            detections.append({
                "class": class_names[class_ids[i]],
                "confidence": float(scores[i]),
                "bbox": [float(left), float(top), float(left + width), float(top + height)]
            })

        logger.debug(f"Processed image, found {len(detections)} detections")
        logger.debug(detections)
        return detections

    except Exception as e:
        logger.error(f"Error in process_image: {str(e)}")
        logger.error(traceback.format_exc())
        return None


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
        for i, base64_image in enumerate(input_data['data']):
            logger.debug(f"Processing image {i + 1}/{len(input_data['data'])}")
            result = process_image(base64_image)
            if result is not None:
                results.append({'original_image': base64_image, 'detections': result})

        output = {"data": results}
        output_json = json.dumps(output)
        writer.write(output_json.encode())
        await writer.drain()
        logger.info(f"Processed request successfully, returned {len(results)} results")
    except ValueError as ve:
        logger.error(str(ve))
        error_msg = json.dumps({"error": str(ve)})
        writer.write(error_msg.encode())
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        logger.error(traceback.format_exc())
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
