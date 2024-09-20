import json
import numpy as np
import asyncio
import logging
import traceback
import cv2
import matplotlib.pyplot as plt
import os
from tflite_runtime.interpreter import Interpreter

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load the TFLite model
try:
    interpreter = Interpreter(model_path="ecg_arrhythmia_model_quant.tflite")
    interpreter.allocate_tensors()

    # Get input and output tensor details
    input_details = interpreter.get_input_details()
    output_details = interpreter.get_output_details()

    logger.info("Successfully loaded the TFLite ECG Arrhythmia classification model")
except Exception as e:
    logger.error(f"Failed to load the model: {str(e)}")
    raise

# Define arrhythmia classes
ARRHYTHMIA_CLASSES = ['APC', 'Normal', 'LBB', 'PAB', 'PVC', 'RBB', 'VEB']


def signal_to_image(signal):
    fig = plt.figure(frameon=False)
    plt.plot(signal)
    plt.xticks([]), plt.yticks([])
    for spine in plt.gca().spines.values():
        spine.set_visible(False)

    filename = 'temp.png'
    fig.savefig(filename)
    plt.close(fig)

    im_gray = cv2.imread(filename, cv2.IMREAD_GRAYSCALE)
    im_gray = cv2.resize(im_gray, (128, 128), interpolation=cv2.INTER_LANCZOS4)
    os.remove(filename)
    return im_gray


def cropping(image):
    crops = []
    crop_positions = [
        (0, 0), (0, 16), (0, 32),
        (16, 0), (16, 16), (16, 32),
        (32, 0), (32, 16), (32, 32)
    ]

    for top, left in crop_positions:
        crop = image[top:top + 96, left:left + 96]
        crop = cv2.resize(crop, (128, 128))
        crops.append(crop)

    return crops


def detect_arrhythmias(window_data):
    try:
        signal = np.array(window_data['values'])
        image = signal_to_image(signal)
        crops = cropping(image)

        predictions = []
        for crop in crops:
            # Convert grayscale to RGB by repeating the channel 3 times
            crop_rgb = np.repeat(crop[:, :, np.newaxis], 3, axis=2)

            # Ensure the image is in float format and normalized
            crop_rgb = crop_rgb.astype(np.float32) / 255.0

            # Reshape the input to match the model's expected input shape
            input_shape = input_details[0]['shape']
            input_data = crop_rgb.reshape(input_shape)

            # Set the input tensor
            interpreter.set_tensor(input_details[0]['index'], input_data)

            # Run inference
            interpreter.invoke()

            # Get the output tensor
            output_data = interpreter.get_tensor(output_details[0]['index'])
            predictions.append(output_data[0])

        avg_prediction = np.mean(predictions, axis=0)
        predicted_class = np.argmax(avg_prediction)
        probability = float(avg_prediction[predicted_class])

        arrhythmia = {
            "type": ARRHYTHMIA_CLASSES[predicted_class],
            "probability": probability,
            "time": window_data['start_time']
        }

        logger.debug(f"Detected arrhythmia: {arrhythmia}")
        return arrhythmia

    except Exception as e:
        logger.error(f"Error in detect_arrhythmias: {str(e)}")
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
        for i, window in enumerate(input_data['data']):
            logger.debug(f"Processing window {i + 1}/{len(input_data['data'])}")
            arrhythmia = detect_arrhythmias(window)
            if arrhythmia is not None:
                results.append({
                    'window_start_time': window['start_time'],
                    'window_end_time': window['end_time'],
                    'arrhythmia': arrhythmia
                })

        output = {
            "data": results
        }
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
