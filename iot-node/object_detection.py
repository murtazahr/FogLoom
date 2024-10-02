import argparse
import logging
import base64
from time import sleep

from transaction_initiator.transaction_initiator import transaction_creator
from response_manager.response_manager import IoTDeviceManager

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def encode_image(image_path):
    try:
        with open(image_path, "rb") as image_file:
            return base64.b64encode(image_file.read()).decode('utf-8')
    except Exception as e:
        logger.error(f"Error encoding image {image_path}: {str(e)}")
        return None


def process_images(workflow_id, image_paths):
    logger.info(f"Processing images for workflow ID: {workflow_id}")
    encoded_images = []

    for image_path in image_paths:
        encoded_image = encode_image(image_path)
        if encoded_image:
            encoded_images.append(encoded_image)

    if not encoded_images:
        logger.error("No images were successfully encoded.")
        return

    try:
        # Send data to transaction creator
        schedule_id = transaction_creator.create_and_send_transactions(
            encoded_images,
            workflow_id,
            iot_device_manager.port,
            iot_device_manager.public_key
        )
        logger.info(f"Data sent to blockchain. Schedule ID: {schedule_id}")
        sleep(20)
    except Exception as ex:
        logger.error(f"Error in image processing: {str(ex)}")
    finally:
        iot_device_manager.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process multiple images and send to Sawtooth blockchain.')
    parser.add_argument('workflow_id', help='Workflow ID for the image processing')
    parser.add_argument('images', nargs='+', help='Paths to the images to process')
    args = parser.parse_args()

    # Initialize IoTDeviceManager
    iot_device_manager = IoTDeviceManager(5556)
    iot_device_manager.start()

    process_images(args.workflow_id, args.images)
