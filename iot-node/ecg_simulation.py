import argparse
import logging
import time
import pandas as pd
import numpy as np
from itertools import cycle

from transaction_initiator.transaction_initiator import transaction_creator
from response_manager.response_manager import IoTDeviceManager

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
SAMPLE_RATE = 250  # 250 Hz
N_SECONDS = 30  # Generate and send data every 30 seconds
WINDOW_SIZE = 10  # 10 seconds
OVERLAP = 5  # 5 seconds overlap
CSV_FILE_PATH = 'datasets/ecg.csv'


class ECGSimulator:
    def __init__(self, csv_file_path):
        self.df = pd.read_csv(csv_file_path)
        self.ecg_data = self.df.iloc[:, :140].values  # Assuming the first 140 columns are ECG data
        self.labels = self.df.iloc[:, 140].values  # Assuming the last column is the label
        self.data_cycle = cycle(zip(self.ecg_data, self.labels))
        self.current_time = 0

    def get_next_ecg(self):
        ecg, label = next(self.data_cycle)
        return ecg, label

    def generate_ecg_data(self, duration):
        num_points = int(duration * SAMPLE_RATE)
        data = []
        labels = []

        while len(data) < num_points:
            ecg, label = self.get_next_ecg()
            data.extend(ecg)
            labels.extend([label] * len(ecg))

        data = data[:num_points]
        labels = labels[:num_points]

        time_array = np.arange(num_points) / SAMPLE_RATE + self.current_time
        self.current_time += duration

        return list(zip(time_array.tolist(), data)), labels


def process_window(window_data, window_labels):
    times, values = zip(*window_data)
    return {
        "start_time": times[0],
        "end_time": times[-1],
        "values": list(values),
        "mean_value": float(np.mean(values)),
        "max_value": float(np.max(values)),
        "min_value": float(np.min(values)),
        "std_value": float(np.std(values)),
        "label": int(np.mean(window_labels))  # Use majority vote for the window label
    }


def continuous_ecg_simulation(workflow_id):
    logger.info(f"Starting continuous ECG simulation for workflow ID: {workflow_id}")
    simulator = ECGSimulator(CSV_FILE_PATH)

    while True:
        try:
            ecg_data, labels = simulator.generate_ecg_data(N_SECONDS + WINDOW_SIZE)

            processed_windows = []
            for i in range(0, len(ecg_data) - WINDOW_SIZE * SAMPLE_RATE, OVERLAP * SAMPLE_RATE):
                window = ecg_data[i:i + WINDOW_SIZE * SAMPLE_RATE]
                window_labels = labels[i:i + WINDOW_SIZE * SAMPLE_RATE]
                processed_window = process_window(window, window_labels)
                processed_windows.append(processed_window)

            # Send data to transaction creator
            schedule_id = transaction_creator.create_and_send_transactions(processed_windows, workflow_id,
                                                                           iot_device_manager.port,
                                                                           iot_device_manager.public_key)
            logger.info(f"Data sent to blockchain. Schedule ID: {schedule_id}")

            time.sleep(N_SECONDS)  # Wait before generating next batch of data

        except Exception as ex:
            logger.error(f"Error in ECG simulation: {str(ex)}")
            iot_device_manager.stop()
            time.sleep(5)  # Wait a bit before retrying in case of error


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Continuously generate ECG data from CSV and send to Sawtooth blockchain.')
    parser.add_argument('workflow_id', help='Workflow ID for the ECG monitoring process')
    args = parser.parse_args()

    # initiate IOTDeviceManager
    iot_device_manager = IoTDeviceManager(5555)
    iot_device_manager.start()

    continuous_ecg_simulation(args.workflow_id)
