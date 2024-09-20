import argparse
import logging
import time

import numpy as np

from transaction_initiator.transaction_initiator import transaction_creator

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
SAMPLE_RATE = 250  # 250 Hz
N_SECONDS = 30  # Generate and send data every 30 seconds
WINDOW_SIZE = 10  # 10 seconds
OVERLAP = 5  # 5 seconds overlap


class ECGSimulator:
    def __init__(self):
        self.t = 0

    def generate_baseline_wander(self, n_samples):
        return 0.1 * np.sin(2 * np.pi * 0.05 * np.arange(n_samples) / SAMPLE_RATE)

    def generate_normal_beat(self, n_samples):
        t = np.arange(n_samples) / SAMPLE_RATE
        p_wave = 0.15 * np.sin(2 * np.pi * 1 * t) * np.exp(-(t - 0.2) ** 2 / 0.01)
        qrs_complex = np.zeros_like(t)
        qrs_complex[int(0.3 * SAMPLE_RATE):int(0.4 * SAMPLE_RATE)] = -0.5
        qrs_complex[int(0.4 * SAMPLE_RATE):int(0.45 * SAMPLE_RATE)] = 1.5
        qrs_complex[int(0.45 * SAMPLE_RATE):int(0.5 * SAMPLE_RATE)] = -0.5
        t_wave = 0.3 * np.sin(2 * np.pi * 0.7 * t) * np.exp(-(t - 0.7) ** 2 / 0.04)
        return p_wave + qrs_complex + t_wave

    def generate_pvc(self, n_samples):
        t = np.arange(n_samples) / SAMPLE_RATE
        pvc = np.zeros_like(t)
        pvc[int(0.4 * SAMPLE_RATE):int(0.5 * SAMPLE_RATE)] = -1.5
        pvc[int(0.5 * SAMPLE_RATE):int(0.55 * SAMPLE_RATE)] = 2.5
        pvc[int(0.55 * SAMPLE_RATE):int(0.65 * SAMPLE_RATE)] = -1.5
        return pvc

    def generate_ecg_data(self, duration):
        n_samples = int(duration * SAMPLE_RATE)
        ecg_data = np.zeros(n_samples)
        baseline = self.generate_baseline_wander(n_samples)

        beat_duration = int(0.8 * SAMPLE_RATE)
        n_beats = n_samples // beat_duration

        for i in range(n_beats):
            start = i * beat_duration
            end = start + beat_duration
            if np.random.random() < 0.1:  # 10% chance of PVC
                ecg_data[start:end] += self.generate_pvc(beat_duration)
            else:
                ecg_data[start:end] += self.generate_normal_beat(beat_duration)

        ecg_data += baseline
        ecg_data += np.random.normal(0, 0.01, n_samples)  # Add some noise

        time_array = np.arange(n_samples) / SAMPLE_RATE + self.t
        self.t += duration
        return list(zip(time_array.tolist(), ecg_data.tolist()))


def process_window(window_data):
    times, values = zip(*window_data)
    return {
        "start_time": times[0],
        "end_time": times[-1],
        "mean_value": float(np.mean(values)),
        "max_value": float(np.max(values)),
        "min_value": float(np.min(values)),
        "std_value": float(np.std(values))
    }


def continuous_ecg_simulation(workflow_id):
    logger.info(f"Starting continuous ECG simulation for workflow ID: {workflow_id}")
    simulator = ECGSimulator()

    while True:
        try:
            ecg_data = simulator.generate_ecg_data(N_SECONDS + WINDOW_SIZE)

            processed_windows = []
            for i in range(0, len(ecg_data) - WINDOW_SIZE * SAMPLE_RATE, OVERLAP * SAMPLE_RATE):
                window = ecg_data[i:i + WINDOW_SIZE * SAMPLE_RATE]
                processed_window = process_window(window)
                processed_windows.append(processed_window)

            # Prepare payload for transaction
            payload = {
                "iot_data": processed_windows,
                "workflow_id": workflow_id
            }

            # Send data to transaction creator
            schedule_id = transaction_creator.create_and_send_transaction(payload)
            logger.info(f"Data sent to blockchain. Schedule ID: {schedule_id}")

            time.sleep(N_SECONDS)  # Wait before generating next batch of data

        except Exception as ex:
            logger.error(f"Error in ECG simulation: {str(ex)}")
            time.sleep(5)  # Wait a bit before retrying in case of error


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Continuously generate ECG data and send to Sawtooth blockchain.')
    parser.add_argument('workflow_id', help='Workflow ID for the ECG monitoring process')
    args = parser.parse_args()

    continuous_ecg_simulation(args.workflow_id)
