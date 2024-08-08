import sys
import json
import joblib
import numpy as np
from datetime import datetime
import warnings
from sklearn.exceptions import InconsistentVersionWarning

warnings.filterwarnings("ignore", category=InconsistentVersionWarning)

# Load the pre-trained model
model = joblib.load('temp_anomaly_model.joblib')


def detect_anomaly(data):
    # Extract features
    temperature = data['temperature']
    humidity = data['humidity']
    timestamp = datetime.fromisoformat(data['timestamp'])
    hour = timestamp.hour

    # Create feature array
    features = np.array([temperature, humidity, hour]).reshape(1, -3)

    # Predict anomaly
    is_anomaly = model.predict(features)[0] == -1
    anomaly_score = model.score_samples(features)[0]

    return {
        'timestamp': data['timestamp'],
        'temperature': temperature,
        'humidity': humidity,
        'is_anomaly': bool(is_anomaly),
        'anomaly_score': float(anomaly_score)
    }


def main():
    for line in sys.stdin:
        try:
            data = json.loads(line)
            result = detect_anomaly(data)
            print(json.dumps(result))
            sys.stdout.flush()
        except json.JSONDecodeError:
            print(json.dumps({"error": "Invalid JSON input"}))
        except KeyError as e:
            print(json.dumps({"error": f"Missing required field: {str(e)}"}))
        except Exception as e:
            print(json.dumps({"error": f"Unexpected error: {str(e)}"}))


if __name__ == "__main__":
    main()
