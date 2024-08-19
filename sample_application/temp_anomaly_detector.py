import json
import joblib
import numpy as np
from datetime import datetime
import warnings
from sklearn.exceptions import InconsistentVersionWarning
from flask import Flask, request, jsonify

warnings.filterwarnings("ignore", category=InconsistentVersionWarning)

# Load the pre-trained model
model = joblib.load('temp_anomaly_model.joblib')

app = Flask(__name__)


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


@app.route('/detect_anomaly', methods=['POST'])
def anomaly_detection():
    try:
        data = request.json
        result = detect_anomaly(data)
        return jsonify(result)
    except KeyError as e:
        return jsonify({"error": f"Missing required field: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": f"Unexpected error: {str(e)}"}), 500


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)
