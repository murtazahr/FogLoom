import joblib
import numpy as np
from datetime import datetime
import warnings
import logging
from sklearn.exceptions import InconsistentVersionWarning
from flask import Flask, request, jsonify

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore", category=InconsistentVersionWarning)

# Load the pre-trained model
try:
    logger.info("Loading model from temp_anomaly_model.joblib")
    model = joblib.load('temp_anomaly_model.joblib')
    logger.info("Model loaded successfully")
except Exception as e:
    logger.error(f"Failed to load model: {str(e)}")
    raise

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
        logger.info(f"Received data: {data}")
        result = detect_anomaly(data)
        logger.info(f"Detection result: {result}")
        return jsonify(result)
    except KeyError as err:
        logger.error(f"Missing required field: {str(err)}")
        return jsonify({"error": f"Missing required field: {str(err)}"}), 400
    except Exception as err:
        logger.error(f"Unexpected error: {str(err)}")
        return jsonify({"error": f"Unexpected error: {str(err)}"}), 500


@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200


if __name__ == "__main__":
    logger.info("Starting Flask server")
    app.run(host='0.0.0.0', port=8080, debug=True)
