FROM python:3.9-slim

WORKDIR /app

RUN pip install numpy scikit-learn==1.3.2 joblib

COPY temp_anomaly_detector.py .
COPY temp_anomaly_model.joblib .

CMD ["python", "temp_anomaly_detector.py"]