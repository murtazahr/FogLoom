FROM python:3.9-slim

WORKDIR /app

# Install dependencies
RUN pip install numpy scikit-learn==1.3.2 joblib flask

# Copy the application file and the model
COPY temp_anomaly_detector.py .
COPY temp_anomaly_model.joblib .

# Expose the port the app runs on
EXPOSE 8080

# Run the application
ENTRYPOINT ["python", "-u", "temp_anomaly_detector.py"]