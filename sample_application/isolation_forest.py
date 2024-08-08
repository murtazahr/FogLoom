from sklearn.ensemble import IsolationForest
import numpy as np
import joblib

# Create a synthetic dataset (for demonstration purposes)
np.random.seed(42)
n_samples = 1000
temperatures = np.random.normal(22, 5, n_samples)
humidities = np.random.normal(50, 10, n_samples)
hours = np.random.randint(0, 24, n_samples)

X = np.column_stack((temperatures, humidities, hours))

# Train an Isolation Forest
model = IsolationForest(contamination=0.1, random_state=42)
model.fit(X)

# Save the model
joblib.dump(model, 'temp_anomaly_model.joblib')
