FROM python:3.8
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY event-handler/event_handler.py .
CMD ["python", "event_handler.py"]