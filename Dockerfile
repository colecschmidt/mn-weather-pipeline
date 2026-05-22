FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY mn_weather_poller.py .

CMD ["python", "mn_weather_poller.py"]