FROM python:3.10-slim
WORKDIR /app
COPY consume.py .
RUN pip install pika
CMD ["python", "consume.py"]
