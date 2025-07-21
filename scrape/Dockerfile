FROM python:3.10-slim
WORKDIR /app

COPY scrape.py .
COPY publish.py .
RUN pip install requests beautifulsoup4 pika beautifulsoup4

CMD ["sh", "-c", "python scrape.py && python publish.py"]
