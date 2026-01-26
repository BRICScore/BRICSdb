FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ .

CMD ["uvicorn", "main:app",
     "--host", "0.0.0.0",
     "--port", "443",
     "--ssl-certfile", "/certs/server.crt",
     "--ssl-keyfile", "/certs/server.key"]