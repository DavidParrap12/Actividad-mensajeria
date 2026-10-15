FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY productor.py relay.py consumidor.py ./
CMD ["python", "-u", "relay.py"]
