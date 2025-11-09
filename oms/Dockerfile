FROM python:3.12-slim

WORKDIR /app

# Abhängigkeiten installieren
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 👇 gRPC Python-Code aus der Proto-Datei generieren
RUN python -m grpc_tools.protoc \
    -I./oms/contracts \
    --python_out=./oms/contracts \
    --grpc_python_out=./oms/contracts \
    ./oms/contracts/inventory.proto

EXPOSE 8000

CMD ["uvicorn", "oms.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
