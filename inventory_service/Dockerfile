# --- Basis-Image ---
FROM python:3.12-slim

# --- Arbeitsverzeichnis im Container ---
WORKDIR /app

# --- Abhängigkeiten kopieren & installieren ---
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- Source Code kopieren ---
COPY . .

# --- gRPC-Code beim Build automatisch generieren (optional, aber praktisch) ---
RUN python -m grpc_tools.protoc \
    -I./proto \
    --python_out=./server \
    --grpc_python_out=./server \
    ./proto/inventory.proto

# --- Exponiere den Port (gRPC läuft typischerweise auf 50051) ---
EXPOSE 50051

ENV PYTHONPATH=/app/server

# --- Starte den Service ---
CMD ["python", "-m", "server.inventory_server"]
