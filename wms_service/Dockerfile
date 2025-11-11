FROM python:3.12-slim

WORKDIR /app

# Requirements (du kannst das direkt inline machen)
RUN pip install pika

# Kopiere alle Python-Dateien in den Container
COPY . .

# Standardbefehl: starte den Empfänger
CMD ["python", "-m", "wms_service.receive"]
