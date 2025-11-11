import json
import os
import logging
import pika

logger = logging.getLogger()


def send_log_message(service: str, event: str, message: str):
    """Sendet Log-Nachrichten an RabbitMQ."""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
        channel = connection.channel()

        channel.exchange_declare(exchange="event_log", exchange_type="topic")

        payload = {
            "service": service,
            "event": event,
            "message": message
        }

        channel.basic_publish(
            exchange="event_log",
            routing_key=f"log.{service}",
            body=json.dumps(payload)
        )
        logging.info(f" Sent log message: {payload}")
        connection.close()

    except Exception as e:
        logging.error(f"Failed to send log message: {e}")


def send_wms_message(order: dict):
    """Sendet item_picked-Nachricht an RabbitMQ."""

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
        channel = connection.channel()

        channel.exchange_declare(exchange='wms_event', exchange_type='topic')
        payload = {
            "order": order
        }

        channel.basic_publish(
            exchange='wms_event',
            routing_key='order.wms',
            body=json.dumps(payload)
        )
        print(f" [x] Sent {payload}")
        connection.close()

    except Exception as e:
        print(f" [!] Failed to send message: {e}")
