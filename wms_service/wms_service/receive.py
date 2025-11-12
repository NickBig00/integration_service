import json
import sys
import time

import pika
from wms_service.publisher import publish_message

from wms_service.rabbitmq.message_sender import send_log_message

EXCHANGE_NAME = "wms_event"


def callback(ch, method, properties, body):
    """Wird aufgerufen, wenn eine Nachricht empfangen wird."""
    data = json.loads(body)
    print(f"data: {data}")
    send_log_message("wms", "order_received", f"Received order: {data.get('order')}")
    order_id = json.loads(data.get("order")).get("orderId")
    print("order id :" + order_id)
    publish_message(order_id, "items_picked", f"{order_id}: Picked the ordered items")
    send_log_message("wms", "items_picked", f"Picked items for order {order_id}")
    time.sleep(5)
    publish_message(order_id, "order_packed", f"{order_id}: Packed the complete order. Ready for shipping")
    send_log_message("wms", "order_packed", f"Packed order {order_id}")
    time.sleep(5)
    publish_message(order_id, "order_shipped", f"{order_id}: Order was shipped.")
    send_log_message("wms", "order_shipped", f"Shipped order {order_id}")


def connect_to_rabbitmq(max_retries=10, delay=5):
    """Versucht, wiederholt eine Verbindung zu RabbitMQ aufzubauen."""
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Verbindungsversuch {attempt}/{max_retries} zu RabbitMQ...")
            send_log_message("wms", "connection_attempt", f"Attempt {attempt} to connect to RabbitMQ")
            connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
            print("Verbindung zu RabbitMQ erfolgreich!")
            send_log_message("wms", "connection_success", "Successfully connected to RabbitMQ")
            return connection
        except pika.exceptions.AMQPConnectionError:
            print(f"RabbitMQ noch nicht erreichbar, warte {delay} Sekunden...")
            send_log_message("wms", "connection_failed", f"Attempt {attempt} failed to connect to RabbitMQ")
            time.sleep(delay)
    print("Konnte keine Verbindung zu RabbitMQ herstellen. Beende Service.")
    send_log_message("wms", "connection_failed_final", "Failed to connect to RabbitMQ after multiple attempts")
    sys.exit(1)


def main():
    """Startet den Logging-Service und wartet auf Nachrichten."""
    connection = connect_to_rabbitmq()
    channel = connection.channel()

    # Exchange deklarieren
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type="topic")

    # Queue für Logs deklarieren
    result = channel.queue_declare(queue="warehouse_queue", durable=True)
    queue_name = result.method.queue

    # Queue mit Exchange verbinden
    channel.queue_bind(exchange=EXCHANGE_NAME, queue=queue_name, routing_key="order.wms")

    print("Warehouse-Service läuft und wartet auf Nachrichten ")
    send_log_message("wms", "service_started", "Warehouse service started and waiting for messages")
    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Warehouse-Service beendet.")
        send_log_message("wms", "service_stopped", "Warehouse service stopped by user")
        connection.close()


if __name__ == "__main__":
    main()
