import json

import pika


# https://www.rabbitmq.com/tutorials/tutorial-one-python
def publ_item_picked(service: str, event: str, message: str):
    """Sendet item_picked-Nachricht an RabbitMQ."""

    event_to_routing_key = {"item_picked": "item.picked",
                           "order_packed": "order.packed",
                           "order_shipped": "order.shipped"}

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
        channel = connection.channel()

        channel.exchange_declare(exchange='warehouse_event', exchange_type='topic')
        payload = {
            "service": service,
            "event": event,
            "message": message
        }

        # https://medium.com/@sujakhu.umesh/rabbitmq-with-python-from-basics-to-advanced-messaging-patterns-a-practical-guide-18f8b43b94f8
        # Working with JSON Messages in RabbitMQ
        channel.basic_publish(
            exchange='warehouse_event',
            routing_key=f'{event_to_routing_key[event]}.{service}',
            body=json.dumps(payload)
        )
        print(f" [x] Sent {payload}")
        connection.close()

    except Exception as e:
        print(f" [!] Failed to send message: {e}")
