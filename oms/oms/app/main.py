import json
import threading
import time

import pika
from fastapi import FastAPI
from .rabbitmq.receive import start_wms_listener
from .routers.orders import router as orders
from oms.app.service.oms_service import write_in_store

app = FastAPI(title="OMS API", version="1.0.0")
app.include_router(orders, prefix="/orders", tags=["Orders"])


def start_wms_listener_blocking():
    print("[OMS] Listener-Thread gestartet!", flush=True)
    while True:
        print("[OMS] Thread lebt noch!", flush=True)
        time.sleep(5)
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters("rabbitmq"))
            channel = connection.channel()
            channel.exchange_declare(exchange="oms_event", exchange_type="topic")
            queue = channel.queue_declare(queue="oms_queue", durable=True)
            channel.queue_bind(exchange="oms_event", queue="oms_queue", routing_key="oms")

            def callback(ch, method, properties, body):
                print("[OMS] Nachricht empfangen:", body.decode())
                data = json.loads(body)
                order_id = data.get("orderId")
                event = data.get("event")
                write_in_store(order_id, event)

            channel.basic_consume(queue="oms_queue", on_message_callback=callback, auto_ack=True)
            print("[OMS] Listener aktiv.")
            channel.start_consuming()
        except Exception as e:
            print("[OMS] Fehler:", e)
            time.sleep(5)

@app.on_event("startup")
def startup_event():
    print("[OMS] Starte Listener-Thread …")
    threading.Thread(target=start_wms_listener_blocking, daemon=True).start()
