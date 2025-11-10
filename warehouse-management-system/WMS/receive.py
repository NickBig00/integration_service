
import pika, sys, os

EXCHANGE_NAME = 'warehouse_event'
ROUTING_KEY = '#'

#https://www.rabbitmq.com/tutorials/tutorial-one-python
#https://medium.com/@sujakhu.umesh/rabbitmq-with-python-from-basics-to-advanced-messaging-patterns-a-practical-guide-18f8b43b94f8

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic')
    
    # Declare an anonymous, non-durable, exclusive, auto-delete queue
    result = channel.queue_declare(queue='', exclusive=True)
    # get the name of a queue declared by RabbitMQ
    queue_name = result.method.queue

    # Bind the anonymous queue to the Exchange with routing key
    channel.queue_bind(
        exchange=EXCHANGE_NAME, 
        queue=queue_name, 
        routing_key=ROUTING_KEY
    )

    def callback(ch, method, properties, body):
        print(f" [x] Received {body}")
    
    channel.basic_consume(
        queue=queue_name, 
        on_message_callback=callback, 
        auto_ack=True
    )

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)