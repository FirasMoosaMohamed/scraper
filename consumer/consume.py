
# import pika

# def consume_jobs(exchange_name='jobs_exchange', routing_key='jobs.#'):
#     connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',port=5672))
#     channel = connection.channel()

#     channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

#     # Create a temporary queue and bind to the topic
#     result = channel.queue_declare('', exclusive=True)
#     queue_name = result.method.queue

#     channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)

#     def callback(ch, method, properties, body):
#         print(f"📥 Received: {body.decode('utf-8')}")

#     channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

#     print(f"🚀 Waiting for messages on '{routing_key}' from exchange '{exchange_name}'...\n")
#     channel.start_consuming()

# if __name__ == "__main__":
#     consume_jobs()

import pika

def consume_jobs(exchange_name='jobs_exchange', routing_key='jobs.#', queue_name='jobs_queue'):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    # Declare the exchange (topic type)
    channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

    # Declare a durable named queue (instead of temporary one)
    channel.queue_declare(queue=queue_name, durable=True)

    # Bind the queue to the exchange
    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=routing_key)

    def callback(ch, method, properties, body):
        print(f"📥 Received: {body.decode('utf-8')}")

    channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

    print(f"🚀 Waiting for messages on '{routing_key}' from exchange '{exchange_name}'...\n")
    channel.start_consuming()
