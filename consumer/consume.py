import pika

def consume_jobs(exchange_name='jobs_exchange', routing_key='jobs.#', queue_name='jobs_queue'):
    credentials = pika.PlainCredentials('guest', 'guest')
    connection = pika.BlockingConnection(pika.ConnectionParameters(credentials=credentials,host='localhost'))
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
if __name__ =="__main__":
    consume_jobs()