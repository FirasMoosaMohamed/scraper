import json
import pika

def publish_jobs_to_rabbitmq(json_file, exchange_name='jobs_exchange', routing_key='jobs.tech'):
    # Load jobs from JSON file
    credentials = pika.PlainCredentials('guest', 'guest')
    # RabbitMQ connection
    connection = pika.BlockingConnection(pika.ConnectionParameters(credentials=credentials,host='localhost',port=5672))
    channel = connection.channel()

    # Declare exchange (type=topic)
    channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

    # Publish each job
    job_message = json.dumps(json_file, ensure_ascii=False).encode("utf-8")
    channel.basic_publish(
        exchange=exchange_name,
        routing_key=routing_key,
        body=job_message
    )
    print(f"✅ Published job {exchange_name}")

    connection.close()
    print(f"\n🎯 All  jobs pushed to RabbitMQ exchange '{exchange_name}' with routing key '{routing_key}'")
