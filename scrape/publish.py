import json
import pika

def publish_jobs_to_rabbitmq(json_file, exchange_name='jobs_exchange', routing_key='jobs.tech'):
    # Load jobs from JSON file
    with open(json_file, "r", encoding="utf-8") as f:
        jobs = json.load(f)

    # RabbitMQ connection
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',port=5672))
    channel = connection.channel()

    # Declare exchange (type=topic)
    channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

    # Publish each job
    for i, job in enumerate(jobs, start=1):
        job_message = json.dumps(job, ensure_ascii=False)
        channel.basic_publish(
            exchange=exchange_name,
            routing_key=routing_key,
            body=job_message.encode("utf-8")
        )
        print(f"✅ Published job {i}: {job['job_title']}")

    connection.close()
    print(f"\n🎯 All {len(jobs)} jobs pushed to RabbitMQ exchange '{exchange_name}' with routing key '{routing_key}'")

if __name__ == "__main__":
    publish_jobs_to_rabbitmq("job.json")