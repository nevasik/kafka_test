from confluent_kafka.admin import AdminClient

conf = {'bootstrap.servers': 'localhost:9092'}
admin_client = AdminClient(conf)


metadata = admin_client.list_topics(timeout=10)
for topic in metadata.topics:
    print(topic)