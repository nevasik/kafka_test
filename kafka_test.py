from confluent_kafka import Producer, Consumer, KafkaException

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'test'

# Producer
def send_message():
    producer = Producer({'bootstrap.servers': KAFKA_BROKER})
    try:
        producer.produce(TOPIC_NAME, key='test_key', value='test')  # Отправка
        producer.flush()  # Ожидание отправки
        print(f"Сообщение отправлено в топик '{TOPIC_NAME}'")
    except KafkaException as e:
        print(f"Ошибка отправки сообщения: {e}")


# Consumer (Чтение сообщения из топика)
def receive_message():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'test_group',
        'auto.offset.reset': 'earliest'  # Чтобы получить старые сообщения, если они есть
    })

    consumer.subscribe([TOPIC_NAME])

    print(f"Ожидание сообщений из топика '{TOPIC_NAME}'...")
    try:
        while True:
            msg = consumer.poll(1.0)  # Проверяем каждые 1 сек, есть ли сообщения
            if msg is None:
                print('сообщения не найдено')
                continue
            if msg.error():
                raise KafkaException(msg.error())

            print(f"Получено сообщение: {msg.value().decode('utf-8')} (key: {msg.key().decode('utf-8')})")
            break  # После получения сообщения выходим
    except KafkaException as e:
        print(f"Ошибка при получении сообщения: {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
    send_message()  # Отправляем сообщение
    receive_message()  # Получаем сообщение
