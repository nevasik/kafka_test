from flask import Flask, jsonify, request
from confluent_kafka import Consumer, KafkaException
import threading

app = Flask(__name__)

KAFKA_BROKER = 'localhost:9092'
TOPIC_NAME = 'test'


def consume_messages():
    consumer = Consumer({
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'test_group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([TOPIC_NAME])

    print(f"Ожидание сообщений из топика '{TOPIC_NAME}'...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Ошибка Kafka: {msg.error()}")
                continue

            # print(f"Получено сообщение: {msg.value().decode('utf-8')} (key: {msg.key().decode('utf-8')})")
            print(msg.value().decode('utf-8'))
    except KafkaException as e:
        print(f"Ошибка при получении сообщений: {e}")
    finally:
        consumer.close()


def start_kafka_consumer():
    threading.Thread(target=consume_messages, daemon=True).start()


@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'GET':
        print('GET запрос')
        return jsonify({"message": "Kafka Consumer is running (GET)..."})

    if request.method == 'POST':
        print('POST запрос')
        return jsonify({"message": "Kafka Consumer is running (POST)..."})


@app.route('/v1/metamodel', methods=['GET', 'POST'])
def home2():
    if request.method == 'GET':
        print('GET запрос')
        return jsonify({"message": "Kafka Consumer is running (GET)..."})

    if request.method == 'POST':
        print('POST запрос')
        return jsonify({"message": "Kafka Consumer is running (POST)..."})


@app.route('/v1/event', methods=['GET', 'POST'])
def home3():
    if request.method == 'GET':
        print('GET запрос')
        return jsonify({"id": "0001", "code": "code0001", "message": "ok", "description": ""})

    if request.method == 'POST':
        print('POST запрос')
        return jsonify({"id": "0001", "code": "code0001", "message": "ok", "description": ""})


if __name__ == "__main__":
    start_kafka_consumer()

    app.run(host='0.0.0.0', port=8081)
