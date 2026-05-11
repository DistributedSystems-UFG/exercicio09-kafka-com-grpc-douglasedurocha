"""Etapa (2): janela deslizante das leituras; publica a media periodicamente."""
import json
import threading
import time
from collections import deque

from kafka import KafkaConsumer, KafkaProducer

from const import (
    AVERAGE_INTERVAL_SECONDS,
    BROKER_ADDR,
    BROKER_PORT,
    SENSOR_ID,
    TOPIC_AVERAGES,
    TOPIC_RAW_READINGS,
    WINDOW_SECONDS,
)


window = deque()
window_lock = threading.Lock()


def consume_loop():
    consumer = KafkaConsumer(
        TOPIC_RAW_READINGS,
        bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
        value_deserializer=lambda b: json.loads(b.decode('utf-8')),
        auto_offset_reset='latest',
    )
    print(f'[processor] consumindo "{TOPIC_RAW_READINGS}"')
    for msg in consumer:
        reading = msg.value
        with window_lock:
            window.append((reading['timestamp'], reading['value']))
        print(f'[processor] leitura recebida: {reading}')


def prune_window(now):
    cutoff = now - WINDOW_SECONDS
    while window and window[0][0] < cutoff:
        window.popleft()


def publish_loop():
    producer = KafkaProducer(
        bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
    print(f'[processor] publicando medias em "{TOPIC_AVERAGES}" a cada '
          f'{AVERAGE_INTERVAL_SECONDS}s (janela: {WINDOW_SECONDS}s)')
    while True:
        time.sleep(AVERAGE_INTERVAL_SECONDS)
        now = time.time()
        with window_lock:
            prune_window(now)
            count = len(window)
            if count == 0:
                print('[processor] janela vazia, nada a publicar')
                continue
            avg = sum(v for _, v in window) / count

        event = {
            'sensor_id': SENSOR_ID,
            'timestamp': now,
            'avg_value': round(avg, 3),
            'sample_count': count,
        }
        producer.send(TOPIC_AVERAGES, value=event)
        producer.flush()
        print(f'[processor] media publicada: {event}')


def main():
    t = threading.Thread(target=consume_loop, daemon=True)
    t.start()
    publish_loop()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\n[processor] encerrado')
