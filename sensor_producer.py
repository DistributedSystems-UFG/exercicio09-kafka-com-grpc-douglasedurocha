"""Etapa (1): sensor simulado que publica leituras com variacao significativa."""
import json
import random
import time

from kafka import KafkaProducer

from const import (
    BROKER_ADDR,
    BROKER_PORT,
    SENSOR_ID,
    SENSOR_TICK_SECONDS,
    SIGNIFICANT_DELTA,
    TOPIC_RAW_READINGS,
)


def main():
    producer = KafkaProducer(
        bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )

    current = 25.0
    last_published = None

    print(f'[sensor] iniciando publicacoes em "{TOPIC_RAW_READINGS}"')
    while True:
        current += random.uniform(-0.4, 0.4)

        if last_published is None or abs(current - last_published) >= SIGNIFICANT_DELTA:
            event = {
                'sensor_id': SENSOR_ID,
                'timestamp': time.time(),
                'value': round(current, 3),
            }
            producer.send(TOPIC_RAW_READINGS, value=event)
            producer.flush()
            print(f'[sensor] publicado: {event}')
            last_published = current

        time.sleep(SENSOR_TICK_SECONDS)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\n[sensor] encerrado')
