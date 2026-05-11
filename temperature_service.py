"""Etapa (3): consome as medias, armazena em memoria e expoe via gRPC."""
import json
import logging
import threading
from concurrent import futures

import grpc
from kafka import KafkaConsumer

import TemperatureService_pb2 as pb
import TemperatureService_pb2_grpc as pb_grpc
from const import (
    BROKER_ADDR,
    BROKER_PORT,
    GRPC_PORT,
    TOPIC_AVERAGES,
)


db = []
db_lock = threading.Lock()


class TemperatureServer(pb_grpc.TemperatureServiceServicer):

    def GetLatestAverage(self, request, context):
        with db_lock:
            if not db:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details('nenhuma media disponivel ainda')
                return pb.TemperatureAverage()
            item = db[-1]
        return pb.TemperatureAverage(
            timestamp=item['timestamp'],
            avg_value=item['avg_value'],
            sample_count=item['sample_count'],
            sensor_id=item['sensor_id'],
        )

    def GetHistoricalAverages(self, request, context):
        reply = pb.TemperatureAverageList()
        with db_lock:
            for item in db:
                if request.start_ts <= item['timestamp'] <= request.end_ts:
                    reply.items.append(pb.TemperatureAverage(
                        timestamp=item['timestamp'],
                        avg_value=item['avg_value'],
                        sample_count=item['sample_count'],
                        sensor_id=item['sensor_id'],
                    ))
        return reply

    def ListAllAverages(self, request, context):
        reply = pb.TemperatureAverageList()
        with db_lock:
            for item in db:
                reply.items.append(pb.TemperatureAverage(
                    timestamp=item['timestamp'],
                    avg_value=item['avg_value'],
                    sample_count=item['sample_count'],
                    sensor_id=item['sensor_id'],
                ))
        return reply

    def CountAverages(self, request, context):
        with db_lock:
            return pb.CountReply(count=len(db))


def kafka_loop():
    consumer = KafkaConsumer(
        TOPIC_AVERAGES,
        bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
        value_deserializer=lambda b: json.loads(b.decode('utf-8')),
        auto_offset_reset='earliest',
    )
    print(f'[service] consumindo "{TOPIC_AVERAGES}"')
    for msg in consumer:
        event = msg.value
        with db_lock:
            db.append(event)
        print(f'[service] armazenado: {event} (total: {len(db)})')


def serve():
    t = threading.Thread(target=kafka_loop, daemon=True)
    t.start()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb_grpc.add_TemperatureServiceServicer_to_server(TemperatureServer(), server)
    server.add_insecure_port('[::]:' + GRPC_PORT)
    server.start()
    print(f'[service] gRPC ouvindo na porta {GRPC_PORT}')
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    try:
        serve()
    except KeyboardInterrupt:
        print('\n[service] encerrado')
