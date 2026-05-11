"""Etapa (4): cliente gRPC que demonstra as consultas."""
from __future__ import print_function

import time

import grpc

import TemperatureService_pb2 as pb
import TemperatureService_pb2_grpc as pb_grpc
from const import GRPC_HOST, GRPC_PORT


def run():
    target = GRPC_HOST + ':' + GRPC_PORT
    print(f'[client] conectando em {target}')
    with grpc.insecure_channel(target) as channel:
        stub = pb_grpc.TemperatureServiceStub(channel)

        count = stub.CountAverages(pb.EmptyMessage())
        print(f'Total de medias armazenadas: {count.count}')

        try:
            latest = stub.GetLatestAverage(pb.EmptyMessage())
            print(f'Ultima media: {latest.avg_value} graus '
                  f'(sensor={latest.sensor_id}, '
                  f'amostras={latest.sample_count}, '
                  f'ts={latest.timestamp})')
        except grpc.RpcError as e:
            print(f'GetLatestAverage falhou: {e.details()}')

        now = time.time()
        tr = pb.TimeRange(start_ts=now - 3600, end_ts=now)
        hist = stub.GetHistoricalAverages(tr)
        print(f'Medias na ultima hora: {len(hist.items)}')
        for item in hist.items[-5:]:
            print(f'  ts={item.timestamp:.0f} avg={item.avg_value} '
                  f'amostras={item.sample_count}')

        all_avgs = stub.ListAllAverages(pb.EmptyMessage())
        print(f'Total via ListAllAverages: {len(all_avgs.items)}')


if __name__ == '__main__':
    run()
