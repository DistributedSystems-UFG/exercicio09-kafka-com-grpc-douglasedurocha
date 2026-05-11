BROKER_ADDR = 'localhost'
BROKER_PORT = '9092'

TOPIC_RAW_READINGS = 'temperature-readings'
TOPIC_AVERAGES = 'temperature-averages'

GRPC_HOST = 'localhost'
GRPC_PORT = '50051'

# Use 7200 para representar "ultimas 2 horas"; valor menor facilita a demo.
WINDOW_SECONDS = 30
AVERAGE_INTERVAL_SECONDS = 5

# Variacao minima (graus) para o sensor publicar uma nova leitura.
SIGNIFICANT_DELTA = 0.5
SENSOR_TICK_SECONDS = 1.0

SENSOR_ID = 'sensor-01'
