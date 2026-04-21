# NOT TO BE USED FOR INTENSE OPERATIONS (USE CELERY)

import grpc
from trafficgym.api import engine_pb2_grpc

options: list[tuple[str, int]] = [
    # ("grpc.enable_retries", 1),
    # ("grpc.initial_reconnect_backoff_ms", 100),
    # ("grpc.max_reconnect_backoff_ms", 1000),
]

channel = grpc.insecure_channel("127.0.0.1:50051", options=options)
light_engine_client = engine_pb2_grpc.EngineServiceStub(channel)
