"""Entry point: `PYTHONPATH=. python -m oncall.replay.server` (loopback only by default)."""
from __future__ import annotations

import argparse
import logging
import os
import signal
import threading
from concurrent import futures

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc

from oncall.v1 import replay_pb2_grpc as pb_grpc

from . import logging_json
from .dlq import KafkaDlq
from .lake import DeltaLake
from .publisher import AvroCdcPublisher
from .service import ReplayServicer
from .validation import SparkRecordValidator

log = logging.getLogger("replay.server")


def build_servicer() -> tuple[ReplayServicer, SparkRecordValidator]:
    bootstrap = os.environ.get("ONCALL_KAFKA_BROKERS", "127.0.0.1:29092")
    endpoint = os.environ.get("ONCALL_S3_ENDPOINT", "http://127.0.0.1:9000")
    opts = {
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ACCESS_KEY_ID": os.environ.get("ONCALL_S3_ACCESS_KEY", "minioadmin"),
        "AWS_SECRET_ACCESS_KEY": os.environ.get("ONCALL_S3_SECRET_KEY", "minioadmin"),
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",  # read-only use; the single-writer Spark job owns commits
    }
    validator = SparkRecordValidator()
    servicer = ReplayServicer(
        lake=DeltaLake(os.environ.get("ONCALL_LAKE_URI", "s3://payments-lake/transactions"), opts),
        dlq=KafkaDlq(bootstrap, os.environ.get("ONCALL_DLQ_TOPIC", "payments.transactions.dlq")),
        validator=validator,
        publisher=AvroCdcPublisher(),
    )
    return servicer, validator


def serve(listen: str, servicer, ready_hook=None) -> grpc.Server:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    pb_grpc.add_ReplayServiceServicer_to_server(servicer, server)
    hs = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(hs, server)
    hs.set("", health_pb2.HealthCheckResponse.NOT_SERVING)
    hs.set("oncall.v1.ReplayService", health_pb2.HealthCheckResponse.NOT_SERVING)
    port = server.add_insecure_port(listen)
    server.start()
    log.info("listening", extra={"fields": {"addr": listen, "port": port}})

    def warm() -> None:
        if ready_hook:
            ready_hook()
        for name in ("", "oncall.v1.ReplayService"):
            hs.set(name, health_pb2.HealthCheckResponse.SERVING)
        log.info("ready")

    threading.Thread(target=warm, daemon=True).start()
    return server


def main() -> None:
    logging_json.setup()
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", default=os.environ.get("ONCALL_REPLAY_ADDR", "127.0.0.1:50051"))
    args = ap.parse_args()
    servicer, validator = build_servicer()
    server = serve(args.listen, servicer, ready_hook=validator.warm_up)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    stop.wait()
    server.stop(grace=5)


if __name__ == "__main__":
    main()
