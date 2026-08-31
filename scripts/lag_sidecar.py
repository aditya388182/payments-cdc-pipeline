#!/usr/bin/env python3
import argparse, time, requests
from confluent_kafka import Consumer, TopicPartition

PUSHGATEWAY_URL = "http://localhost:9091/metrics/job/payments_cdc"

def get_end_offsets(bootstrap: str, topic: str):
    c = Consumer({'bootstrap.servers': bootstrap, 'group.id': 'lag-sidecar-temp'})
    try:
        metadata = c.list_topics(topic, timeout=10)
        if topic not in metadata.topics: return {}
        parts = metadata.topics[topic].partitions
        offsets = {}
        for p in parts:
            tp = TopicPartition(topic, p)
            _, high = c.get_watermark_offsets(tp, timeout=5, cached=False)
            offsets[str(p)] = high
        return offsets
    except Exception: return {}
    finally: c.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:29092")
    parser.add_argument("--topic", default="payments.public.transactions")
    parser.add_argument("--interval", type=int, default=15)
    args = parser.parse_args()

    while True:
        try:
            offsets = get_end_offsets(args.bootstrap, args.topic)
            if offsets:
                lines = [f'payments_cdc_topic_end_offset{{partition="{p}"}} {off}' for p, off in offsets.items()]
                requests.post(PUSHGATEWAY_URL, data="\n".join(lines) + "\n", timeout=2)
                print(f"Pushed sidecar offsets: {offsets}")
        except Exception as e: print(f"Sidecar error: {e}")
        time.sleep(args.interval)

if __name__ == "__main__": main()
