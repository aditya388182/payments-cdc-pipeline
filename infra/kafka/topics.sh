#!/usr/bin/env bash
set -e

B="docker exec kafka kafka-topics --bootstrap-server kafka:9092"

# Main CDC topic - zstd, 7-day retention, 1GB segments
$B --create --if-not-exists --topic payments.public.transactions \
   --partitions 3 --replication-factor 1 \
   --config compression.type=zstd \
   --config retention.ms=604800000 \
   --config segment.bytes=1073741824

# Business-rule DLQ (Spark validator will write here)
$B --create --if-not-exists --topic payments.transactions.dlq \
   --partitions 1 --replication-factor 1 \
   --config retention.ms=604800000

# Connect serialization DLQ
$B --create --if-not-exists --topic connect-dlq \
   --partitions 1 --replication-factor 1

echo "--- Kafka topics created ---"
$B --list