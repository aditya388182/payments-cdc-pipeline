from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Optional, Set

import psycopg2
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger("payments.dlq")

# Constants
KAFKA_BOOTSTRAP = "localhost:29092"
DLQ_TOPIC = "payments.transactions.dlq"

PG_CONNINFO = {
    "host": "localhost",
    "port": 5432,
    "dbname": "payments",
    "user": "payments",
    "password": "payments",
    "connect_timeout": 5,          # fail fast into stale-set path
}

MERCHANT_REFRESH_SECONDS = 120

class MerchantRegistry:
    def __init__(self) -> None:
        self._merchant_ids: Set[str] = set()
        self._loaded_at: float = 0.0
        self.refresh(force=True)

    @property
    def merchant_ids(self) -> Set[str]:
        return self._merchant_ids

    @property
    def age_seconds(self) -> float:
        return time.time() - self._loaded_at

    def refresh(self, force: bool = False) -> None:
        if not force and self.age_seconds < MERCHANT_REFRESH_SECONDS:
            return

        conn = None
        try:
            conn = psycopg2.connect(**PG_CONNINFO)
            with conn.cursor() as cur:
                cur.execute("SELECT merchant_id FROM merchants")
                rows = cur.fetchall()

            new_set = {r[0] for r in rows if r[0]}
            if not new_set:
                raise RuntimeError("merchants table returned zero rows")

            prior_age = self.age_seconds if self._loaded_at else 0.0
            self._merchant_ids = new_set
            self._loaded_at = time.time()

            logger.info(
                "MerchantRegistry refreshed – %d merchants "
                "(previous set was %.1fs old)",
                len(new_set),
                prior_age,
            )
        except Exception as exc:
            logger.error("Failed to refresh MerchantRegistry: %s", exc)
            if not self._merchant_ids:
                raise
        finally:
            if conn is not None:
                conn.close()

_merchant_registry: Optional[MerchantRegistry] = None

def get_merchant_registry() -> MerchantRegistry:
    global _merchant_registry
    if _merchant_registry is None:
        _merchant_registry = MerchantRegistry()
    return _merchant_registry

def merchants() -> Set[str]:
    reg = get_merchant_registry()
    reg.refresh()
    return reg.merchant_ids

def write_dlq(invalid: DataFrame) -> int:
    """
    connect-dlq = serialization/infrastructure failures (Kafka Connect).
    payments.transactions.dlq = business rules/application rejections (Spark).
    """
    n = invalid.count()
    if n == 0:
        return 0

    rejected_at = datetime.now(timezone.utc).isoformat()

    payload = (
        invalid.withColumn("rejected_at", F.lit(rejected_at))
               .selectExpr(
                   "coalesce(CAST(transaction_id AS STRING), uuid()) AS key",
                   "to_json(struct(*)) AS value",
               )
    )

    (
        payload.write
               .format("kafka")
               .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
               .option("topic", DLQ_TOPIC)
               .save()
    )

    logger.warning("Wrote %d invalid row(s) to DLQ topic %s", n, DLQ_TOPIC)
    return n