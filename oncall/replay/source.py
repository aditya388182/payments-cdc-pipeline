"""Which DLQ keys still exist in the source database (Postgres `transactions`)."""
from __future__ import annotations

import re
from typing import Iterable

import psycopg2

_UUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


class PostgresSourceKeys:
    """Used by the parity accounting: a quarantined record only explains a missing lake row if its key is really
    a row in Postgres. Rogue messages (a malformed key that cannot be a uuid) and rows deleted since are not."""

    def __init__(self, conninfo: dict | None = None) -> None:
        if conninfo is None:
            from spark.utils.dlq_writer import PG_CONNINFO

            conninfo = PG_CONNINFO
        self._conninfo = conninfo

    def present(self, keys: Iterable[str]) -> set[str]:
        valid = [k for k in set(keys) if _UUID.match(k)]
        if not valid:
            return set()
        conn = psycopg2.connect(**self._conninfo)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT transaction_id::text FROM transactions WHERE transaction_id = ANY(%s::uuid[])", (valid,))
                return {r[0] for r in cur.fetchall()}
        finally:
            conn.close()
