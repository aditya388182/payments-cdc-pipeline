"""Reads the Delta table with delta-rs (the `deltalake` package); never writes to it."""
from __future__ import annotations

import datetime as dt
import threading
import time

import pyarrow.compute as pc
from deltalake import DeltaTable

from .models import LakeSnapshot, LakeStatsData


class DeltaLake:
    def __init__(self, uri: str, storage_options: dict[str, str], min_refresh_interval: float = 0.5) -> None:
        self._uri = uri
        self._opts = storage_options
        self._lock = threading.Lock()
        self._table: DeltaTable | None = None
        self._snap: LakeSnapshot | None = None
        self._snap_version = -1
        self._checked_at = 0.0
        self._min_interval = min_refresh_interval

    def snapshot(self, *, refresh: bool = False) -> LakeSnapshot:
        with self._lock:
            now = time.monotonic()
            if not refresh and self._snap is not None and now - self._checked_at < self._min_interval:
                return self._snap
            if self._table is None:
                self._table = DeltaTable(self._uri, storage_options=self._opts)
            else:
                self._table.update_incremental()
            self._checked_at = now
            version = self._table.version()
            if self._snap is None or version != self._snap_version:
                self._snap = self._load(self._table, version)
                self._snap_version = version
            return self._snap

    @staticmethod
    def _load(table: DeltaTable, version: int) -> LakeSnapshot:
        tbl = table.to_pyarrow_dataset().to_table(columns=["transaction_id", "lsn", "is_delete"])
        ids = tbl.column("transaction_id").to_pylist()
        lsns = tbl.column("lsn").to_pylist()
        dele = tbl.column("is_delete").to_pylist()
        m: dict[str, int] = {}
        for k, v in zip(ids, lsns):
            m[k] = int(v) if v is not None else 0
        tomb = sum(1 for d in dele if d)
        live = len(dele) - tomb
        max_lsn = int(pc.max(tbl.column("lsn")).as_py() or 0) if tbl.num_rows else 0
        hist = table.history(1)
        ts_ms = hist[0].get("timestamp") if hist else None
        last = (
            dt.datetime.fromtimestamp(ts_ms / 1000, tz=dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            if ts_ms
            else ""
        )
        return LakeSnapshot(m, LakeStatsData(live, tomb, max_lsn, last, version))
