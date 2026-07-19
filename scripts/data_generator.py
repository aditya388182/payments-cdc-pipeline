#!/usr/bin/env python3

from __future__ import annotations

import argparse
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import List, Optional

import psycopg2
from psycopg2.extras import execute_values

# Configuration defaults
DEFAULT_RATE = 80          # transactions per second
DEFAULT_DURATION = 0       # 0 = run forever
WHALE_PROBABILITY = 0.03   # 3% chance of a whale amount
WHALE_MIN = 500_000        # $5,000.00 in minor units
WHALE_MAX = 5_000_000      # $50,000.00

STATUSES = ["PENDING", "SETTLED", "FAILED"]
EVENT_TYPES = ["AUTHORIZATION", "CAPTURE", "REFUND", "CHARGEBACK"]
CURRENCIES = ["USD", "EUR", "GBP", "JPY"]


class GracefulKiller:
    def __init__(self):
        self.kill_now = False
        signal.signal(signal.SIGINT, self._exit)
        signal.signal(signal.SIGTERM, self._exit)

    def _exit(self, signum, frame):
        print("\n[generator] Shutdown signal received – finishing current batch...")
        self.kill_now = True


def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="payments",
        user="payments",
        password="payments",
    )


def load_merchant_ids(conn) -> List[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT merchant_id FROM merchants ORDER BY merchant_id")
        return [row[0] for row in cur.fetchall()]


def load_existing_transaction_ids(conn, limit: int = 5000) -> List[str]:
    """Load a sample of existing transaction_ids for UPDATE / DELETE."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT transaction_id
            FROM transactions
            ORDER BY random()
            LIMIT %s
            """,
            (limit,),
        )
        return [str(row[0]) for row in cur.fetchall()]


def generate_amount(whale_mode: bool) -> int:
    if whale_mode and random.random() < WHALE_PROBABILITY:
        return random.randint(WHALE_MIN, WHALE_MAX)
    return random.randint(100, 99_900)


def do_inserts(conn, merchant_ids: List[str], count: int, whale_mode: bool) -> int:
    if count <= 0:
        return 0

    rows = []
    now = datetime.now(timezone.utc)
    for _ in range(count):
        rows.append(
            (
                str(uuid.uuid4()),
                random.choice(merchant_ids),
                generate_amount(whale_mode),
                random.choice(CURRENCIES),
                random.choice(STATUSES),
                random.choice(EVENT_TYPES),
                now,
                now,
            )
        )

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO transactions (
                transaction_id, merchant_id, amount_minor, currency,
                status, event_type, created_at, updated_at
            ) VALUES %s
            """,
            rows,
        )
    conn.commit()
    return count


def do_updates(conn, existing_ids: List[str], count: int) -> int:
    if count <= 0 or not existing_ids:
        return 0

    updated = 0
    with conn.cursor() as cur:
        for _ in range(count):
            tid = random.choice(existing_ids)
            new_status = random.choice(STATUSES)
            cur.execute(
                """
                UPDATE transactions
                SET status = %s,
                    updated_at = now()
                WHERE transaction_id = %s
                """,
                (new_status, tid),
            )
            updated += cur.rowcount
    conn.commit()
    return updated


def do_deletes(conn, existing_ids: List[str], count: int) -> int:
    if count <= 0 or not existing_ids:
        return 0

    deleted = 0
    # Work on a copy so we can remove deleted IDs
    ids = existing_ids.copy()
    with conn.cursor() as cur:
        for _ in range(count):
            if not ids:
                break
            tid = random.choice(ids)
            cur.execute(
                "DELETE FROM transactions WHERE transaction_id = %s",
                (tid,),
            )
            if cur.rowcount:
                deleted += 1
                ids.remove(tid)
    conn.commit()
    return deleted


def main() -> None:
    parser = argparse.ArgumentParser(description="Payments data generator")
    parser.add_argument(
        "--rate",
        type=int,
        default=DEFAULT_RATE,
        help=f"Target transactions per second (default: {DEFAULT_RATE})",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=DEFAULT_DURATION,
        help="Run for N seconds (0 = forever)",
    )
    parser.add_argument(
        "--whale",
        action="store_true",
        help="Enable occasional large 'whale' amounts",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="How many operations to group before sleeping",
    )
    args = parser.parse_args()

    if args.rate < 1:
        print("--rate must be >= 1", file=sys.stderr)
        sys.exit(1)

    killer = GracefulKiller()
    conn = get_connection()

    print("[generator] Loading merchants ...")
    merchant_ids = load_merchant_ids(conn)
    if not merchant_ids:
        print("No merchants found. Did you run init.sql?", file=sys.stderr)
        sys.exit(1)
    print(f"[generator] {len(merchant_ids)} merchants loaded")

    print("[generator] Loading sample of existing transaction_ids ...")
    existing_ids = load_existing_transaction_ids(conn)
    print(f"[generator] {len(existing_ids)} existing IDs available for UPDATE/DELETE")

    print(
        f"[generator] Starting – rate={args.rate} tps, "
        f"duration={'forever' if args.duration == 0 else args.duration}, "
        f"whale={args.whale}"
    )
    print("[generator] Mix ≈ 70% INSERT / 20% UPDATE / 10% DELETE")
    print("[generator] Press Ctrl+C to stop cleanly\n")

    start_time = time.time()
    total_ops = 0
    total_inserts = 0
    total_updates = 0
    total_deletes = 0

    # Operations per batch
    ops_per_batch = args.batch_size
    sleep_time = ops_per_batch / args.rate

    try:
        while not killer.kill_now:
            if args.duration > 0 and (time.time() - start_time) >= args.duration:
                print("\n[generator] Duration reached – stopping")
                break

            # 70 / 20 / 10 mix
            n_insert = int(ops_per_batch * 0.70)
            n_update = int(ops_per_batch * 0.20)
            n_delete = ops_per_batch - n_insert - n_update

            i = do_inserts(conn, merchant_ids, n_insert, args.whale)
            u = do_updates(conn, existing_ids, n_update)
            d = do_deletes(conn, existing_ids, n_delete)

            total_inserts += i
            total_updates += u
            total_deletes += d
            total_ops += i + u + d

            # Refresh existing IDs occasionally so UPDATEs/DELETEs stay valid
            if total_ops % 2000 < ops_per_batch:
                existing_ids = load_existing_transaction_ids(conn)

            elapsed = time.time() - start_time
            current_rate = total_ops / elapsed if elapsed > 0 else 0
            print(
                f"\r[generator] ops={total_ops:,}  "
                f"ins={total_inserts:,} upd={total_updates:,} del={total_deletes:,}  "
                f"rate={current_rate:.1f} tps",
                end="",
                flush=True,
            )

            time.sleep(max(0.0, sleep_time))

    finally:
        conn.close()
        print("\n\n[generator] Final totals")
        print(f"  Total operations : {total_ops:,}")
        print(f"  Inserts          : {total_inserts:,}")
        print(f"  Updates          : {total_updates:,}")
        print(f"  Deletes          : {total_deletes:,}")
        elapsed = time.time() - start_time
        if elapsed > 0:
            print(f"  Average rate     : {total_ops / elapsed:.1f} tps")
        print("[generator] Stopped cleanly")


if __name__ == "__main__":
    main()