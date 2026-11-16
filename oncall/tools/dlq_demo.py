"""The DLQ demo scenario (SPEC 5): a transaction for a merchant the registry does not know is quarantined;
adding the merchant to the reference table (Postgres `merchants`) and replaying succeeds.

  python oncall/tools/dlq_demo.py inject [MERCHANT_ID]   -> inserts one transaction row, prints its id
  python oncall/tools/dlq_demo.py fix MERCHANT_ID        -> adds the merchant to the reference table
"""
import sys
import uuid

import psycopg2

DSN = dict(host="127.0.0.1", port=5432, dbname="payments", user="payments", password="payments")


def main() -> None:
    cmd = sys.argv[1]
    conn = psycopg2.connect(**DSN)
    try:
        with conn.cursor() as cur:
            if cmd == "inject":
                merchant = sys.argv[2] if len(sys.argv) > 2 else "MERCH_" + uuid.uuid4().hex[:6].upper()
                tid = str(uuid.uuid4())
                cur.execute(
                    "INSERT INTO transactions (transaction_id, merchant_id, amount_minor, currency, status, event_type) "
                    "VALUES (%s, %s, %s, 'USD', 'PENDING', 'AUTHORIZATION')", (tid, merchant, 4200))
                print(f"{tid} {merchant}")
            elif cmd == "fix":
                cur.execute("INSERT INTO merchants (merchant_id, merchant_name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                            (sys.argv[2], "Demo merchant " + sys.argv[2]))
                print("merchant added", sys.argv[2])
            else:
                raise SystemExit(__doc__)
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
