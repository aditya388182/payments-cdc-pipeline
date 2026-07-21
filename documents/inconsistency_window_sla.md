# Inconsistency Window SLA

**Project:** Real-Time Payments Ledger Ingestion with Exactly-Once CDC  
**Date measured:** YYYY-MM-DD  
**Measured by:** Day 3 Block 3.4

## 1. Mechanism

The only window in which a downstream reader can observe a duplicate is the gap between:

1. Successful Delta MERGE (the sink is already correct), and
2. Spark writing the new Kafka offsets into the checkpoint directory.

If the driver is killed in that gap, the next restart will re-read the same offsets.
Both the batch ledger and the LSN guard turn those re-reads into pure no-ops, so the
**sink itself never sees a duplicate**. Downstream consumers that read the Delta table
directly can see the same rows again for at most one trigger interval.

## 2. Measured Window (Day 3 experiment)

| Metric                          | Value          |
|--------------------------------|----------------|
| Trigger interval               | 10 s           |
| Observed replay window         | X.X s          |
| Last committed batch_id        | NNN            |
| Checkpoint offset at kill      | \ldots         |
| Kafka offset at kill           | \ldots         |
| Replayed offsets               | \ldots         |
| Duplicate transaction_ids after recovery | 0     |

**SLA sentence (use this in interviews):**

> Maximum 10 seconds of duplicate exposure to downstream readers; zero exposure to the sink itself.

## 3. Evidence

```sql
-- DESCRIBE HISTORY excerpt after the induced crash + recovery
-- (paste the real output here)