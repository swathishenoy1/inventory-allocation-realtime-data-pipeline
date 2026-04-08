# Seed Data for Fulfillment Inventory Pipeline

This folder contains lightweight seed data you can use to drive the pipeline:

- `events.jsonl`: Kafka-ready JSON lines (one event per line)
- `inventory_snapshots.csv`: Snapshot-style inventory data (optional)

## Event Schema (JSONL)
Each line is a JSON object with these fields:

- `event_type`: `STOCK_UPDATE` | `ORDER_CREATED` | `ALLOCATED` | `BACKORDERED` | `INVENTORY_ADJUSTMENT`
- `event_ts`: ISO-8601 UTC timestamp (e.g. `2026-04-08T10:41:12Z`)
- `sku_id`, `fc_id`, `region`
- `order_id` (for order events)
- `qty` (order quantity or adjustment delta)
- `available_qty`, `allocated_qty`, `atp_qty`
- `inventory_snapshot_id` (for stock updates)
- `reason` (for adjustments/backorders)

## Quick Peek
```bash
head -n 3 events.jsonl
```

## Regenerate Seed Data
```bash
python3 generate_seed.py
```

The generator is deterministic (fixed seed) so you’ll get the same output each run.
