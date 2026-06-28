This project simulates a real-time inventory allocation system where inventory, order, allocation, backorder, and stock update events flow through a pipeline. The goal is to track whether products are available, whether customer orders are being allocated successfully, where stock-outs are happening, and how quickly inventory gets allocated after an order is created.

# Inventory Allocation Realtime Data Pipeline — MVP Assets

This repo currently contains the lightweight MVP artifacts we produced so far: a static dashboard mockup and deterministic seed data to drive the pipeline.

## What’s Included

- Dashboard shell (HTML/CSS)
- Seed data + generator

## Dashboard (HTML)

The file is a minimal, scannable dashboard layout for:
- Allocation success rate
- Negative inventory count
- P95 allocation lag
- Stock-outs and backorders
- Allocation trend and lag distribution
- Tabbed tables for stock-outs, backorders, and accuracy

## Seed Data

The seed set is designed to be Kafka-ready and deterministic.

Contents:
- `events.jsonl`: JSONL event stream
- `inventory_snapshots.csv`: snapshot-style inventory data
- `generate_seed.py`: deterministic generator
- `README.md`: schema and usage notes

Regenerate the seed data:

```bash
python3 generate_seed.py
```

## Kafka Producer

There is a simple JSONL-to-Kafka producer you can use to stream the seed events.

Install dependency:

```bash
pip install -r requirements.txt
```

Run (from repo root):

```bash
python3 scripts/produce_events.py \
  --topic inventory_events \
  --bootstrap-servers localhost:9092 \
  --events-per-second 5
```

Optional flags:
- `--loop` to continuously replay the file
- `--max-events N` to send only N events
- `--key-field sku_id` to key by SKU instead of order

## Spark Aggregations

The Spark job computes the dashboard-ready metric tables from the seed event stream.

Run a local batch aggregation from `events.jsonl`:

```bash
python3 scripts/spark_aggregations.py \
  --source jsonl \
  --input seed_data/events.jsonl \
  --output data/aggregates
```

Generated Parquet tables:
- `data/aggregates/stock_outs`
- `data/aggregates/backorders`
- `data/aggregates/inventory_accuracy`
- `data/aggregates/allocation_success_5m`
- `data/aggregates/allocation_lag`

The script also includes a Kafka streaming reader mode that writes raw partitioned Parquet for continuous ingestion:

```bash
python3 scripts/spark_aggregations.py \
  --source kafka \
  --topic inventory_events \
  --bootstrap-servers localhost:9092 \
  --output data/streaming
```

## Metrics API

After generating the Spark aggregate tables, start the local JSON API:

```bash
python3 scripts/serve_metrics_api.py \
  --aggregate-dir data/aggregates \
  --port 8000
```

Available endpoints:
- `GET /health`
- `GET /api/tables`
- `GET /api/stock-outs`
- `GET /api/backorders`
- `GET /api/inventory-accuracy`
- `GET /api/allocation-success`
- `GET /api/allocation-lag`

Each metric endpoint supports an optional `limit` query parameter:

```bash
curl "http://127.0.0.1:8000/api/backorders?limit=20"
```

## Notes

- The dashboard is static HTML intended as a visual wireframe.
- The seed generator uses a fixed random seed to keep outputs consistent.

## Project Tree

```text
inventory-allocation-realtime-data-pipeline
├─ inventory_dashboard.html
├─ README.md
├─ requirements.txt
├─ scripts
│  ├─ spark_aggregations.py
│  ├─ serve_metrics_api.py
│  └─ produce_events.py
└─ seed_data
   ├─ events.jsonl
   ├─ generate_seed.py
   ├─ inventory_snapshots.csv
   └─ README.md
```

## Next Steps (Optional)

1. Wire the static dashboard to the metrics API.
2. Add Docker Compose for Kafka + local Spark demo startup.
3. Add a small smoke test for the aggregation and API scripts.
