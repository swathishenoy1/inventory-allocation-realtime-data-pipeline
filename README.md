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

## Notes

- The dashboard is static HTML intended as a visual wireframe.
- The seed generator uses a fixed random seed to keep outputs consistent.

## Project Tree

```text
inventory-allocation-realtime-data-pipeline
├─ inventory_dashboard.html
├─ README.md
└─ seed_data
   ├─ events.jsonl
   ├─ generate_seed.py
   ├─ inventory_snapshots.csv
   └─ README.md
```

## Next Steps (Optional)

1. Add a Kafka producer script to stream `events.jsonl` into a topic for live demo.
2. Add a minimal Spark Structured Streaming job to compute the MVP aggregates.
3. Write aggregated Parquet locally and point the dashboard to a small API or query layer.
