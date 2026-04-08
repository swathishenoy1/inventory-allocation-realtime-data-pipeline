#!/usr/bin/env python3
"""
Generate seed events for the fulfillment pipeline.
Outputs:
  - events.jsonl (Kafka-ready JSON lines)
  - inventory_snapshots.csv (optional snapshot style inputs)
"""
from __future__ import annotations

import csv
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List


OUT_DIR = Path(__file__).resolve().parent
NOW = datetime(2026, 4, 8, 10, 45, 0, tzinfo=timezone.utc)

random.seed(42)


FC_IDS = ["FC-01", "FC-02", "FC-03"]
REGIONS = {"FC-01": "US-WEST", "FC-02": "US-CENTRAL", "FC-03": "US-EAST"}
SKUS = [
    "SKU-18420",
    "SKU-55219",
    "SKU-00983",
    "SKU-77120",
    "SKU-33991",
    "SKU-66402",
    "SKU-24511",
    "SKU-88012",
    "SKU-54001",
    "SKU-61770",
]


@dataclass
class InventoryState:
    available_qty: int
    allocated_qty: int
    atp_qty: int


def iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def new_order_id(n: int) -> str:
    return f"ORD-{n:06d}"


def emit_event(
    events: List[dict],
    event_type: str,
    event_ts: datetime,
    sku_id: str,
    fc_id: str,
    order_id: str | None = None,
    qty: int | None = None,
    available_qty: int | None = None,
    allocated_qty: int | None = None,
    atp_qty: int | None = None,
    inventory_snapshot_id: str | None = None,
    reason: str | None = None,
) -> None:
    events.append(
        {
            "event_type": event_type,
            "event_ts": iso(event_ts),
            "sku_id": sku_id,
            "fc_id": fc_id,
            "region": REGIONS[fc_id],
            "order_id": order_id,
            "qty": qty,
            "available_qty": available_qty,
            "allocated_qty": allocated_qty,
            "atp_qty": atp_qty,
            "inventory_snapshot_id": inventory_snapshot_id,
            "reason": reason,
        }
    )


def generate_events() -> List[dict]:
    events: List[dict] = []
    inventory: Dict[str, Dict[str, InventoryState]] = {}

    # Initialize inventory
    for fc_id in FC_IDS:
        inventory[fc_id] = {}
        for sku in SKUS:
            available = random.randint(20, 120)
            allocated = random.randint(0, 10)
            atp = max(available - allocated, 0)
            inventory[fc_id][sku] = InventoryState(available, allocated, atp)
            emit_event(
                events,
                "STOCK_UPDATE",
                NOW - timedelta(minutes=30),
                sku,
                fc_id,
                available_qty=available,
                allocated_qty=allocated,
                atp_qty=atp,
                inventory_snapshot_id="SNAP-INIT",
            )

    # Generate orders
    order_count = 160
    for i in range(order_count):
        order_id = new_order_id(i + 1)
        fc_id = random.choice(FC_IDS)
        sku = random.choice(SKUS)
        qty = random.randint(1, 5)
        created_ts = NOW - timedelta(minutes=random.randint(0, 60))

        state = inventory[fc_id][sku]
        emit_event(
            events,
            "ORDER_CREATED",
            created_ts,
            sku,
            fc_id,
            order_id=order_id,
            qty=qty,
            available_qty=state.available_qty,
            allocated_qty=state.allocated_qty,
            atp_qty=state.atp_qty,
        )

        # Allocation decision
        if state.atp_qty >= qty:
            # Allocate after a short delay
            alloc_ts = created_ts + timedelta(minutes=random.randint(1, 8))
            state.allocated_qty += qty
            state.atp_qty = max(state.available_qty - state.allocated_qty, 0)
            emit_event(
                events,
                "ALLOCATED",
                alloc_ts,
                sku,
                fc_id,
                order_id=order_id,
                qty=qty,
                available_qty=state.available_qty,
                allocated_qty=state.allocated_qty,
                atp_qty=state.atp_qty,
            )
        else:
            backorder_ts = created_ts + timedelta(minutes=random.randint(1, 5))
            emit_event(
                events,
                "BACKORDERED",
                backorder_ts,
                sku,
                fc_id,
                order_id=order_id,
                qty=qty,
                available_qty=state.available_qty,
                allocated_qty=state.allocated_qty,
                atp_qty=state.atp_qty,
                reason="INSUFFICIENT_ATP",
            )

        # Occasional inventory adjustments
        if random.random() < 0.2:
            adjust = random.randint(-8, 12)
            state.available_qty += adjust
            # allow a few negatives for accuracy monitoring
            state.atp_qty = max(state.available_qty - state.allocated_qty, 0)
            adj_ts = created_ts + timedelta(minutes=random.randint(1, 10))
            emit_event(
                events,
                "INVENTORY_ADJUSTMENT",
                adj_ts,
                sku,
                fc_id,
                qty=adjust,
                available_qty=state.available_qty,
                allocated_qty=state.allocated_qty,
                atp_qty=state.atp_qty,
                reason="CYCLE_COUNT",
            )

        # Occasional stock update snapshots
        if random.random() < 0.15:
            snap_ts = created_ts + timedelta(minutes=random.randint(2, 12))
            emit_event(
                events,
                "STOCK_UPDATE",
                snap_ts,
                sku,
                fc_id,
                available_qty=state.available_qty,
                allocated_qty=state.allocated_qty,
                atp_qty=state.atp_qty,
                inventory_snapshot_id=f"SNAP-{fc_id}-{snap_ts.strftime('%H%M')}",
            )

    # Force a few stock-out events
    for fc_id in FC_IDS:
        sku = random.choice(SKUS)
        state = inventory[fc_id][sku]
        state.available_qty = 0
        state.atp_qty = 0
        emit_event(
            events,
            "STOCK_UPDATE",
            NOW - timedelta(minutes=random.randint(1, 10)),
            sku,
            fc_id,
            available_qty=state.available_qty,
            allocated_qty=state.allocated_qty,
            atp_qty=state.atp_qty,
            inventory_snapshot_id=f"SNAP-OUT-{fc_id}",
        )

    # Shuffle to simulate out-of-order arrivals
    random.shuffle(events)
    return events


def write_events(events: List[dict]) -> None:
    out_path = OUT_DIR / "events.jsonl"
    with out_path.open("w", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")


def write_snapshots(events: List[dict]) -> None:
    out_path = OUT_DIR / "inventory_snapshots.csv"
    headers = [
        "inventory_snapshot_id",
        "event_ts",
        "sku_id",
        "fc_id",
        "region",
        "available_qty",
        "allocated_qty",
        "atp_qty",
    ]
    rows = [
        e
        for e in events
        if e["event_type"] == "STOCK_UPDATE" and e.get("inventory_snapshot_id")
    ]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for e in rows:
            w.writerow({h: e.get(h) for h in headers})


def main() -> None:
    events = generate_events()
    write_events(events)
    write_snapshots(events)
    print(f"Wrote {len(events)} events to {OUT_DIR / 'events.jsonl'}")
    print(f"Wrote inventory snapshots to {OUT_DIR / 'inventory_snapshots.csv'}")


if __name__ == "__main__":
    main()
