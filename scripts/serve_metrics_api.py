#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import date, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import duckdb


TABLES = {
    "stock-outs": {
        "path": "stock_outs",
        "order_by": "stock_out_count DESC, last_stock_out_time DESC",
    },
    "backorders": {
        "path": "backorders",
        "order_by": "backordered_qty DESC, orders_affected DESC",
    },
    "inventory-accuracy": {
        "path": "inventory_accuracy",
        "order_by": "negative_inventory_count DESC, mismatch_count DESC",
    },
    "allocation-success": {
        "path": "allocation_success_5m",
        "order_by": "bucket_start DESC, order_fc_id ASC",
    },
    "allocation-lag": {
        "path": "allocation_lag",
        "order_by": "p95_lag_minutes DESC",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Serve JSON dashboard metrics from generated Parquet aggregates."
    )
    parser.add_argument(
        "--aggregate-dir",
        default="data/aggregates",
        help="Directory containing aggregate Parquet folders. Default: data/aggregates",
    )
    parser.add_argument("--host", default="127.0.0.1", help="API host. Default: 127.0.0.1")
    parser.add_argument("--port", type=int, default=8000, help="API port. Default: 8000")
    return parser.parse_args()


def json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def response_payload(payload, status=HTTPStatus.OK) -> tuple[int, bytes]:
    return status, json.dumps(payload, default=json_default, indent=2).encode("utf-8")


class MetricsHandler(BaseHTTPRequestHandler):
    aggregate_dir: Path

    def log_message(self, format: str, *args) -> None:
        return

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)

        if parsed.path == "/health":
            self.write_json({"status": "ok", "aggregate_dir": str(self.aggregate_dir)})
            return

        if parsed.path == "/api/tables":
            self.write_json({"tables": sorted(TABLES.keys())})
            return

        if parsed.path.startswith("/api/"):
            table_name = parsed.path.removeprefix("/api/")
            self.handle_table(table_name, query)
            return

        self.write_json(
            {
                "message": "Inventory metrics API",
                "endpoints": ["/health", "/api/tables"]
                + [f"/api/{name}" for name in sorted(TABLES.keys())],
            }
        )

    def handle_table(self, table_name: str, query: dict[str, list[str]]) -> None:
        table = TABLES.get(table_name)
        if table is None:
            self.write_json(
                {"error": f"Unknown table '{table_name}'", "known_tables": sorted(TABLES)},
                HTTPStatus.NOT_FOUND,
            )
            return

        limit = self.parse_limit(query)
        if limit is None:
            return

        parquet_dir = self.aggregate_dir / table["path"]
        if not parquet_dir.exists():
            self.write_json(
                {
                    "error": "Aggregate table not found",
                    "table": table_name,
                    "path": str(parquet_dir),
                    "hint": "Run scripts/spark_aggregations.py before starting the API.",
                },
                HTTPStatus.NOT_FOUND,
            )
            return

        records = self.query_table(parquet_dir, table["order_by"], limit)
        self.write_json({"table": table_name, "count": len(records), "rows": records})

    def parse_limit(self, query: dict[str, list[str]]) -> int | None:
        raw_limit = query.get("limit", ["50"])[0]
        try:
            limit = int(raw_limit)
        except ValueError:
            self.write_json({"error": "limit must be an integer"}, HTTPStatus.BAD_REQUEST)
            return None

        if limit < 1 or limit > 500:
            self.write_json(
                {"error": "limit must be between 1 and 500"},
                HTTPStatus.BAD_REQUEST,
            )
            return None
        return limit

    def query_table(self, parquet_dir: Path, order_by: str, limit: int) -> list[dict]:
        parquet_glob = str(parquet_dir / "*.parquet")
        sql = f"""
            SELECT *
            FROM read_parquet(?)
            ORDER BY {order_by}
            LIMIT ?
        """
        with duckdb.connect(database=":memory:") as conn:
            result = conn.execute(sql, [parquet_glob, limit])
            columns = [column[0] for column in result.description]
            return [dict(zip(columns, row)) for row in result.fetchall()]

    def write_json(self, payload, status=HTTPStatus.OK) -> None:
        status_code, body = response_payload(payload, status)
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)


def main() -> int:
    args = parse_args()
    MetricsHandler.aggregate_dir = Path(args.aggregate_dir)

    server = ThreadingHTTPServer((args.host, args.port), MetricsHandler)
    print(f"Serving inventory metrics at http://{args.host}:{args.port}")
    print(f"Reading aggregates from {MetricsHandler.aggregate_dir}")
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
