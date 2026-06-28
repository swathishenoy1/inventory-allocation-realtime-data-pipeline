#!/usr/bin/env python3
import argparse
import json
import sys
import time

try:
    from kafka import KafkaProducer
except Exception as exc:  # pragma: no cover - runtime import guard
    print(
        "Missing dependency: kafka-python\n"
        "Install with: pip install kafka-python\n"
        f"Original error: {exc}",
        file=sys.stderr,
    )
    sys.exit(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream JSONL events to Kafka at a controlled rate."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (comma-separated). Default: localhost:9092",
    )
    parser.add_argument("--topic", required=True, help="Kafka topic name")
    parser.add_argument(
        "--file",
        default="seed_data/events.jsonl",
        help="Path to JSONL events file",
    )
    parser.add_argument(
        "--events-per-second",
        type=float,
        default=5.0,
        help="Send rate. Use 0 for no throttling. Default: 5",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Loop over the file continuously",
    )
    parser.add_argument(
        "--loop-delay",
        type=float,
        default=2.0,
        help="Seconds to wait between loops. Default: 2",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Stop after N events (0 = no limit)",
    )
    parser.add_argument(
        "--key-field",
        default="order_id",
        help="JSON field to use as Kafka message key (default: order_id)",
    )
    return parser.parse_args()


def iter_events(path: str):
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def main() -> int:
    args = parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        linger_ms=5,
    )

    sent = 0
    while True:
        start_time = time.monotonic()
        for event in iter_events(args.file):
            key_value = event.get(args.key_field)
            key = str(key_value) if key_value is not None else None
            producer.send(args.topic, key=key, value=event)
            sent += 1

            if args.max_events and sent >= args.max_events:
                producer.flush()
                return 0

            if args.events_per_second and args.events_per_second > 0:
                elapsed = time.monotonic() - start_time
                expected = sent / args.events_per_second
                sleep_for = max(0.0, expected - elapsed)
                if sleep_for > 0:
                    time.sleep(sleep_for)

        producer.flush()

        if not args.loop:
            break
        time.sleep(max(0.0, args.loop_delay))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
