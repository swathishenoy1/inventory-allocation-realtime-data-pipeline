#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


EVENT_SCHEMA = StructType(
    [
        StructField("event_type", StringType(), False),
        StructField("event_ts", StringType(), False),
        StructField("sku_id", StringType(), False),
        StructField("fc_id", StringType(), False),
        StructField("region", StringType(), False),
        StructField("order_id", StringType(), True),
        StructField("qty", IntegerType(), True),
        StructField("available_qty", IntegerType(), True),
        StructField("allocated_qty", IntegerType(), True),
        StructField("atp_qty", IntegerType(), True),
        StructField("inventory_snapshot_id", StringType(), True),
        StructField("reason", StringType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute dashboard-ready inventory allocation aggregates."
    )
    parser.add_argument(
        "--source",
        choices=["jsonl", "kafka"],
        default="jsonl",
        help="Read from local JSONL seed data or Kafka. Default: jsonl",
    )
    parser.add_argument(
        "--input",
        default="seed_data/events.jsonl",
        help="JSONL input path when --source=jsonl. Default: seed_data/events.jsonl",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers when --source=kafka. Default: localhost:9092",
    )
    parser.add_argument(
        "--topic",
        default="inventory_events",
        help="Kafka topic when --source=kafka. Default: inventory_events",
    )
    parser.add_argument(
        "--output",
        default="data/aggregates",
        help="Output directory for aggregate tables. Default: data/aggregates",
    )
    parser.add_argument(
        "--checkpoint",
        default="data/checkpoints/inventory_metrics",
        help="Checkpoint directory for streaming mode.",
    )
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("inventory-allocation-aggregates")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def normalize_events(raw_events: DataFrame) -> DataFrame:
    return raw_events.withColumn("event_time", F.to_timestamp("event_ts")).withColumn(
        "event_date", F.to_date("event_time")
    )


def read_jsonl(spark: SparkSession, input_path: str) -> DataFrame:
    return normalize_events(spark.read.schema(EVENT_SCHEMA).json(input_path))


def read_kafka(spark: SparkSession, bootstrap_servers: str, topic: str) -> DataFrame:
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = kafka_df.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("event")
    ).select("event.*")
    return normalize_events(parsed).withWatermark("event_time", "15 minutes")


def stock_outs(events: DataFrame) -> DataFrame:
    return (
        events.where(
            (F.col("event_type") == "STOCK_UPDATE") & (F.col("available_qty") == 0)
        )
        .groupBy("sku_id", "fc_id", "region")
        .agg(
            F.count("*").alias("stock_out_count"),
            F.max("event_time").alias("last_stock_out_time"),
        )
        .orderBy(F.desc("stock_out_count"), F.desc("last_stock_out_time"))
    )


def backorders(events: DataFrame) -> DataFrame:
    return (
        events.where(F.col("event_type") == "BACKORDERED")
        .groupBy("sku_id", "fc_id", "region")
        .agg(
            F.sum(F.coalesce(F.col("qty"), F.lit(0))).alias("backordered_qty"),
            F.countDistinct("order_id").alias("orders_affected"),
            F.max("event_time").alias("last_backorder_time"),
        )
        .orderBy(F.desc("backordered_qty"), F.desc("orders_affected"))
    )


def inventory_accuracy(events: DataFrame) -> DataFrame:
    return (
        events.groupBy("sku_id", "fc_id", "region")
        .agg(
            F.sum(F.when(F.col("available_qty") < 0, 1).otherwise(0)).alias(
                "negative_inventory_count"
            ),
            F.sum(
                F.when(
                    F.abs(
                        F.coalesce(F.col("available_qty"), F.lit(0))
                        - F.coalesce(F.col("atp_qty"), F.lit(0))
                        - F.coalesce(F.col("allocated_qty"), F.lit(0))
                    )
                    > 5,
                    1,
                ).otherwise(0)
            ).alias("mismatch_count"),
        )
        .where(
            (F.col("negative_inventory_count") > 0) | (F.col("mismatch_count") > 0)
        )
        .orderBy(F.desc("negative_inventory_count"), F.desc("mismatch_count"))
    )


def order_allocation_pairs(events: DataFrame) -> DataFrame:
    orders = events.where(F.col("event_type") == "ORDER_CREATED").select(
        "order_id",
        F.col("sku_id").alias("order_sku_id"),
        F.col("fc_id").alias("order_fc_id"),
        F.col("region").alias("order_region"),
        F.col("qty").alias("order_qty"),
        F.col("atp_qty").alias("order_atp_qty"),
        F.col("event_time").alias("order_created_time"),
    )
    allocations = events.where(F.col("event_type") == "ALLOCATED").select(
        "order_id", F.col("event_time").alias("allocated_time")
    )

    return (
        orders.join(allocations, on="order_id", how="left")
        .withColumn(
            "allocation_lag_minutes",
            (F.col("allocated_time").cast("long") - F.col("order_created_time").cast("long"))
            / 60.0,
        )
        .withColumn(
            "allocated_within_10_min",
            F.col("allocation_lag_minutes").between(0, 10),
        )
        .withColumn("atp_available_at_order", F.col("order_atp_qty") >= F.col("order_qty"))
    )


def allocation_success(events: DataFrame) -> DataFrame:
    pairs = order_allocation_pairs(events)
    return (
        pairs.groupBy(
            F.window("order_created_time", "5 minutes").alias("bucket"),
            "order_fc_id",
            "order_region",
        )
        .agg(
            F.count("*").alias("orders_created"),
            F.sum(F.when(F.col("allocated_within_10_min"), 1).otherwise(0)).alias(
                "orders_allocated_within_10_min"
            ),
        )
        .withColumn(
            "allocation_success_rate",
            F.round(
                F.col("orders_allocated_within_10_min") / F.col("orders_created") * 100,
                2,
            ),
        )
        .select(
            F.col("bucket.start").alias("bucket_start"),
            F.col("bucket.end").alias("bucket_end"),
            "order_fc_id",
            "order_region",
            "orders_created",
            "orders_allocated_within_10_min",
            "allocation_success_rate",
        )
    )


def allocation_lag(events: DataFrame) -> DataFrame:
    return (
        order_allocation_pairs(events)
        .where(F.col("atp_available_at_order") & F.col("allocated_time").isNotNull())
        .groupBy("order_sku_id", "order_fc_id", "order_region")
        .agg(
            F.count("*").alias("allocated_orders"),
            F.round(F.avg("allocation_lag_minutes"), 2).alias("avg_lag_minutes"),
            F.expr("percentile_approx(allocation_lag_minutes, 0.95)").alias(
                "p95_lag_minutes"
            ),
        )
        .orderBy(F.desc("p95_lag_minutes"))
    )


def write_batch_aggregates(events: DataFrame, output: str) -> None:
    output_dir = Path(output)
    aggregates = {
        "stock_outs": stock_outs(events),
        "backorders": backorders(events),
        "inventory_accuracy": inventory_accuracy(events),
        "allocation_success_5m": allocation_success(events),
        "allocation_lag": allocation_lag(events),
    }

    for name, aggregate in aggregates.items():
        aggregate.write.mode("overwrite").parquet(str(output_dir / name))


def write_streaming_raw(events: DataFrame, output: str, checkpoint: str):
    return (
        events.writeStream.format("parquet")
        .option("path", str(Path(output) / "raw_events"))
        .option("checkpointLocation", checkpoint)
        .partitionBy("event_date", "event_type")
        .outputMode("append")
        .start()
    )


def main() -> int:
    args = parse_args()
    spark = build_spark()

    if args.source == "jsonl":
        events = read_jsonl(spark, args.input)
        write_batch_aggregates(events, args.output)
        return 0

    events = read_kafka(spark, args.bootstrap_servers, args.topic)
    query = write_streaming_raw(events, args.output, args.checkpoint)
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
