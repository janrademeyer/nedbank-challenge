"""
Stage 3 — Poll `/data/stream/` for JSONL micro-batches, upsert `stream_gold` Delta tables.

Contract (summarised from docs):
  • Discover `stream_*.jsonl` in lexicographic order; track processed paths so files are not replayed.
  • `current_balances`: MERGE upsert on account_id; seed balance from batch Gold dim_accounts on first insert.
  • `recent_transactions`: MERGE on (account_id, transaction_id), then prune to latest 50 rows per account.
  • `updated_at` uses current_timestamp() at merge time — keeps SLA (≤300s vs event ts) practical under normal runs.

See docs/stream_interface_spec.md and docs/stage3_spec_addendum.md.
"""

from __future__ import annotations

import logging
import os
import time
from glob import glob
from typing import Any

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType, StructField, StructType, TimestampType
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


def _stream_cfg(config: dict) -> dict[str, Any]:
    """Normalised `streaming` block from pipeline_config.yaml."""
    return config.get("streaming") or {}


def _balance_delta_expr():
    """Signed delta applied to running balance
    """
    tt = F.upper(F.trim(F.col("transaction_type").cast("string")))
    amt = F.col("amount").cast("decimal(18,2)")
    return (
        F.when(tt == "CREDIT", amt)
        .when(tt == "DEBIT", -amt)
        .when(tt == "FEE", -amt)
        .when(tt == "REVERSAL", amt)
        .otherwise(F.lit(0).cast("decimal(18,2)"))
    )


def _event_timestamp_expr():
    """Event time for SLA + ordering — aligned with provision._transaction_timestamp_expr behaviour."""
    date_part = F.date_format(F.col("transaction_date").cast("date"), "yyyy-MM-dd")
    time_raw = F.trim(F.col("transaction_time").cast("string"))
    time_part = F.when(time_raw.isNull() | (time_raw == F.lit("")), F.lit("00:00:00")).otherwise(
        time_raw
    )
    combined = F.concat_ws(" ", date_part, time_part)
    parsed = F.to_timestamp(combined, "yyyy-MM-dd HH:mm:ss")
    return F.when(parsed.isNotNull(), parsed).otherwise(F.current_timestamp())


def _ensure_delta_table(spark: SparkSession, path: str, schema: StructType) -> None:
    """Empty Delta target so MERGE always has a table (avoids branch-on-first-write in callers)."""
    if DeltaTable.isDeltaTable(spark, path):
        return
    parent = os.path.dirname(path.rstrip("/"))
    if parent:
        os.makedirs(parent, mode=0o755, exist_ok=True)
    # emptyRDD + explicit schema avoids createDataFrame([], ...) which pulls pandas/NumPy in PySpark.
    spark.createDataFrame(spark.sparkContext.emptyRDD(), schema).write.format("delta").save(path)


def _prepare_events(spark: SparkSession, file_path: str, dim_accounts_path: str):
    """Parse one JSONL micro-batch; join Gold dim_accounts for seed_balance + FK filter."""
    raw = spark.read.json(file_path)
    if raw.rdd.isEmpty():
        return None

    ev = (
        raw.withColumn("_ts", _event_timestamp_expr())
        .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
        .withColumn("transaction_type", F.upper(F.trim(F.col("transaction_type").cast("string"))))
        .filter(F.col("amount").isNotNull())
        .filter(
            F.col("transaction_type").isin("DEBIT", "CREDIT", "FEE", "REVERSAL")
            & F.col("account_id").isNotNull()
            & (F.trim(F.col("account_id").cast("string")) != F.lit(""))
            & F.col("transaction_id").isNotNull()
        )
        .withColumn("delta_amt", _balance_delta_expr())  # per-row signed amount for balance roll-up
    )

    dim = spark.read.format("delta").load(dim_accounts_path).select(
        F.col("account_id"),
        F.col("current_balance").cast("decimal(18,2)").alias("seed_balance"),
    )
    # Valid accounts only (stream_interface_spec §8).
    return ev.join(dim, on="account_id", how="inner")


def _merge_current_balances(spark: SparkSession, path: str, agg_batch) -> None:
    """Per-file aggregate: one row per account_id with sum(delta), max(ts), first(seed_balance)."""
    agg_batch.createOrReplaceTempView("_stream_bal_updates")
    spark.sql(
        f"""
        MERGE INTO delta.`{path}` AS t
        USING _stream_bal_updates AS s
        ON t.account_id = s.account_id
        WHEN MATCHED THEN UPDATE SET
          current_balance = t.current_balance + s.batch_delta,
          last_transaction_timestamp = greatest(t.last_transaction_timestamp, s.batch_max_ts),
          updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (account_id, current_balance, last_transaction_timestamp, updated_at)
        VALUES (s.account_id, s.seed_balance + s.batch_delta, s.batch_max_ts, current_timestamp())
        """
    )


def _merge_recent_transactions(spark: SparkSession, path: str, recent_rows) -> None:
    """Upsert by (account_id, transaction_id); if Delta does not exist yet (race), append-create from this batch."""
    recent_rows.createOrReplaceTempView("_stream_recent_src")
    if DeltaTable.isDeltaTable(spark, path):
        spark.sql(
            f"""
            MERGE INTO delta.`{path}` AS t
            USING _stream_recent_src AS s
            ON t.account_id = s.account_id AND t.transaction_id = s.transaction_id
            WHEN MATCHED THEN UPDATE SET
              transaction_timestamp = s.transaction_timestamp,
              amount = s.amount,
              transaction_type = s.transaction_type,
              channel = s.channel,
              updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT (
              account_id, transaction_id, transaction_timestamp,
              amount, transaction_type, channel, updated_at
            ) VALUES (
              s.account_id, s.transaction_id, s.transaction_timestamp,
              s.amount, s.transaction_type, s.channel, s.updated_at
            )
            """
        )
    else:
        recent_rows.write.format("delta").save(path)


def _prune_recent_to_top50(spark: SparkSession, path: str) -> None:
    """Retention: output_schema_spec §6 — keep 50 newest rows per account (overwrite rewrite)."""
    df = spark.read.format("delta").load(path)
    if df.rdd.isEmpty():
        return
    w = Window.partitionBy("account_id").orderBy(F.col("transaction_timestamp").desc())
    pruned = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") <= 50).drop("_rn")
    pruned.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)


def _list_pending(stream_dir: str, processed: set[str]) -> list[str]:
    """Filename order matches chronological delivery per stream_interface_spec §1."""
    pattern = os.path.join(stream_dir, "stream_*.jsonl")
    all_files = sorted(glob(pattern))
    return [p for p in all_files if p not in processed]


def _load_processed(state_path: str) -> set[str]:
    """Resume-safe processed list (survives only if same /tmp volume — acceptable for challenge scope)."""
    if not os.path.isfile(state_path):
        return set()
    with open(state_path, encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def _append_processed(state_path: str, paths: list[str]) -> None:
    """Append absolute paths after successful merge so poll loop skips replays."""
    if not paths:
        return
    d = os.path.dirname(state_path)
    if d:
        os.makedirs(d, mode=0o755, exist_ok=True)
    with open(state_path, "a", encoding="utf-8") as f:
        for p in paths:
            f.write(p + "\n")


def run_stream_ingestion(spark: SparkSession, config: dict) -> None:
    """Poll loop: process new stream files until idle exceeds quiesce_timeout (not the 5‑min row SLA)."""
    scfg = _stream_cfg(config)
    stream_dir = str(scfg.get("stream_input_path") or "/data/stream").rstrip("/")
    gold_root = str(scfg.get("stream_gold_path") or "/data/output/stream_gold").rstrip("/")
    poll_interval = float(scfg.get("poll_interval_seconds", 10))
    quiesce_timeout = float(scfg.get("quiesce_timeout_seconds", 60))
    state_path = str(scfg.get("processed_state_path") or "/tmp/stream_processed_files.txt")

    out_cfg = config["output"]
    dim_accounts_path = os.path.join(out_cfg["gold_path"], "dim_accounts")

    bal_path = os.path.join(gold_root, "current_balances")
    rt_path = os.path.join(gold_root, "recent_transactions")

    # Fixed schemas match output_schema_spec.md §5–6 (Delta readable by DuckDB/PySpark).
    bal_schema = StructType(
        [
            StructField("account_id", StringType(), False),
            StructField("current_balance", DecimalType(18, 2), False),
            StructField("last_transaction_timestamp", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
        ]
    )
    rt_schema = StructType(
        [
            StructField("account_id", StringType(), False),
            StructField("transaction_id", StringType(), False),
            StructField("transaction_timestamp", TimestampType(), False),
            StructField("amount", DecimalType(18, 2), False),
            StructField("transaction_type", StringType(), False),
            StructField("channel", StringType(), True),
            StructField("updated_at", TimestampType(), False),
        ]
    )

    os.makedirs(gold_root, mode=0o755, exist_ok=True)
    _ensure_delta_table(spark, bal_path, bal_schema)
    _ensure_delta_table(spark, rt_path, rt_schema)

    processed = _load_processed(state_path)
    idle_seconds = 0.0  # accumulates only on empty polls (see quiesce vs SLA in stage3 docs)

    logger.info(
        "Stream: polling %s → gold %s (poll=%ss quiesce=%ss)",
        stream_dir,
        gold_root,
        poll_interval,
        quiesce_timeout,
    )

    while True:
        pending = _list_pending(stream_dir, processed)
        if pending:
            idle_seconds = 0.0
            # Process every newly discovered file this cycle (spec: ascending filename order).
            for fp in pending:
                logger.info("Stream: processing %s", fp)
                events = _prepare_events(spark, fp, dim_accounts_path)
                if events is None or events.rdd.isEmpty():
                    processed.add(fp)
                    continue

                # Balance upsert: commutative sum within file + greatest(ts) for last_transaction_timestamp.
                agg = events.groupBy("account_id").agg(
                    F.sum("delta_amt").alias("batch_delta"),
                    F.max("_ts").alias("batch_max_ts"),
                    F.first("seed_balance").alias("seed_balance"),
                )
                _merge_current_balances(spark, bal_path, agg)

                # SLA-friendly write time (processing latency); distinct from event _ts.
                now_ts = F.current_timestamp()
                recent_rows = events.select(
                    F.col("account_id"),
                    F.col("transaction_id"),
                    F.col("_ts").alias("transaction_timestamp"),
                    F.col("amount"),
                    F.col("transaction_type"),
                    F.col("channel").cast("string").alias("channel"),
                    now_ts.alias("updated_at"),
                ).dropDuplicates(["account_id", "transaction_id"])

                _merge_recent_transactions(spark, rt_path, recent_rows)
                _prune_recent_to_top50(spark, rt_path)

                processed.add(fp)
                _append_processed(state_path, [fp])
        else:
            # No unseen files: creep idle toward quiesce then exit (container must not loop forever).
            idle_seconds += poll_interval
            if idle_seconds >= quiesce_timeout:
                logger.info("Stream: quiescent %.0fs — exiting poll loop", idle_seconds)
                break

        time.sleep(poll_interval)  # spacing between directory listings (stream_interface_spec §3)
