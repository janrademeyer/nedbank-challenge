"""
Pipeline entry point.

Orchestrates the medallion stages in order:
  1. Ingest  — reads raw source files into Bronze layer Delta tables
  2. Transform — cleans and conforms Bronze into Silver layer Delta tables
  3. Provision — joins and aggregates Silver into Gold layer Delta tables

When ``pipeline_stage`` is ``"3"`` (see pipeline_config.yaml), run_stream_ingestion
runs after batch Gold: polls ``/data/stream`` for JSONL micro-batches and writes
``stream_gold`` Delta tables. DQ report generation still runs last so counts and
timing reflect batch + stream together (stage ``"1"`` skips DQ entirely).

The scoring system invokes this file directly:
  docker run ... python pipeline/run_all.py

Do not add interactive prompts, argument parsing that blocks execution,
or any code that reads from stdin. The container has no TTY attached.
"""

import logging
import os
import time
from datetime import datetime, timezone

# Before PySpark starts the JVM (docker --network=none has no DNS for the container hostname).
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

from pipeline.dq_report import write_dq_report
from pipeline.helper import build_spark_session, load_config
from pipeline.ingest import run_ingestion
from pipeline.provision import run_provisioning
from pipeline.transform import run_transformation

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    wall_start = time.perf_counter()
    run_started_utc = datetime.now(timezone.utc)

    config = load_config()
    spark = build_spark_session(config)

    logger.info("=== Stage: Bronze ingestion ===")
    run_ingestion(spark, config)

    logger.info("=== Stage: Silver transformation ===")
    run_transformation(spark, config)

    logger.info("=== Stage: Gold provisioning ===")
    run_provisioning(spark, config)

    pipeline_stage = str(config.get("pipeline_stage", "3"))
    # Lazy import keeps Stage 1–2 cold paths free of stream_ingest (and Delta merge paths) until needed.
    if pipeline_stage == "3":
        logger.info("=== Stage: Stream ingestion (Stage 3) ===")
        from pipeline.stream_ingest import run_stream_ingestion as run_stream

        run_stream(spark, config)

    duration_sec = int(round(time.perf_counter() - wall_start))
    # DQ after stream so execution_seconds and Gold/stream counts match panel expectations for Stage 3.
    if pipeline_stage != "1":
        logger.info("=== DQ report (stage %s) ===", pipeline_stage)
        write_dq_report(
            spark,
            config,
            run_started_utc=run_started_utc,
            execution_seconds=duration_sec,
            stage=pipeline_stage,
        )
