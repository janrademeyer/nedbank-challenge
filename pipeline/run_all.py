"""
Pipeline entry point.

Orchestrates the three medallion architecture stages in order:
  1. Ingest  — reads raw source files into Bronze layer Delta tables
  2. Transform — cleans and conforms Bronze into Silver layer Delta tables
  3. Provision — joins and aggregates Silver into Gold layer Delta tables

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

    duration_sec = int(round(time.perf_counter() - wall_start))
    pipeline_stage = str(config.get("pipeline_stage", "2"))
    if pipeline_stage != "1":
        logger.info("=== DQ report (stage %s) ===", pipeline_stage)
        write_dq_report(
            spark,
            config,
            run_started_utc=run_started_utc,
            execution_seconds=duration_sec,
            stage=pipeline_stage,
        )
