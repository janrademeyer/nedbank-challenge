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

import os

# Before PySpark starts the JVM (docker --network=none has no DNS for the container hostname).
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

import yaml
import logging

from pipeline.ingest import run_ingestion
from pipeline.transform import run_transformation
from pipeline.provision import run_provisioning
from pipeline.helper import *
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":

    # Load config and build the spark session
    config = load_config()
    spark = build_spark_session(config)

    logger.info("=== Stage: Bronze ingestion ===")
    run_ingestion(spark, config)

    logger.info("=== Stage: Silver transformation ===")
    run_transformation(spark, config)

    logger.info("=== Stage: Gold provisioning ===")
    run_provisioning()
