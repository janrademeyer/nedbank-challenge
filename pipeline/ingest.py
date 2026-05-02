"""
Bronze layer: Ingest raw source data into Delta Parquet tables.

Input paths (read-only mounts — do not write here):
  /data/input/accounts.csv
  /data/input/transactions.jsonl
  /data/input/customers.csv

Output paths (your pipeline must create these directories):
  /data/output/bronze/accounts/
  /data/output/bronze/transactions/
  /data/output/bronze/customers/

Requirements:
  - Preserve source data as-is; do not transform at this layer.
  - Add an `ingestion_timestamp` column (TIMESTAMP) recording when each
    record entered the Bronze layer. Use a consistent timestamp for the
    entire ingestion run (not per-row).
  - Write each table as a Delta Parquet table (not plain Parquet).
  - Read paths from config/pipeline_config.yaml — do not hardcode paths.
  - All paths are absolute inside the container (e.g. /data/input/accounts.csv).

Spark configuration tip:
  Run Spark in local[2] mode to stay within the 2-vCPU resource constraint.
  Configure Delta Lake using the builder pattern shown in the base image docs.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

def run_ingestion(spark: SparkSession, config: dict):
    # Bronze: raw landing — CSV columns stay STRING (inferSchema=False); typing happens in Silver.
    ingestion_ts = F.lit(datetime.now()).cast("timestamp")

    inp = config["input"]
    bronze_base = config["output"]["bronze_path"]

    # Accounts
    logger.info("Bronze: accounts — reading")
    accounts_df = spark.read.csv(inp["accounts_path"], header=True, inferSchema=False).withColumn(
        "ingestion_timestamp", ingestion_ts
    )
    logger.info("Bronze: accounts — writing Delta")
    accounts_df.write.format("delta").mode("overwrite").save(os.path.join(bronze_base, "accounts"))

    # Transactions
    logger.info("Bronze: transactions — reading")
    transactions_df = spark.read.json(inp["transactions_path"]).withColumn(
        "ingestion_timestamp", ingestion_ts
    )
    logger.info("Bronze: transactions — writing Delta")
    transactions_df.write.format("delta").mode("overwrite").save(os.path.join(bronze_base, "transactions"))

    # Customers
    logger.info("Bronze: customers — reading")
    customers_df = spark.read.csv(inp["customers_path"], header=True, inferSchema=False).withColumn(
        "ingestion_timestamp", ingestion_ts
    )
    logger.info("Bronze: customers — writing Delta")
    customers_df.write.format("delta").mode("overwrite").save(os.path.join(bronze_base, "customers"))
