"""
Gold layer: Join and aggregate Silver tables into the scored output schema.

Input paths (Silver layer output — read these, do not modify):
  /data/output/silver/accounts/
  /data/output/silver/transactions/
  /data/output/silver/customers/

Output paths (your pipeline must create these directories):
  /data/output/gold/fact_transactions/     — 15 fields (see output_schema_spec.md §2)
  /data/output/gold/dim_accounts/          — 11 fields (see output_schema_spec.md §3)
  /data/output/gold/dim_customers/         — 9 fields  (see output_schema_spec.md §4)

Requirements:
  - Generate surrogate keys (_sk fields) that are unique, non-null, and stable
    across pipeline re-runs on the same input data. Use row_number() with a
    stable ORDER BY on the natural key, or sha2(natural_key, 256) cast to BIGINT.
  - Resolve all foreign key relationships:
      fact_transactions.account_sk  → dim_accounts.account_sk
      fact_transactions.customer_sk → dim_customers.customer_sk
      dim_accounts.customer_id      → dim_customers.customer_id
  - Rename accounts.customer_ref → dim_accounts.customer_id at this layer.
  - Derive dim_customers.age_band from dob (do not copy dob directly).
  - Write each table as a Delta Parquet table.
  - Do not hardcode file paths — read from config/pipeline_config.yaml.
  - At Stage 2, also write /data/output/dq_report.json summarising DQ outcomes.

See output_schema_spec.md for the complete field-by-field specification.
"""

from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def _sk_expr_natural(col):
    """Stable BIGINT surrogate from a natural key (Spark xxhash64)."""
    # Coalesce avoids null literal keys collapsing onto one SK in degenerate rows.
    return F.xxhash64(F.coalesce(col.cast("string"), F.lit("__NULL__"))).cast("long")


def _age_band_from_dob(dob_col):
    """output_schema_spec.md §4 — floor((run_date - dob) / 365.25)."""
    age = F.floor(F.datediff(F.current_date(), dob_col) / F.lit(365.25)).cast("int")
    return (
        F.when(age >= 65, F.lit("65+"))
        .when(age >= 56, F.lit("56-65"))
        .when(age >= 46, F.lit("46-55"))
        .when(age >= 36, F.lit("36-45"))
        .when(age >= 26, F.lit("26-35"))
        .when(age >= 18, F.lit("18-25"))
        .otherwise(F.lit(None).cast("string"))
    )


def _transaction_timestamp_expr():
    """Combine Silver transaction_date (DATE) + transaction_time (STRING)."""
    date_part = F.date_format(F.col("transaction_date"), "yyyy-MM-dd")
    time_raw = F.trim(F.col("transaction_time").cast("string"))
    time_part = F.when(time_raw.isNull() | (time_raw == F.lit("")), F.lit("00:00:00")).otherwise(
        time_raw
    )
    combined = F.concat_ws(" ", date_part, time_part)
    parsed = F.to_timestamp(combined, "yyyy-MM-dd HH:mm:ss")
    # Fallback keeps TIMESTAMP NOT NULL when parse fails but dq_flag may flag DATE_FORMAT.
    return F.when(parsed.isNotNull(), parsed).otherwise(F.col("ingestion_timestamp"))


def _write_gold_delta(df, path: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )


def run_provisioning(spark: SparkSession, config: dict) -> None:
    out_cfg = config["output"]
    silver_base = out_cfg["silver_path"]
    gold_base = out_cfg["gold_path"]

    logger.info("Gold: reading Silver tables from %s", silver_base)
    silver_customers = spark.read.format("delta").load(os.path.join(silver_base, "customers"))
    silver_accounts = spark.read.format("delta").load(os.path.join(silver_base, "accounts"))
    silver_tx = spark.read.format("delta").load(os.path.join(silver_base, "transactions"))

    # Stage 1 JSON may omit merchant_subcategory entirely — Silver may omit column too.
    subcat = (
        F.col("merchant_subcategory")
        if "merchant_subcategory" in silver_tx.columns
        else F.lit(None).cast("string")
    )

    logger.info("Gold: building dim_customers")
    # No raw dob in Gold — age_band only (output_schema_spec §4).
    dim_customers = silver_customers.select(
        _sk_expr_natural(F.col("customer_id")).alias("customer_sk"),
        F.col("customer_id"),
        F.col("gender"),
        F.col("province"),
        F.col("income_band"),
        F.col("segment"),
        F.col("risk_score").cast("int").alias("risk_score"),
        F.col("kyc_status"),
        _age_band_from_dob(F.col("dob").cast("date")).alias("age_band"),
    )

    logger.info("Gold: building dim_accounts")
    dim_accounts = silver_accounts.select(
        _sk_expr_natural(F.col("account_id")).alias("account_sk"),
        F.col("account_id"),
        F.col("customer_ref").alias("customer_id"),
        F.col("account_type"),
        F.col("account_status"),
        F.col("open_date").cast("date").alias("open_date"),
        F.col("product_tier"),
        F.col("digital_channel"),
        F.col("credit_limit").cast("decimal(18,2)").alias("credit_limit"),
        F.col("current_balance").cast("decimal(18,2)").alias("current_balance"),
        F.col("last_activity_date").cast("date").alias("last_activity_date"),
    )

    dim_acc_keys = dim_accounts.select("account_sk", "account_id", "customer_id")
    dim_cust_keys = dim_customers.select("customer_sk", "customer_id")

    logger.info("Gold: building fact_transactions (inner join → orphans excluded)")
    # Inner joins enforce FK integrity for scoring; ORPHANED rows stay out of facts.
    tx_ok = silver_tx.join(dim_acc_keys, on="account_id", how="inner").join(
        dim_cust_keys,
        on="customer_id",
        how="inner",
    )

    ts_expr = _transaction_timestamp_expr()

    fact_transactions = tx_ok.select(
        _sk_expr_natural(F.col("transaction_id")).alias("transaction_sk"),
        F.col("transaction_id"),
        F.col("account_sk"),
        F.col("customer_sk"),
        F.col("transaction_date").cast("date").alias("transaction_date"),
        ts_expr.alias("transaction_timestamp"),
        F.upper(F.trim(F.col("transaction_type").cast("string"))).alias("transaction_type"),
        F.col("merchant_category"),
        subcat.alias("merchant_subcategory"),
        F.col("amount").cast("decimal(18,2)").alias("amount"),
        # Literal ZAR satisfies currency conformance check; variants already flagged in Silver.
        F.lit("ZAR").alias("currency"),
        F.upper(F.trim(F.col("channel").cast("string"))).alias("channel"),
        F.col("province"),
        F.col("dq_flag"),
        F.col("ingestion_timestamp").cast("timestamp").alias("ingestion_timestamp"),
    )

    gold_fact = os.path.join(gold_base, "fact_transactions")
    gold_dim_a = os.path.join(gold_base, "dim_accounts")
    gold_dim_c = os.path.join(gold_base, "dim_customers")

    # Write dims before facts — readable ops order; Delta has no FK constraint enforcement.
    logger.info("Gold: writing dim_customers → %s", gold_dim_c)
    _write_gold_delta(dim_customers, gold_dim_c)

    logger.info("Gold: writing dim_accounts → %s", gold_dim_a)
    _write_gold_delta(dim_accounts, gold_dim_a)

    logger.info("Gold: writing fact_transactions → %s", gold_fact)
    _write_gold_delta(fact_transactions, gold_fact)
