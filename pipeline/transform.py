"""
Silver layer: Clean and conform Bronze tables into validated Silver Delta tables.

Paths, casting hints (`silver.casting`), and DQ file location (`dq.rules_path`)
live in pipeline_config.yaml. Rule definitions live in dq_rules.yaml
"""

from __future__ import annotations

import logging
import os
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


def _dq_rules_path(config: dict) -> str:
    dq_cfg = config.get("dq") or {}
    path = dq_cfg.get("rules_path") or "/data/config/dq_rules.yaml"
    if os.path.isfile(path):
        return path
    packaged = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "config", "dq_rules.yaml")
    )
    return packaged if os.path.isfile(packaged) else path


def _load_dq_rules(config: dict) -> dict[str, Any]:
    path = _dq_rules_path(config)
    logger.info("Loading DQ rules from %s", path)
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _write_silver_delta(df: DataFrame, path: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )


def _parse_multi_format_date(column_name: str):
    """ISO YYYY-MM-DD, DD/MM/YYYY (Stage 2), or Unix epoch seconds."""
    return _parse_multi_format_date_expr(F.col(column_name))


def _parse_multi_format_date_expr(col):
    c = F.trim(col.cast("string"))
    iso = F.to_date(c, "yyyy-MM-dd")
    dmy = F.coalesce(F.to_date(c, "dd/MM/yyyy"), F.to_date(c, "d/M/yyyy"))
    epoch = F.when(
        c.rlike(r"^\d{9,13}$"),
        F.to_date(F.from_unixtime(c.cast("long"))),
    )
    return F.coalesce(iso, dmy, epoch)


def _dedupe_latest(df: DataFrame, key_col: str, order_col: str) -> DataFrame:
    w = Window.partitionBy(key_col).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def _apply_customer_casting(df: DataFrame, silver_cfg: dict) -> DataFrame:
    spec = (silver_cfg.get("casting") or {}).get("customers") or {}
    out = df
    for col_name in spec.get("date_columns", ["dob"]):
        if col_name in out.columns:
            out = out.withColumn(col_name, _parse_multi_format_date(col_name))
    for col_name in spec.get("integer_columns", ["risk_score"]):
        if col_name in out.columns:
            out = out.withColumn(col_name, F.col(col_name).cast("int"))
    return out


def _apply_accounts_casting(df: DataFrame, silver_cfg: dict) -> DataFrame:
    spec = (silver_cfg.get("casting") or {}).get("accounts") or {}
    out = df
    for col_name in spec.get("date_columns", ["open_date", "last_activity_date"]):
        if col_name in out.columns:
            out = out.withColumn(col_name, _parse_multi_format_date(col_name))
    for col_name in spec.get("decimal_columns", ["credit_limit", "current_balance"]):
        if col_name in out.columns:
            out = out.withColumn(col_name, F.col(col_name).cast("decimal(18,2)"))
    return out


def _normalize_currency_exprs(dq_rules: dict[str, Any]):
    cur_rules = dq_rules.get("currency") or {}
    canonical = str(cur_rules.get("canonical", "ZAR")).upper()
    raw_map = cur_rules.get("normalise_map") or {"ZAR": "ZAR"}

    raw_disp = F.trim(F.col("currency").cast("string"))
    cur_upper = F.upper(raw_disp)
    normalized = F.lit(canonical)
    for src, tgt in raw_map.items():
        key = str(src).strip().upper()
        normalized = F.when(cur_upper == F.lit(key), F.lit(str(tgt).upper())).otherwise(
            normalized
        )
    normalized = F.when(raw_disp.isNull(), F.lit(None).cast("string")).otherwise(normalized)

    # Case-sensitive strict "ZAR" only is clean; "zar", "R", "710", etc. → CURRENCY_VARIANT.
    variant_flag = raw_disp.isNotNull() & ~raw_disp.eqNullSafe(F.lit("ZAR"))
    return normalized, variant_flag


def _transactions_flatten(bronze_tx: DataFrame) -> DataFrame:
    cols = bronze_tx.columns
    province_col = (
        F.col("location.province") if "location" in cols else F.lit(None).cast("string")
    )
    subcat = (
        F.col("merchant_subcategory")
        if "merchant_subcategory" in cols
        else F.lit(None).cast("string")
    )

    return bronze_tx.select(
        F.col("transaction_id"),
        F.col("account_id"),
        F.col("transaction_date"),
        F.col("transaction_time"),
        F.col("transaction_type"),
        F.col("merchant_category"),
        subcat.alias("merchant_subcategory"),
        F.col("amount"),
        F.col("currency"),
        F.col("channel"),
        province_col.alias("province"),
        F.col("ingestion_timestamp"),
    )


def run_transformation(spark: SparkSession, config: dict) -> None:
    dq_rules = _load_dq_rules(config)
    silver_cfg = config.get("silver") or {}
    order_col = silver_cfg.get("dedupe_order_column", "ingestion_timestamp")

    out_cfg = config["output"]
    bronze_base = out_cfg["bronze_path"]
    silver_base = out_cfg["silver_path"]

    # ── Customers ───────────────────────────────────────────────────────────
    logger.info("Silver: customers — reading Bronze")
    cust_bronze = spark.read.format("delta").load(os.path.join(bronze_base, "customers"))
    cust_work = _dedupe_latest(cust_bronze, "customer_id", order_col)
    cust_work = _apply_customer_casting(cust_work, silver_cfg)
    logger.info("Silver: customers — writing Delta")
    _write_silver_delta(cust_work, os.path.join(silver_base, "customers"))

    # ── Accounts (customer_ref must exist on Silver customers) ─────────────
    logger.info("Silver: accounts — reading Bronze")
    acct_bronze = spark.read.format("delta").load(os.path.join(bronze_base, "accounts"))
    acct_work = acct_bronze.filter(
        F.col("account_id").isNotNull()
        & (F.trim(F.col("account_id").cast("string")) != F.lit(""))
    )
    acct_work = _dedupe_latest(acct_work, "account_id", order_col)
    acct_work = _apply_accounts_casting(acct_work, silver_cfg)

    cust_keys = cust_work.select(F.col("customer_id").alias("_cust_join_key")).distinct()
    acct_work = acct_work.join(
        cust_keys,
        acct_work.customer_ref == cust_keys._cust_join_key,
        "inner",
    ).drop("_cust_join_key")

    logger.info("Silver: accounts — writing Delta")
    _write_silver_delta(acct_work, os.path.join(silver_base, "accounts"))

    # ── Transactions ──────────────────────────────────────────────────────────
    logger.info("Silver: transactions — reading Bronze")
    tx_bronze = spark.read.format("delta").load(os.path.join(bronze_base, "transactions"))
    tx_work = _dedupe_latest(tx_bronze, "transaction_id", order_col)
    tx_work = _transactions_flatten(tx_work)

    silver_cfg_tx = (silver_cfg.get("casting") or {}).get("transactions") or {}
    tx_work = tx_work.withColumn(
        "_transaction_date_raw",
        F.trim(F.col("transaction_date").cast("string")),
    )
    for col_name in silver_cfg_tx.get("date_columns") or ["transaction_date"]:
        if col_name == "transaction_date":
            tx_work = tx_work.withColumn(
                "transaction_date",
                _parse_multi_format_date_expr(F.col("_transaction_date_raw")),
            )

    tx_work = tx_work.withColumn("_amount_raw", F.col("amount")).withColumn(
        "amount",
        F.col("amount").cast("decimal(18,2)"),
    )

    tx_rules = dq_rules.get("transactions") or {}
    required = tx_rules.get("required_non_null") or []
    allowed_tt = list(tx_rules.get("transaction_type_allowed") or [])
    allowed_ch = list(tx_rules.get("channel_allowed") or [])

    null_req = None
    for field in required:
        if field not in tx_work.columns:
            continue
        if field == "transaction_date":
            cond = F.col("_transaction_date_raw").isNull() | (
                F.col("_transaction_date_raw") == F.lit("")
            )
        else:
            cond = F.col(field).isNull() | (
                F.trim(F.col(field).cast("string")) == F.lit("")
            )
        null_req = cond if null_req is None else (null_req | cond)

    raw_amt_str = F.trim(F.col("_amount_raw").cast("string"))
    type_mismatch_amt = (
        F.col("_amount_raw").isNotNull()
        & (raw_amt_str != F.lit(""))
        & F.col("amount").isNull()
    )

    date_bad = (
        ~F.col("_transaction_date_raw").isNull()
        & (F.col("_transaction_date_raw") != F.lit(""))
        & F.col("transaction_date").isNull()
    )

    tt_upper = F.upper(F.trim(F.col("transaction_type").cast("string")))
    if allowed_tt:
        tt_bad = tt_upper.isNotNull() & ~tt_upper.isin(*allowed_tt)
    else:
        tt_bad = F.lit(False)

    ch_upper = F.upper(F.trim(F.col("channel").cast("string")))
    if allowed_ch:
        ch_bad = ch_upper.isNotNull() & ~ch_upper.isin(*allowed_ch)
    else:
        ch_bad = F.lit(False)

    norm_cur, cur_variant = _normalize_currency_exprs(dq_rules)

    acc_hits = acct_work.select("account_id").distinct().withColumn("_acct_hit", F.lit(1))
    tx_join = tx_work.join(acc_hits, on="account_id", how="left")
    orphaned = tx_join.account_id.isNotNull() & (
        F.trim(tx_join.account_id.cast("string")) != F.lit("")
    ) & tx_join._acct_hit.isNull()

    domain_bad = tt_bad | ch_bad

    dq_flag = (
        F.when(null_req if null_req is not None else F.lit(False), F.lit("NULL_REQUIRED"))
        .when(orphaned, F.lit("ORPHANED_ACCOUNT"))
        .when(type_mismatch_amt | domain_bad, F.lit("TYPE_MISMATCH"))
        .when(date_bad, F.lit("DATE_FORMAT"))
        .when(cur_variant, F.lit("CURRENCY_VARIANT"))
        .otherwise(F.lit(None).cast("string"))
    )

    # dq_flag must be attached while `_transaction_date_raw` / `_amount_raw` still exist
    # (Spark resolves Column refs against the DataFrame at analysis time).
    tx_out = (
        tx_join.withColumn("dq_flag", dq_flag)
        .drop("_acct_hit", "_amount_raw", "_transaction_date_raw")
        .withColumn("currency", norm_cur)
    )

    logger.info("Silver: transactions — writing Delta")
    _write_silver_delta(tx_out, os.path.join(silver_base, "transactions"))
