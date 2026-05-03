"""
Silver layer: Clean and conform Bronze tables into validated Silver Delta tables.

"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


def _dq_rules_path(config: dict) -> Optional[str]:
    """First existing file wins: config → DQ_RULES_PATH env → default mount → packaged copy."""
    dq_cfg = config.get("dq") or {}
    packaged = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "..", "config", "dq_rules.yaml")
    )
    candidates: list[str] = []
    rp = dq_cfg.get("rules_path")
    if rp:
        candidates.append(str(rp).strip())
    env_p = os.environ.get("DQ_RULES_PATH")
    if env_p:
        candidates.append(env_p.strip())
    candidates.append("/data/config/dq_rules.yaml")
    candidates.append(packaged)

    seen: set[str] = set()
    for p in candidates:
        if not p or p in seen:
            continue
        seen.add(p)
        if os.path.isfile(p):
            return p
    return None


def _load_dq_rules(config: dict) -> dict[str, Any]:
    path = _dq_rules_path(config)
    if path is None:
        logger.warning(
            "DQ rules file not found (tried dq.rules_path, DQ_RULES_PATH, "
            "/data/config/dq_rules.yaml, packaged config/dq_rules.yaml); "
            "continuing with empty rule defaults."
        )
        return {}
    logger.info("Loading DQ rules from %s", path)
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _write_silver_delta(df: DataFrame, path: str) -> None:
    # Single helper here just to prevent code duplication.
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
    # Order: ISO first (Stage 1), then DD/MM/variants, then epoch strings (Stage 2).
    c = F.trim(col.cast("string"))
    iso = F.to_date(c, "yyyy-MM-dd")
    dmy = F.coalesce(F.to_date(c, "dd/MM/yyyy"), F.to_date(c, "d/M/yyyy"))
    epoch = F.when(
        c.rlike(r"^\d{9,13}$"),
        F.to_date(F.from_unixtime(c.cast("long"))),
    )
    return F.coalesce(iso, dmy, epoch)


def _dedupe_latest(df: DataFrame, key_col: str, order_col: str) -> DataFrame:
    # row_number shuffle: costly but deterministic; order_col ties duplicate tx_ids to one row.
    w = Window.partitionBy(key_col).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def _apply_customer_casting(df: DataFrame, silver_cfg: dict) -> DataFrame:
    # Column lists default to Stage 1 shapes when YAML omits silver.casting.*.
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
    # Returns (normalized column, variant flag): Gold output_schema requires display "ZAR"
    # while still flagging non-literal-ZAR source values as CURRENCY_VARIANT.
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
    # Stage 1 JSON omits merchant_subcategory key; Spark may omit column — use lit(None).
    # Nested location kept out of Silver surface as province only (Gold needs province).
    cols = bronze_tx.columns
    province_col = (
        F.col("location.province") if "location" in cols else F.lit(None).cast("string")
    )
    subcat = (
        F.col("merchant_subcategory")
        if "merchant_subcategory" in cols
        else F.lit(None).cast("string")
    )

    dup_cnt = F.col("_dup_cnt") if "_dup_cnt" in cols else F.lit(1)

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
        dup_cnt.alias("_dup_cnt"),
    )


def run_transformation(spark: SparkSession, config: dict) -> None:
    dq_rules = _load_dq_rules(config)
    silver_cfg = config.get("silver") or {}
    order_col = silver_cfg.get("dedupe_order_column", "ingestion_timestamp")

    out_cfg = config["output"]
    bronze_base = out_cfg["bronze_path"]
    silver_base = out_cfg["silver_path"]

    # ── Customers (drive referential envelope for accounts) ─────────────────
    logger.info("Silver: customers — reading Bronze")
    cust_bronze = spark.read.format("delta").load(os.path.join(bronze_base, "customers"))
    cust_work = _dedupe_latest(cust_bronze, "customer_id", order_col)
    cust_work = _apply_customer_casting(cust_work, silver_cfg)
    logger.info("Silver: customers — writing Delta")
    _write_silver_delta(cust_work, os.path.join(silver_base, "customers"))

    # ── Accounts: drop NULL_REQUIRED PK rows (Stage 2); inner join valid customers
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

    # ── Transactions: DQ flags + normalized currency; orphans vs Silver accounts slice
    logger.info("Silver: transactions — reading Bronze")
    tx_bronze = spark.read.format("delta").load(os.path.join(bronze_base, "transactions"))
    w_tid = Window.partitionBy("transaction_id")
    tx_bronze = tx_bronze.withColumn("_dup_cnt", F.count(F.lit(1)).over(w_tid))
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

    # Keep raw amount string-side for TYPE_MISMATCH when cast fails (string numerics).
    tx_work = tx_work.withColumn("_amount_raw", F.col("amount")).withColumn(
        "amount",
        F.col("amount").cast("decimal(18,2)"),
    )

    tx_rules = dq_rules.get("transactions") or {}
    required = tx_rules.get("required_non_null") or []
    allowed_tt = list(tx_rules.get("transaction_type_allowed") or [])
    allowed_ch = list(tx_rules.get("channel_allowed") or [])
    # Checks will be disabled at stage 1 as we have empty lists for required. 

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
    cast_failed_amt = (
        F.col("_amount_raw").isNotNull()
        & (raw_amt_str != F.lit(""))
        & F.col("amount").isNull()
    )
    tx_amt_rules = dq_rules.get("transactions") or {}
    # Default false: Spark JSON often widens amount to StringType for all rows, which would
    # over-flag TYPE_MISMATCH; set true in dq_rules when your Spark schema keeps native numeric.
    flag_str_amt = bool(tx_amt_rules.get("flag_string_amount_type_mismatch", False))
    amt_was_string = F.expr('typeof(_amount_raw) = "string"')
    string_numeric_ok = (
        amt_was_string & F.col("amount").isNotNull() if flag_str_amt else F.lit(False)
    )
    type_mismatch_amt = cast_failed_amt | string_numeric_ok

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

    # Orphan = tx.account_id not in post-filter Silver accounts.
    acc_hits = acct_work.select("account_id").distinct().withColumn("_acct_hit", F.lit(1))
    tx_join = tx_work.join(acc_hits, on="account_id", how="left")
    orphaned = tx_join.account_id.isNotNull() & (
        F.trim(tx_join.account_id.cast("string")) != F.lit("")
    ) & tx_join._acct_hit.isNull()

    domain_bad = tt_bad | ch_bad

    duplicate_delivery = F.col("_dup_cnt") > F.lit(1)

    # Precedence matches output_schema single-flag semantics (first hit wins).
    dq_flag = (
        F.when(null_req if null_req is not None else F.lit(False), F.lit("NULL_REQUIRED"))
        .when(orphaned, F.lit("ORPHANED_ACCOUNT"))
        .when(duplicate_delivery, F.lit("DUPLICATE_DEDUPED"))
        .when(type_mismatch_amt | domain_bad, F.lit("TYPE_MISMATCH"))
        .when(date_bad, F.lit("DATE_FORMAT"))
        .when(cur_variant, F.lit("CURRENCY_VARIANT"))
        .otherwise(F.lit(None).cast("string"))
    )

    # Build dq_flag before dropping helper cols (Spark binds columns by DF lineage).
    tx_out = (
        tx_join.withColumn("dq_flag", dq_flag)
        .drop("_acct_hit", "_amount_raw", "_transaction_date_raw", "_dup_cnt")
        .withColumn("currency", norm_cur)
    )

    logger.info("Silver: transactions — writing Delta")
    _write_silver_delta(tx_out, os.path.join(silver_base, "transactions"))
