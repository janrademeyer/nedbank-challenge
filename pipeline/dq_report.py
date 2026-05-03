"""
Stage 2+ DQ summary written to ``output.dq_report_path`` (default ``/data/output/dq_report.json``).

The JSON shape follows ``docs/dq_report_template.json``. The scorer checks:

- ``handling_action`` values against ``config/dq_rules.yaml`` (``dq_report.issues.*``).
- Rough reconciliation between ``records_affected``, Gold row counts, and flag distributions.

Counts are computed after Bronze/Silver/Gold have been written (typically invoked at the end of ``run_all.py``).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.transform import _load_dq_rules

logger = logging.getLogger(__name__)


def _pct(records: int, denom: int) -> float:
    """Percentage rounded to 2 decimal places; template expects float."""
    if denom <= 0:
        return 0.0
    return round(float(records) * 100.0 / float(denom), 2)


def _flag_totals(spark: SparkSession, path: str) -> dict[str | None, int]:
    """Row counts per ``dq_flag`` value (``None`` key = clean rows with NULL flag)."""
    rows = (
        spark.read.format("delta")
        .load(path)
        .groupBy("dq_flag")
        .agg(F.count(F.lit(1)).alias("n"))
        .collect()
    )
    out: dict[str | None, int] = {}
    for r in rows:
        k = r["dq_flag"]
        out[k] = int(r["n"])
    return out


def write_dq_report(
    spark: SparkSession,
    config: dict,
    *,
    run_started_utc: datetime,
    execution_seconds: int,
    stage: str = "2",
) -> None:
    # handling_action overrides live under dq_report.issues.<issue_key> in dq_rules.yaml
    dq_rules = _load_dq_rules(config)
    dq_rep_meta = dq_rules.get("dq_report") or {}
    issue_defs: dict[str, Any] = dq_rep_meta.get("issues") or {}

    out_cfg = config["output"]
    bronze_base = out_cfg["bronze_path"]
    silver_base = out_cfg["silver_path"]
    gold_base = out_cfg["gold_path"]
    report_path = str(out_cfg.get("dq_report_path") or "/data/output/dq_report.json")

    # --- source_record_counts: full Bronze row counts (before Silver filters / dedupe) ---
    bc_accounts = spark.read.format("delta").load(os.path.join(bronze_base, "accounts"))
    bc_tx = spark.read.format("delta").load(os.path.join(bronze_base, "transactions"))
    bc_cust = spark.read.format("delta").load(os.path.join(bronze_base, "customers"))

    accounts_raw = bc_accounts.count()
    transactions_raw = bc_tx.count()
    customers_raw = bc_cust.count()

    # Duplicate *extra* deliveries: same natural key appears on more than one Bronze row.
    # (After transform dedupe, one row per transaction_id survives; losers are dropped from Silver.)
    distinct_tx_ids = bc_tx.select("transaction_id").distinct().count()
    duplicate_extra_rows = max(0, transactions_raw - distinct_tx_ids)

    # NULL_REQUIRED on accounts: blank/null account_id rows never reach Silver/Gold dims.
    null_pk_accounts = bc_accounts.filter(
        F.col("account_id").isNull()
        | (F.trim(F.col("account_id").cast("string")) == F.lit(""))
    ).count()

    silver_tx_path = os.path.join(silver_base, "transactions")
    gold_fact_path = os.path.join(gold_base, "fact_transactions")

    # Silver holds the canonical per-row dq_flag (all tx that made it past Bronze ingest + dedupe).
    silver_flags = _flag_totals(spark, silver_tx_path)
    # Gold fact only includes joinable rows (e.g. ORPHANED_ACCOUNT rows are absent from facts).
    gold_flags = _flag_totals(spark, gold_fact_path)

    def meta(key: str, defaults: dict[str, Any]) -> dict[str, Any]:
        """Merge template defaults with optional overrides from dq_rules.yaml."""
        base = dict(defaults)
        base.update(issue_defs.get(key) or {})
        return base

    # One object per issue type *encountered*; template says omit zero-count categories.
    dq_issues: list[dict[str, Any]] = []

    dup_m = meta(
        "duplicate_transactions",
        {"handling_action": "DEDUPLICATED_KEEP_FIRST", "denominator": "transactions_raw"},
    )
    if duplicate_extra_rows > 0:
        # records_in_output: facts that kept DUPLICATE_DEDUPED (orphans excluded from Gold even if duplicated in Bronze).
        gold_dup = gold_flags.get("DUPLICATE_DEDUPED", 0)
        dq_issues.append(
            {
                "issue_type": "duplicate_transactions",
                "records_affected": duplicate_extra_rows,
                "percentage_of_total": _pct(duplicate_extra_rows, transactions_raw),
                "handling_action": dup_m["handling_action"],
                "records_in_output": gold_dup,
            }
        )

    def add_tx_issue(
        yaml_key: str,
        dq_code: str,
        defaults: dict[str, Any],
        records_affected: int,
    ) -> None:
        """Append a transaction-scoped dq_issue; percentage uses transactions_raw denominator."""
        if records_affected <= 0:
            return
        m = meta(yaml_key, defaults)
        dq_issues.append(
            {
                "issue_type": yaml_key,
                "records_affected": records_affected,
                "percentage_of_total": _pct(records_affected, transactions_raw),
                "handling_action": m["handling_action"],
                "records_in_output": gold_flags.get(dq_code, 0),
            }
        )

    add_tx_issue(
        "orphaned_transactions",
        "ORPHANED_ACCOUNT",
        {"handling_action": "QUARANTINED", "denominator": "transactions_raw"},
        silver_flags.get("ORPHANED_ACCOUNT", 0),
    )

    # All TYPE_MISMATCH rows (amount, domain, etc.) roll up under template key amount_type_mismatch.
    tm_count = silver_flags.get("TYPE_MISMATCH", 0)
    add_tx_issue(
        "amount_type_mismatch",
        "TYPE_MISMATCH",
        {"handling_action": "CAST_TO_DECIMAL", "denominator": "transactions_raw"},
        tm_count,
    )

    df_count = silver_flags.get("DATE_FORMAT", 0)
    add_tx_issue(
        "date_format_inconsistency",
        "DATE_FORMAT",
        {"handling_action": "NORMALISED_DATE", "denominator": "transactions_raw"},
        df_count,
    )

    cv_count = silver_flags.get("CURRENCY_VARIANT", 0)
    add_tx_issue(
        "currency_variants",
        "CURRENCY_VARIANT",
        {"handling_action": "NORMALISED_CURRENCY", "denominator": "transactions_raw"},
        cv_count,
    )

    null_m = meta(
        "null_account_id",
        {"handling_action": "EXCLUDED_NULL_PK", "denominator": "accounts_raw"},
    )
    if null_pk_accounts > 0:
        dq_issues.append(
            {
                "issue_type": "null_account_id",
                "records_affected": null_pk_accounts,
                "percentage_of_total": _pct(null_pk_accounts, accounts_raw),
                "handling_action": null_m["handling_action"],
                "records_in_output": 0,
            }
        )

    # --- gold_layer_record_counts: final Delta table cardinalities ---
    dim_a_ct = spark.read.format("delta").load(os.path.join(gold_base, "dim_accounts")).count()
    dim_c_ct = spark.read.format("delta").load(os.path.join(gold_base, "dim_customers")).count()
    fact_ct = spark.read.format("delta").load(gold_fact_path).count()

    payload = {
        "$schema": "nedbank-de-challenge/dq-report/v1",
        "run_timestamp": run_started_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "stage": str(stage),
        "source_record_counts": {
            "accounts_raw": accounts_raw,
            "transactions_raw": transactions_raw,
            "customers_raw": customers_raw,
        },
        "dq_issues": dq_issues,
        "gold_layer_record_counts": {
            "fact_transactions": fact_ct,
            "dim_accounts": dim_a_ct,
            "dim_customers": dim_c_ct,
        },
        "execution_duration_seconds": int(max(0, execution_seconds)),
    }

    os.makedirs(os.path.dirname(report_path), mode=0o755, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    logger.info("DQ report written to %s", report_path)
