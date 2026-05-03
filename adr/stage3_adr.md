# Architecture Decision Record: Stage 3 Streaming Extension

**File:** `adr/stage3_adr.md`  
**Author:** Jan Rademeyer  
**Date:** 03/05/2026  
**Status:** Final

---

## Context

The Stage 3 brief adds a near-real-time path alongside the existing batch medallion pipeline: the mobile product team needs current balances and recent activity without waiting for the daily batch. The fintech exposes this as a directory of micro-batch JSONL files under `/data/stream/`, named `stream_{YYYYMMDD}_{HHMMSS}_{sequence}.jsonl`. The pipeline must poll, discover files, and process them in chronologically. 

Two new Delta Gold outputs live under `/data/output/stream_gold/`: `current_balances` (one upserted row per `account_id`) and `recent_transactions` (merge on `(account_id, transaction_id)` with retention of the latest 50 rows per account). The SLA is measured as `updated_at` versus the source event timestamp, with a max of 300s.

Coming from Stage 1–2, this repo already had Bronze -> Silver -> Gold as batches, config-driven paths, and Stage 2 DQ reporting. Stage 3 extends orchestration in `pipeline/run_all.py` when `pipeline_stage` is `"3"`, batch Gold completes first, then `pipeline/stream_ingest.run_stream_ingestion` runs, then `write_dq_report`. 

---

## Decision 1: How did your existing Stage 1 architecture facilitate or hinder the streaming extension?

### What made Stage 3 easier

- **Separate streaming module:** Streaming logic lives in `pipeline/stream_ingest.py` rather than being threaded through ingest/transform/provision, so batch behaviour stays unchanged and stream reads JSONL from `streaming.stream_input_path` explicitly.
- **Reuse of batch Gold for seeds and FK validity:** Stream events are joined to **`dim_accounts`** from batch Gold (`gold_path/dim_accounts`) inside `_prepare_events`, which satisdies the spec requiremnt and prevents reusing code from `provision.py`.
- **Config-first paths:** `streaming` and `output.gold_path` extend existing YAML patterns.
- **Delta MERGE familiarity:** `current_balances` and `recent_transactions` use SQL `MERGE INTO delta.\`path\``, this is an area I am familiar with coming from databricks. 
- **Orchestration hook:** `run_all.py` gates stream execution on `pipeline_stage == "3"`, which keeps code clean between stages.

### What made Stage 3 harder

- **Polling exit strategy:** The poll loop uses **`quiesce_timeout_seconds`**, which is not the same as the 300s row SLA, this was hard to distinguish and I sensibly used 60 seconds because spec didn't specify.
- **Balance semantics:** Signed deltas (`CREDIT`/`DEBIT`/`FEE`/`REVERSAL`) are implemented in `_balance_delta_expr`, this was not specified as to their sign in the spec so I used my own judgement.
- **Processed-file state:** `processed_state_path` tracks completed paths under `/tmp` by default. I am used to using spark streaming with a fixed path location for logging and tracking.

### Code survival rate

Only a small fraction had to be changed, that was `pipeline_config.yaml` and `run_all.py`. The rest were new or left untouched. 

---

## Decision 2: What design decisions in Stage 1 would you change in hindsight?

- **More config driving development:** In my actual work we have dedicated config files for schemas and dq checks. This makes it super easy to add new tables and apply logic to existing ingestion files without the need to hardcode a lot of things.

- **Stream vs Batch Separation:** I would have had a larger distinction between the two. In practice you don't combine streaming and batch workloads so I would have probably split out modules or made it clear that you can run stream and batch separately. 

---

## Decision 3: How would you approach this differently if you had known Stage 3 was coming from the start?

- **A single ingestion layer:** This makes sense since we are ingesting raw files from batch and streaming. The processing on them is actually not relevant since we can keep track of ingestion timestamps. Therefore we can leave merging and gathering of latest balances and the recent transactions to the other stages. This is architecturally an issue because you should not mix real streaming with batch jobs but in this case the streaming are essentially micro batches so this would be sufficient. 

---
