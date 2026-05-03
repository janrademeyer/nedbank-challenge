"""
Shared Spark + config helpers for the Nedbank DE pipeline.

Design notes live here so `run_all.py` stays thin. Choices below follow
`docs/docker_interface_contract.md` (paths, no network at run, 2 vCPU).
"""

import yaml
import os
import pyspark
from pyspark.sql import SparkSession
import logging
import glob

logger = logging.getLogger(__name__)

def load_config(config_path: str | None = None) -> dict:
    """
    Load pipeline_config.yaml.

    Resolution order:
      1. config_path argument (if passed explicitly)
      2. PIPELINE_CONFIG environment variable
      3. Scoring system canonical path: /data/config/pipeline_config.yaml
      4. Packaged default under nedbank-challenge/config/

    Hardening: empty or comment-only YAML yields None from safe_load; an empty
    bind mount at /data/config/... is common locally. We fall back to the
    packaged file when the primary path did not yield a mapping.
    """
    # Prefer explicit arg, then env (tests / overrides), then scorer mount.
    path = (
        config_path
        or os.environ.get("PIPELINE_CONFIG")
        or "/data/config/pipeline_config.yaml"
    )
    # If the mount is missing entirely (e.g. dev without /data), use the image copy.
    if not os.path.exists(path):
        path = os.path.join(os.path.dirname(__file__), "..", "config", "pipeline_config.yaml")

    # `bundled` is the baked-in default, this avoids re-reading it when we're already there.
    bundled = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "config", "pipeline_config.yaml")
    )
    path_abs = os.path.abspath(path)

    def _read_yaml(p: str):
        with open(p) as f:
            return yaml.safe_load(f)

    logger.info("Loading config from: %s", path)
    config = _read_yaml(path)
    # File exists but is empty / non-mapping, then we retry bundled once.
    if not isinstance(config, dict):
        if path_abs != bundled and os.path.isfile(bundled):
            logger.warning(
                "Config at %s did not parse to a YAML mapping (got %r); loading bundled %s",
                path,
                config,
                bundled,
            )
            config = _read_yaml(bundled)
    if not isinstance(config, dict):
        raise ValueError(
            f"Pipeline config must be a YAML mapping at {path}; got {type(config).__name__!r}"
        )

    # Fail fast with a clear error instead of KeyError mid-pipeline.
    for key in ("input", "output"):
        if key not in config or not isinstance(config[key], dict):
            raise ValueError(
                f"Pipeline config must contain a '{key}' object with paths (check {path})"
            )

    logger.info("Config loaded successfully")
    return config

def build_spark_session(config: dict) -> SparkSession:
    # Docker --network=none: JVM must not resolve the container hostname via DNS.
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

    os.environ["SPARK_HOME"] = os.path.dirname(__import__("pyspark").__file__)

    # Delta JARs are copied at image build since we can't download from maven at runtime.
    jar_dir = "/opt/delta-jars"
    jars = ",".join(sorted(glob.glob(f"{jar_dir}/*.jar")))
    if not jars:
        raise RuntimeError(
            f"No JARs under {jar_dir}; add Delta JARs at image build time (no network at runtime)."
        )
    logger.info(f"Using JARs: {jars}")

    spark_cfg = config.get("spark", {})
    # As per specs
    master        = spark_cfg.get("master",             "local[2]")
    app_name      = spark_cfg.get("app_name",           "nedbank-de-pipeline")
    shuffle_parts = spark_cfg.get("shuffle_partitions", 4)
    driver_mem    = spark_cfg.get("driver_memory",      "1g")
    # Default to snappy since it's faster than GZIP and testing shows better memory usage.
    parquet_codec = str(spark_cfg.get("parquet_compression", "snappy")).strip().lower()
    if not parquet_codec:
        parquet_codec = "snappy"

    #Configure spark to use data/output for tmp work, it frees up tmp size constraints and allows snappy to work
    default_scratch = "/data/output/_spark_tmp"
    local_dir = spark_cfg.get("local_dir") or default_scratch
    local_dir = os.path.abspath(os.path.expanduser(str(local_dir).strip()))

    builder = (
        SparkSession.builder
        .master(master)
        .appName(app_name)
        .config("spark.jars", jars)
        .config("spark.sql.extensions", 
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", 
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory",                          driver_mem)
        .config("spark.sql.shuffle.partitions",                 str(shuffle_parts))
        .config("spark.default.parallelism",                    "2")
        .config("spark.sql.adaptive.enabled",                   "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled","true")
        .config("spark.sql.autoBroadcastJoinThreshold",         "10485760")
        .config("spark.ui.enabled",                             "false")
        .config("spark.sql.files.maxPartitionBytes",            "134217728")
        .config("spark.sql.parquet.compression.codec", parquet_codec)
    )

    # spark.local.dir + java.io.tmpdir: same path so shuffle spill and Snappy JNI agree.
    os.makedirs(local_dir, mode=0o755, exist_ok=True)
    logger.info("Spark Parquet compression codec: %s", parquet_codec)
    logger.info("Spark local scratch: %s", local_dir)
    builder = builder.config("spark.local.dir", local_dir)
    tmp_opt = f"-Djava.io.tmpdir={local_dir}"
    builder = (
        builder.config("spark.driver.extraJavaOptions", tmp_opt)
        .config("spark.executor.extraJavaOptions", tmp_opt)
    )

    spark = builder.getOrCreate()

    # Reduce log noise
    spark.sparkContext.setLogLevel("WARN")
    return spark