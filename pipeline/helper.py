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
    """
    path = (
        config_path
        or os.environ.get("PIPELINE_CONFIG")
        or "/data/config/pipeline_config.yaml"
    )
    if not os.path.exists(path):
        path = os.path.join(os.path.dirname(__file__), "..", "config", "pipeline_config.yaml")
    logger.info(f"Loading config from: {path}")
    with open(path) as f:
        config = yaml.safe_load(f)
    logger.info("Config loaded successfully")
    return config

def build_spark_session(config: dict) -> SparkSession:
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

    os.environ["SPARK_HOME"] = os.path.dirname(__import__("pyspark").__file__)

    jar_dir = "/opt/delta-jars"
    jars = ",".join(sorted(glob.glob(f"{jar_dir}/*.jar")))
    if not jars:
        raise RuntimeError(
            f"No JARs under {jar_dir}; add Delta JARs at image build time (no network at runtime)."
        )
    logger.info(f"Using JARs: {jars}")

    spark_cfg = config.get("spark", {})
    master        = spark_cfg.get("master",             "local[2]")
    app_name      = spark_cfg.get("app_name",           "nedbank-de-pipeline")
    shuffle_parts = spark_cfg.get("shuffle_partitions", 4)
    driver_mem    = spark_cfg.get("driver_memory",      "1g")

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
        .config("spark.sql.parquet.compression.codec", "gzip")
    )

    spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark