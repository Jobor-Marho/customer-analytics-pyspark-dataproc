"""
spark_session.py
----------------
Factory functions for creating SparkSessions.

Usage:
    from src.utils.spark_session import get_spark, get_spark_optimised

    spark = get_spark("My App")                  # lightweight session
    spark = get_spark_optimised("Heavy Join App") # tuned for large joins
"""

from pyspark.sql import SparkSession
from configs.spark_config import *

def get_spark_local(app_name: str) -> SparkSession:
    """Return a basic SparkSession suitable for local exploration and lightweight work."""
    return SparkSession.builder.master(LOCAL_MODE).appName(app_name).getOrCreate()

def get_spark(app_name: str) -> SparkSession:
    """Return a basic SparkSession suitable for exploration and lightweight work."""
    return SparkSession.builder.appName(app_name).getOrCreate()


def get_spark_optimised(app_name: str) -> SparkSession:
    """
    Return a tuned SparkSession for large-scale joins and aggregations
    (as used in Module 3 on GCP Dataproc).
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.executor.memory", EXECUTOR_MEMORY)
        .config("spark.executor.cores", EXECUTOR_CORES)
        .config("spark.executor.instances", EXECUTOR_INSTANCES)
        .config("spark.driver.memory", DRIVER_MEMORY)
        .config("spark.driver.maxResultSize", DRIVER_MAX_RESULT_SIZE)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        .config("spark.default.parallelism", DEFAULT_PARALLELISM)
        .config("spark.sql.adaptive.enabled", AQE_ENABLED)
        .config("spark.sql.adaptive.coalescePartitions.enabled", AQE_COALESCE_PARTITIONS_ENABLED)
        .config("spark.sql.adaptive.skewJoin.enabled", AQE_SKEW_JOIN_ENABLED)
        .config("spark.sql.autoBroadcastJoinThreshold", AUTO_BROADCAST_JOIN_THRESHOLD)
        .config("spark.sql.files.maxPartitionBytes", MAX_PARTITION_BYTES)
        .config("spark.sql.files.openCostInBytes", OPEN_COST_IN_BYTES)
        .config("spark.memory.fraction", MEMORY_FRACTION)
        .config("spark.memory.storageFraction", MEMORY_STORAGE_FRACTION)
        .getOrCreate()
    )
