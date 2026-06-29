"""
spark_config.py
---------------
Central reference for all Spark configuration values used in this project.
Import these constants wherever you need to tune a SparkSession rather than
hardcoding strings in notebooks or scripts.
"""

# ---------------------------------------------------------------------------
# HDFS paths  —  update <your-username> to match your Dataproc user
# ---------------------------------------------------------------------------
HDFS_USER = "Marho"  # change this
HDFS_RAW_PATH       = f"/user/{HDFS_USER}/project/olist/data/"
HDFS_PROCESSED_PATH = f"/user/{HDFS_USER}/project/olist/processed/"

# ---------------------------------------------------------------------------
# Executor / driver sizing  (Module 3 tuning)
# ---------------------------------------------------------------------------
EXECUTOR_MEMORY         = "10g"
EXECUTOR_CORES          = 4
EXECUTOR_INSTANCES      = 2
DRIVER_MEMORY           = "4g"
DRIVER_MAX_RESULT_SIZE  = "2gb"

# ---------------------------------------------------------------------------
# Shuffle & parallelism
# ---------------------------------------------------------------------------
SHUFFLE_PARTITIONS  = 64
DEFAULT_PARALLELISM = 64

# ---------------------------------------------------------------------------
# Adaptive Query Execution (AQE)
# ---------------------------------------------------------------------------
AQE_ENABLED                     = True
AQE_COALESCE_PARTITIONS_ENABLED = True
AQE_SKEW_JOIN_ENABLED           = True

# ---------------------------------------------------------------------------
# Join / file sizing
# ---------------------------------------------------------------------------
AUTO_BROADCAST_JOIN_THRESHOLD = "20mb"
MAX_PARTITION_BYTES           = "64mb"
OPEN_COST_IN_BYTES            = "4mb"

# ---------------------------------------------------------------------------
# Memory fractions
# ---------------------------------------------------------------------------
MEMORY_FRACTION         = 0.8
MEMORY_STORAGE_FRACTION = 0.2

# ---------------------------------------------------------------------------
# Customer segmentation thresholds (Module 3)
# ---------------------------------------------------------------------------
PREMIUM_CUSTOMER_AOV_THRESHOLD = 1500
MEDIUM_CUSTOMER_AOV_THRESHOLD  = 700

# ---------------------------------------------------------------------------
# Pspark deploy mode
# ---------------------------------------------------------------------------
LOCAL_MODE = "local[*]"
LOCAL = False  # set to True for local testing, False for GCP Dataprocs