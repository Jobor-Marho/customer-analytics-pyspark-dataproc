"""
helpers.py
----------
Reusable PySpark helper functions used across all three modules.

Usage:
    from src.utils.helpers import get_missing_values, get_schema, load_csvs, load_parquets
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, when
from typing import Dict


# ---------------------------------------------------------------------------
# Schema / diagnostics
# ---------------------------------------------------------------------------

def get_missing_values(df: DataFrame, df_name: str) -> None:
    """Print a row showing the null count for every column in *df*."""
    print(f"Missing Values for: {df_name}")
    df.select(
        [count(when(col(c).isNull(), 1)).alias(c) for c in df.columns]
    ).show()


def get_schema(df: DataFrame, df_name: str) -> None:
    """Print the schema and first two rows of *df*."""
    print(f"Schema of {df_name.title()}")
    df.printSchema()
    df.show(2)


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

CSV_OPTIONS = {"header": True, "inferSchema": True}

PRODUCT_SCHEMA = """
    product_id STRING,
    product_category_name STRING,
    product_name_lenght DOUBLE,
    product_description_lenght DOUBLE,
    product_photos_qty DOUBLE,
    product_weight_g DOUBLE,
    product_length_cm DOUBLE,
    product_height_cm DOUBLE,
    product_width_cm DOUBLE
"""


def load_csvs(spark: SparkSession, hdfs_base_path: str) -> Dict[str, DataFrame]:
    """
    Load all nine raw Olist CSVs from *hdfs_base_path* and return them
    as a dictionary keyed by a short name.

    Args:
        spark:          Active SparkSession.
        hdfs_base_path: HDFS path ending with '/'.  e.g.
                        '/user/Marho/project/olist/data/'

    Returns:
        Dict with keys:
            customers, geolocation, order_items, payments,
            reviews, orders, products, sellers, categories
    """
    opts = CSV_OPTIONS

    return {
        "customers":    spark.read.options(**opts).csv(hdfs_base_path + "olist_customers_dataset.csv"),
        "geolocation":  spark.read.options(**opts).csv(hdfs_base_path + "olist_geolocation_dataset.csv"),
        "order_items":  spark.read.options(**opts).csv(hdfs_base_path + "olist_order_items_dataset.csv"),
        "payments":     spark.read.options(**opts).csv(hdfs_base_path + "olist_order_payments_dataset.csv"),
        "reviews":      spark.read.options(**opts).csv(hdfs_base_path + "olist_order_reviews_dataset.csv"),
        "orders":       spark.read.options(**opts).csv(hdfs_base_path + "olist_orders_dataset.csv"),
        "products":     spark.read.option("header", True).schema(PRODUCT_SCHEMA).csv(
                            hdfs_base_path + "olist_products_dataset.csv"
                        ),
        "sellers":      spark.read.options(**opts).csv(hdfs_base_path + "olist_sellers_dataset.csv"),
        "categories":   spark.read.options(**opts).csv(hdfs_base_path + "product_category_name_translation.csv"),
    }


def load_parquets(spark: SparkSession, hdfs_base_path: str) -> Dict[str, DataFrame]:
    """
    Load all cleaned Parquet datasets written by Module 2.

    Args:
        spark:          Active SparkSession.
        hdfs_base_path: HDFS path ending with '/'.  e.g.
                        '/user/Marho/project/olist/processed/'

    Returns:
        Same keys as load_csvs().
    """
    return {
        "customers":   spark.read.parquet(hdfs_base_path + "customers"),
        "geolocation": spark.read.parquet(hdfs_base_path + "geolocations"),
        "order_items": spark.read.parquet(hdfs_base_path + "orderItems"),
        "payments":    spark.read.parquet(hdfs_base_path + "payments"),
        "reviews":     spark.read.parquet(hdfs_base_path + "reviews"),
        "orders":      spark.read.parquet(hdfs_base_path + "orders"),
        "products":    spark.read.parquet(hdfs_base_path + "products"),
        "sellers":     spark.read.parquet(hdfs_base_path + "sellers"),
        "categories":  spark.read.parquet(hdfs_base_path + "categories_transalation"),
    }
