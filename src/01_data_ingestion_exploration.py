"""
01_data_ingestion_exploration.py
----------------------------------
Data ingestion from HDFS and exploratory analysis of all nine Olist datasets.

Run on a GCP Dataproc cluster:
    spark-submit src/01_data_ingestion_exploration.py
"""

from pyspark.sql.functions import col, count, desc, sum, date_diff, format_number

from src.utils.spark_session import get_spark, get_spark_local
from src.utils.helpers import get_missing_values, load_csvs
from configs.spark_config import HDFS_RAW_PATH, LOCAL


# ---------------------------------------------------------------------------
# 1. Spark session
# ---------------------------------------------------------------------------
if LOCAL:
    # Create a SparkSession for local testing
    spark = get_spark_local("Olist Data Exploration (Local)")
else:
    # Create a SparkSession for data ingestion and exploration on dataproc
    spark = get_spark("Olist Data Exploration")



# ---------------------------------------------------------------------------
# 2. Load raw CSVs from HDFS
# ---------------------------------------------------------------------------

dfs = load_csvs(spark, HDFS_RAW_PATH)

customer_df         = dfs["customers"]
geolocation_df      = dfs["geolocation"]
order_items_df      = dfs["order_items"]
payment_df          = dfs["payments"]
review_df           = dfs["reviews"]
orders_df           = dfs["orders"]
products_df         = dfs["products"]
sellers_df          = dfs["sellers"]
products_category_df = dfs["categories"]


# ---------------------------------------------------------------------------
# 3. Data leakage check — row counts
# ---------------------------------------------------------------------------

print("\n=== Row Counts (data leakage check as at 23/06/2026) ===")
print(f"Customers: {customer_df.count():,}")
print(f"Geolocation: {geolocation_df.count():,}")
print(f"Order Items: {order_items_df.count():,}")
print(f"Payments: {payment_df.count():,}")
print(f"Reviews: {review_df.count():,}")
print(f"Orders: {orders_df.count():,}")
print(f"Products: {products_df.count():,}")
print(f"Sellers: {sellers_df.count():,}")
print(f"Product Categories: {products_category_df.count():,}")


# ---------------------------------------------------------------------------
# 4. Null checks
# ---------------------------------------------------------------------------

print("\n=== Null Checks ===")
get_missing_values(customer_df, "Customers")
get_missing_values(geolocation_df, "Geolocation")
get_missing_values(order_items_df, "Order Items")
get_missing_values(payment_df, "Payments")
get_missing_values(review_df, "Reviews")
get_missing_values(orders_df, "Orders")
get_missing_values(products_df, "Products")
get_missing_values(sellers_df, "Sellers")
get_missing_values(products_category_df, "Product Categories")


# ---------------------------------------------------------------------------
# 5. Duplicate checks
# ---------------------------------------------------------------------------

print("\n=== Duplicate Checks ===")
for name, df in dfs.items():
    dup_count = df.groupBy(df.columns).count().filter(col("count") > 1).count()
    print(f"{name}: {dup_count} duplicate rows")


# ---------------------------------------------------------------------------
# 6. Customer distribution
# ---------------------------------------------------------------------------

print("\n=== Customer Distribution by City ===")
(customer_df
    .groupBy("customer_city")
    .agg(count("customer_id").alias("count"))
    .orderBy(desc("count"))
    .show())

print("\n=== Customer Distribution by State ===")
(customer_df
    .groupBy("customer_state")
    .agg(count("customer_id").alias("count"))
    .orderBy(desc("count"))
    .show())

print("\n=== Customer Distribution by City & State ===")
(customer_df
    .groupBy("customer_city", "customer_state")
    .agg(count("customer_id").alias("count"))
    .orderBy(desc("count"))
    .show())


# ---------------------------------------------------------------------------
# 7. Order distribution
# ---------------------------------------------------------------------------

print("\n=== Order Distribution by Status ===")
(orders_df
    .groupBy("order_status")
    .agg(count("order_id").alias("count"))
    .orderBy(desc("count"))
    .show())

print("\n=== Order Delivery Duration ===")
delivery_duration = (orders_df
    .withColumn("delivery_duration",
                date_diff(col("order_delivered_customer_date"),
                          col("order_purchase_timestamp")))
    .orderBy("delivery_duration", ascending=False))

delivery_duration.select(
    "order_id", "order_status",
    "order_purchase_timestamp",
    "order_delivered_customer_date",
    "delivery_duration"
).show()

delivery_duration.groupby("order_status").agg(count("order_id")).show()


# ---------------------------------------------------------------------------
# 8. Payment distribution
# ---------------------------------------------------------------------------

print("\n=== Payment Type Distribution ===")
(payment_df
    .groupBy("payment_type")
    .count()
    .orderBy("count", ascending=False)
    .show())


# ---------------------------------------------------------------------------
# 9. Order items — top products
# ---------------------------------------------------------------------------

print("\n=== Most Ordered Products ===")
(order_items_df
    .groupBy("product_id")
    .agg(count("order_id").alias("total_order"))
    .orderBy("total_order", ascending=False)
    .show())

print("\n=== Top Selling Products by Revenue ===")
(order_items_df
    .groupBy("product_id")
    .agg(sum("price").alias("total_amount_spent"))
    .orderBy("total_amount_spent", ascending=False)
    .select("product_id",
            format_number("total_amount_spent", 2).alias("total_amount_spent ($)"))
    .show())


# ---------------------------------------------------------------------------
# 10. Terminate Spark session
# ---------------------------------------------------------------------------

spark.stop()
