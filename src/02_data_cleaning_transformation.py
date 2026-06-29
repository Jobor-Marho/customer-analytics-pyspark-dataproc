"""
02_data_cleaning_transformation.py
-------------------------------------
Data cleaning, schema standardisation, feature engineering, and writing
cleaned datasets to HDFS as Parquet.

Run on a GCP Dataproc cluster:
    spark-submit src/02_data_cleaning_transformation.py
"""

from pyspark.sql.functions import (
    col, when, lit, to_timestamp, unix_timestamp,
    date_diff, day, month, year, date_format,
)
from pyspark.sql.types import StringType, IntegerType, TimestampType
from pyspark.ml.feature import Imputer

from src.utils.spark_session import get_spark, get_spark_local
from src.utils.helpers import get_missing_values, get_schema, load_csvs
from configs.spark_config import HDFS_RAW_PATH, HDFS_PROCESSED_PATH, LOCAL


# ---------------------------------------------------------------------------
# 1. Spark session
# ---------------------------------------------------------------------------
if LOCAL:
    # Create a SparkSession for local testing
    spark = get_spark_local("Olist Data Cleaning & Transformation (Local)")
else:
    spark = get_spark("Olist Data Cleaning & Transformation")


# ---------------------------------------------------------------------------
# 2. Load raw CSVs
# ---------------------------------------------------------------------------

dfs = load_csvs(spark, HDFS_RAW_PATH)

customer_df = dfs["customers"]
geolocation_df = dfs["geolocation"]
order_items_df = dfs["order_items"]
payment_df = dfs["payments"]
review_df = dfs["reviews"]
orders_df = dfs["orders"]
products_df = dfs["products"]
sellers_df = dfs["sellers"]
products_category_df = dfs["categories"]


# ---------------------------------------------------------------------------
# 3. Discover missing values
# ---------------------------------------------------------------------------

print("\n=== Missing Value Audit ===")
for name, df in dfs.items():
    get_missing_values(df, name)

# Nulls found in: orders, reviews, products


# ---------------------------------------------------------------------------
# 4. Handle missing values
# ---------------------------------------------------------------------------


print("\n=== Investigating null date fields by order status ===")
all_orders = orders_df.select(
    "order_id", "order_status", "order_purchase_timestamp",
    "order_approved_at", "order_delivered_carrier_date",
    "order_delivered_customer_date"
)
for status in ("delivered", "invoiced", "canceled"):
    filtered = all_orders.filter(col("order_status") == lit(status))
    filtered.show(truncate=False)
    get_missing_values(filtered, status)

# --- 4a. Orders — impute missing dates based on delivered order status ---
orders_df_cleaned = (
    orders_df
    .withColumn(
        "order_approved_at",
        when(
            (col("order_status") == lit("delivered")) &
            col("order_approved_at").isNull() &
            col("order_purchase_timestamp").isNotNull(),
            col("order_purchase_timestamp")
        ).otherwise(col("order_approved_at"))
    )
    .withColumn(
        "order_delivered_customer_date",
        when(
            (col("order_status") == lit("delivered")) &
            col("order_delivered_customer_date").isNull() &
            col("order_estimated_delivery_date").isNotNull(),
            col("order_estimated_delivery_date")
        ).otherwise(col("order_delivered_customer_date"))
    )
    .withColumn(
        "order_delivered_carrier_date",
        when(
            (col("order_status") == lit("delivered")) &
            col("order_delivered_carrier_date").isNull() &
            col("order_approved_at").isNotNull() &
            col("order_delivered_customer_date").isNotNull(),
            to_timestamp(
                (unix_timestamp("order_approved_at") +
                 unix_timestamp("order_delivered_customer_date")) / 2
            )
        ).otherwise(col("order_delivered_carrier_date"))
    )
)

# --- 4b. Products — impute physical dimensions by mode ---
# Ignored (need source data): product_category_name, product_name_lenght,
#                             product_description_lenght, product_photos_qty
# Imputed by mode: weight_g, length_cm, height_cm, width_cm

dimension_cols = [
    "product_weight_g", "product_length_cm",
    "product_height_cm", "product_width_cm"
]
imputer = Imputer(
    inputCols=dimension_cols,
    outputCols=[f"{c}_imp" for c in dimension_cols]
).setStrategy("mode")

products_df_cleaned = (
    imputer.fit(products_df).transform(products_df)
    .drop(*dimension_cols)
    .withColumnRenamed("product_weight_g_imp",  "product_weight_g")
    .withColumnRenamed("product_length_cm_imp", "product_length_cm")
    .withColumnRenamed("product_height_cm_imp", "product_height_cm")
    .withColumnRenamed("product_width_cm_imp",  "product_width_cm")
)


# ---------------------------------------------------------------------------
# 5. Standardise schemas
# ---------------------------------------------------------------------------

print("\n=== Schema Audit ===")
get_schema(customer_df, "customers")
get_schema(orders_df_cleaned, "orders")
get_schema(order_items_df, "order items")
get_schema(payment_df, "payments")
get_schema(geolocation_df, "geolocation")
get_schema(review_df, "reviews")
get_schema(products_df_cleaned, "products")
get_schema(sellers_df, "sellers")
get_schema(products_category_df, "category translation")

# Issues found:
#   customers    — customer_zip_code_prefix:     int   → string
#   geolocation  — geolocation_zip_code_prefix:  int   → string
#   reviews      — review_score:                 str   → int
#                  review_creation_date:          str   → timestamp
#                  review_answer_timestamp:       str   → timestamp
#   sellers      — seller_zip_code_prefix:        int   → string

customers_df_cleaned = customer_df.withColumn(
    "customer_zip_code_prefix",
    col("customer_zip_code_prefix").cast(StringType())
)

geolocation_cleaned = geolocation_df.withColumn(
    "geolocation_zip_code_prefix",
    col("geolocation_zip_code_prefix").cast(StringType())
)

review_cleaned_df = (
    review_df
    .withColumn("review_score", col("review_score").cast(IntegerType()))
    .withColumn("review_creation_date", col("review_creation_date").cast(TimestampType()))
    .withColumn("review_answer_timestamp", col("review_answer_timestamp").cast(TimestampType()))
)

sellers_df_cleaned = sellers_df.withColumn(
    "seller_zip_code_prefix",
    col("seller_zip_code_prefix").cast(StringType())
)


# ---------------------------------------------------------------------------
# 6. Standardise payment types
# ---------------------------------------------------------------------------

print("\n=== Payment Types Before ===")
payment_df.select("payment_type").distinct().show()

paymentDf_cleaned = payment_df.withColumn(
    "payment_type",
    when(col("payment_type") == "credit_card",  "credit card")
    .when(col("payment_type") == "boleto",       "bank transfer")
    .when(col("payment_type") == "debit_card",   "debit card")
    .when(col("payment_type") == "not_defined",  "other")
    .otherwise(col("payment_type"))
)

print("\n=== Payment Types After ===")
paymentDf_cleaned.select("payment_type").distinct().show()


# ---------------------------------------------------------------------------
# 7. Remove duplicates
# ---------------------------------------------------------------------------

customers_df_cleaned = customers_df_cleaned.dropDuplicates()
orders_df_cleaned = orders_df_cleaned.dropDuplicates()
order_items_df_cleaned = order_items_df.dropDuplicates()
paymentDf_cleaned = paymentDf_cleaned.dropDuplicates()
geolocation_df_cleaned = geolocation_cleaned.dropDuplicates()
review_cleaned_df = review_cleaned_df.dropDuplicates()
products_df_cleaned = products_df_cleaned.dropDuplicates()
sellers_df_cleaned = sellers_df_cleaned.dropDuplicates()
products_category_df_cleaned = products_category_df.dropDuplicates()


# ---------------------------------------------------------------------------
# 8. Feature engineering on orders
# ---------------------------------------------------------------------------

orders_df_cleaned = (
    orders_df_cleaned
    # Time-based metrics
    .withColumn("approval_time",
                date_diff(col("order_purchase_timestamp"), col("order_approved_at")))
    .withColumn("carrier_handling_time",
                date_diff(col("order_approved_at"), col("order_delivered_carrier_date")))
    .withColumn("delivery_delay",
                date_diff(col("order_estimated_delivery_date"), col("order_delivered_customer_date")))
    .withColumn("delivery_duration",
                date_diff(col("order_delivered_customer_date"), col("order_purchase_timestamp")))
    .withColumn("shipping_time",
                date_diff(col("order_delivered_carrier_date"), col("order_delivered_customer_date")))
    # Calendar breakdowns
    .withColumn("purchase_day",        day("order_purchase_timestamp"))
    .withColumn("purchase_day_name",   date_format("order_purchase_timestamp", "EEE"))
    .withColumn("purchase_month",      month("order_purchase_timestamp"))
    .withColumn("purchase_month_name", date_format("order_purchase_timestamp", "MMM"))
    .withColumn("purchase_year",       year("order_purchase_timestamp"))
)

orders_df_cleaned.show(3)


# ---------------------------------------------------------------------------
# 9. Advanced transformations
# ---------------------------------------------------------------------------

# --- 9a. Exclude price outliers from order items (1st–99th percentile) ---
low_cutoff, high_cutoff = order_items_df_cleaned.approxQuantile("price", [0.01, 0.99], 0.0)
print(f"\nPrice outlier cutoffs — low: {low_cutoff}, high: {high_cutoff}")

order_items_df_cleaned = order_items_df_cleaned.filter(
    (col("price") >= low_cutoff) & (col("price") <= high_cutoff)
)

# --- 9b. Determine product weight categories (quantile-based) ---
q1, q2, q3, q4 = products_df_cleaned.approxQuantile(
    "product_weight_g", [0.20, 0.40, 0.60, 0.80], 0.00
)

products_df_cleaned = products_df_cleaned.withColumn(
    "product_weight_category",
    when(col("product_weight_g") <= q1, "Very Light")
    .when(col("product_weight_g") <= q2, "Light")
    .when(col("product_weight_g") <= q3, "Medium")
    .when(col("product_weight_g") <= q4, "Heavy")
    .otherwise("Very Heavy")
)
products_df_cleaned.show()


# ---------------------------------------------------------------------------
# 10. Write cleaned datasets to HDFS as Parquet
# ---------------------------------------------------------------------------

print("\n=== Writing cleaned datasets to HDFS ===")

write_map = {
    "customers": customers_df_cleaned,
    "orders": orders_df_cleaned,
    "orderItems": order_items_df_cleaned,
    "products": products_df_cleaned,
    "payments": paymentDf_cleaned,
    "geolocations": geolocation_df_cleaned,
    "sellers": sellers_df_cleaned,
    "reviews": review_cleaned_df,
    "categories_transalation": products_category_df_cleaned,
}

for folder, df in write_map.items():
    path = HDFS_PROCESSED_PATH + folder
    df.coalesce(1).write.mode("overwrite").parquet(path)
    print(f"  Written → {path}")


# ---------------------------------------------------------------------------
# 11. Terminate Spark session
# ---------------------------------------------------------------------------

spark.stop()
