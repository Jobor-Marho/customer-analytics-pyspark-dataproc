"""
03_data_integration_aggregation.py
--------------------------------------
Joins all cleaned datasets into one complete orders DataFrame, runs
aggregations and window functions, enriches data, and writes results
to HDFS Parquet and Hive tables.

Run on a GCP Dataproc cluster:
    spark-submit src/03_data_integration_aggregation.py
"""

from pyspark.sql.functions import (
    col, count, countDistinct, sum, avg, min, max,
    round, stddev, broadcast, desc, lit, when,
    format_number, collect_set, rank, dense_rank,
)
from pyspark.sql.window import Window

from src.utils.spark_session import get_spark_optimised, get_spark_local
from src.utils.helpers import get_schema, load_parquets
from configs.spark_config import (
    HDFS_PROCESSED_PATH,
    PREMIUM_CUSTOMER_AOV_THRESHOLD,
    MEDIUM_CUSTOMER_AOV_THRESHOLD,
    LOCAL
)


# ---------------------------------------------------------------------------
# 1. Spark session — optimised for large joins
# ---------------------------------------------------------------------------

if LOCAL:
    spark = get_spark_local("Olist Data Integration & Aggregation (Local)")
else:
    spark = get_spark_optimised("Olist Data Integration & Aggregation")

# ---------------------------------------------------------------------------
# 2. Load cleaned Parquet datasets
# ---------------------------------------------------------------------------

dfs = load_parquets(spark, HDFS_PROCESSED_PATH)

customers_df = dfs["customers"]
geolocations_df = dfs["geolocation"]
order_items_df = dfs["order_items"]
payments_df = dfs["payments"]
review_df = dfs["reviews"]
orders_df = dfs["orders"]
products_df = dfs["products"]
sellers_df = dfs["sellers"]
products_category_df = dfs["categories"]


# ---------------------------------------------------------------------------
# 3. Data integration — optimised multi-dataset join
# ---------------------------------------------------------------------------

# inner joins: core transactional entities must all exist
order_items_joined_df = orders_df.join(order_items_df, "order_id", "inner")

# broadcast small lookup tables to avoid shuffle
order_items_products_df = order_items_joined_df.join(
    broadcast(products_df), "product_id", "inner"       
)
order_items_products_sellers_df = order_items_products_df.join(
    broadcast(sellers_df), "seller_id", "inner"        
)
full_orders_with_customers_df = order_items_products_sellers_df.join(
    customers_df, "customer_id", "inner"
)

# left joins: preserve orders even when lookup data is missing
full_orders_with_geolocation = full_orders_with_customers_df.join(
    geolocations_df,
    full_orders_with_customers_df.customer_zip_code_prefix ==
    geolocations_df.geolocation_zip_code_prefix,
    "left"
)
full_orders_with_reviews    = full_orders_with_geolocation.join(review_df, "order_id", "left")
full_orders_with_categories = full_orders_with_reviews.join(
    broadcast(products_category_df), "product_category_name", "left"  
)
full_orders_df = full_orders_with_categories.join(
    broadcast(payments_df), "order_id", "left"          
)

# Initial dedup 
full_orders_df = full_orders_df.dropDuplicates(
    ["customer_unique_id", "order_id", "order_purchase_timestamp"]
)

# Cache the final integrated DataFrame to speed up subsequent aggregations and transformations
full_orders_df.cache()

print("\n=== Complete Orders Schema ===")
get_schema(full_orders_df, "Complete Orders")


# ---------------------------------------------------------------------------
# 4. Aggregations
# ---------------------------------------------------------------------------

# --- 4a. Total orders per customer ---
orders_per_customers_df = (
    full_orders_df
    .groupBy("customer_unique_id")
    .agg(countDistinct("order_id").alias("total_order"))
    .orderBy("total_order", ascending=False)
)
print("\n=== Total Orders Per Customer ===")
orders_per_customers_df.select(
    "customer_unique_id",
    format_number("total_order", 0).alias("total_order")
).show(10, truncate=False)

# --- 4b. Average review score per seller ---
seller_avg_review_score_df = (
    full_orders_df
    .groupBy("seller_id")
    .agg(avg("review_score").alias("avg_review_score"))
    .orderBy("avg_review_score", ascending=False)
)
print("\n=== Average Review Score per Seller ===")
seller_avg_review_score_df.select(
    "seller_id",
    format_number("avg_review_score", 0).alias("avg_review_score")
).show(10, truncate=False)

# --- 4c. Top 10 most sold products ---
most_sold_products_df = (
    full_orders_df
    .groupBy("product_id")
    .agg(count("order_id").alias("total_order"))
    .orderBy("total_order", ascending=False)
)
print("\n=== Top 10 Most Sold Products ===")
most_sold_products_df.select(
    "product_id",
    format_number("total_order", 0).alias("total_order")
).show(10)

# --- 4d. Top customers by purchase value ---
revenue_customers_df = (
    full_orders_df
    .groupBy("customer_id")
    .agg(sum("payment_value").alias("total_amount_spent"))
    .orderBy("total_amount_spent", ascending=False)
)
print("\n=== Top Customers by Purchase ===")
revenue_customers_df.select(
    "customer_id",
    format_number("total_amount_spent", 2).alias("total_amount_spent")
).show(10)

# --- 4e. Seller products with revenue ---
seller_products_df = (
    full_orders_df
    .groupBy("seller_id", "product_id", "price")
    .agg(count("order_id").alias("total_orders"))
    .withColumn("total_revenue", col("price") * col("total_orders"))
)
print("\n=== Seller Products ===")
seller_products_df.select(
    "seller_id", "product_id", "price", "total_orders",
    format_number("total_revenue", 2).alias("total_revenue")
).show()


# ---------------------------------------------------------------------------
# 5. Window & ranking functions
# ---------------------------------------------------------------------------

# --- 5a. Top 5 selling products per seller (by order count) ---
window_rank_spec = Window.partitionBy("seller_id").orderBy(col("total_orders").desc())

ranked_top_seller_products_df = (
    seller_products_df
    .withColumn("rank", rank().over(window_rank_spec))
    .filter(col("rank") <= 5)
)
print("\n=== Ranked Top Selling Products Per Seller ===")
ranked_top_seller_products_df.select(
    "seller_id", "product_id", "price", "total_orders", "rank"
).show(truncate=False)

# --- 5b. Top 5 revenue-generating products per seller (dense rank) ---
window_drank_spec = Window.partitionBy("seller_id").orderBy(desc("total_revenue"))

dense_ranked_top_seller_products_df = (
    seller_products_df
    .withColumn("dense_rank", dense_rank().over(window_drank_spec))
    .filter(col("dense_rank") <= 5)
)
print("\n=== Dense Ranked Top Revenue Products Per Seller ===")
dense_ranked_top_seller_products_df.select(
    "seller_id", "product_id", "price", "total_orders",
    format_number("total_revenue", 2).alias("total_revenue"),
    "dense_rank"
).show(truncate=False)


# ---------------------------------------------------------------------------
# 6. Advanced aggregations
# ---------------------------------------------------------------------------

# --- 6a. Customer spending — revenue, AOV, segmentation ---
customer_spending_df = (
    full_orders_df
    .groupBy("customer_unique_id")
    .agg(
        count("order_id").alias("total_orders"),
        sum("payment_value").alias("revenue"),
    )
    .withColumn("AOV", round(col("revenue") / col("total_orders"), 2))
    .orderBy(desc("revenue"))
)
print("\n=== Total Revenue & AOV per Customer ===")
customer_spending_df.select(
    "customer_unique_id", "total_orders",
    format_number("revenue", 2).alias("revenue"), "AOV"
).show()

# --- 6b. Seller performance metrics ---
seller_performance_df = (
    full_orders_df
    .groupBy("seller_id")
    .agg(
        count("order_id").alias("total_orders"),
        sum("price").alias("total_revenue"),
        round(avg("review_score"), 2).alias("avg_review_score"),
        round(stddev("price"), 2).alias("price_variability"),
    )
    .orderBy(desc("total_revenue"))
)
print("\n=== Seller Performance Metrics ===")
seller_performance_df.select(
    "seller_id",
    format_number("total_orders", 2).alias("total_orders"),
    format_number("total_revenue", 2).alias("total_revenue"),
    "avg_review_score", "price_variability"
).show()

# --- 6c. Product popularity metrics ---
product_metrics_df = (
    full_orders_df
    .groupBy("product_id")
    .agg(
        count("order_id").alias("total_sales"),
        sum("price").alias("total_revenue"),
        round(avg("price"), 2).alias("avg_price"),
        round(stddev("price"), 2).alias("price_volatility"),
        collect_set("seller_id").alias("unique_sellers"),
        countDistinct("seller_id").alias("unique_seller_count"),
    )
    .orderBy(desc("total_sales"))
)
print("\n=== Product Popularity Metrics ===")
product_metrics_df.show()

# --- 6d. Monthly revenue & order stats ---
monthly_stat_df = (
    full_orders_df
    .groupBy("purchase_month_name", "purchase_month", "purchase_year")
    .agg(
        sum("price").alias("seller_revenue"),
        sum("payment_value").alias("customer_spend"),
        sum("freight_value").alias("total_freight"),
        count("order_id").alias("total_orders"),
        min("price").alias("min_order_value"),
        max("price").alias("max_order_value"),
    )
    .withColumn("aov_seller",   round(col("seller_revenue")  / col("total_orders"), 2))
    .withColumn("aov_customer", round(col("customer_spend")  / col("total_orders"), 2))
    .orderBy("purchase_year", "purchase_month", desc("seller_revenue"))
)
print("\n=== Monthly Revenue & Order Count ===")
monthly_stat_df.select(
    "purchase_month_name", "purchase_year",
    format_number("seller_revenue", 2).alias("seller_revenue"),
    format_number("customer_spend", 2).alias("customer_spend"),
    format_number("total_freight", 2).alias("total_freight"),
    format_number("total_orders", 0).alias("total_orders"),
    format_number("aov_seller", 2).alias("aov_seller"),
    format_number("aov_customer", 2).alias("aov_customer"),
    format_number("min_order_value", 2).alias("min_order_value"),
    format_number("max_order_value", 2).alias("max_order_value"),
).show()

# --- 6e. Customer retention rate ---
total_order = full_orders_df.count()

customer_retention_df = (
    full_orders_df
    .groupBy("customer_unique_id")
    .agg(
        min("order_purchase_timestamp").alias("first_order_date"),
        max("order_purchase_timestamp").alias("last_order_date"),
        countDistinct("order_id").alias("total_orders"),
        sum("payment_value").alias("total_amount_spent"),
    )
    .withColumn("aov", round(col("total_amount_spent") / col("total_orders"), 2))
    .withColumn("retention_rate",
                round((col("total_orders") / lit(total_order)) * 100, 2))
    .orderBy("first_order_date", "last_order_date",
             desc("total_amount_spent"), desc("retention_rate"))
)
print("\n=== Customer Retention Rate ===")
customer_retention_df.select(
    "customer_unique_id", "first_order_date", "last_order_date",
    format_number("total_orders", 0).alias("total_orders"),
    format_number("total_amount_spent", 2).alias("total_amount_spent"),
    format_number("aov", 2).alias("aov"),
    format_number("retention_rate", 2).alias("retention_rate"),
).orderBy(desc("retention_rate")).show()


# ---------------------------------------------------------------------------
# 7. Data enrichment
# ---------------------------------------------------------------------------

# --- 7a. Boolean order status flags ---
statuses = [
    row["order_status"]
    for row in full_orders_df.select("order_status").distinct().collect()
]
for status in statuses:
    full_orders_df = full_orders_df.withColumn(
        f"is_{status}",
        when(col("order_status") == status, True).otherwise(False)
    )
print("\n=== Order Status Flags ===")
full_orders_df.select("order_id", *[f"is_{s}" for s in statuses]).show()

# --- 7b. Customer segmentation by AOV ---
customer_spending_df = customer_spending_df.withColumn(
    "customer_type",
    when(col("AOV") > PREMIUM_CUSTOMER_AOV_THRESHOLD, "Premium")
    .when(col("AOV") > MEDIUM_CUSTOMER_AOV_THRESHOLD,  "Medium")
    .otherwise("Standard")
)

full_orders_df = full_orders_df.join(
    customer_spending_df.select("customer_unique_id", "customer_type"),
    "customer_unique_id", "left"
)
print("\n=== Customer Segmentation ===")
full_orders_df.select("customer_unique_id", "customer_type").show()

# Final dedup after enrichment joins
full_orders_df = full_orders_df.dropDuplicates(
    ["customer_unique_id", "order_id", "order_purchase_timestamp"]
)


# ---------------------------------------------------------------------------
# 8. Schema previews for all output DataFrames
# ---------------------------------------------------------------------------

output_schemas = {
    "Complete Orders": full_orders_df,
    "Customers Orders": orders_per_customers_df,
    "Seller Review Score": seller_avg_review_score_df,
    "Most Sold Products": most_sold_products_df,
    "Customer Revenue": revenue_customers_df,
    "Seller Products": seller_products_df,
    "Ranked Top Sellers": ranked_top_seller_products_df,
    "Dense Ranked Sellers": dense_ranked_top_seller_products_df,
    "Customer Spending Habit":  customer_spending_df,
    "Seller Performance": seller_performance_df,
    "Product Metrics": product_metrics_df,
    "Monthly Stat": monthly_stat_df,
    "Customer Retention": customer_retention_df,
}
for name, df in output_schemas.items():
    get_schema(df, name)


# ---------------------------------------------------------------------------
# 9. Write analysed datasets to HDFS Parquet
# ---------------------------------------------------------------------------

HDFS_ANALYZED_PATH = HDFS_PROCESSED_PATH.replace("processed", "analyzed")

parquet_write_map = {
    "completeOrderdetails":    full_orders_df,
    "customerOrders":          orders_per_customers_df,
    "avgSellerRevenueScore":   seller_avg_review_score_df,
    "mostSoldProducts":        most_sold_products_df,
    "customerPurchase":        revenue_customers_df,
    "sellersProduct":          seller_products_df,
    "rankedSellers":           ranked_top_seller_products_df,
    "denseRankedSellers":      dense_ranked_top_seller_products_df,
    "customerSpending":        customer_spending_df,
    "sellerPerformance":       seller_performance_df,
    "productsMetric":          product_metrics_df,
    "monthlyStat":             monthly_stat_df,
    "customerRetention":       customer_retention_df,
}

print("\n=== Writing analysed datasets to HDFS ===")
for folder, df in parquet_write_map.items():
    path = HDFS_ANALYZED_PATH + folder
    df.write.mode("overwrite").parquet(path)
    print(f"  Written → {path}")


# ---------------------------------------------------------------------------
# 10. Write to Hive tables (olist database must be pre-created)
# ---------------------------------------------------------------------------

hive_table_map = {
    "olist.completeOrderdetails":  full_orders_df,
    "olist.customerOrders":        orders_per_customers_df,
    "olist.avgSellerRevenueScore": seller_avg_review_score_df,
    "olist.mostSoldProducts":      most_sold_products_df,
    "olist.customerPurchase":      revenue_customers_df,
    "olist.sellersProduct":        seller_products_df,
    "olist.rankedSellers":         ranked_top_seller_products_df,
    "olist.denseRankedSellers":    dense_ranked_top_seller_products_df,
    "olist.customerSpending":      customer_spending_df,
    "olist.sellerPerformance":     seller_performance_df,
    "olist.productsMetric":        product_metrics_df,
    "olist.monthlyStat":           monthly_stat_df,
    "olist.customerRetention":     customer_retention_df,
}

print("\n=== Writing to Hive tables ===")
for table, df in hive_table_map.items():
    df.write.format("parquet").mode("overwrite").saveAsTable(table)
    print(f"  Saved  → {table}")


# ---------------------------------------------------------------------------
# 11. Clear cache and terminate Spark session
# ---------------------------------------------------------------------------
full_orders_df.unpersist()
spark.stop()
