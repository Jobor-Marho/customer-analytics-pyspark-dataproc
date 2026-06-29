# Pipeline Overview

## Architecture

```
Kaggle (CSV)
     │
     ▼
Local Machine  ──curl──►  olist/data/*.csv
     │
     ▼  hdfs dfs -put
HDFS  /user/<username>/project/olist/data/
     │
     ▼   1 — Data Ingestion & Exploration
Spark (Dataproc)  ── row counts, nulls, duplicates, distributions
     │
     ▼   2 — Data Cleaning & Transformation
HDFS  /user/<username>/project/olist/processed/  (Parquet)
     │
     ▼   3 — Data Integration & Aggregation
HDFS  /user/<username>/project/olist/analyzed/   (Parquet)
Hive  olist.*  (tables)
```

## HDFS Layout

```
/user/<username>/project/olist/
├── data/                          # raw CSVs (input)
│   ├── olist_customers_dataset.csv
│   ├── olist_geolocation_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_order_payments_dataset.csv
│   ├── olist_order_reviews_dataset.csv
│   ├── olist_orders_dataset.csv
│   ├── olist_products_dataset.csv
│   ├── olist_sellers_dataset.csv
│   └── product_category_name_translation.csv
├── processed/                     # cleaned Parquet (Module 2 output)
│   ├── customers/
│   ├── orders/
│   ├── orderItems/
│   ├── products/
│   ├── payments/
│   ├── geolocations/
│   ├── sellers/
│   ├── reviews/
│   └── categories_transalation/
└── analyzed/                      # aggregated Parquet (Module 3 output)
    ├── completeOrderdetails/
    ├── customerOrders/
    ├── avgSellerRevenueScore/
    ├── mostSoldProducts/
    ├── customerPurchase/
    ├── sellersProduct/
    ├── rankedSellers/
    ├── denseRankedSellers/
    ├── customerSpending/
    ├── sellerPerformance/
    ├── productsMetric/
    ├── monthlyStat/
    └── customerRetention/
```

## Spark Optimisations (Module 3)

| Technique | Applied to |
|-----------|-----------|
| `broadcast()` | products (~1.3 MB), sellers (~128 KB), categories (~3 KB), payments (~3.7 MB) |
| Adaptive Query Execution (AQE) | All joins and shuffles |
| AQE skew join handling | Customer and geolocation joins |
| Partition tuning (`shuffle.partitions=64`) | All aggregations |
| `coalesce(1)` on write | Module 2 cleaned Parquet files |
| `.cache()` | `full_orders_df` in Module 3 |

## Missing Value Strategy

| Dataset | Column(s) | Strategy |
|---------|-----------|----------|
| Orders | `order_approved_at` | Impute with `order_purchase_timestamp` for delivered orders |
| Orders | `order_delivered_customer_date` | Impute with `order_estimated_delivery_date` for delivered orders |
| Orders | `order_delivered_carrier_date` | Impute with midpoint of approved_at and customer_date for delivered orders |
| Products | `product_weight_g`, `*_cm` | Impute by mode via `pyspark.ml.feature.Imputer` |
| Products | `product_category_name`, `product_name_lenght`, etc. | Left as-is (requires source data) |
| Reviews | Nulls | Preserved via left joins in Module 3 |
