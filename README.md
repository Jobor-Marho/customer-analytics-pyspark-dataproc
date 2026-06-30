# Olist E-Commerce Analytics — PySpark on GCP Dataproc

End-to-end big data pipeline analysing the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data) using **PySpark** on **Google Cloud Dataproc** with **HDFS** as the storage layer.

---

## Project Overview

The pipeline is split across three modules:

| Module | Notebook | Description |
|--------|----------|-------------|
| 1 | `01_data_ingestion_exploration.ipynb` | Ingest raw CSVs into HDFS, create Spark DataFrames, and explore distributions (customers, orders, payments, products) |
| 2 | `02_data_cleaning_transformation.ipynb` | Handle missing values (order dates, product dimensions), standardise schemas, and write cleaned data to HDFS as Parquet |
| 3 | `03_data_integration_aggregation.ipynb` | Join all datasets into a single orders DataFrame, run aggregations (revenue, retention, seller performance, product metrics), and apply window/ranking functions |

---

## Dataset

**Source:** [Kaggle — Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data)

The dataset contains ~100k orders placed on the Olist marketplace between 2016 and 2018, spread across 9 CSV files:

```
olist_customers_dataset.csv
olist_geolocation_dataset.csv
olist_order_items_dataset.csv
olist_order_payments_dataset.csv
olist_order_reviews_dataset.csv
olist_orders_dataset.csv
olist_products_dataset.csv
olist_sellers_dataset.csv
product_category_name_translation.csv
```

> Raw data files are **not** committed to this repository. See [Data Setup](#data-setup) below.

---

## Project Structure

```
olist-pyspark/
├── notebooks/
│   ├── 01_data_ingestion_exploration.ipynb
│   ├── 02_data_cleaning_transformation.ipynb
│   └── 03_data_integration_aggregation.ipynb
├── src/
│   └── utils/
│       ├── spark_session.py        # SparkSession factory
│       └── helpers.py              # Reusable helper functions
├── configs/
│   └── spark_config.py             # Centralised Spark tuning config
├── data/
│   └── sample/                     # Small sample CSVs for local testing (optional)
├── docs/
│   ├── pipeline_overview.md            # Architecture and pipeline notes
│   ├── schema_all_dataframes.csv       # Combined schema dictionary for every DataFrame
│   ├── schema_module_1_ingestion.csv   # Schema for raw DataFrames (Module 1)
│   ├── schema_module_2_cleaning.csv    # Schema for cleaned DataFrames (Module 2)
│   └── schema_module_3_aggregation.csv # Schema for aggregated DataFrames (Module 3)
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Data Setup

### 1. Download the dataset

```bash
mkdir olist && cd olist
curl -L -o brazilian-ecommerce.zip \
    https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce
mkdir data && unzip brazilian-ecommerce.zip -d data/
```

### 2. Upload to HDFS on Dataproc

```bash
# SSH into your Dataproc master node, then:
hdfs dfs -mkdir -p /user/<your-username>/project/olist/data
hdfs dfs -put ~/olist/data/ /user/<your-username>/project/olist/data
hdfs dfs -ls /user/<your-username>/project/olist/data
```

> Update the `hdfs_base_path` variable at the top of each notebook to match your HDFS username.

---

## Running the Notebooks

Run the notebooks **in order** on your Dataproc cluster:

1. `01_data_ingestion_exploration.ipynb` — reads raw CSVs from HDFS
2. `02_data_cleaning_transformation.ipynb` — writes cleaned Parquet files to `/user/<username>/project/olist/processed/`
3. `03_data_integration_aggregation.ipynb` — reads from `processed/`, joins, aggregates, and analyses

### Opening Jupyter on Dataproc

```bash
# Port-forward the Jupyter server from the master node
gcloud compute ssh <cluster-master-node> -- -L 8888:localhost:8888
```

Then open `http://localhost:8888` in your browser.

---

## Key Analyses

- **Customer distribution** by city and state
- **Order status** breakdown and delivery duration
- **Payment type** distribution
- **Top selling products** by volume and revenue
- **Missing value handling** — order dates imputed by status logic; product dimensions imputed by mode
- **Optimised joins** using `broadcast()` for small lookup tables (products, sellers, categories, payments)
- **Window functions** — ranked and dense-ranked top products per seller
- **Advanced aggregations** — monthly revenue trends, AOV, seller performance metrics, customer retention rate
- **Customer segmentation** — Premium / Medium / Standard based on AOV thresholds

---

## Tech Stack

| Tool | Role |
|------|------|
| PySpark | Distributed data processing |
| GCP Dataproc | Managed Spark cluster |
| HDFS | Distributed file storage |
| Parquet | Columnar storage for processed data |
| Kaggle API | Data source |

---

## Requirements

See `requirements.txt`. Core dependencies:

```
pyspark>=3.3.0
```

> PySpark is pre-installed on Dataproc. `requirements.txt` is mainly useful for local development.
