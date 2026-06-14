# Databricks notebook source
# MAGIC %md
# MAGIC # Assignment 3: Build a Production-Style Lakehouse Pipeline in Databricks
# MAGIC
# MAGIC We work in a **group of 3**:
# MAGIC * _Filipovych Marharyta_
# MAGIC * _Ksenia Hanziuk_
# MAGIC * _Sofia Churikova_
# MAGIC
# MAGIC Our _raw layer_ contains **8 distinct tables**. We took the flat
# MAGIC Global Superstore file and **normalized** it into a small star schema (1 fact + 7 dimensions).
# MAGIC
# MAGIC
# MAGIC ```
# MAGIC Raw Files (8 CSVs)  →  Bronze (8 Delta tables)  →  Silver (clean + validated)  →  Gold (aggregates)  →  Analytics
# MAGIC ```
# MAGIC
# MAGIC **Raw tables (8):** `order_items` (the main one), `orders`, `customers`, `products`, `locations`, `categories`, `regions`, `ship_modes`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup & Configuration
# MAGIC
# MAGIC **Before running:** upload the CSV files to a Unity Catalog **Volume** and set `RAW_PATH` below.
# MAGIC
# MAGIC In Databricks Free Edition: *Catalog → your catalog → Schema → Volumes → Create volume → Upload*.
# MAGIC
# MAGIC We uploaded the 8 `raw/*.csv` files and the 3 `incremental/day_*.csv` files.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast

CATALOG = "workspace"
SCHEMA = "superstore"
RAW_PATH = "/Volumes/workspace/default/superstore/raw"
INC_PATH = "/Volumes/workspace/default/superstore/incremental"

DELTA = "delta"
OVERWRITE = "overwrite"
OVERWRITE_SCHEMA = "overwriteSchema"
HEADER = "header"
INFER_SCHEMA = "inferSchema"
SOURCE_FILE_NAME = "source_file_name"
INGESTION_TIMESTAMP = "ingestion_timestamp"
LEFT = "left"
DATE_FORMAT = "dd-MM-yyyy"

SILVER_ORDERS = "silver_orders"
SILVER_REJECTED_ORDERS = "silver_rejected_orders"

ORDER_DATE = "order_date"
SHIP_DATE = "ship_date"
SALES = "sales"
QUANTITY = "quantity"
REGION = "region"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")
print(f"Using schema: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: Bronze Layer
# MAGIC
# MAGIC The Bronze layer is a **raw copy** of the source files. We do not clean anything here.
# MAGIC We only add two audit columns:
# MAGIC * `ingestion_timestamp` — when the row was loaded (`current_timestamp()`),
# MAGIC * `source_file_name` — which file it came from.

# COMMAND ----------

RAW_TABLES = ["order_items", "orders", "customers", "products",
              "locations", "categories", "regions", "ship_modes"]

def load_bronze(name: str):
    df = (spark.read
          .option(HEADER, True)
          .option(INFER_SCHEMA, True)
          .csv(f"{RAW_PATH}/{name}.csv")
          .withColumn(INGESTION_TIMESTAMP, F.current_timestamp())
          .withColumn(SOURCE_FILE_NAME, F.lit(f"{name}.csv")))
    (df.write.format(DELTA).mode(OVERWRITE)
     .option(OVERWRITE_SCHEMA, True)
     .saveAsTable(f"bronze_{name}"))
    print(f"bronze_{name} -> {spark.table(f'bronze_{name}').count()} rows")

for raw_table in RAW_TABLES:
    load_bronze(raw_table)

# COMMAND ----------

# MAGIC %md
# MAGIC _Quick check_: `bronze_orders` exists, is a Delta table, and has the two audit columns.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_orders LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: Silver Layer
# MAGIC
# MAGIC Silver = **clean and query-ready** data. Steps:
# MAGIC 1. **Integrate** `order_items` with its dimensions (`customers`, `products`, `locations`)
# MAGIC    using many-to-one left joins.
# MAGIC 2. **Parse dates** (`dd-MM-yyyy` format) and **standardize text** (`trim`, `initcap`).
# MAGIC 3. **Remove duplicates** with `ROW_NUMBER()` over the business key `row_id` (window function).
# MAGIC 4. **Validate** the rules and split: good rows → `silver_orders`, bad rows → `silver_rejected_orders`.

# COMMAND ----------

order_items = spark.table("bronze_order_items")
customers = spark.table("bronze_customers").select("customer_id", "customer_name", "segment")
products = spark.table("bronze_products").select("product_id", "category", "sub_category", "product_name")
locations = spark.table("bronze_locations").select("location_id", "city", "state", "country", "market", REGION)


def join_and_clean(df):
    return (df
            .join(customers, "customer_id", LEFT)
            .join(products, "product_id", LEFT)
            .join(locations, "location_id", LEFT)
            .withColumn(ORDER_DATE, F.to_date(ORDER_DATE, DATE_FORMAT))
            .withColumn(SHIP_DATE, F.to_date(SHIP_DATE, DATE_FORMAT))
            .withColumn("segment", F.initcap(F.trim("segment")))
            .withColumn(REGION, F.initcap(F.trim(REGION)))
            .withColumn("ship_mode", F.trim("ship_mode"))
            .withColumn("category", F.trim("category"))
            .withColumn("product_name", F.trim("product_name")))


cleaned = join_and_clean(order_items)

ROW_NUMBER = "row_number"
window = Window.partitionBy("row_id").orderBy(F.col(INGESTION_TIMESTAMP).desc())
deduplicated = (cleaned
                .withColumn(ROW_NUMBER, F.row_number().over(window))
                .filter(F.col(ROW_NUMBER) == 1)
                .drop(ROW_NUMBER))


def split_valid_and_rejected(df):
    validation_rule = (
        (F.col(SALES).isNotNull()) &
        (F.col(QUANTITY).isNotNull()) &
        (F.col(SALES) >= 0) &
        (F.col(QUANTITY) > 0) &
        (F.col(SHIP_DATE) >= F.col(ORDER_DATE))
    )

    valid = df.filter(validation_rule)
    rejected = (df.filter(~validation_rule)
                .withColumn("rejection_reason",
                            F.when(F.col(SALES).isNull(), "sales is null")
                            .when(F.col(QUANTITY).isNull(), "quantity is null")
                            .when(F.col(SALES) < 0, "sales < 0")
                            .when(F.col(QUANTITY) <= 0, "quantity <= 0")
                            .when(F.col(SHIP_DATE) < F.col(ORDER_DATE), "ship_date < order_date")
                            .otherwise("unknown")))
    return valid, rejected


silver_orders, silver_rejected = split_valid_and_rejected(deduplicated)
(silver_orders.write.format(DELTA).mode(OVERWRITE)
 .option(OVERWRITE_SCHEMA, True).saveAsTable(SILVER_ORDERS))
(silver_rejected.write.format(DELTA).mode(OVERWRITE)
    .option(OVERWRITE_SCHEMA, True).saveAsTable(SILVER_REJECTED_ORDERS))

print(f"silver_orders has {spark.table(SILVER_ORDERS).count()} rows")
print(f"silver_rejected_orders has {spark.table(SILVER_REJECTED_ORDERS).count()} rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_orders LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: Incremental Loading
# MAGIC
# MAGIC We simulate new data arriving every day as `day_1.csv`, `day_2.csv`, `day_3.csv`.
# MAGIC
# MAGIC We clean each daily file the **same way** as silver, then use **`MERGE INTO`** on the business key
# MAGIC `row_id` so that resent rows are **not** duplicated.
# MAGIC
# MAGIC * `day_2.csv` re-sends 3 rows from `day_1` → MERGE must keep them once.
# MAGIC * `day_3.csv` contains 3 deliberately invalid rows → they must be rejected, not merged.

# COMMAND ----------

def process_day(day_file: str):
    raw = (spark.read.option(HEADER, True)
           .option(INFER_SCHEMA, True)
           .csv(f"{INC_PATH}/{day_file}")
           .withColumn(INGESTION_TIMESTAMP, F.current_timestamp())
           .withColumn(SOURCE_FILE_NAME, F.lit(day_file)))

    polished = join_and_clean(raw)
    good_data, bad_data = split_valid_and_rejected(polished)

    good_data.createOrReplaceTempView("incoming_good_data")

    spark.sql("""
            MERGE INTO silver_orders AS t
            USING incoming_good AS s
            ON t.row_id = s.row_id
            WHEN NOT MATCHED THEN INSERT *
        """)

    bad_data.write.format(DELTA).mode("append").option("mergeSchema", True).saveAsTable(SILVER_REJECTED_ORDERS)

    print(f"{day_file}: incoming valid={good_data.count()}, invalid={bad_data.count()}, "
          f"silver_orders now={spark.table(SILVER_ORDERS).count()}")


print("silver_orders before increment:", spark.table(SILVER_ORDERS).count())

for file in ["day_1.csv", "day_2.csv", "day_3.csv"]:
    process_day(file)

# COMMAND ----------

# MAGIC %md
# MAGIC Proof that the MERGE is idempotent: rerunning `day_2` must not change the count: `total_rows` must equal `distinct_keys`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_rows, COUNT(DISTINCT row_id) AS distinct_keys FROM silver_orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4: Gold Layer
# MAGIC
# MAGIC Gold = **business-level aggregates** built straight from `silver_orders`. Revenue = `SUM(sales)`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_sales_daily AS
# MAGIC SELECT order_date AS date, ROUND(SUM(sales), 2) AS revenue
# MAGIC FROM silver_orders
# MAGIC GROUP BY order_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_sales_region AS
# MAGIC SELECT region, ROUND(SUM(sales), 2) AS revenue
# MAGIC FROM silver_orders
# MAGIC GROUP BY region;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_sales_category AS
# MAGIC SELECT category, ROUND(SUM(sales), 2) AS revenue
# MAGIC FROM silver_orders
# MAGIC GROUP BY category;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_customer_metrics AS
# MAGIC SELECT customer_name AS customer,
# MAGIC        ROUND(SUM(sales), 2)       AS revenue,
# MAGIC        COUNT(DISTINCT order_id)   AS orders
# MAGIC FROM silver_orders
# MAGIC GROUP BY customer_name;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_sales_region ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_sales_category ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5: Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Top 5 products by revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_name, ROUND(SUM(sales), 2) AS revenue
# MAGIC FROM silver_orders
# MAGIC GROUP BY product_name
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Top region by revenue

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT region, ROUND(SUM(sales), 2) AS revenue
# MAGIC FROM silver_orders
# MAGIC GROUP BY region
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3 & 4. Monthly revenue trend and running total

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH monthly AS (
# MAGIC   SELECT date_format(order_date, 'yyyy-MM') AS the_month, SUM(sales) AS revenue
# MAGIC   FROM silver_orders
# MAGIC   GROUP BY date_format(order_date, 'yyyy-MM')
# MAGIC )
# MAGIC SELECT
# MAGIC   the_month, ROUND(revenue, 2) AS monthly_revenue,
# MAGIC   ROUND(LAG(revenue) OVER (ORDER BY the_month), 2) AS previous_month_revenue,
# MAGIC   ROUND(revenue - LAG(revenue) OVER (ORDER BY the_month), 2) AS month_change,
# MAGIC   ROUND(SUM(revenue) OVER (ORDER BY the_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS running_total
# MAGIC FROM monthly
# MAGIC ORDER BY the_month;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5. Top product per region

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ranked AS (
# MAGIC   SELECT region, product_name, SUM(sales) AS revenue,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY region ORDER BY SUM(sales) DESC) AS row_number
# MAGIC   FROM silver_orders
# MAGIC   GROUP BY region, product_name
# MAGIC )
# MAGIC SELECT region, product_name, ROUND(revenue, 2) AS revenue
# MAGIC FROM ranked
# MAGIC WHERE row_number = 1
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 6: Performance Analysis
# MAGIC
# MAGIC We analyze a join between the large `silver_orders` table and the tiny `regions` table (13 rows).
# MAGIC We compare the execution plan **without** and **with** `broadcast()`.

# COMMAND ----------

regions = (spark.table("bronze_regions")
           .select(F.initcap(F.trim(REGION)).alias(REGION), "market"))
silver_orders_table = spark.table(SILVER_ORDERS)

print("Default join:")
default_join = silver_orders_table.join(regions, REGION, LEFT)
default_join.explain()

print("Optimized join with broadcast:")
broadcast_join = silver_orders_table.join(broadcast(regions), REGION, LEFT)
broadcast_join.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **What we observe in the plans:**
# MAGIC
# MAGIC The default plan uses standard **SortMergeJoin** join that requires a **shuffle** (movement) of the data between executors (**Exchange** appears - physical plan operation that triggers a shuffle).
# MAGIC So, when spark joins two tables:
# MAGIC 1. it shuffles data across all worker nodes
# MAGIC 2. it matches keys across partitions
# MAGIC This process causes network IO, memory pressure, and slow execution.
# MAGIC
# MAGIC After wrapping region table within `broadcast()` function, the plan switches to **BroadcastHashJoin**: spark replicates a small table to all executors for a faster join and avoid shuffles.
# MAGIC
# MAGIC However, we should use `broadcast()` only when joining with a small table because broadcasting large datasets will crash the driver.
# MAGIC Hence, we should use `broadcast()` on a small dataset and always verify its success with `explain()`.