# Databricks notebook source
# DBTITLE 1,Schemas and Paths
CATALOG = "swiftbite"
RAW_ROOT = f"/Volumes/{CATALOG}/landing/raw"
CHECKPOINT_ROOT = f"/Volumes/{CATALOG}/landing/checkpoints"

BRONZE_SCHEMA = "bronze"
QUARANTINE_SCHEMA = "quarantine"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# COMMAND ----------

# DBTITLE 1,Tables with File Formats and Directory Paths
TABLES = {
    "customers": dict(format="csv", path="customers/customers*.csv"),
    "restaurants": dict(format="csv", path="restaurants/restaurants*.csv"),
    "drivers": dict(format="csv", path="drivers/drivers*.csv"),
    "orders": dict(format="csv", path="orders/orders*.csv"),
    "order_items": dict(format="csv", path="order_items/order_items*.csv"),
    "payments": dict(format="csv", path="payments/payments*.csv"),
    "support_tickets": dict(format="csv", path="support_tickets/support_tickets*.csv"),
    "traffic": dict(format="csv", path="traffic/traffic*.csv"),
    "weather": dict(format="csv", path="weather/weather*.csv"),
    "order_events": dict(format="json", path="order_events/*/*/*/*.jsonl"),
    "driver_gps": dict(format="json", path="driver_gps/*/*/*/*.jsonl"),
    "restaurant_status_events": dict(format="json", path="restaurant_status_events/*/*/*/*.jsonl")
}

# COMMAND ----------

# DBTITLE 1,Deduplication Keys and Ordering Configuration
DEDUP_KEYS = {
    "restaurants": dict(key="restaurant_id", order_by="_ingested_at", desc=True),
    "drivers": dict(key="driver_id", order_by="_ingested_at", desc=True),
    "orders": dict(key="order_id", order_by="updated_at", desc=True),
    "order_items": dict(key="order_item_id", order_by="_ingested_at", desc=True),
    "order_events": dict(key="event_id", order_by="ingestion_time", desc=False),
    "driver_gps": dict(key="event_id", order_by="ingestion_time", desc=False),
    "restaurant_status_events": dict(key="event_id", order_by="ingestion_time", desc=False),
}

# COMMAND ----------

# DBTITLE 1,Import PySpark SQL Functions Module
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Generate Row Fingerprint Hash for each row
def with_row_fingerprint(df):
    # Select columns containing actual data, excluding metadata
    data_columns = [column for column in df.columns if not column.startswith("_")]

    # If no data columns exist, create an empty fingerprint
    if not data_columns:
        return df.withColumn("_row_fingerprint", F.lit(None).cast("long"))

    # Convert values to strings and replace nulls with a placeholder
    data_values = [F.coalesce(F.col(column).cast("string"), F.lit("\\N")) for column in data_columns]

    # Create a hash from the data values
    return df.withColumn("_row_fingerprint", F.xxhash64(*data_values))

# COMMAND ----------

# DBTITLE 1,Overwrite a Delta table
def overwrite(df, table_name):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )