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

# DBTITLE 1,Overwrite a Delta table
def overwrite(df, table_name):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )

# COMMAND ----------

# DBTITLE 1,Create a stable surrogate key
def surrogate_key(*columns):
    values = [F.col(column).cast("string") for column in columns]
    return F.pmod(F.xxhash64(*values), F.lit(2**63 - 1))

# COMMAND ----------

# DBTITLE 1,Add an unknown member to a dimension
def add_unknown_member(df, **values):
    row = {column: None for column in df.columns}
    row.update(values)
    unknown = spark.createDataFrame([Row(**row)], schema=df.schema)
    return df.unionByName(unknown)