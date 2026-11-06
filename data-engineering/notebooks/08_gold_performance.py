# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Shared Configuration Setup
# MAGIC %run "./01_shared_config"

# COMMAND ----------

# DBTITLE 1,Import Spark Functions
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Fact Orders Table Partitioned by Order Month
fact_orders = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_orders")
fact_orders = fact_orders.withColumn("order_month", F.date_format("order_timestamp", "yyyy-MM"))

(
    fact_orders.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("order_month")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.fact_orders")
)

display(spark.sql(f"SHOW PARTITIONS {CATALOG}.{GOLD_SCHEMA}.fact_orders"))

# COMMAND ----------

# DBTITLE 1,Optimize Fact Orders with Z-Ordering by Keys
spark.sql(f"OPTIMIZE {CATALOG}.{GOLD_SCHEMA}.fact_orders ZORDER BY (restaurant_sk, city_sk)")

# COMMAND ----------

# DBTITLE 1,Data completeness check
def data_completeness_check(name: str):
    bronze_n = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{name}").count()
    silver_n = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{name}").count()

    quarantine_table = f"{CATALOG}.{QUARANTINE_SCHEMA}.{name}_quarantine"
    if spark.catalog.tableExists(quarantine_table):
        quarantine_n = spark.table(quarantine_table).count()
    else:
        quarantine_n = 0

    duplicates_removed = bronze_n - quarantine_n - silver_n
    ok = duplicates_removed >= 0
    return (name, bronze_n, quarantine_n, silver_n, duplicates_removed, ok)


table_names = [
    "customers",
    "restaurants",
    "drivers",
    "orders",
    "order_items",
    "order_events",
    "driver_gps",
    "restaurant_status_events",
]
rows = [data_completeness_check(name) for name in table_names]

data_check_df = spark.createDataFrame(
    rows,
    [
        "dataset",
        "bronze_rows",
        "quarantine_rows",
        "silver_rows",
        "duplicates_removed",
        "checks",
    ],
)
display(data_check_df)

failing = data_check_df.filter("NOT checks")
if failing.count() > 0:
    display(failing)
    raise AssertionError("Data completeness check failed for at least one table, see rows above.")

print("All datasets completeness check: bronze = quarantine + silver + duplicates_removed.")