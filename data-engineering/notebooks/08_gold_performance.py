# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Shared Configuration Setup
# MAGIC %run "./01_shared_config"

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Check fact_orders exists
fact_orders_table = f"{CATALOG}.{GOLD_SCHEMA}.fact_orders"

if not spark.catalog.tableExists(fact_orders_table):
    raise ValueError(
        f"{fact_orders_table} does not exist. Run 07_dimensional_model first."
    )

# COMMAND ----------

# DBTITLE 1,Compact and cluster fact_orders
display(spark.sql(f"OPTIMIZE {fact_orders_table} ZORDER BY (restaurant_sk, city_sk)"))

# COMMAND ----------

# DBTITLE 1,Check fact grain
fact_orders = spark.table(fact_orders_table)
silver_orders = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.orders")

fact_count = fact_orders.count()
silver_count = silver_orders.count()

assert fact_count == silver_count, (
    f"fact_orders has {fact_count:,} rows but silver.orders has "
    f"{silver_count:,}. A dimension join fanned out."
)

print(f"OK fact_orders grain: {fact_count:,} rows")

# COMMAND ----------

# DBTITLE 1,Check keys and unknown members
assert (
    fact_orders.filter(F.col("order_id").isNull()).count() == 0
), "fact_orders.order_id has nulls"

print("OK fact_orders.order_id: no nulls")

foreign_keys = [
    ("customer_sk", "dim_customer", "customer_sk"),
    ("restaurant_sk", "dim_restaurant", "restaurant_sk"),
    ("driver_sk", "dim_driver", "driver_sk"),
    ("date_sk", "dim_date", "date_sk"),
    ("city_sk", "dim_city", "city_sk"),
]

for fact_key, dim_name, dim_key in foreign_keys:
    null_count = fact_orders.filter(F.col(fact_key).isNull()).count()
    assert null_count == 0, f"fact_orders.{fact_key} has {null_count} nulls"

    dimension = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.{dim_name}").select(dim_key)

    unknown_count = dimension.filter(F.col(dim_key) == -1).count()
    assert unknown_count == 1, (
        f"{dim_name} must have exactly one {dim_key} = -1 row, "
        f"found {unknown_count}"
    )

    missing = (
        fact_orders.alias("f")
        .filter(F.col(f"f.{fact_key}") != -1)
        .join(
            dimension.alias("d"),
            F.col(f"f.{fact_key}") == F.col(f"d.{dim_key}"),
            "left_anti",
        )
        .count()
    )
    assert missing == 0, (
        f"{missing:,} fact_orders rows point to a missing {dim_name}.{dim_key}"
    )

    print(f"OK {fact_key} -> {dim_name} (unknown member present)")

print("All Gold checks passed.")