# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Shared Configuration Setup
# MAGIC %run "./01_shared_config"

# COMMAND ----------

# DBTITLE 1,Imports
from datetime import date
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensions

# COMMAND ----------

# DBTITLE 1,Build dim_restaurant
dim_restaurant = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.restaurants")
    .select(
        "restaurant_id",
        "restaurant_name",
        "owner_name",
        "city",
        "area",
        "cuisine_primary",
        "cuisine_secondary",
        "rating",
        "review_count",
        "average_order_value",
        "opening_time",
        "closing_time",
        "restaurant_segment",
        "onboarding_date",
        "status",
    )
    .withColumn("restaurant_sk", surrogate_key("restaurant_id"))
)

dim_restaurant = add_unknown_member(
    dim_restaurant,
    restaurant_sk=-1,
    restaurant_id="UNKNOWN",
    restaurant_name="Unknown Restaurant",
)

overwrite(dim_restaurant, f"{CATALOG}.{GOLD_SCHEMA}.dim_restaurant")
print(f"dim_restaurant: {dim_restaurant.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Build dim_driver
dim_driver = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.drivers")
    .select(
        "driver_id",
        "first_name",
        "last_name",
        "phone",
        "city",
        "area",
        "vehicle_type",
        "joining_date",
        "employment_status",
        "rating",
        "total_deliveries",
        "bank_account_status",
    )
    .withColumn("driver_sk", surrogate_key("driver_id"))
)

dim_driver = add_unknown_member(
    dim_driver,
    driver_sk=-1,
    driver_id="NO_DRIVER",
    first_name="No Driver Assigned",
)

overwrite(dim_driver, f"{CATALOG}.{GOLD_SCHEMA}.dim_driver")
print(f"dim_driver: {dim_driver.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Build dim_date
date_bounds = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.orders")
    .select(
        F.min("order_timestamp").alias("min_ts"),
        F.max("order_timestamp").alias("max_ts"),
    )
    .first()
)

if date_bounds["min_ts"] is None:
    raise ValueError("silver.orders has no order_timestamp values")

dim_date = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{date_bounds["min_ts"]}'),
        to_date('{date_bounds["max_ts"]}'),
        interval 1 day
    )) AS full_date
""")

dim_date = dim_date.withColumns(
    {
        "date_sk": F.date_format("full_date", "yyyyMMdd").cast("int"),
        "year": F.year("full_date"),
        "month": F.month("full_date"),
        "day_of_month": F.dayofmonth("full_date"),
        "day_name": F.date_format("full_date", "EEEE"),
        "is_weekend": F.dayofweek("full_date").isin([1, 7]),
    }
)

dim_date = add_unknown_member(
    dim_date,
    date_sk=-1,
    full_date=date(1900, 1, 1),
    year=1900,
    month=1,
    day_of_month=1,
    day_name="Monday",
    is_weekend=False,
)

overwrite(dim_date, f"{CATALOG}.{GOLD_SCHEMA}.dim_date")
print(f"dim_date: {dim_date.count():,} days")

# COMMAND ----------

# DBTITLE 1,Build dim_city
dim_city = spark.createDataFrame(
    [
        ("Mumbai", "West"),
        ("Delhi", "North"),
        ("Bengaluru", "South"),
        ("Hyderabad", "South"),
        ("Chennai", "South"),
        ("Pune", "West"),
        ("Kolkata", "East"),
        ("Ahmedabad", "West"),
        ("Jaipur", "North"),
        ("Chandigarh", "North"),
    ],
    ["city_name", "region"],
).withColumn("city_sk", surrogate_key("city_name"))

dim_city = add_unknown_member(
    dim_city,
    city_sk=-1,
    city_name="UNKNOWN",
    region="UNKNOWN",
)

overwrite(dim_city, f"{CATALOG}.{GOLD_SCHEMA}.dim_city")
print(f"dim_city: {dim_city.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Facts

# COMMAND ----------

# DBTITLE 1,Build fact_orders
orders = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.orders")

dim_customer = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_customer")
dim_restaurant = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_restaurant")
dim_driver = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_driver")
dim_date = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_date")
dim_city = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_city")

fact_orders = (
    orders.alias("o")
    # Pick the customer version that was valid when the order was placed.
    .join(
        dim_customer.alias("c"),
        (F.col("o.customer_id") == F.col("c.customer_id"))
        & (F.col("o.order_timestamp") >= F.col("c.valid_from"))
        & (
            (F.col("o.order_timestamp") < F.col("c.valid_to"))
            | F.col("c.valid_to").isNull()
        ),
        "left",
    )
    .join(
        dim_restaurant.alias("r"),
        F.col("o.restaurant_id") == F.col("r.restaurant_id"),
        "left",
    )
    .join(
        dim_driver.alias("d"),
        F.col("o.driver_id") == F.col("d.driver_id"),
        "left",
    )
    .join(
        dim_date.alias("dt"),
        F.to_date("o.order_timestamp") == F.col("dt.full_date"),
        "left",
    )
    .join(
        dim_city.alias("ci"),
        F.col("o.delivery_city") == F.col("ci.city_name"),
        "left",
    )
    .select(
        F.col("o.order_id"),
        F.coalesce(F.col("c.customer_sk"), F.lit(-1)).alias("customer_sk"),
        F.coalesce(F.col("r.restaurant_sk"), F.lit(-1)).alias("restaurant_sk"),
        F.coalesce(F.col("d.driver_sk"), F.lit(-1)).alias("driver_sk"),
        F.coalesce(F.col("dt.date_sk"), F.lit(-1)).alias("date_sk"),
        F.coalesce(F.col("ci.city_sk"), F.lit(-1)).alias("city_sk"),
        F.col("o.order_status"),
        F.col("o.cancellation_reason"),
        F.col("o.payment_method"),
        F.col("o.source_channel"),
        F.col("o.promo_code"),
        F.col("o.subtotal"),
        F.col("o.discount_amount"),
        F.col("o.tax_amount"),
        F.col("o.delivery_fee"),
        F.col("o.surge_fee"),
        F.col("o.tip_amount"),
        F.col("o.total_amount"),
        F.col("o.distance_km"),
        F.col("o.estimated_delivery_minutes"),
        F.col("o.actual_delivery_minutes"),
        F.col("o.order_timestamp"),
        F.col("o.delivered_timestamp"),
    )
)

# One Silver order must produce exactly one fact row. A higher count means a
# dimension join fanned out, usually from duplicate keys or overlapping SCD2.
orders_count = orders.count()
fact_count = fact_orders.count()

assert fact_count == orders_count, (
    f"fact_orders fan-out: silver.orders has {orders_count:,} rows but the "
    f"join has {fact_count:,} rows. Check dimension keys and SCD2 ranges."
)

overwrite(fact_orders, f"{CATALOG}.{GOLD_SCHEMA}.fact_orders")
print(f"fact_orders: {fact_count:,} rows")

# COMMAND ----------

# DBTITLE 1,Copy pass-through facts
for source_name, gold_name in [
    ("order_items", "fact_order_items"),
    ("payments", "fact_payments"),
    ("support_tickets", "fact_support_tickets"),
]:
    df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{source_name}").drop(
        "_ingested_at", "_source_file", "_rescued_data"
    )

    overwrite(df, f"{CATALOG}.{GOLD_SCHEMA}.{gold_name}")
    print(f"{gold_name}: {df.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Count orders that fell back to an unknown member
display(
    spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_orders").selectExpr(
        "sum(case when restaurant_sk = -1 then 1 else 0 end) as unknown_restaurant_orders",
        "sum(case when driver_sk = -1 then 1 else 0 end) as no_driver_orders",
        "sum(case when customer_sk = -1 then 1 else 0 end) as unknown_customer_orders",
        "sum(case when city_sk = -1 then 1 else 0 end) as unknown_city_orders",
        "sum(case when date_sk = -1 then 1 else 0 end) as unknown_date_orders",
        "count(*) as total_orders",
    )
)