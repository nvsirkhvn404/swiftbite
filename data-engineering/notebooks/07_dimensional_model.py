# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Shared Configuration Setup
# MAGIC %run "./01_shared_config"

# COMMAND ----------

# DBTITLE 1,Import Spark SQL Functions and Row Class
from pyspark.sql import functions as F, Row

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensions

# COMMAND ----------

# DBTITLE 1,Restaurant Dimension Creation and Data Enrichment
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
    .withColumn("restaurant_sk", F.monotonically_increasing_id())
)

unknown_restaurant_values = {c: None for c in dim_restaurant.columns}
unknown_restaurant_values.update(
    restaurant_id="UNKNOWN", restaurant_name="Unknown Restaurant", restaurant_sk=-1
)

unknown_restaurant = spark.createDataFrame(
    [Row(**unknown_restaurant_values)], schema=dim_restaurant.schema
)
dim_restaurant = dim_restaurant.unionByName(unknown_restaurant)

(
    dim_restaurant.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.dim_restaurant")
)
print(f"dim_restaurant: {dim_restaurant.count():,} rows (including the Unknown member)")

# COMMAND ----------

# DBTITLE 1,Driver Dimension with Unknown Driver Handling
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
    .withColumn("driver_sk", F.monotonically_increasing_id())
)

unknown_driver_values = {c: None for c in dim_driver.columns}
unknown_driver_values.update(
    driver_id="NO_DRIVER", first_name="No Driver Assigned", driver_sk=-1
)

unknown_driver = spark.createDataFrame(
    [Row(**unknown_driver_values)], schema=dim_driver.schema
)

dim_driver = dim_driver.unionByName(unknown_driver)

(
    dim_driver.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.dim_driver")
)
print(f"dim_driver: {dim_driver.count():,} rows (including the Unknown member)")

# COMMAND ----------

# DBTITLE 1,Date Dimension Table Creation with Date Range Expansion
date_bounds = (
    spark.table(f"{CATALOG}.{SILVER_SCHEMA}.orders")
    .select(
        F.min("order_timestamp").alias("min_ts"),
        F.max("order_timestamp").alias("max_ts"),
    )
    .first()
)


dim_date = spark.sql(f"""
    SELECT explode(sequence(
        to_date('{date_bounds["min_ts"]}'), to_date('{date_bounds["max_ts"]}'), interval 1 day
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


(
    dim_date.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.dim_date")
)
print(f"dim_date: {dim_date.count():,} days")

# COMMAND ----------

# DBTITLE 1,City Dimension
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
).withColumn("city_sk", F.monotonically_increasing_id())

(
    dim_city.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.dim_city")
)
print(f"dim_city: {dim_city.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Facts

# COMMAND ----------

# DBTITLE 1,fact_orders
orders = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.orders")
dim_customer = spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_customer")
 
fact_orders = (
    orders.alias("o")
    .join(
        dim_customer.alias("c"),
        (F.col("o.customer_id") == F.col("c.customer_id")) &
        (F.col("o.order_timestamp") >= F.col("c.valid_from")) &
        ((F.col("o.order_timestamp") < F.col("c.valid_to")) | F.col("c.valid_to").isNull()),
        "left",
    )
    .join(dim_restaurant.alias("r"), F.col("o.restaurant_id") == F.col("r.restaurant_id"), "left")
    .join(dim_driver.alias("d"), F.col("o.driver_id") == F.col("d.driver_id"), "left")
    .join(dim_date.alias("dt"), F.to_date("o.order_timestamp") == F.col("dt.full_date"), "left")
    .join(dim_city.alias("ci"), F.col("o.delivery_city") == F.col("ci.city_name"), "left")
    .select(
        F.col("o.order_id"),
        F.coalesce(F.col("c.customer_sk"), F.lit(-1)).alias("customer_sk"),
        F.coalesce(F.col("r.restaurant_sk"), F.lit(-1)).alias("restaurant_sk"),
        F.coalesce(F.col("d.driver_sk"), F.lit(-1)).alias("driver_sk"),
        F.col("dt.date_sk"),
        F.coalesce(F.col("ci.city_sk"), F.lit(-1)).alias("city_sk"),
        F.col("o.order_status"), F.col("o.cancellation_reason"),
        F.col("o.payment_method"), F.col("o.source_channel"), F.col("o.promo_code"),
        F.col("o.subtotal"), F.col("o.discount_amount"), F.col("o.tax_amount"),
        F.col("o.delivery_fee"), F.col("o.surge_fee"), F.col("o.tip_amount"), F.col("o.total_amount"),
        F.col("o.distance_km"), F.col("o.estimated_delivery_minutes"), F.col("o.actual_delivery_minutes"),
        F.col("o.order_timestamp"), F.col("o.delivered_timestamp"),
    )
)

(fact_orders.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.fact_orders"))

orders_in, facts_out = orders.count(), fact_orders.count()
print(f"orders in: {orders_in:,}   fact_orders out: {facts_out:,}\n"
      f"{'OK, counts match' if orders_in == facts_out else '!! MISMATCH -- a join fanned out somewhere, stop and investigate'}")

# COMMAND ----------

# DBTITLE 1,fact_order_items, fact_payments, fact_support_tickets
for source_name, gold_name in [
    ("order_items", "fact_order_items"),
    ("payments", "fact_payments"),
    ("support_tickets", "fact_support_tickets"),
]:
    df = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{source_name}")

    df = df.drop("_ingested_at", "_source_file", "_rescued_data")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.{gold_name}")
    )
    print(f"{gold_name}: {df.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Summary of Missing Keys in Orderd
display(
    spark.table(f"{CATALOG}.{GOLD_SCHEMA}.fact_orders").selectExpr(
        "sum(case when restaurant_sk = -1 then 1 else 0 end) as unknown_restaurant_orders",
        "sum(case when driver_sk = -1 then 1 else 0 end) as no_driver_orders",
        "sum(case when customer_sk = -1 then 1 else 0 end) as unknown_customer_orders",
        "count(*) as total_orders",
    )
)