# Databricks notebook source
# DBTITLE 1,Load Shared Configurations
# MAGIC %run "./01_shared_config"

# COMMAND ----------

# DBTITLE 1,Import PySpark SQL Functions
from pyspark.sql.functions import col, trim, desc, regexp_replace, when, initcap, upper, row_number, length, abs, asc, count
from pyspark.sql import Window

# COMMAND ----------

# DBTITLE 1,Load and Preview Bronze Table Data
def get_bronze(name:str):
    df = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{name}")
    print(f"{name.capitalize()} count:", df.count())
    display(df.limit(10))
    return df

# COMMAND ----------

# DBTITLE 1,Deduplicate Rows with Window Ranking
def dedupe(df, name: str):
    if name not in DEDUP_KEYS:
        return df

    dk = DEDUP_KEYS[name]
    key = dk["key"]

    order_col = col(dk["order_by"]).desc() if dk["desc"] else col(dk["order_by"]).asc()

    w = Window.partitionBy(key).orderBy(order_col)

    ranked = df.withColumn("_rn", row_number().over(w))

    good_df = ranked.filter(col("_rn") == 1).drop("_rn")

    row_count = ranked.agg(
        count(when(col("_rn") == 1, True)).alias("good"),
        count(when(col("_rn") > 1, True)).alias("dup"),
    ).first()
    good_n, bad_n = row_count["good"], row_count["dup"]

    print(f"{name}: good rows count = {good_n:,} and duplicate rows count = {bad_n:,}")

    return good_df

# COMMAND ----------

# DBTITLE 1,load dataframe in silver schema
def load_to_silver(df, name):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.{name}")
    )
    print(f"silver.{name} written, {df.count():,} rows")

# COMMAND ----------

# DBTITLE 1,Store DataFrame in Quarantine Schema
def load_to_quarantine(df, name):
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{QUARANTINE_SCHEMA}.{name}_quarantine")
    )
    print(f"quarantine.{name}_quarantine written, {df.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

# DBTITLE 1,Get bronze customers
customers_df = get_bronze("customers")

# COMMAND ----------

# DBTITLE 1,Clean and validate emails
email_regex = (
    r"^[a-z]+\.[a-z'’]+\d+@"
    r"(gmail\.com|yahoo\.com|outlook\.com|rediffmail\.com)$"
)

customers_df = (
    customers_df.withColumn(
        "email", regexp_replace("email", r"@@", "@")
    )  # Fix double @
    .withColumn(
        "email", regexp_replace("email", r"gmial\.con$", "gmail.com")
    )  # Fix typo gmial.con to gmail.com
    .withColumn(
        "email", regexp_replace("email", r"@1\.com$", "@gmail.com")
    )  # Fix bad domain
    .withColumn(
        "email",
        regexp_replace(
            "email", r"@(gmail|yahoo|outlook|rediffmail)$", r"@$1.com"
        ),  # Fix missing .com
    )
    .withColumn(
        "email",
        regexp_replace(
            "email",
            r"^([a-z.'’]+\d+)(gmail\.com|yahoo\.com|outlook\.com|rediffmail\.com)$",
            r"$1@$2",
        ),  # Fix missing @
    )
    .withColumn(
        "email",
        when(col("email").rlike(email_regex), col("email")).otherwise(
            None
        )  # Replace bad values with Null
    )
)

# Count bad email
customers_df.select("email").filter(col("email").isNull()).count()

# COMMAND ----------

# DBTITLE 1,Normalize and validate phone
customers_df = (
    customers_df.withColumn("phone", regexp_replace("phone", r"^\+91|[-\s]", ""))
    .withColumn("phone", regexp_replace("phone", r"^0", ""))
    .withColumn("phone", when(col("phone").rlike(r"^\d{10}$"), col("phone")).otherwise(None))
)

customers_df.select("phone").filter(col("phone").isNull()).count()

# COMMAND ----------

# DBTITLE 1,Standardize city names
customers_df = customers_df.withColumn("city", trim(initcap( col("city"))))
customers_df.select("city").distinct().show()

# COMMAND ----------

# DBTITLE 1,Load Customers DataFrame
load_to_silver(customers_df, "customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restaurants

# COMMAND ----------

# DBTITLE 1,Get bronze restaurants
restaurants_df = get_bronze("restaurants")

# COMMAND ----------

# DBTITLE 1,Dedupe restaurants dataframe and count duplicate ids
restaurants_df = dedupe(restaurants_df, "restaurants")

# COMMAND ----------

# DBTITLE 1,Split restaurants into good and bad rating groups
good_restaurants_df = restaurants_df.filter(col("rating").isNull() | col("rating").between(1, 5)) # Keep
bad_restaurants_df = restaurants_df.filter(col("rating").isNotNull() & ~col("rating").between(1, 5)) # Quarantine

# COMMAND ----------

# DBTITLE 1,Load Clean Data to Silver and Bad to Quarantine
load_to_silver(good_restaurants_df, "restaurants")
load_to_quarantine(bad_restaurants_df, "restaurants")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drivers

# COMMAND ----------

# DBTITLE 1,Get bronze drivers
drivers_df = get_bronze("drivers")

# COMMAND ----------

# DBTITLE 1,Deduplicate Drivers by Highest Deliveries per Driver ID
drivers_df = dedupe(drivers_df, "drivers")

# COMMAND ----------

# DBTITLE 1,Standardize Employment Status Values
drivers_df = drivers_df.withColumn("employment_status", upper(trim(col("employment_status"))))
drivers_df.select("employment_status").distinct().show()

# COMMAND ----------

# DBTITLE 1,Load drivers dataframe into silver schema
load_to_silver(drivers_df, "drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders

# COMMAND ----------

# DBTITLE 1,Get bronze orders
orders_df = get_bronze("orders")

# COMMAND ----------

# DBTITLE 1,Remove Duplicate Orders
orders_df = dedupe(orders_df, "orders")

# COMMAND ----------

# DBTITLE 1,Clean and Standardize Promo Codes
orders_df = (
    orders_df
        .withColumn("promo_code", trim(upper(col("promo_code"))))
        .withColumn("promo_code", regexp_replace(col("promo_code"), r"#+$", ""))
        .withColumn("promo_code", when(length(col("promo_code")) < 3, None).otherwise(col("promo_code")))
)
orders_df.select("promo_code").distinct().show()

# COMMAND ----------

# DBTITLE 1,Validate Orders by Comparing Calculated and Reported To ...
calculated_total = (
    col("subtotal")
    - col("discount_amount")
    + col("tax_amount")
    + col("delivery_fee")
    + col("surge_fee")
    + col("tip_amount")
)

good_orders_df = orders_df.filter(
    (col("discount_amount") >= 0) &
    (abs(calculated_total - col("total_amount")) <= 0.01)
)
bad_orders_df = orders_df.join(good_orders_df, on="order_id", how="left_anti")

# COMMAND ----------

# DBTITLE 1,Load Clean Data to Silver and Bad to Quarantine
load_to_silver(good_orders_df, "orders")
load_to_quarantine(bad_orders_df, "orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order items

# COMMAND ----------

# DBTITLE 1,Get brone order items
order_items_df = get_bronze("order_items")

# COMMAND ----------

# DBTITLE 1,Identify and Count Duplicate Order Items
order_items_df = dedupe(order_items_df, "order_items")

# COMMAND ----------

# DBTITLE 1,Filter and separate valid and invalid order items
good_order_items_df = order_items_df.filter(
    col("product_name").isNotNull()
    & (trim(col("product_name")) != "")
    & (col("quantity") > 0)
    & (
        abs(
            ((col("quantity") * col("unit_price")) - col("item_discount"))
            - col("line_total")
        )
        <= 0.01
    )
)

bad_order_items_df = order_items_df.join(
    good_order_items_df, on="order_item_id", how="left_anti"
)

print("Good row count:", good_order_items_df.count())
print("Bad row count:", bad_order_items_df.count())

# COMMAND ----------

# DBTITLE 1,Load Clean Data to Silver and Bad to Quarantine
load_to_silver(good_order_items_df, "order_items")
load_to_quarantine(bad_order_items_df, "order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Payments, Support tickets, Traffic, Weather

# COMMAND ----------

# DBTITLE 1,Load Multiple Bronze Tables into Silver
for name in ["payments", "support_tickets", "traffic", "weather"]:
    df = get_bronze(name)
    load_to_silver(df, name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order events

# COMMAND ----------

# DBTITLE 1,Get bronze order events
order_events_df = get_bronze("order_events")

# COMMAND ----------

# DBTITLE 1,Remove Duplicate Records from Order Events DataFrame
order_events_df = dedupe(order_events_df, "order_events")

# COMMAND ----------

# DBTITLE 1,Clean empty driver IDs and dropping app version column
order_events_df = order_events_df.withColumns(
    {
        "event_time": col("event_time").cast("timestamp"),
        "ingestion_time": col("ingestion_time").cast("timestamp"),
        "driver_id": when((trim(col("driver_id")) == ""), None).otherwise(col("driver_id"))
    }
).drop("app_version")
order_events_df.select("driver_id").filter((trim(col("driver_id")) == "")).count()

# COMMAND ----------

# DBTITLE 1,Load Order Events DataFrame to Silver
load_to_silver(order_events_df, "order_events")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Driver GPS

# COMMAND ----------

# DBTITLE 1,Get bronze driver GPS
driver_gps_df = get_bronze("driver_gps")

# COMMAND ----------

# DBTITLE 1,Apply Deduplication Function to Driver GPS Dataframe
driver_gps_df = dedupe(driver_gps_df, "driver_gps")

# COMMAND ----------

# DBTITLE 1,Clean Empty Order Ids in Driver GPS DataFrame
driver_gps_df = driver_gps_df.withColumns(
    {
        "timestamp": col("timestamp").cast("timestamp"),
        "ingestion_time": col("ingestion_time").cast("timestamp"),
        "order_id": when(trim(col("order_id")) == "", None).otherwise(col("order_id")),
        "speed_anomaly": col("speed_kmh") > 80,
    }
)

# COMMAND ----------

# DBTITLE 1,Filter Driver GPS Data by Battery Percentage Validity
good_driver_gps_df = driver_gps_df.filter(
    col("battery_percent").isNotNull() &
    col("battery_percent").between(1, 100)
)

bad_driver_gps_df = driver_gps_df.filter(
    col("battery_percent").isNull() |
    ~col("battery_percent").between(1, 100)
)

# COMMAND ----------

# DBTITLE 1,Load Clean Data to Silver and Bad to Quarantine
load_to_silver(good_driver_gps_df, "driver_gps")
load_to_quarantine(bad_driver_gps_df, "driver_gps")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restaurant status events

# COMMAND ----------

# DBTITLE 1,Get bronze restaurant status events
restaurant_status_events_df = get_bronze("restaurant_status_events")

# COMMAND ----------

# DBTITLE 0,Verify Duplicate Event IDs
# restaurant_status_events_df.groupBy("event_id").count().filter(col("count")>1).count() # 0
restaurant_status_events_df = restaurant_status_events_df.withColumns(
    {
        "reason": when(trim(col("reason")) == "", None).otherwise(col("reason")),
        "event_time": col("event_time").cast("timestamp"),
        "ingestion_time": col("ingestion_time").cast("timestamp"),
    }
)

display(restaurant_status_events_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Save Restaurant Status Events to Silver Layer
load_to_silver(restaurant_status_events_df, "restaurant_status_events")