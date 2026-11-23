# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Load Shared Configurations
# MAGIC %run "./01_shared_config"

# COMMAND ----------

# DBTITLE 1,Import PySpark SQL Functions
from pyspark.sql import functions as F
from pyspark.sql import Window
from uuid import uuid4
from delta.tables import DeltaTable

RUN_ID = str(uuid4())

# COMMAND ----------

# DBTITLE 1,Load and Preview Bronze Table Data
def get_bronze(name):
    return spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{name}")

# COMMAND ----------

# DBTITLE 1,Deduplicate Rows
def dedupe(df, name):
    config = DEDUP_KEYS.get(name)

    # Tables without a dedupe rule are passed through unchanged.
    if not config:
        return df

    key = config["key"]

    # Newest or oldest value first, depending on the rule.
    if config["desc"]:
        order_columns = [F.col(config["order_by"]).desc_nulls_last()]
    else:
        order_columns = [F.col(config["order_by"]).asc_nulls_last()]

    # Tie-breakers make the result deterministic.
    if "_ingested_at" in df.columns and config["order_by"] != "_ingested_at":
        order_columns.append(F.col("_ingested_at").desc_nulls_last())

    if "_source_file" in df.columns and config["order_by"] != "_source_file":
        order_columns.append(F.col("_source_file").desc_nulls_last())

    # Hash the data columns so ties are broken deterministically.
    # Ignore Bronze metadata columns so the hash depends only on the data.
    data_columns = [column for column in df.columns if not column.startswith("_")]
    data_values = [
        F.coalesce(F.col(column).cast("string"), F.lit("\\N"))
        for column in data_columns
    ]
    df = df.withColumn("_row_fingerprint", F.xxhash64(*data_values))
    order_columns.append(F.col("_row_fingerprint").asc_nulls_last())

    window = Window.partitionBy(key).orderBy(*order_columns)
    ranked = df.withColumn("_rn", F.row_number().over(window))

    removed = ranked.filter(F.col("_rn") > 1).count()
    print(f"{name}: removed {removed:,} duplicate row(s) on {key}")

    return ranked.filter(F.col("_rn") == 1).drop("_rn", "_row_fingerprint")

# COMMAND ----------

# DBTITLE 1,Write a Silver table
def write_silver(df, name, keys=None):
    target = f"{CATALOG}.{SILVER_SCHEMA}.{name}"

    if keys:
        null_condition = " OR ".join([f"`{key}` IS NULL" for key in keys])
        if df.filter(null_condition).limit(1).count() > 0:
            raise ValueError(f"silver.{name}: a key column in {keys} is null")

    if not keys or not spark.catalog.tableExists(target):
        overwrite(df, target)
        print(f"silver.{name}: overwritten")
        return

    condition = " AND ".join([f"t.`{key}` = s.`{key}`" for key in keys])

    (
        DeltaTable.forName(spark, target)
        .alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"silver.{name}: merged on {keys}")

# COMMAND ----------

# DBTITLE 1,Write rejected rows with a reason
def write_quarantine(df, name, reason):
    target = f"{CATALOG}.{QUARANTINE_SCHEMA}.{name}_quarantine"

    quarantine = df.withColumns(
        {
            "_quarantined_at": F.current_timestamp(),
            "_run_id": F.lit(RUN_ID),
            "_reason": F.lit(reason),
        }
    )

    overwrite(quarantine, target)
    print(f"quarantine.{name}: {quarantine.count():,} row(s) - {reason}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

# DBTITLE 1,Clean customer fields
customers_df = get_bronze("customers")

email_regex = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

customers_df = (
    customers_df.withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn(
        "email",
        F.when(F.col("email").rlike(email_regex), F.col("email")).otherwise(None),
    )
    .withColumn("phone", F.regexp_replace("phone", r"^\+91|[-\s]", ""))
    .withColumn("phone", F.regexp_replace("phone", r"^0", ""))
    .withColumn(
        "phone",
        F.when(F.col("phone").rlike(r"^\d{10}$"), F.col("phone")).otherwise(None),
    )
    .withColumn("city", F.trim(F.initcap(F.col("city"))))
    .withColumn("signup_timestamp", F.col("signup_timestamp").cast("timestamp"))
)

bad_emails = customers_df.filter(F.col("email").isNull()).count()
bad_phones = customers_df.filter(F.col("phone").isNull()).count()
print(f"customers: {bad_emails:,} missing or invalid email(s)")
print(f"customers: {bad_phones:,} missing or invalid phone(s)")

# COMMAND ----------

# DBTITLE 1,Write customers to Silver
# Customers are a full snapshot history, not one row per customer.
# Replacing the table keeps every version and avoids dropping SCD2 history.
write_silver(customers_df, "customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restaurants

# COMMAND ----------

# DBTITLE 1,Keep valid restaurants and quarantine bad ratings
restaurants_df = dedupe(get_bronze("restaurants"), "restaurants")

good_restaurants = F.col("restaurant_id").isNotNull() & (
    F.col("rating").isNull() | F.col("rating").between(1, 5)
)

good_restaurants_df = restaurants_df.filter(good_restaurants)
bad_restaurants_df = restaurants_df.filter(~good_restaurants)

write_silver(good_restaurants_df, "restaurants", ["restaurant_id"])
write_quarantine(
    bad_restaurants_df,
    "restaurants",
    "missing restaurant_id or rating outside 1-5",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drivers

# COMMAND ----------

# DBTITLE 1,Clean and write drivers
drivers_df = dedupe(get_bronze("drivers"), "drivers")
drivers_df = drivers_df.withColumn(
    "employment_status", F.upper(F.trim(F.col("employment_status")))
)

write_silver(drivers_df, "drivers", ["driver_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders

# COMMAND ----------

# DBTITLE 1,Clean order fields
orders_df = dedupe(get_bronze("orders"), "orders")

orders_df = (
    orders_df
    .withColumn("promo_code", F.trim(F.upper(F.col("promo_code"))))
    .withColumn("promo_code", F.regexp_replace(F.col("promo_code"), r"#+$", ""))
    .withColumn(
        "promo_code",
        F.when(F.length(F.col("promo_code")) < 3, None).otherwise(F.col("promo_code")),
    )
    .withColumn("delivery_city", F.trim(F.initcap(F.col("delivery_city"))))
    .withColumn("order_timestamp", F.col("order_timestamp").cast("timestamp"))
    .withColumn("delivered_timestamp", F.col("delivered_timestamp").cast("timestamp"))
)

# COMMAND ----------

# DBTITLE 1,Keep orders whose reported total matches the line items
calculated_total = (
    F.col("subtotal")
    - F.col("discount_amount")
    + F.col("tax_amount")
    + F.col("delivery_fee")
    + F.col("surge_fee")
    + F.col("tip_amount")
)

# A null comparison counts as invalid, so the row goes to quarantine
# instead of disappearing from both outputs.
good_orders = F.coalesce(
    F.col("order_id").isNotNull()
    & (F.col("discount_amount") >= 0)
    & (F.abs(calculated_total - F.col("total_amount")) <= 0.01),
    F.lit(False),
)

good_orders_df = orders_df.filter(good_orders)
bad_orders_df = orders_df.filter(~good_orders)

write_silver(good_orders_df, "orders", ["order_id"])
write_quarantine(
    bad_orders_df,
    "orders",
    "missing order_id, null or negative discount, or total mismatch",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order items

# COMMAND ----------

# DBTITLE 1,Clean and validate order items
order_items_df = dedupe(get_bronze("order_items"), "order_items")
order_items_df = order_items_df.withColumn(
    "product_name", F.trim(F.col("product_name"))
)

calculated_line_total = (
    (F.col("quantity") * F.col("unit_price")) - F.col("item_discount")
)

# A null comparison counts as invalid, so the row goes to quarantine.
good_order_items = F.coalesce(
    F.col("order_item_id").isNotNull()
    & F.col("product_name").isNotNull()
    & (F.col("product_name") != "")
    & (F.col("quantity") > 0)
    & (F.abs(calculated_line_total - F.col("line_total")) <= 0.01),
    F.lit(False),
)

good_order_items_df = order_items_df.filter(good_order_items)
bad_order_items_df = order_items_df.filter(~good_order_items)

write_silver(good_order_items_df, "order_items", ["order_item_id"])
write_quarantine(
    bad_order_items_df,
    "order_items",
    "missing order_item_id or product, quantity <= 0, or line total mismatch",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Payments, Support tickets, Traffic, Weather

# COMMAND ----------

# DBTITLE 1,Copy small tables to Silver
# These tables have no cleaning rules yet, so replacing them is enough.
for name in ["payments", "support_tickets", "traffic", "weather"]:
    write_silver(get_bronze(name), name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order events

# COMMAND ----------

# DBTITLE 1,Clean order events
order_events_df = dedupe(get_bronze("order_events"), "order_events")

order_events_df = order_events_df.withColumns(
    {
        "event_time": F.col("event_time").cast("timestamp"),
        "ingestion_time": F.col("ingestion_time").cast("timestamp"),
        "driver_id": F.when(F.trim(F.col("driver_id")) == "", None).otherwise(
            F.col("driver_id")
        ),
    }
).drop("app_version")

write_silver(order_events_df, "order_events", ["event_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Driver GPS

# COMMAND ----------

# DBTITLE 1,Clean driver GPS
driver_gps_df = dedupe(get_bronze("driver_gps"), "driver_gps")

driver_gps_df = driver_gps_df.withColumns(
    {
        "timestamp": F.col("timestamp").cast("timestamp"),
        "ingestion_time": F.col("ingestion_time").cast("timestamp"),
        "order_id": F.when(
            F.trim(F.col("order_id")) == "", None
        ).otherwise(F.col("order_id")),
        "speed_anomaly": F.col("speed_kmh") > 80,
    }
)

good_driver_gps = (
    F.col("event_id").isNotNull()
    & F.col("battery_percent").isNotNull()
    & F.col("battery_percent").between(1, 100)
)

good_driver_gps_df = driver_gps_df.filter(good_driver_gps)
bad_driver_gps_df = driver_gps_df.filter(~good_driver_gps)

write_silver(good_driver_gps_df, "driver_gps", ["event_id"])
write_quarantine(
    bad_driver_gps_df,
    "driver_gps",
    "missing event_id or battery_percent, or battery outside 1-100",
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Restaurant status events

# COMMAND ----------

# DBTITLE 1,Clean restaurant status events
restaurant_status_events_df = dedupe(
    get_bronze("restaurant_status_events"), "restaurant_status_events"
)

restaurant_status_events_df = restaurant_status_events_df.withColumns(
    {
        "reason": F.when(
            F.trim(F.col("reason")) == "", None
        ).otherwise(F.col("reason")),
        "event_time": F.col("event_time").cast("timestamp"),
        "ingestion_time": F.col("ingestion_time").cast("timestamp"),
    }
)

write_silver(
    restaurant_status_events_df,
    "restaurant_status_events",
    ["event_id"],
)

# COMMAND ----------

print(f"Silver transformation complete. RUN_ID={RUN_ID}")