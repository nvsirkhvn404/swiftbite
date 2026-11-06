# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Load Shared Config
# MAGIC %run "./01_shared_config"

# COMMAND ----------

# DBTITLE 1,Define Columns to Track for Versioning Changes
from pyspark.sql import functions as F, Window

# Only these columns get a new "version" when they change. Name/email/phone aren't tracked
TRACKED_COLUMNS = [
    "city",
    "customer_segment",
    "preferred_payment_method",
    "account_status",
]

# COMMAND ----------

# DBTITLE 1,Count and Remove Duplicate Customers
customers = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.customers")
print(f"Rows in silver.customers before deduping: {customers.count():,}")

# dedupe customers by customer_id,  signup_timestamp and TRACKED_COLUMNS
dedup_columns = ["customer_id", "signup_timestamp"] + TRACKED_COLUMNS
customers = customers.dropDuplicates(subset=dedup_columns)
print(f"Rows after deduping (real snapshots remaining): {customers.count():,}")

# COMMAND ----------

# DBTITLE 1,Track Customer Changes Over Time with Versioning Logic
by_customer_over_time = Window.partitionBy("customer_id").orderBy("signup_timestamp")

for c in TRACKED_COLUMNS:
    # create a column containing the previous value of c for the same customer
    customers = customers.withColumns(
        {f"_previous_{c}": F.lag(c).over(by_customer_over_time)}
    )

# create column "_changed", tracks if something changed in the TRACKED_COLUMNS
customers = customers.withColumn("_changed", F.lit(False))
for c in TRACKED_COLUMNS:
    # True when the current value is not equal to the previous value
    changed = ~F.col(c).eqNullSafe(F.col(f"_previous_{c}"))

    # Update _changed to True if this column changed or a previous column already changed
    customers = customers.withColumn("_changed", F.col("_changed") | changed)

customers = customers.withColumns(
    {
        # create row_number by customer_id over signup_timestamp
        "_row_number": F.row_number().over(by_customer_over_time),
        # Mark the first row for each customer, or any row where a tracked column changed, as a new version (1), otherwise 0
        "_is_new_version": ((F.col("_row_number") == 1) | F.col("_changed")).cast("int")
    }
)

# COMMAND ----------

# DBTITLE 1,Calculate Running Total of Customer Version Changes
running_total = by_customer_over_time.rowsBetween(Window.unboundedPreceding, Window.currentRow)

customers = customers.withColumn("_version_number", F.sum("_is_new_version").over(running_total))

# COMMAND ----------

# DBTITLE 1,Build SCD2 dim_customer and Write to Gold
by_customer_valid_from = Window.partitionBy("customer_id").orderBy("valid_from")

versions = customers.filter(F.col("_is_new_version") == 1)
versions = versions.withColumn("valid_from", F.col("signup_timestamp"))

versions = (
    versions.withColumn("valid_to", F.lead("valid_from").over(by_customer_valid_from))
    .withColumn("is_current", F.col("valid_to").isNull())
    .withColumn("customer_sk", F.monotonically_increasing_id())
)

dim_customer = versions.select(
    "customer_sk",
    "customer_id",
    "first_name",
    "last_name",
    "city",
    "customer_segment",
    "preferred_payment_method",
    "account_status",
    "valid_from",
    "valid_to",
    "is_current",
)

# Write dim_customer to GOLD schema
(
    dim_customer.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.dim_customer")
)

# Print total SCD version rows and number of unique customers
print(
    f"dim_customer: {dim_customer.count():,} version-rows for "
    f"{dim_customer.select('customer_id').distinct().count():,} customers"
)

# COMMAND ----------

# DBTITLE 1,Verify SCD2 Multi-Version Customers
# find a customer with real history
multi_version_customers = (
    spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_customer")
    .groupBy("customer_id")
    .count()
    .filter("count > 1")
)
print(f"{multi_version_customers.count():,} customers have more than one SCD2 version.")

if multi_version_customers.count() > 0:
    example_id = multi_version_customers.first()["customer_id"]
    display(
        spark.table(f"{CATALOG}.{GOLD_SCHEMA}.dim_customer")
        .filter(F.col("customer_id") == example_id)
        .select("customer_id", "customer_segment", "preferred_payment_method", "city", "valid_from", "valid_to", "is_current")
        .orderBy("valid_from")
    )