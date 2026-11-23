# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Load Shared Config
# MAGIC %run "./01_shared_config"

# COMMAND ----------

# DBTITLE 1,Columns that create a new customer version
from pyspark.sql import functions as F, Window

# Only these columns get a new version when they change.
TRACKED_COLUMNS = [
    "city",
    "customer_segment",
    "preferred_payment_method",
    "account_status",
]

# COMMAND ----------

# DBTITLE 1,Read one row per customer snapshot
customers = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.customers")

missing_keys = customers.filter(
    F.col("customer_id").isNull() | F.col("signup_timestamp").isNull()
).count()

if missing_keys:
    print(f"Skipping {missing_keys:,} customer row(s) with no id or signup_timestamp")

customers = customers.filter(
    F.col("customer_id").isNotNull() & F.col("signup_timestamp").isNotNull()
)

# A customer can have more than one row for the same signup time if the
# source was ingested twice. Keep the latest ingested copy.
snapshot_window = Window.partitionBy("customer_id", "signup_timestamp").orderBy(
    F.col("_ingested_at").desc_nulls_last()
)

customers = (
    customers.withColumn("_snapshot_rank", F.row_number().over(snapshot_window))
    .filter(F.col("_snapshot_rank") == 1)
    .drop("_snapshot_rank")
)

print(f"customer snapshots: {customers.count():,}")

# COMMAND ----------

# DBTITLE 1,Find where a tracked column changes
by_customer_time = Window.partitionBy("customer_id").orderBy("signup_timestamp")

for column in TRACKED_COLUMNS:
    customers = customers.withColumn(
        f"_previous_{column}", F.lag(column).over(by_customer_time)
    )

changed = F.lit(False)
for column in TRACKED_COLUMNS:
    # eqNullSafe treats null == null as true, so null-to-null is not a change.
    changed = changed | ~F.col(column).eqNullSafe(F.col(f"_previous_{column}"))

customers = customers.withColumns(
    {
        "_row_number": F.row_number().over(by_customer_time),
        "_changed": changed,
    }
)

# The first snapshot and every changed snapshot start a new version.
versions = customers.filter((F.col("_row_number") == 1) | F.col("_changed"))

# The first version starts at midnight so orders placed earlier on the
# signup date still find the customer. Later versions keep the exact time.
versions = versions.withColumn(
    "valid_from",
    F.when(
        F.col("_row_number") == 1,
        F.date_trunc("day", F.col("signup_timestamp")),
    ).otherwise(F.col("signup_timestamp")),
)

# COMMAND ----------

# DBTITLE 1,Build the SCD2 dimension
by_valid_from = Window.partitionBy("customer_id").orderBy("valid_from")

dim_customer = (
    versions.withColumn("valid_to", F.lead("valid_from").over(by_valid_from))
    .withColumn("is_current", F.col("valid_to").isNull())
    .withColumn("customer_sk", surrogate_key("customer_id", "valid_from"))
    .select(
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
)

dim_customer = add_unknown_member(
    dim_customer,
    customer_sk=-1,
    customer_id="UNKNOWN",
    first_name="Unknown",
    last_name="Customer",
    is_current=True,
)

overwrite(dim_customer, f"{CATALOG}.{GOLD_SCHEMA}.dim_customer")

print(
    f"dim_customer: {dim_customer.count():,} version-rows for "
    f"{dim_customer.filter(F.col('customer_id') != 'UNKNOWN').select('customer_id').distinct().count():,} customers"
)

duplicate_keys = dim_customer.groupBy("customer_sk").count().filter("count > 1").count()
assert duplicate_keys == 0, "dim_customer.customer_sk is not unique"

print("OK dim_customer: customer_sk is unique")

# COMMAND ----------

# DBTITLE 1,Check that each customer has one current row
current_counts = (
    dim_customer.filter(F.col("customer_id") != "UNKNOWN")
    .groupBy("customer_id")
    .agg(F.sum(F.col("is_current").cast("int")).alias("current_rows"))
)

assert current_counts.filter(F.col("current_rows") != 1).count() == 0, (
    "Each customer must have exactly one current SCD2 row"
)

print("OK dim_customer: one current row per customer")

# COMMAND ----------

# DBTITLE 1,Show an example customer with history
multi_version_customers = (
    dim_customer.filter(F.col("customer_id") != "UNKNOWN")
    .groupBy("customer_id")
    .count()
    .filter("count > 1")
)

print(f"{multi_version_customers.count():,} customers have more than one version")

if multi_version_customers.limit(1).count() > 0:
    example_id = multi_version_customers.first()["customer_id"]
    display(
        dim_customer.filter(F.col("customer_id") == example_id)
        .select(
            "customer_id",
            "city",
            "customer_segment",
            "valid_from",
            "valid_to",
            "is_current",
        )
        .orderBy("valid_from")
    )