# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %run "./01_shared_config"

# COMMAND ----------

from pyspark.sql.functions import col, window, row_number
from pyspark.sql import Window

order_events = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.order_events")

# COMMAND ----------

# DBTITLE 1,Calculate and Summarize Order Event Lateness Statistics
with_lateness = order_events.withColumn(
    "lateness_seconds",
    col("ingestion_time").cast("long") - col("event_time").cast("long")
)
display(with_lateness.select("lateness_seconds").summary("min", "50%", "90%", "99%", "max"))

# COMMAND ----------

by_true_time = Window.partitionBy("order_id").orderBy("event_time")
by_arrival = Window.partitionBy("order_id").orderBy("ingestion_time")

sequenced = order_events.withColumn(
    "true_sequence", row_number().over(by_true_time)
).withColumn("arrival_sequence", row_number().over(by_arrival))

out_of_order = sequenced.filter(col("true_sequence") != col("arrival_sequence"))
total = sequenced.count()
out_of_order_count = out_of_order.count()

if total:
    print(
        f"{out_of_order_count:,} of {total:,} events "
        f"({out_of_order_count / total:.1%}) arrived in a different order "
        f"than they happened."
    )

# COMMAND ----------

hourly_counts = (
    order_events
    .groupBy(window("event_time", "1 hour"), "event_type")
    .count()
    .selectExpr("window.start as hour_start", "window.end as hour_end", "event_type", "count")
)

overwrite(hourly_counts, f"{CATALOG}.{GOLD_SCHEMA}.hourly_event_counts")

display(spark.table(f"{CATALOG}.{GOLD_SCHEMA}.hourly_event_counts").orderBy("hour_start").limit(20))