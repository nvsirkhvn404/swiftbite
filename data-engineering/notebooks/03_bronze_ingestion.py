# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Run shared config
# MAGIC %run "./01_shared_config"

# COMMAND ----------

# DBTITLE 1,Define ingest table function
from pyspark.sql import functions as F


def ingest_table(name):
    # Define path variables
    config = TABLES[name]
    source_path = f"{RAW_ROOT}/{config['path']}"
    schema_path = f"{CHECKPOINT_ROOT}/{name}/schema"
    checkpoint_path = f"{CHECKPOINT_ROOT}/{name}/checkpoint"
    target_table = f"{CATALOG}.{BRONZE_SCHEMA}.{name}"

    # Configure Auto Loader (cloudFiles) to read files incrementally
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", config["format"])  # Input file format (csv, json, parquet, etc.)
        .option("cloudFiles.inferColumnTypes", "true")  # Automatically infer column data types
        .option("cloudFiles.schemaLocation", schema_path) # Store schema metadata for tracking changes
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # Capture unexpected columns instead giving error
        .option("rescuedDataColumn", "_rescued_data")  # Column for rescued data
    )

    # Enable CSV-specific options
    if config["format"] == "csv":
        reader = reader.option("header", True)  # Treat first row as column headers

    # Load source files as a streaming DataFrame
    raw_stream = reader.load(source_path)

    # Add ingestion metadata columns
    enriched = raw_stream.withColumns(
        {
            "_ingested_at": F.current_timestamp(),  # Timestamp when record was ingested
            "_source_file": F.col("_metadata.file_path"),  # Source file path
        }
    )

    # Write streaming data into a Delta table
    query = (
        enriched.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)  # Checkpoint location for fault tolerance
        .option("mergeSchema", "true")  # Automatically evolve target schema
        .trigger(availableNow=True)  # Process all available files and then stop
        .toTable(target_table)  # Write output to target Delta table
    )

    # Wait for ingestion job to complete
    query.awaitTermination()

    # Print completion message
    print(f"{name} table ingestion finished")

# COMMAND ----------

# DBTITLE 1,Call function for each table
# Call ingest_table for each tables
for table_name in TABLES:
    ingest_table(table_name)

# COMMAND ----------

# DBTITLE 1,Print row count
# Print row count
for table_name in TABLES:
    full_name = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    print(f"{full_name:<42} {spark.table(full_name).count():>10,} rows")