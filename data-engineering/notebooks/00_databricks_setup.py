# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Creating Schemas and Volumes
# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS swiftbite;
# MAGIC
# MAGIC USE CATALOG swiftbite;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS swiftbite.landing;
# MAGIC CREATE SCHEMA IF NOT EXISTS swiftbite.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS swiftbite.quarantine;
# MAGIC CREATE SCHEMA IF NOT EXISTS swiftbite.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS swiftbite.gold;
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS swiftbite.landing.raw_data;
# MAGIC CREATE VOLUME IF NOT EXISTS swiftbite.landing.checkpoints;

# COMMAND ----------

# DBTITLE 1,Displaying Schemas
# MAGIC %sql
# MAGIC
# MAGIC SHOW SCHEMAS IN swiftbite;

# COMMAND ----------

# DBTITLE 1,Displaying Volumes
# MAGIC %sql
# MAGIC
# MAGIC SHOW VOLUMES IN swiftbite.landing;