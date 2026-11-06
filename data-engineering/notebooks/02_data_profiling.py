# Databricks notebook source
# DBTITLE 1,Run config
# MAGIC %run "./01_shared_config"

# COMMAND ----------

# DBTITLE 1,Define read table function
from pyspark.sql import functions as F

def read_table(table_name):
    config = TABLES[table_name]
    path = f"{RAW_ROOT}/{config['path']}"

    if config['format'] == 'csv':
        return spark.read.option("header", True).option("inferSchema", True).csv(path, header=True, inferSchema=True)
    else:
        return spark.read.json(path)

# COMMAND ----------

# DBTITLE 1,Data summary
summary = []
frames = {}
for name in TABLES:
    df = read_table(name)
    frames[name] = df
    no_of_rows = df.count()
    no_of_cols = len(df.columns)
    summary.append([name, no_of_rows, no_of_cols])
    print(f"{name}: rows = {no_of_rows}, cols = {no_of_cols}")

summary_df = spark.createDataFrame(summary, ["name", "rows", "cols"])
display(summary_df.orderBy(F.desc(F.col("rows"))))

# COMMAND ----------

# DBTITLE 1,Enum check
ENUM_COLUMNS_TO_CHECK = {
    "customers": ["gender", "account_status", "customer_segment"],
    "restaurants": ["status", "restaurant_segment"],
    "drivers": ["employment_status", "vehicle_type"],
    "orders": ["order_status", "cancellation_reason", "payment_method"],
    "payments": ["payment_status", "payment_method", "gateway"],
    "support_tickets": ["resolution_status", "priority", "issue_type"],
}

for name, cols in ENUM_COLUMNS_TO_CHECK.items():
    df = frames[name]
    for c in cols:
        print(f"{name}.{c}: distinct values: ")
        display(df.groupBy(c).count().orderBy(F.desc(F.col("count"))))

# COMMAND ----------

# DBTITLE 1,Null rate report
def null_rate_report(name, df):
    total_rows = df.count()
    if total_rows == 0:
        return
    
    found_nulls = False
    for column, type in df.dtypes:
        if type == "string":
            null_count = df.filter(
                F.col(column).isNull() |
                (F.trim(F.col(column)) == "")
            ).count()

            null_rate = null_count / total_rows

            if null_count > 0:
                if not found_nulls:
                    print(f"\n{name}: null/empty rate by column")
                    found_nulls = True
                print(f"  {column}: {null_rate:.2%}")

for name, df in frames.items():
    null_rate_report(name, df)

# COMMAND ----------

# DBTITLE 1,Quick table check
for name, df in frames.items():
    print(f"{name}:")
    display(df.limit(5))