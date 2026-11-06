# Pipeline guide

Run the notebooks in this order:

1. **`00_databricks_setup`** creates the catalog, schemas, and volumes.
2. **`02_data_profiling`** checks the source data and shows row counts, columns, missing values, and samples.
3. **`03_bronze_ingestion`** loads CSV and JSON files into Bronze Delta tables with Auto Loader.
4. **`04_silver_transformation`** cleans the data and writes rejected rows to quarantine.
5. **`05_event_time_analysis`** checks event timing and creates hourly event counts.
6. **`06_scd2_dim_customer`** builds customer history.
7. **`07_dimensional_model`** creates the Gold dimensions and fact tables. Run the customer history notebook first.
8. **`08_gold_performance`** adds a month column to `fact_orders`, partitions the table, and runs Z-Ordering.

`01_shared_config` holds settings and helper code. Other notebooks call it with `%run`, so you do not need to run it separately.

## Where to put the files

Put the raw files under:

`/Volumes/swiftbite/landing/raw_data/raw`

Use the folder names and layout set in `01_shared_config.py`. For example, customer CSV files go in `customers/`. Order event JSONL files go in the configured `order_events/` folders.

Auto Loader checkpoints are stored under:

`/Volumes/swiftbite/landing/checkpoints`

Keep these checkpoint folders between runs. Auto Loader uses them to remember which files it has already processed.

## Running the pipeline again

Bronze uses Auto Loader with `availableNow=True`. It processes the files it can see and then stops.

The Silver and Gold notebooks write their tables in overwrite mode. Running them again rebuilds those tables from the data in the previous layer. Do not delete the Bronze checkpoint folders unless you intend to make Auto Loader process the files again.
