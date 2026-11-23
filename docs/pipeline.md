# Pipeline guide

Run the notebooks in this order:

1. **`00_databricks_setup`** creates the catalog, schemas, and volumes.
2. **`03_bronze_ingestion`** loads CSV and JSON files into Bronze Delta tables with Auto Loader.
3. **`04_silver_transformation`** cleans the data, deduplicates it, and writes rejected rows to quarantine.
4. **`06_scd2_dim_customer`** builds customer history.
5. **`07_dimensional_model`** creates the Gold dimensions and `fact_orders`. Run the customer history notebook first.
6. **`08_gold_performance`** optimizes `fact_orders` and runs the Gold checks.

`02_data_profiling` and `05_event_time_analysis` are optional. Run `02` before `03` if you want to inspect the raw files. Run `05` after `04` if you want event timing statistics and `gold.hourly_event_counts`.

`01_shared_config` holds settings and helper code. Other notebooks call it with `%run`, so you do not need to run it separately.

## Where to put the files

Put the raw files under:

```text
/Volumes/swiftbite/landing/raw
```

Use the folder names and layout set in `01_shared_config.py`. For example, customer CSV files go in `customers/`. Order event JSONL files go in the configured `order_events/` folders.

Auto Loader checkpoints are stored under:

```text
/Volumes/swiftbite/landing/checkpoints
```

Keep these checkpoint folders between runs. Auto Loader uses them to remember which files it has already processed.

## How the writes work

`01_shared_config.py` has the general `overwrite(df, table)` helper. `04_silver_transformation.py` adds `write_silver` on top of it.

- **`overwrite(df, table)`** replaces a table. It is used for dimensions, quarantine snapshots, full customer snapshots, and pass-through tables.
- **`write_silver(df, name, keys)`** lives in `04` because Silver is its only caller. It replaces the table when `keys` is omitted. When `keys` is provided, it uses Delta `MERGE` on the natural key, so a rerun updates rows instead of inserting duplicates.

Keyed Silver tables use `write_silver` with a key: restaurants, drivers, orders, order items, order events, driver GPS, and restaurant status events.

Customers use `write_silver` without a key because Silver must keep every customer snapshot. Payments, support tickets, traffic, and weather are also overwritten because they have no cleaning rules in this version.

## Running the pipeline again

Bronze uses Auto Loader with `availableNow=True`. It processes the files it can see and then stops.

Silver and Gold can be rerun:

- `04` reads the full Bronze table, so it rebuilds the current clean view.
- Keyed Silver tables MERGE on their natural key.
- Customers, quarantine, dimensions, and facts are replaced.
- `06` rebuilds `dim_customer` from the full Silver customer history.
- `07` rebuilds the dimensions and `fact_orders` from Silver.
- `08` runs `OPTIMIZE` and checks the result.

Do not delete the Bronze checkpoint folders unless you intend to make Auto Loader process the files again.

## What `08` does

`08` does not rewrite `fact_orders`. It:

- runs `OPTIMIZE ... ZORDER BY (restaurant_sk, city_sk)`;
- checks that `fact_orders` has one row per Silver order;
- checks that `order_id` and every foreign key are not null;
- checks that every non-`-1` foreign key exists in its dimension;
- checks that every dimension has exactly one `-1` unknown member.

`fact_orders` is unpartitioned by design at this data size. `OPTIMIZE` compacts small files, and `ZORDER` clusters the restaurant and city keys inside files for data skipping. If the fact grows large and queries filter by month, add a month column and partition on it deliberately, then measure the result.

## If a table was created by an older version

The older notebooks used `monotonically_increasing_id()` for surrogate keys and partitioned `fact_orders` by `order_month`. The current version uses stable hash keys and an unpartitioned fact table.

If `gold.fact_orders` was created by the old version, drop it once before running `07`. It is derived from Silver and rebuilds cleanly. The other Gold dimensions are overwritten by `07`, so they do not need to be dropped.

If an old Silver table still has duplicate natural keys, `write_silver` may stop when it tries to MERGE. Drop that Silver table once and rerun `04`.
