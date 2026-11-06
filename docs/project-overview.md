# Project overview

SwiftBite is a fictional food delivery app. This project uses its data to build a data pipeline in Databricks.

The data includes customers, restaurants, drivers, orders, payments, delivery events, GPS records, traffic, and weather. It also includes common data problems, such as duplicates, missing values, incorrect amounts, and late events.

## Pipeline layers

- **Landing:** holds the source files and Auto Loader checkpoints.
- **Bronze:** stores the ingested data as Delta tables.
- **Silver:** cleans and checks the data. Some rejected rows are saved in quarantine tables.
- **Gold:** contains tables prepared for analysis, including the order model and hourly event counts.

## Source files

The notebooks expect these CSV files: customers, restaurants, drivers, orders, order items, payments, support tickets, traffic, and weather.

They also read JSONL files for order events, driver GPS, and restaurant status events.

The raw data is kept outside this GitHub repository. The repository contains the notebooks and project documentation.
