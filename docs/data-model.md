# Gold tables

The main Gold table is `fact_orders`. It is joined to customer, restaurant, driver, date, and city dimensions.

## Dimensions

| Table | Grain | What it contains |
|---|---|---|
| `dim_customer` | One row per customer version | Customer details and SCD2 history. |
| `dim_restaurant` | One row per restaurant | Restaurant details, city, cuisine, rating, and status. |
| `dim_driver` | One row per driver | Driver details, location, vehicle, employment status, and rating. |
| `dim_date` | One row per date | Calendar details for the dates found in Silver orders. |
| `dim_city` | One row per city | City names and regions. |

## Facts

| Table | Grain | What it contains |
|---|---|---|
| `fact_orders` | One row per order | Dimension keys, order status, payment, amounts, delivery times, and timestamps. |
| `fact_order_items` | One row per order item | Pass-through copy of deduplicated Silver order items. |
| `fact_payments` | Source grain (pass-through) | Copy of Silver payments. |
| `fact_support_tickets` | Source grain (pass-through) | Copy of Silver support tickets. |

`fact_order_items`, `fact_payments`, and `fact_support_tickets` are not joined to the dimensions in this version. `gold.hourly_event_counts` is created by the optional event-time notebook and is separate from the order model.

## Surrogate keys

Dimensions use `xxhash64` hashes of their natural keys:

```text
restaurant_sk = xxhash64(restaurant_id)
driver_sk     = xxhash64(driver_id)
city_sk       = xxhash64(city_name)
customer_sk   = xxhash64(customer_id, valid_from)
date_sk       = yyyyMMdd
```

The hash is kept positive with `pmod`. It is stable across reruns, so rebuilding a dimension does not change the keys that facts already point to.

`monotonically_increasing_id()` is not used because its values can change when the query plan changes.

## Unknown members

Every dimension used by `fact_orders` has exactly one row with key `-1`:

- `dim_customer`: `customer_id = 'UNKNOWN'`
- `dim_restaurant`: `restaurant_id = 'UNKNOWN'`
- `dim_driver`: `driver_id = 'NO_DRIVER'`
- `dim_date`: `full_date = 1900-01-01`
- `dim_city`: `city_name = 'UNKNOWN'`

The fact join falls back to `-1` when a lookup fails. This keeps every foreign key non-null and gives analysts an explicit unknown bucket.

## Customer SCD2

`dim_customer` tracks changes in:

- `city`
- `customer_segment`
- `preferred_payment_method`
- `account_status`

Each version has:

- `valid_from`: when the version starts.
- `valid_to`: when the next version starts. This is exclusive.
- `is_current`: true for the latest version.

The first version starts at midnight of the customer's signup date so an order placed earlier on that date still finds the customer. Later versions keep the exact snapshot time.

The order join uses:

```text
order_timestamp >= valid_from
AND (order_timestamp < valid_to OR valid_to IS NULL)
```

This picks the customer version that was valid when the order was placed.

## Fact grain and checks

`07_dimensional_model.py` asserts that `fact_orders` has the same row count as `silver.orders`. A higher count means a dimension join fanned out, usually from duplicate dimension keys or overlapping SCD2 ranges.

`08_gold_performance.py` checks:

- one fact row per Silver order;
- no null `order_id` or foreign keys;
- every non-`-1` foreign key exists in its dimension;
- every dimension has exactly one `-1` unknown member.

## File layout

`fact_orders` is written unpartitioned. At this project's data size, partitioning would create small files and add overhead for little pruning benefit.

`08_gold_performance.py` runs:

```sql
OPTIMIZE gold.fact_orders ZORDER BY (restaurant_sk, city_sk)
```

`OPTIMIZE` compacts small files. `ZORDER` clusters the restaurant and city keys inside files so queries that filter on those keys can skip data.

## Limitations

- The pass-through fact tables do not have dimension keys or measures yet.
- `dim_city` is a hardcoded list. Cities outside the list resolve to `-1`. A real project would load the list from a reference table.
- `fact_orders` is rebuilt in full on each run. This is deliberate at the current data size.
- The customer SCD2 keeps one row per `customer_id` and `signup_timestamp`. If the source sends two different snapshots with the same timestamp, the latest ingested copy wins.
