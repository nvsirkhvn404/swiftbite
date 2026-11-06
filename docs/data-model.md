# Gold tables

The main Gold table is `fact_orders`. It is joined to tables for customers, restaurants, drivers, dates, and cities.

## Lookup tables

| Table | What it contains |
|---|---|
| `dim_customer` | Customer details and customer history. |
| `dim_restaurant` | Restaurant details, city, cuisine, rating, and status. |
| `dim_driver` | Driver details, location, vehicle, employment status, and rating. |
| `dim_date` | Calendar details for the dates found in Silver orders. |
| `dim_city` | City names and their regions. |

`dim_customer` keeps a new version when the customer's city, segment, preferred payment method, or account status changes. Each version has `valid_from`, `valid_to`, and `is_current` fields. The order table uses the version that was valid when the order was placed.

## Fact tables and event counts

| Table | What it contains |
|---|---|
| `fact_orders` | Orders, amounts, status, payment method, promo code, delivery times, and dimension keys. |
| `fact_order_items` | Order item records from Silver. |
| `fact_payments` | Payment records from Silver. |
| `fact_support_tickets` | Support ticket records from Silver. |
| `hourly_event_counts` | Number of order events by event type and hour. |

The order items, payments, and support tickets tables are copied from Silver. They are not joined to the dimensions in the current notebook. The hourly event counts table is separate from the order model.

## Table keys and layout

The restaurant and driver dimensions have an unknown row with key `-1`. The order table uses `-1` when it cannot find a matching customer, restaurant, driver, or city. If no date matches, the date key stays empty.

The performance notebook adds `order_month` to `fact_orders`, partitions the table by that column, and runs Z-Ordering on `restaurant_sk` and `city_sk`.
