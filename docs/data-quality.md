# Data quality

The Bronze layer keeps the ingested data. The Silver layer cleans it and checks selected fields. Rows that fail some checks are saved in quarantine tables.

## What gets checked

- **Customers:** fixes a few known email issues, checks email format, cleans phone numbers, and standardizes city names.
- **Restaurants:** keeps one row per restaurant and sends ratings outside the 1 to 5 range to quarantine.
- **Drivers:** keeps one row per driver and standardizes employment status.
- **Orders:** keeps the latest version of each order, standardizes promo codes, and checks discounts and order totals.
- **Order items:** removes duplicate items and checks product names, quantities, and line totals.
- **Order events:** removes duplicate event IDs and cleans timestamp and driver ID fields.
- **Driver GPS:** removes duplicate event IDs, cleans timestamps and order IDs, checks speed, and sends invalid battery values to quarantine.
- **Restaurant status events:** converts timestamps and changes empty reason values to null.

Payments, support tickets, traffic, and weather are copied from Bronze to Silver without extra checks in the current notebook.

## Quarantine tables

The notebook creates these tables:

- `quarantine.restaurants_quarantine`
- `quarantine.orders_quarantine`
- `quarantine.order_items_quarantine`
- `quarantine.driver_gps_quarantine`

At the end, the notebook compares row counts in Bronze, Silver, and quarantine for selected datasets. This helps check that rows have not gone missing during the steps. It does not check every field or every possible data issue.
