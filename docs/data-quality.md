# Data quality

Bronze keeps the ingested data as-is. Silver cleans it, checks selected fields, and writes rejected rows to quarantine.

The rules are deliberately small. This is not a full data-quality framework.

## What gets checked

| Dataset | Silver rule | Quarantine condition |
|---|---|---|
| Customers | Lowercase and trim email, validate with one generic regex, normalize phone, title-case city, cast `signup_timestamp`. | No quarantine table. An invalid email or phone is set to null. |
| Restaurants | Keep the latest row per `restaurant_id`. A null rating is allowed. | Missing `restaurant_id`, or a non-null rating outside 1-5. |
| Drivers | Keep the latest row per `driver_id`, uppercase `employment_status`. | No quarantine table. A null `driver_id` fails the `write_silver` key check. |
| Orders | Keep the latest row per `order_id` by `updated_at`, clean promo codes, title-case `delivery_city`, cast timestamps. | Missing `order_id`, null or negative `discount_amount`, or a null/mismatched total calculation. |
| Order items | Keep the latest row per `order_item_id`, trim `product_name`. | Missing `order_item_id` or product name, quantity <= 0, or line total mismatch. |
| Order events | Keep the earliest event per `event_id` by `ingestion_time`, cast timestamps, turn an empty `driver_id` into null, drop `app_version`. | No quarantine table. A null `event_id` fails the `write_silver` key check. |
| Driver GPS | Keep the earliest event per `event_id`, cast timestamps, turn an empty `order_id` into null, flag speed above 80 km/h. | Missing `event_id` or `battery_percent`, or battery outside 1-100. |
| Restaurant status events | Keep the earliest event per `event_id`, turn an empty `reason` into null, cast timestamps. | No quarantine table. A null `event_id` fails the `write_silver` key check. |

Payments, support tickets, traffic, and weather are copied from Bronze to Silver without extra checks in this version.

## Email rule

The old notebook tried to repair common email mistakes, such as `gmial.con`, a missing `@`, or a missing `.com`. That invents data.

The current notebook only:

1. lowercases and trims the value;
2. checks it against a generic email pattern;
3. sets the value to null when it does not match.

A null email is honest. A guessed email is not.

## Quarantine tables

The notebook creates these tables:

- `quarantine.restaurants_quarantine`
- `quarantine.orders_quarantine`
- `quarantine.order_items_quarantine`
- `quarantine.driver_gps_quarantine`

Each row keeps the source columns plus:

- `_reason`: why the row was rejected.
- `_run_id`: the notebook run that produced it.
- `_quarantined_at`: when it was written.

Quarantine is overwritten on each run, so it always shows the current rejected rows.

## What is not checked

- Silver does not enforce every column type or every allowed value.
- Referential integrity is checked in Gold, not Silver.
- Quarantine tables are not fed back into the pipeline automatically.
- There is no alerting or orchestration around failed checks.
