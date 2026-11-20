# code-snippetes
Public code-snippets for warehouse native product analytics.

| Snippet | Warehouse | What it does |
|---|---|---|
| [`ga4_events_to_json/`](ga4_events_to_json/) | BigQuery | Turns a GA4 or Firebase export into daily tables with JSON `event_params` and `user_properties`, kept up to date by one scheduled query. |
| [`eventify_ga4_data.sql`](eventify_ga4_data.sql) | BigQuery | One-off rebuild of a whole GA4 export with JSON parameters, every value as a string. Superseded by `ga4_events_to_json/`, which is incremental and keeps numbers as numbers. |
| [`event_sessionisation.sql`](event_sessionisation.sql) | BigQuery | Splits each user's events into sessions after 30 minutes of inactivity. |
| [`stripe_revenue_transformation.sql`](stripe_revenue_transformation.sql) | BigQuery | Turns Stripe invoice line items into MRR and amount paid per billing period. |
| [`User Aliases Table.ipynb`](User%20Aliases%20Table.ipynb) | Databricks | Merges device IDs and user IDs that belong to the same person into one ID, with Spark GraphFrames. |
