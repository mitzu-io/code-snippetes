# GA4 / Firebase export to JSON tables

A BigQuery stored procedure that copies a GA4 or Firebase export into tables where `event_params` and `user_properties` are JSON columns, and keeps them up to date with one daily scheduled query.

GA4 and Firebase export to BigQuery as one table per day (`events_20261111`, `events_20261112`, …). Event parameters and user properties are nested key/value lists, so every query needs an `UNNEST` per parameter. After this job, a parameter is one expression away:

```sql
SELECT JSON_VALUE(event_params['page_location']) AS page_location
FROM `<project_id>.<mitzu_dataset>.events_*`
```

The same SQL works for any GA4 property or Firebase app, and a parameter you start sending tomorrow shows up as a new JSON key without changing anything.

## What it does

- For every daily table `events_YYYYMMDD` in the export dataset, it writes a table with the same name into a dataset you choose.
- It rebuilds a day only when that table is missing or the export table changed since the last build. Each copied table carries a `source_modified` label with the export table's last-modified time at the moment it was read, which is what the next run compares against. GA4 keeps adding late events to a daily table for up to 72 hours, so the daily run looks a few days back.
- `event_params` and `user_properties` become JSON objects keyed by parameter name, with the original value types kept (see [How values are converted](#how-values-are-converted)).
- Every other column is copied as it is, including `items`, `device`, `geo` and `app_info`. The only thing dropped is `set_timestamp_micros` inside `user_properties`.
- It adds `event_time` (TIMESTAMP) and `event_day` (DATE).
- Output tables are clustered by `event_name` and `user_pseudo_id`.
- If one day fails, the others are still built, and the run then fails with a message naming each failed day, so the scheduled query's run history shows it.

Because the output keeps one table per day with the export's own names, a column Google adds to the export later simply appears in the new day's table. Nothing has to be migrated.

## Requirements

- The BigQuery export enabled in GA4 or Firebase, with **daily** export. A streaming-only export writes only `events_intraday_YYYYMMDD` tables, which the job does not read.
- Permission to create a dataset in the project.
- For the account that runs the daily job: **BigQuery Job User** on the project, **BigQuery Data Viewer** on the export dataset, and **BigQuery Data Editor** on the target dataset. When the procedure lives in a different dataset from the target (several apps, see below), also **BigQuery Data Viewer** on the dataset that holds the procedure.

## Placeholders

| Placeholder | What to put there | Example |
|---|---|---|
| `<project_id>` | The Google Cloud project that holds the export | `my-game-123456` |
| `<ga4_dataset>` | The export dataset, `analytics_` followed by the property ID | `analytics_151441753` |
| `<mitzu_dataset>` | A new dataset for the JSON tables (any name) | `ga4_json` |
| `<location>` | The location of the export dataset, from its **Details** tab | `US`, `EU`, `europe-west1` |
| `<table_prefix>` | The daily table name without the date: `events_` for GA4 and Firebase | `events_` |

The target dataset must be in the same location as the export dataset. BigQuery cannot read from one location and write to another in the same query.

## 1. Create the dataset and the procedure

Replace the placeholders in [`ga4_events_to_json.sql`](ga4_events_to_json.sql) and run it once in the BigQuery console. It creates the target dataset and the procedure `ga4_events_to_json`. Nothing is copied yet. Its first line, `SET @@location`, makes BigQuery run the script in your export's location; without it the console may pick `US` and the dataset creation fails.

The procedure takes four arguments:

| Argument | Meaning |
|---|---|
| `source_dataset` | The export dataset, as `project.dataset` |
| `target_dataset` | The target dataset, as `project.dataset` |
| `table_prefix` | The daily table name without the date, `events_` for GA4 and Firebase |
| `lookback_days` | How many days back from today to check for new or changed daily tables |

## 2. Backfill history

Run the procedure once with a long look back. The export keeps its daily tables until you delete them, except in BigQuery sandbox projects, which expire tables after 60 days. 400 days covers about 13 months:

```sql
CALL `<project_id>.<mitzu_dataset>.ga4_events_to_json`(
  '<project_id>.<ga4_dataset>',
  '<project_id>.<mitzu_dataset>',
  '<table_prefix>',
  400
);
```

The backfill reads every daily table once, so it costs about one full scan of the export. It is safe to run again: days that are up to date are skipped. On a very large export it can hit BigQuery's six-hour limit for a script; run the same `CALL` again and it carries on with the days that are still missing.

## 3. Schedule the daily job

1. In the BigQuery console, open a new query and paste the `CALL` below.
2. Click **Schedule** → **Create new scheduled query**.
3. Set it to repeat **Daily**, a few hours after Google usually writes the daily table, for example 06:00 UTC.
4. Do not set a destination table. The procedure writes the tables itself.
5. Pick the service account that runs it and save.

```sql
CALL `<project_id>.<mitzu_dataset>.ga4_events_to_json`(
  '<project_id>.<ga4_dataset>',
  '<project_id>.<mitzu_dataset>',
  '<table_prefix>',
  5
);
```

GA4 can update a daily table for up to 72 hours, and the table's date follows the property's time zone while the job counts days in UTC. Five days covers that for every time zone with a day of margin. Each run rebuilds only the days that changed, usually two or three.

## Several apps or GA4 properties

Each GA4 property and Firebase app has its own `analytics_` dataset. Give each one its own target dataset, since the copied tables keep their names, and add one `CALL` per app to the same scheduled query. The procedure only needs to exist once per location: a scheduled query, and the procedure it calls, run in one location, so apps exported to `US` and to `EU` need a procedure and a scheduled query each.

```sql
CALL `<project_id>.<mitzu_dataset>.ga4_events_to_json`(
  '<project_id>.analytics_111111111', '<project_id>.ga4_json_app_one', 'events_', 5
);
CALL `<project_id>.<mitzu_dataset>.ga4_events_to_json`(
  '<project_id>.analytics_222222222', '<project_id>.ga4_json_app_two', 'events_', 5
);
```

Create each target dataset first with `CREATE SCHEMA`, in the same location as its export.

## How values are converted

GA4 stores each parameter value in one of four fields. The job keeps the value's type, so numbers stay JSON numbers and can be summed or averaged.

| GA4 value | In the JSON |
|---|---|
| `string_value` | A string |
| `int_value` | A number |
| `int_value` larger than 2<sup>53</sup>, such as some screen IDs | A string, so no digits are lost |
| `double_value` or `float_value` | A number. Infinity and NaN, which JSON cannot hold as numbers, become the strings `"Infinity"`, `"-Infinity"` and `"NaN"` |
| No value set, or no key | The parameter is left out |

If the same parameter is sent twice on one event, the first one in the export's order is kept. An event with no parameters gets `{}`.

`JSON_VALUE` always returns a string, so cast numbers before doing maths:

```sql
SELECT
  event_day,
  SUM(CAST(JSON_VALUE(event_params['value']) AS FLOAT64)) AS revenue
FROM `<project_id>.<mitzu_dataset>.events_*`
WHERE _TABLE_SUFFIX >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
  AND event_name = 'purchase'
GROUP BY event_day
ORDER BY event_day
```

Filter on `_TABLE_SUFFIX` so BigQuery reads only the days you need.

## Using the tables in Mitzu

[Mitzu](https://mitzu.io) reads the JSON columns directly and lists every key as its own event property. Add the target dataset as a single event table named `events_2*` with user ID `user_pseudo_id`, event time `event_timestamp`, event name `event_name` and date partition `event_date`. The full walkthrough is in the Mitzu docs: [GA4 and Firebase exports as JSON](https://docs.mitzu.io/ga4-json-export).

## Troubleshooting

- **`Location specified in query ... is not consistent with current execution region US`**: the script ran in the default location. Keep the `SET @@location` line at the top of the setup script.
- **`Not found: Dataset ... was not found in location ...`**: the target dataset and the export dataset are in different locations. Delete the target dataset and create it again with the export's location.
- **`Access Denied`** in the scheduled query's run history: the account that runs it is missing one of the roles under [Requirements](#requirements).
- **No tables after the backfill**: check that the export writes daily tables named `events_YYYYMMDD`, and that `<table_prefix>` matches them.
- **`ga4_events_to_json: N table(s) failed, the others were built.`**: the message lists each failed day with BigQuery's own error. Fix the cause and run the `CALL` again; only the failed days are rebuilt.
- **A day you edited by hand keeps being rebuilt, or never is**: the job compares the export table with the `source_modified` label on the copy. Removing the label forces a rebuild on the next run.

## Tested on

Google's public GA4 sample, `bigquery-public-data.ga4_obfuscated_sample_ecommerce` (92 daily tables). On `events_20210131` the sum of `engagement_time_msec` read from the JSON matches the raw export exactly. A day that fails does not stop the others, and the argument checks reject a missing look back and a target equal to the source.
