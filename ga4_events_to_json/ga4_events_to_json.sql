-- GA4 / Firebase BigQuery export -> JSON tables, rebuilt incrementally every day.
-- Replace <project_id>, <mitzu_dataset> and <location>, run this file once, then CALL the
-- procedure (see README.md for the backfill, the daily scheduled query and every argument).
-- https://github.com/mitzu-io/code-snippetes/tree/main/ga4_events_to_json

CREATE SCHEMA IF NOT EXISTS `<project_id>.<mitzu_dataset>`
OPTIONS (location = '<location>');

CREATE OR REPLACE PROCEDURE `<project_id>.<mitzu_dataset>.ga4_events_to_json`(
  source_dataset STRING,
  target_dataset STRING,
  table_prefix STRING,
  lookback_days INT64
)
BEGIN
  DECLARE shards ARRAY<STRUCT<table_id STRING, source_modified INT64>>;
  DECLARE failures ARRAY<STRING> DEFAULT [];

  IF source_dataset IS NULL OR target_dataset IS NULL OR table_prefix IS NULL OR lookback_days IS NULL OR lookback_days < 0 THEN
    RAISE USING MESSAGE = 'ga4_events_to_json: every argument is required and lookback_days must be 0 or more';
  END IF;
  IF LOWER(source_dataset) = LOWER(target_dataset) THEN
    RAISE USING MESSAGE = 'ga4_events_to_json: target_dataset must differ from source_dataset, or the export would be overwritten';
  END IF;

  EXECUTE IMMEDIATE FORMAT("""
    WITH built AS (
      SELECT
        table_name AS table_id,
        SAFE_CAST(REGEXP_EXTRACT(option_value, r'"source_modified", "([0-9]+)"') AS INT64) AS source_modified
      FROM `%s.INFORMATION_SCHEMA.TABLE_OPTIONS`
      WHERE option_name = 'labels'
    )
    SELECT IFNULL(ARRAY_AGG(STRUCT(s.table_id, s.last_modified_time AS source_modified) ORDER BY s.table_id), [])
    FROM `%s.__TABLES__` AS s
    LEFT JOIN built AS b USING (table_id)
    WHERE STARTS_WITH(s.table_id, %T)
      AND REGEXP_CONTAINS(SUBSTR(s.table_id, LENGTH(%T) + 1), r'^[0-9]{8}$')
      AND SUBSTR(s.table_id, LENGTH(%T) + 1) >= FORMAT_DATE('%%Y%%m%%d', DATE_SUB(CURRENT_DATE(), INTERVAL %d DAY))
      AND (b.source_modified IS NULL OR s.last_modified_time > b.source_modified)
  """, target_dataset, source_dataset, table_prefix, table_prefix, table_prefix, lookback_days)
  INTO shards;

  FOR shard IN (SELECT * FROM UNNEST(shards) ORDER BY table_id) DO
    BEGIN
      EXECUTE IMMEDIATE FORMAT("""
        CREATE OR REPLACE TABLE `%s.%s`
        CLUSTER BY event_name, user_pseudo_id
        OPTIONS (
          description = 'GA4 export with event_params and user_properties as JSON, built by ga4_events_to_json',
          labels = [('source_modified', '%d')]
        )
        AS
        SELECT
          PARSE_DATE('%%Y%%m%%d', event_date) AS event_day,
          TIMESTAMP_MICROS(event_timestamp) AS event_time,
          * EXCEPT (event_params, user_properties),
          (
            SELECT JSON_OBJECT(
              IFNULL(ARRAY_AGG(p.key ORDER BY p.pos), ARRAY<STRING>[]),
              IFNULL(ARRAY_AGG(p.v ORDER BY p.pos), ARRAY<JSON>[])
            )
            FROM (
              SELECT
                pos,
                CAST(kv.key AS STRING) AS key,
                CASE
                  WHEN kv.value.string_value IS NOT NULL THEN TO_JSON(kv.value.string_value)
                  WHEN kv.value.int_value IS NOT NULL THEN TO_JSON(kv.value.int_value, stringify_wide_numbers => TRUE)
                  WHEN kv.value.double_value IS NOT NULL THEN TO_JSON(kv.value.double_value)
                  WHEN kv.value.float_value IS NOT NULL THEN TO_JSON(kv.value.float_value)
                END AS v
              FROM UNNEST(event_params) AS kv WITH OFFSET AS pos
            ) AS p
            WHERE p.key IS NOT NULL AND p.v IS NOT NULL
          ) AS event_params,
          (
            SELECT JSON_OBJECT(
              IFNULL(ARRAY_AGG(p.key ORDER BY p.pos), ARRAY<STRING>[]),
              IFNULL(ARRAY_AGG(p.v ORDER BY p.pos), ARRAY<JSON>[])
            )
            FROM (
              SELECT
                pos,
                CAST(kv.key AS STRING) AS key,
                CASE
                  WHEN kv.value.string_value IS NOT NULL THEN TO_JSON(kv.value.string_value)
                  WHEN kv.value.int_value IS NOT NULL THEN TO_JSON(kv.value.int_value, stringify_wide_numbers => TRUE)
                  WHEN kv.value.double_value IS NOT NULL THEN TO_JSON(kv.value.double_value)
                  WHEN kv.value.float_value IS NOT NULL THEN TO_JSON(kv.value.float_value)
                END AS v
              FROM UNNEST(user_properties) AS kv WITH OFFSET AS pos
            ) AS p
            WHERE p.key IS NOT NULL AND p.v IS NOT NULL
          ) AS user_properties
        FROM `%s.%s`
      """, target_dataset, shard.table_id, shard.source_modified, source_dataset, shard.table_id);
    EXCEPTION WHEN ERROR THEN
      SET failures = ARRAY_CONCAT(failures, [FORMAT('%s: %s', shard.table_id, @@error.message)]);
    END;
  END FOR;

  IF ARRAY_LENGTH(failures) > 0 THEN
    RAISE USING MESSAGE = FORMAT('ga4_events_to_json: %d table(s) failed, the others were built. %s', ARRAY_LENGTH(failures), ARRAY_TO_STRING(failures, ' | '));
  END IF;
END;
