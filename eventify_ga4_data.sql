CREATE OR REPLACE TABLE
  <schema>.ga4_events 
AS (
WITH
  event_properties AS (
  SELECT
    event_timestamp,
    user_id,
    user_pseudo_id,
    event_name,
    PARSE_JSON("{\""|| STRING_AGG(p.key || "\":\""|| COALESCE(coalesce(COALESCE(p.value.string_value, CAST(p.value.int_value AS string)),
            CAST(p.value.float_value AS string)), CAST(p.value.double_value AS string)),"\",\"") || "\"}") AS event_params,
  FROM
    `<project>.<schema>.events_*`,
    UNNEST(event_params) AS p
  GROUP BY
    1,
    2,
    3,
    4 ),
  user_properties AS (
  SELECT
    event_timestamp,
    user_id,
    user_pseudo_id,
    event_name,
    PARSE_JSON("{\""|| STRING_AGG(p.key || "\":\""|| COALESCE(coalesce(COALESCE(p.value.string_value, CAST(p.value.int_value AS string)),
            CAST(p.value.float_value AS string)), CAST(p.value.double_value AS string)),"\",\"") || "\"}") AS user_properties,
  FROM
    `<project>.<schema>.events_*`,
    UNNEST(user_properties) AS p
  GROUP BY
    1,
    2,
    3,
    4 )
SELECT
  TIMESTAMP_MICROS(evt.event_timestamp) AS event_time,
  ep.event_params,
  up.user_properties,
  evt.* EXCEPT (event_timestamp,
    event_params,
    user_properties,
    items)
FROM
  `<project>.<schema>.events_*` AS evt
LEFT JOIN
  event_properties ep
ON
  evt.event_timestamp = ep.event_timestamp
  AND COALESCE(evt.user_id,'')=COALESCE(ep.user_id,'')
  AND evt.user_pseudo_id =ep.user_pseudo_id
  AND evt.event_name = ep.event_name
LEFT JOIN
  user_properties up
ON
  evt.event_timestamp = up.event_timestamp
  AND COALESCE(evt.user_id,'')=COALESCE(up.user_id,'')
  AND evt.user_pseudo_id =up.user_pseudo_id
  AND evt.event_name = up.event_name)
