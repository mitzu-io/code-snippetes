WITH ordered_events AS (
    SELECT 
        user_id,
        event_time,
        event_name,
        LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) as previous_event_time
    FROM 
        analytics.events
),
sessionized_events AS (
    SELECT
        user_id,
        event_time,
        event_name,
        CASE 
            WHEN previous_event_time IS NULL OR 
                 DATETIME_DIFF(event_time, previous_event_time, MINUTE) > 30 -- 30 minutes grace period
            THEN 1
            ELSE 0 
        END as is_new_session
    FROM 
        ordered_events
)

SELECT
    user_id,
    event_time,
    event_name,
    SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY event_time) as session_id
FROM 
    sessionized_events
