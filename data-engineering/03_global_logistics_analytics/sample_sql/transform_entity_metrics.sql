CREATE TABLE work.entity_metrics_daily AS
WITH source_events AS (
    SELECT
        CAST(event_ts AS DATE) AS metric_date,
        entity_id,
        event_value
    FROM raw.entity_events
),
current_status AS (
    SELECT
        entity_id,
        entity_status
    FROM reference.entity_status
)
SELECT
    source_events.metric_date,
    current_status.entity_status,
    COUNT(DISTINCT source_events.entity_id) AS entity_count,
    SUM(source_events.event_value) AS total_event_value
FROM source_events
INNER JOIN current_status
    ON source_events.entity_id = current_status.entity_id
GROUP BY
    source_events.metric_date,
    current_status.entity_status;
