INSERT INTO mart.entity_metrics_daily
SELECT
    metric_date,
    entity_status,
    entity_count,
    total_event_value
FROM work.entity_metrics_daily;
