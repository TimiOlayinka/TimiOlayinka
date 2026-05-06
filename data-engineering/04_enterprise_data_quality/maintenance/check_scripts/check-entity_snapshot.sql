SELECT
    'duplicate_key_check' AS check_name,
    COUNT(*) AS failing_rows
FROM (
    SELECT
        entity_id,
        snapshot_date
    FROM core.dim_entity_snapshot
    GROUP BY
        entity_id,
        snapshot_date
    HAVING COUNT(*) > 1
) duplicate_keys

UNION ALL

SELECT
    'missing_required_values' AS check_name,
    COUNT(*) AS failing_rows
FROM core.dim_entity_snapshot
WHERE entity_id IS NULL
   OR snapshot_date IS NULL
   OR entity_status IS NULL;
