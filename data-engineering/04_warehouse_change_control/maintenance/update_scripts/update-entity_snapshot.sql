BEGIN;

DELETE FROM core.dim_entity_snapshot
WHERE snapshot_date = :business_date;

INSERT INTO core.dim_entity_snapshot (
    entity_id,
    snapshot_date,
    entity_status,
    entity_value,
    source_record_hash,
    loaded_at_utc,
    updated_at_utc
)
SELECT
    entity_id,
    snapshot_date,
    entity_status,
    entity_value,
    source_record_hash,
    GETDATE() AS loaded_at_utc,
    GETDATE() AS updated_at_utc
FROM stage.entity_snapshot_delta
WHERE snapshot_date = :business_date;

COMMIT;
