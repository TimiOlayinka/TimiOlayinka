CREATE TABLE IF NOT EXISTS core.dim_entity_snapshot (
    entity_id            VARCHAR(64)   NOT NULL,
    snapshot_date        DATE          NOT NULL,
    entity_status        VARCHAR(40)   NOT NULL,
    entity_value         NUMERIC(18, 4),
    source_record_hash   VARCHAR(64)   NOT NULL,
    loaded_at_utc        TIMESTAMP     NOT NULL DEFAULT GETDATE(),
    updated_at_utc       TIMESTAMP     NOT NULL DEFAULT GETDATE()
)
DISTSTYLE KEY
DISTKEY (entity_id)
SORTKEY (snapshot_date, entity_id);

COMMENT ON TABLE core.dim_entity_snapshot IS
'Generic daily entity snapshot used for analytical conformance examples.';
