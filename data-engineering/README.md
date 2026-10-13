# Data & Analytics Engineering Reference Toolkit

This folder contains five anonymized, self-contained engineering modules with automated unit tests. Each module demonstrates a core pillar of how I architect, govern, and optimize enterprise data platforms across **dbt**, **Google BigQuery**, **Amazon Redshift**, and **Snowflake**.

## Modules

1. **[`01_redshift_warehouse_modernisation`](./01_redshift_warehouse_modernisation)** — Metadata-driven ingestion engine that resolves DAG execution order, enforces schema contracts, and decouples extraction from dbt transformations (**40% faster overnight batch completion**).
2. **[`02_gcp_realtime_pipelines`](./02_gcp_realtime_pipelines)** — Real-time JSON event stream contract validator and Dead-Letter Queue (DLQ) quarantine router (**50M+ daily events, 24h → sub-minute latency**).
3. **[`03_global_logistics_analytics`](./03_global_logistics_analytics)** — Automated SQL AST/regex lineage extractor that maps upstream-to-downstream table dependencies before warehouse deployments (**16.5M+ parcels & £20M+ peak Black Friday trading**).
4. **[`04_enterprise_data_quality`](./04_enterprise_data_quality)** — Warehouse release manifest gatekeeper ensuring DDL, backfill update scripts, data quality checks, and rollback plans accompany every production release.
5. **[`05_dbt_governance_and_slim_ci`](./05_dbt_governance_and_slim_ci)** — Automated **dbt Contract, FinOps & Coverage Quality Gate** enforcing primary key uniqueness, `contract: {enforced: true}`, source freshness SLAs, BigQuery partitioning/clustering, and Redshift `DISTKEY`/`SORTKEY` tuning (**45% → 97% trusted data coverage, 30% fewer data incidents**). Includes production-grade incremental dbt models for **OTIF Parcel Fulfillment** and **Subscription MRR Retention**.

## Running All Tests

```bash
python data-engineering/run_project_tests.py
```
