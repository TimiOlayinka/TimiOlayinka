# Automated dbt Governance, FinOps & Slim CI Quality Gate

## The Business Challenge
Across fast-growing enterprise data estates (1,420+ production dbt and SQL marts spanning Retail, E-Commerce, Supply Chain, and Subscriptions), ad-hoc model deployments without standardized data contracts or warehouse physical tuning caused two recurring issues:
1. **Low Stakeholder Trust (45% Coverage):** Unvalidated upstream schema changes and missing primary-key constraints caused silent fan-outs in executive Looker and Power BI dashboards.
2. **Warehouse Compute Sprawl:** Large fact tables deployed without BigQuery `partition_by`/`cluster_by` or Redshift `dist`/`sort` keys scanned full multi-terabyte tables on every BI refresh.

## The Solution
I architected and enforced an **Automated dbt Governance & Slim CI Quality Gate** integrated directly into GitHub Actions / GitLab CI. Before `dbt build --select state:modified+ --defer` executes in a pull request, this gate inspects the project metadata and enforces strict architectural standards across every modified model.

**Business Impact:**
- **45% → 97% Trusted Data Coverage** across production dimensional marts, reducing downstream data incident tickets by **30%**.
- **Zero Unpartitioned Fact Deployments:** Automatically blocked fact table PRs lacking BigQuery partition/cluster definitions or Redshift `DISTKEY`/`SORTKEY` tuning, contributing to **40% fewer scanned query bytes** on BigQuery and **22–25% lower query cost & latency** on Redshift.
- **Guaranteed SLA Freshness:** Enforced explicit `warn_after` / `error_after` freshness SLAs and column-level documentation on 100% of gold (`adl` / `marts`) tables.

---

## Technical Implementation (Code Sample)
This folder contains a self-contained implementation of the **dbt Contract, FinOps & Coverage Gate** (`src/dbt_contract_gate.py`) along with two representative production SQL models in `sample_dbt_project/`:
- `fct_otif_parcel_fulfillment.sql`: End-to-end supply chain OTIF (On-Time In-Full) parcel tracking mart (modeled for 16.5M+ parcels & peak Black Friday robotics fulfillment).
- `fct_subscription_mrr_retention.sql`: Subscription cohort & MRR retention mart (modeled for 894K+ active subscribers & £220K+ monthly recurring revenue).

### What the Quality Gate Enforces
1. **Primary Key Integrity:** Every `odl` / `adl` / `marts` model must declare a primary key with both `unique` and `not_null` tests.
2. **Enforced Data Contracts:** Certified BI (`adl` / `marts`) models must enable `contract_enforced: true` and provide non-empty descriptions and data types for every column.
3. **Source Freshness SLAs:** Every upstream source table must define `freshness.warn_after_hours` and `freshness.error_after_hours`.
4. **FinOps Physical Tuning:** Every `incremental` or `table` fact model (`fact_*` / `fct_*`) must specify either BigQuery `partition_by` + `cluster_by` or Redshift `dist_key` + `sort_keys`.
5. **Trusted Coverage Threshold:** Computes the estate-wide **Trusted Data Coverage %** and fails the CI pipeline if coverage drops below the configured threshold (default `95.0%`).

### How to Run the Tests
```bash
python -m unittest discover -s data-engineering/05_dbt_governance_and_slim_ci/tests
```
