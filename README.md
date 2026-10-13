<div align="center">
  <h1>Timi Olayinka</h1>
  <h3>Senior Analytics Engineer & CDMP-Certified Data Architect</h3>
  <p><strong>dbt (Core & Cloud) • Kimball Dimensional Modeling • Google BigQuery • Amazon Redshift • Snowflake • Apache Airflow</strong></p>
  <p>
    <img src="https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white" alt="dbt">
    <img src="https://img.shields.io/badge/Google_BigQuery-669DF6?style=for-the-badge&logo=googlebigquery&logoColor=white" alt="BigQuery">
    <img src="https://img.shields.io/badge/Amazon_Redshift-8C4FFF?style=for-the-badge&logo=amazonredshift&logoColor=white" alt="Redshift">
    <img src="https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white" alt="Snowflake">
    <img src="https://img.shields.io/badge/Apache_Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="Airflow">
    <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python">
    <img src="https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white" alt="Terraform">
  </p>
  <p>
    <a href="https://www.upwork.com/freelancers/~017051376282b81462"><b>💼 Hire Me on Upwork</b></a> •
    <a href="https://www.linkedin.com/in/timi-olayinka-8b299392/"><b>👔 LinkedIn</b></a> •
    <a href="https://timiolayinka.co.uk"><b>🌐 timiolayinka.co.uk</b></a>
  </p>
</div>

---

## 🏛️ Executive Summary

I am a **Senior Analytics Engineer & CDMP-Certified Data Architect** with **8+ years of enterprise impact** across **Holland & Barrett**, **Boohoo Group**, and **The Hut Group (THG)**. I specialize in turning fragile, slow, or high-cost legacy SQL pipelines into modular, contract-tested **dbt** and **Medallion (RDL → ODL → ADL)** platforms that executives and analysts trust.

### 📈 Quantified Enterprise Impact
- **1,420+ Production dbt & SQL Mart Deployments** across Retail, E-Commerce, Supply Chain, Subscriptions & Finance (**£830M+ turnover estate**), ranked **#1 in the data department for business modeling focus (76.1%)**.
- **£6.4M ($8.1M) Annual Revenue Uplift** uncovered by architecting governed dimensional marts tracking **£26.9M** in commercial spend, customer retention, and NPS/CSAT attribution.
- **£30K/Month (£360K/Yr) Cloud FinOps Savings** leading an enterprise migration from legacy SQL Server/Oracle to **Google Cloud BigQuery**, plus **40% fewer scanned query bytes** via strategic date partitioning and multi-column clustering.
- **22–25% Lower Warehouse Cost & Latency** on **Amazon Redshift** through FinOps workload tuning, `DISTKEY`/`SORTKEY` optimization, and incremental merge materializations.
- **45% → 97% Trusted Data Coverage (30% Fewer Incident Tickets)** by implementing automated **dbt test suites**, enforced data contracts, SQLFluff linting, Slim CI (`state:modified+`), and source freshness SLAs.
- **£20M+ ($25M+) Peak Black Friday Trading Supported** & **16.5M+ Supply Chain Parcels** modeled end-to-end with automated OTIF fulfillment reporting and **894K+ active subscriptions (£220K+ MRR)**.

---

## 🚀 Flagship Production Architecture Repositories

Explore my end-to-end reference architectures and production-style codebases across AWS and GCP:

| Repository | Architecture & Business Domain | Key Engineering Highlights |
| :--- | :--- | :--- |
| **[`aws-data-warehouse`](https://github.com/TimiOlayinka/aws-data-warehouse)** | **Enterprise dbt & Amazon Redshift Dimensional Marts** *(Customer Experience, NPS/CSAT & Commercial Attribution)* | • **dbt Core (`RDL → ODL → ADL`)** with Kimball Star Schema, SCD Type 2 snapshots & custom macros<br>• **Redshift FinOps:** Explicit `DISTKEY`/`SORTKEY` engineering & `incremental_strategy='merge'`<br>• **Data Quality & Slim CI:** Enforced dbt contracts, unit tests, SQLFluff & GitHub Actions CI/CD<br>• **Full Stack:** AWS Lambda generators, S3 Data Lake, Airflow DAGs & Terraform IaC |
| **[`boohoo-data-pipeline`](https://github.com/TimiOlayinka/boohoo-data-pipeline)** | **Multi-Brand E-Commerce Warehouse on GCP** *(BigQuery, dbt & Dataform Medallion Architecture — 7 Brands, 48+ Models)* | • **Dual Transformation showcase:** Complete **dbt-bigquery** (`dbt/`) & **GCP Dataform** (`definitions/`) implementations<br>• **BigQuery FinOps (40% fewer bytes scanned):** `partition_by`, `cluster_by`, `require_partition_filter=true` & `maximum_bytes_billed` cost guards<br>• **Enterprise Governance:** Per-model `model.yml` SLAs, incident `runbook.md` & automated `tests.yml`<br>• **Serverless Ingestion:** 9 Cloud Run Jobs, GCS lifecycle tiering, Instance-Scheduled Airflow VM & Terraform |
| **[`bellosdata-lakehouse`](https://github.com/TimiOlayinka/bellosdata-lakehouse)** | **Metadata-Driven AWS S3 Delta Lakehouse** *(Airflow 3.2, YAML Dimensional Builders, AWS Glue & Redshift Serverless)* | • **YAML-Driven Modeling Engine:** Declarative `dimensions.yml`, `facts.yml` & `mappings.yml` registries constructing flat & hierarchy dimensions automatically<br>• **ACID Lakehouse:** S3 Bronze/Silver Delta Lake tables synced with **AWS Glue Data Catalog** and queried via **Redshift Serverless** (8 RPU auto-pause)<br>• **18 Production Airflow DAGs** & complete Terraform infrastructure |

---

## 📂 In-Repo Engineering & Governance Toolkit (`./data-engineering`)

Inside this repository's [`data-engineering`](./data-engineering) folder are **5 zero-dependency, unit-tested reference implementations** of the automation tools I use to keep enterprise warehouses reliable:

| Module | What It Demonstrates |
| :--- | :--- |
| **[01. Redshift Warehouse Modernisation](./data-engineering/01_redshift_warehouse_modernisation)** | Metadata-driven ingestion engine with topological dependency resolution and deterministic run-plan generation (**40% faster batch completion**). |
| **[02. GCP Real-Time Event Quality Gate](./data-engineering/02_gcp_realtime_pipelines)** | Streaming JSON event contract validation, automatic Dead-Letter Queue (DLQ) quarantine routing, and batch SLA telemetry (**50M+ events/day**). |
| **[03. Global Logistics SQL Lineage Audit](./data-engineering/03_global_logistics_analytics)** | Automated source-to-target SQL dependency extraction for pre-deployment impact analysis (**16.5M+ supply chain parcels**). |
| **[04. Enterprise Warehouse Release Gate](./data-engineering/04_enterprise_data_quality)** | Change-control manifest validator ensuring DDL, update scripts, data quality checks, and rollback plans exist before deployment. |
| **[05. dbt Governance, FinOps & Slim CI Gate](./data-engineering/05_dbt_governance_and_slim_ci)** | Enforces primary key tests, `contract_enforced: true`, source freshness SLAs, and BigQuery/Redshift partition/sort tuning (**45% → 97% trusted coverage**). Includes sample dbt incremental models for **OTIF Parcel Fulfillment** and **Subscription MRR Retention**. |

> **Run all 5 test suites locally:** `python data-engineering/run_project_tests.py`

---

## 🏅 Verified Certifications & Credentials

- **[Certified Data Management Professional (CDMP – Associate)](https://certificates.cdmp.info/1c7acf0e-1c9b-4d33-901b-fa7292aa947c#acc.lpozwqtF)** — DAMA International
- **[AWS Certified Solutions Architect – Associate](https://www.credly.com/badges/4bca9b35-39a3-4186-907a-99532b5e6811/public_url)** — Amazon Web Services
- **[AWS Certified Data Engineer – Associate](https://www.credly.com/badges/5d31da0f-1c6e-4bbd-a644-d60bd3e43c09/public_url)** — Amazon Web Services
- **[AWS Certified Cloud Practitioner](https://www.credly.com/badges/94d9b5ac-e75e-4b22-9a18-5ab831690f77/public_url)** — Amazon Web Services
- **[PCAP™ – Certified Associate Python Programmer](https://www.credly.com/badges/9f20fd2d-70d9-41d3-9e04-c9c2595c7cda/public_url)** — Python Institute
- **MSc Digital Marketing & Analytics (Distinction)**

---

## 🛠️ Core Technical Stack

- **Transformation & Modeling:** `dbt (Core & Cloud)` • `Dataform` • `Kimball Star Schema` • `SCD Type 2 Snapshots` • `Medallion Architecture (Bronze / Silver / Gold)` • `Data Contracts` • `SQLFluff`
- **Cloud Data Warehouses & Lakehouses:** `Google BigQuery` • `Amazon Redshift (Provisioned & Serverless)` • `Snowflake` • `Delta Lake on S3` • `AWS Glue Data Catalog` • `PostgreSQL`
- **Orchestration & Ingestion:** `Apache Airflow` • `Google Cloud Composer` • `Cloud Run` • `AWS Lambda` • `EventBridge` • `Fivetran` • `Kafka / Dataflow`
- **BI & Semantic Layers:** `Looker (LookML)` • `Microsoft Power BI (DAX)` • `Tableau`
- **DevOps & IaC:** `Terraform` • `Docker` • `GitHub Actions / GitLab Slim CI` • `Python 3.11`

<div align="center">
  <br/>
  <i>"Architecture is not just about moving data from point A to B; it's about building a governed foundation of executive trust."</i>
</div>
