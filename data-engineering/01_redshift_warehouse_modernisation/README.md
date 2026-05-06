# Redshift Data Warehouse Modernisation

## The Business Challenge
A major health and wellness retailer (Holland & Barrett) was constrained by legacy, siloed databases that caused overnight ETL jobs to frequently fail, leaving business analysts without reliable morning reports. They needed a unified, high-performance Enterprise Data Warehouse (EDW) to serve as a single source of truth for global sales and inventory data.

## The Solution
I spearheaded the migration to an Amazon Redshift data warehouse, establishing a clear **Medallion Architecture**. By leveraging Python for metadata-driven ingestion and dbt for transformations, we decoupled data extraction from business logic. 

**Business Impact:**
- Reduced overnight batch processing times by **40%**.
- Automated schema evolution, preventing pipeline failures when upstream APIs changed.
- Enabled self-service analytics for the C-Suite via Tableau.

---

## Technical Implementation (Code Sample)
This folder contains a mock implementation of the **Metadata-Driven Ingestion Engine** used to power the Bronze layer of the Redshift migration.

Instead of writing custom Python scripts for every new API endpoint, this pipeline uses a JSON config file to automatically generate extraction, schema enforcement, and loading logic.

### How to Run the Tests
```bash
python -m unittest tests.test_metadata_pipeline
```
