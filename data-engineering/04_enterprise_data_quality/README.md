# Enterprise Data Quality Framework

## The Business Challenge
Across multiple enterprise organizations, I consistently encountered the same foundational issue: **a lack of trust in the data**. Stakeholders were frequently finding anomalies in their executive dashboards before the data engineering teams were aware of them. This reactive approach eroded confidence in data-driven decision-making and forced engineers to spend countless hours firefighting data discrepancies instead of building new features.

## The Solution
I designed and deployed an independent **Enterprise Data Quality Framework**. This system acted as an automated immune system for the data warehouse, continuously scanning inbound datasets for anomalies, missing values, and structural drift before they reached the Business Intelligence layer.

**Business Impact:**
- Fostered complete stakeholder trust by exposing a transparent "Data Health Dashboard" directly to business users.
- Shifted the data engineering culture from *reactive firefighting* to *proactive monitoring*.
- Intercepted thousands of bad records before they could corrupt financial and operational reporting.

---

## Technical Implementation (Code Sample)
This folder contains a mock implementation of the **Warehouse Change Control Engine** that powers the Data Quality Framework's deployment cycle. 

Implementing a data quality framework is useless if developers can accidentally bypass it. This Python utility acts as a CI/CD gatekeeper. It parses deployment manifests, verifies that proper data quality check scripts (DDL/DML) are attached to every new data model release, and ensures strict naming conventions are followed before any code reaches production.

### How to Run the Tests
```bash
python -m unittest tests.test_change_manifest
```
