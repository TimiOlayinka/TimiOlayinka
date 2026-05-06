# Global Logistics Analytics Ecosystem

## The Business Challenge
A massive e-commerce logistics group (The Hut Group) was struggling with fragmented supply chain data. Their highly automated, AI-powered warehouse systems were generating terabytes of operational data, but it was siloed across different operational databases. This prevented leadership from getting a holistic view of global inventory, fulfillment bottlenecks, and shipping costs.

## The Solution
I architected a modern, centralized analytics ecosystem designed specifically to ingest and unify complex global logistics data. Utilizing advanced SQL modelling and a cloud-native data stack, I built out enterprise-grade data models that abstracted the complexity of the raw warehouse systems into intuitive, business-ready datasets.

**Business Impact:**
- Empowered the C-Suite with highly accurate, unified Power BI dashboards to track global fulfillment SLAs in real-time.
- Decoupled analytical workloads from operational databases, improving the performance of the AI-powered warehouse robots.
- Standardized data definitions across 5 different international logistics hubs.

---

## Technical Implementation (Code Sample)
This folder contains a mock implementation of the **SQL Lineage & Audit Tool** I built to map out the complex dependencies of the logistics data models.

When maintaining hundreds of SQL transformations in an enterprise warehouse, dropping a single column can break critical downstream dashboards. This Python utility parses complex SQL files, automatically extracts the source-to-target mapping, and generates an audit manifest to prevent breaking changes during deployments.

### How to Run the Tests
```bash
python -m unittest tests.test_sql_lineage_audit
```
