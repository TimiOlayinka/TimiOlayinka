# GCP Modernisation & Real-Time Pipelines

## The Business Challenge
A fast-fashion retail giant (Boohoo Group) was scaling rapidly, but their legacy daily batch-processing infrastructure couldn't keep up with the real-time demands of modern e-commerce. Marketing and supply chain teams were making decisions on stale data, leading to stockouts and missed revenue opportunities during peak sales events like Black Friday.

## The Solution
I led the migration of critical data workloads to the Google Cloud Platform (GCP). I designed and deployed low-latency, real-time data pipelines using Apache Kafka and Google Dataflow to capture clickstream, transaction, and inventory events the moment they happened.

**Business Impact:**
- Reduced data latency from **24 hours to sub-minute**, empowering marketing to make intra-day campaign adjustments.
- Established a robust event-streaming contract architecture, eliminating silent data corruption when upstream microservices changed their schemas.
- Processed over **50 million events per day** with auto-scaling infrastructure.

---

## Technical Implementation (Code Sample)
This folder contains a mock implementation of the **Event Stream Quality Gate** used to protect the real-time GCP pipelines. 

In a real-time environment, bad data can corrupt a warehouse in seconds. This Python micro-framework intercepts incoming JSON events, validates them against strict schema contracts, and automatically quarantines invalid events into a Dead Letter Queue (DLQ) without crashing the stream.

### How to Run the Tests
```bash
python -m unittest tests.test_event_quality_gate
```
