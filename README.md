# CCBD Project — Schema Evolution & Backward-Compatible Reads

## 📌 Overview
This project explores **schema evolution in data lakes** using Parquet files stored on object storage (S3-like).  
We simulate how datasets evolve over time and implement a system that allows **backward-compatible reads** across multiple dataset versions.

---


## Dataset Generation

This script generates two dataset versions:

- **v1**: events from January 2026 (baseline schema)
- **v2**: events from February 2026 with an additional column (`device_type`)

Unlike a simple copy, `v2` represents new incoming data collected after the schema evolution.

This design reflects a realistic data pipeline where:
- historical data remains unchanged
- new data is written with an updated schema

Both datasets are stored in Parquet format for efficient processing.
---

## 🧱 Dataset Schema

### Version 1 (v1)
- `ts` (timestamp)  
- `user_id` (int)  
- `region` (string)  
- `event_type` (string)  
- `value` (float)  

### Version 2 (v2)
- Same as v1  
- + `device_type` (string, nullable)  

When reading both versions together, missing values in v1 are handled as `NULL`.

---
## 🚀 How to Run

### 1. Install dependencies
```bash
pip install -r requirements.txt
