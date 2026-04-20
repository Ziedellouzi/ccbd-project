# CCBD Project — Schema Evolution & Backward-Compatible Reads

## 📌 Overview
This project explores **schema evolution in data lakes** using Parquet files stored on object storage (S3-like).  
We simulate how datasets evolve over time and implement a system that allows **backward-compatible reads** across multiple dataset versions.

---

## 🎯 Project Variant
**Variant 7 — Schema evolution & backward-compatible reads**

We implemented a simple and safe schema evolution scenario:
- **v1 dataset**: original schema  
- **v2 dataset**: same schema + one additional column (`device_type`)  

This ensures backward compatibility and avoids complex transformations.

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
