# CCBD Project — Schema Evolution & Backward-Compatible Reads

## 📌 Overview
This project explores **schema evolution in data lakes** using Parquet files stored on object storage (S3-like).  
We simulate how datasets evolve over time and implement a system that allows **backward-compatible reads** across multiple dataset versions.

---

##  🧱 Dataset Generation

This script generates two dataset versions:

- **v1**: events from January 2026 (baseline schema)
- **v2**: events from February 2026 with an additional column (`device_type`)

Unlike a simple copy, `v2` represents new incoming data collected after the schema evolution.

This design reflects a realistic data pipeline where:
- historical data remains unchanged
- new data is written with an updated schema

Both datasets are stored in Parquet format for efficient processing.

---

## 🧭 Project Workflow (Step-by-Step)

This section describes the exact steps required to complete the project.

### 1. Dataset Generation
- Generate a synthetic dataset with the following schema:
  - `ts`, `user_id`, `region`, `event_type`, `value`
- Create two versions:
  - **v1**: original schema
  - **v2**: same schema + additional column `device_type`
- Save both datasets in **Parquet format**

---

### 2. Data Storage
- Organize datasets using the following structure:
curated/<dataset_id>/v1/
curated/<dataset_id>/v2/
 
- Store data locally or upload to cloud storage (Azure Blob / S3 equivalent)

---

### 3. Schema Contract
- Define a schema contract file (`schema.json`)
- Include all expected fields and types
- Ensure compatibility between v1 and v2

---

### 4. Data Reading (Core Task)
- Implement a reader that supports:
  - Reading **v1 only**
  - Reading **v2 only**
  - Reading **v1 + v2 together**
- Handle missing columns (e.g., `device_type` in v1 → NULL values)
- Validate data against the schema contract

---

### 5. Query Implementation
- Implement a fixed analytics query:
  - Filter by `region` and time range (`ts`)
  - Group by `event_type`
  - Compute:
    - count
    - average of `value`

---

### 6. Benchmarking
- Run experiments on:
  - v1 dataset
  - v2 dataset
  - combined dataset (v1 + v2)
- Measure:
  - Query runtime
  - Data size
  - Number of files

---

### 7. Analysis
- Use a Jupyter Notebook (`analysis.ipynb`) to:
  - Visualize results (plots, tables)
  - Compare performance
  - Interpret findings

---

### 8. Report
- Describe:
  - Methodology
  - Implementation choices
  - Results and analysis
  - Limitations

---

### 9. Demo
- Prepare a short video (2–3 minutes)
- Demonstrate:
  - Dataset generation
  - Query execution
  - Key results
