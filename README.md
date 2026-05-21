# Schema Evolution on S3-Compatible Object Storage

Author: Zied, Ellouzi, Nassim Maliki

---

## Overview

This project implements and benchmarks a simple schema evolution workflow on S3-compatible object storage using MinIO and Parquet datasets.

The project simulates a realistic situation where a dataset evolves over time while older data still needs to remain queryable.

Two dataset versions are generated:

* `v1` → original schema
* `v2` → evolved schema with a new column

The benchmark evaluates how the system behaves when reading both versions independently and together.

---

## Key Features

* Versioned Parquet datasets
* Schema evolution support
* Backward-compatible mixed reads
* MinIO S3-compatible object storage
* Automated benchmark workflow
* Schema contract validation
* Runtime and throughput analysis

---

## Schema Evolution Scenario

### Version 1 (`v1`)

| Column     |
| ---------- |
| ts         |
| user_id    |
| region     |
| event_type |
| value      |

### Version 2 (`v2`)

| Column      |
| ----------- |
| ts          |
| user_id     |
| region      |
| event_type  |
| value       |
| device_type |

The `device_type` column represents the schema evolution introduced in `v2`.

The reader aligns both versions to a unified schema before querying.

---

## Synthetic Dataset Characteristics

The synthetic datasets use non-uniform distributions in order to simulate a more realistic analytical workload.

Examples:

* `view` events are dominant;
* `click` events are less frequent;
* `purchase` events remain rare;
* `device_type` is dominated by mobile traffic.

These distributions are analyzed in the notebook.

---

## Project Structure

```text id="h3wqov"
project-root/
│
├── src/
│   ├── dataset_gen.py
│   ├── upload.py
│   ├── download.py
│   ├── reader_s3.py
│   └── bench.py
│
├── notebooks/
│   └── analysis.ipynb
│
├── results/
│   └── results.csv
│
├── data_bench/
├── downloads_bench/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Object Storage Layout

Datasets are stored inside the `ccbd-data` MinIO bucket.

```text id="v9h2sg"
ccbd-data/
├── curated/
│   └── events/
│       ├── v1/
│       └── v2/
└── published/
    └── events/
        └── schema.json
```

* `curated/` stores Parquet datasets
* `published/` stores the schema contract

---

# Quick Start

## 1. Install Dependencies

```bash id="rqfrwo"
pip install -r requirements.txt
```

---

## 2. Start MinIO

Start the local S3-compatible object-storage server:

```bash id="j1h7gz"
docker compose up -d
```

MinIO interface:

```text id="v6d6qf"
http://localhost:9001
```

Default credentials:

```text id="syv2ca"
Username: minioadmin
Password: minioadmin
```

---

## 3. Run the Benchmark

Execute the complete workflow:

```bash id="pwe6zv"
python src/bench.py
```

The benchmark automatically:

1. generates datasets;
2. uploads datasets to MinIO;
3. downloads datasets locally;
4. executes analytical reads;
5. stores benchmark results.

---

# Benchmark Modes

| Mode    | Description                        |
| ------- | ---------------------------------- |
| `v1`    | Read original schema only          |
| `v2`    | Read evolved schema only           |
| `mixed` | Read both schema versions together |

The `mixed` mode represents the main schema evolution scenario of the project.

---

# Dataset Sizes

| Label | Rows per Version |
| ----- | ---------------- |
| S     | 10,000           |
| M     | 100,000          |
| L     | 1,000,000        |

The `mixed` mode combines both dataset versions during query execution.

---

# Benchmark Metrics

The benchmark records:

* query runtime;
* upload throughput;
* download throughput;
* listing time;
* object count;
* rows loaded.

Benchmark results are stored in:

```text id="qshv9d"
results/results.csv
```

---

# Analysis Notebook

The notebook:

```text id="fcl6vo"
notebooks/analysis.ipynb
```

contains:

* dataset visualizations;
* schema evolution validation;
* runtime analysis;
* throughput analysis;
* benchmark interpretation.

---

# Results Summary

The benchmark confirms that:

* mixed-version reads remain compatible after schema evolution;
* the `mixed` mode introduces additional processing overhead;
* throughput scales consistently with dataset size;
* the workflow behaves consistently across all tested configurations.

---

# Demo Video

Google Drive link:

https://drive.google.com/file/d/1kSqRQ2ArWDe0jnBVXeeNsN9MzatAq53J/view?usp=drivesdk

---

# Main Result

The project successfully demonstrates a complete schema evolution workflow on S3-compatible object storage while preserving backward compatibility between dataset versions.
