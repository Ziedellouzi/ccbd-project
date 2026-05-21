Zied Ellouzi, Nassim Maliki

---

# Schema Evolution on S3-Compatible Object Storage

## Project Overview

This project implements a schema evolution workflow on S3-compatible object storage.

The implementation combines:

- Parquet datasets;
- versioned dataset layouts;
- MinIO object storage;
- schema contracts;
- backward-compatible reads.

The objective was to simulate a small cloud-style data lake architecture where multiple dataset versions remain queryable through a unified schema.

A benchmark workflow was also implemented to evaluate:

- mixed-version reads;
- query runtime;
- storage behavior;
- transfer performance.

---

# Project Structure

```text
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
├── compose.yaml
├── requirements.txt
└── README.md
```

---

# Technologies Used

The project uses:

- Python
- Pandas
- NumPy
- PyArrow
- MinIO
- Docker
- Boto3

---

# Schema Evolution Scenario

The project simulates two dataset versions.

## Version 1 (`v1`)

The baseline schema contains:

- ts
- user_id
- region
- event_type
- value

## Version 2 (`v2`)

The second version introduces one additional nullable column:

- device_type

The objective is to maintain compatibility between both versions during reads.

The reader therefore aligns all datasets to a unified schema before validation and querying.

Older datasets automatically receive null values for missing optional columns.

---

# Object Storage Architecture

The project uses MinIO as a local S3-compatible object storage system.

The bucket used in the project is:

```text
ccbd-data
```

The object-storage structure is organized as follows:

```text
ccbd-data/
├── curated/
│   └── events/
│       ├── v1/
│       └── v2/
└── published/
    └── events/
        └── schema.json
```

The `curated/` prefix contains the Parquet datasets.

The `published/` prefix contains the schema contract used during reads.

---

# Installation

## Install dependencies

```bash
pip install -r requirements.txt
```

## Start MinIO

```bash
docker compose up -d
```

MinIO API:

```text
http://localhost:9000
```

MinIO Console:

```text
http://localhost:9001
```

Default credentials:

```text
username: minioadmin
password: minioadmin
```

---

# Running the Project

## Generate datasets

```bash
python src/dataset_gen.py --rows 10000
```

This generates:

- `v1`
- `v2`
- `schema.json`

---

## Upload datasets to MinIO

```bash
python src/upload.py
```

---

## Download datasets from MinIO

```bash
python src/download.py
```

---

## Read datasets from object storage

### Read `v1`

```bash
python src/reader_s3.py --mode v1
```

### Read `v2`

```bash
python src/reader_s3.py --mode v2
```

### Read mixed versions

```bash
python src/reader_s3.py --mode mixed
```

The `mixed` mode combines both dataset versions into a unified table before executing the analytics query.

---

# Benchmark Workflow

The benchmark workflow is implemented in:

```text
src/bench.py
```

The benchmark automatically:

1. generates datasets;
2. uploads datasets to MinIO;
3. downloads datasets;
4. reads datasets from object storage;
5. records benchmark metrics.

Run the benchmark with:

```bash
python src/bench.py
```

The benchmark results are written to:

```text
results/results.csv
```

---

# Dataset Sizes

The benchmark uses three dataset sizes.

| Label | Rows per Version |
|---|---|
| S | 10,000 |
| M | 100,000 |
| L | 1,000,000 |

---

# Benchmark Metrics

The benchmark records:

- upload throughput;
- download throughput;
- listing time;
- object count;
- total stored bytes;
- query runtime;
- rows loaded.

The benchmark compares these metrics across:

- `v1`
- `v2`
- `mixed`

for all dataset sizes.

The main objective of the benchmark is evaluating the additional processing cost introduced by mixed-version reads.

---

# Analysis Notebook

The notebook:

```text
notebooks/analysis.ipynb
```

analyzes the benchmark results stored in:

```text
results/results.csv
```

The analysis focuses on:

- validation of mixed-version reads;
- query runtime;
- transfer performance;
- storage behavior;
- benchmark interpretation.

The notebook also discusses the limitations of the current experiment and the trade-offs introduced by mixed-version reads.

---

# Reproducibility

The complete benchmark workflow can be reproduced with:

```bash
docker compose up -d
python src/bench.py
```

This command regenerates datasets, uploads them to MinIO, executes the benchmark, and recreates `results/results.csv`.

---

# Limitations

This project runs locally using Docker and MinIO.

Therefore, the benchmark results should not be interpreted as production cloud performance measurements.

Several limitations remain:

- only one benchmark run was executed per configuration;
- the benchmark uses local storage;
- network latency is minimal;
- only one schema evolution scenario was tested.

The current implementation focuses on adding one nullable column (`device_type`).

More complex schema changes would require additional compatibility logic.

---

# Conclusion

This project demonstrates a complete schema evolution workflow on S3-compatible object storage.

The implementation supports:

- versioned Parquet datasets;
- schema contracts;
- upload and download through the S3 API;
- backward-compatible reads;
- reproducible benchmark execution.

The benchmark confirms that mixed-version reads remain functional across all tested dataset sizes while introducing additional processing overhead compared to single-version reads.
