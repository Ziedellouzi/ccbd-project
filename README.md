Authors: Zied Ellouzi, Nassim Maliki

Project Overview

The objective of this project was implementing and benchmarking a simple schema evolution workflow on S3-compatible object storage using MinIO and Parquet datasets.

The project simulates a realistic situation where a dataset evolves over time while older data still needs to remain queryable.

To reproduce this scenario, two dataset versions were generated:

v1, representing the original schema;
v2, representing the evolved schema.

The implementation focuses on validating that both schema versions can still be queried together after schema evolution.

The project combines:

versioned Parquet datasets;
schema contracts;
MinIO object storage;
backward-compatible reads;
automated benchmark execution.

The implementation uses:

Python
PyArrow
Pandas
Boto3
Docker
MinIO
Schema Evolution Scenario

The project implements two dataset versions.

v1 — Original Schema

The first version contains the following columns:

ts
user_id
region
event_type
value
v2 — Evolved Schema

The second version extends the schema by introducing a new nullable column:

device_type

The final schema becomes:

ts
user_id
region
event_type
value
device_type

The main challenge of the project is ensuring that older datasets (v1) remain compatible after the schema evolves.

The reader therefore aligns all datasets to a unified schema before querying.

Synthetic Dataset Characteristics

The synthetic datasets were intentionally generated using non-uniform distributions in order to simulate a more realistic analytical workload.

For example:

most generated events are view events;
click events are less frequent;
purchase events remain relatively rare.

This creates a simple funnel-like event structure.

The evolved device_type column is also dominated by mobile traffic.

These distributions are visualized in the analysis notebook.

Project Structure
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
MinIO Object Storage Layout

The datasets are stored inside the ccbd-data bucket using separate prefixes for each schema version.

ccbd-data/
├── curated/
│   └── events/
│       ├── v1/
│       └── v2/
└── published/
    └── events/
        └── schema.json

The curated/ directory stores the Parquet datasets, while published/ contains the schema contract used during validation.

Installation

Install Python dependencies:

pip install -r requirements.txt
Start MinIO

The project uses MinIO as a local S3-compatible object-storage server.

Start MinIO using Docker Compose:

docker compose up -d

MinIO interface:

http://localhost:9001

Default credentials:

Username: minioadmin
Password: minioadmin
Running the Benchmark

Once MinIO is running, the complete workflow can be executed using:

python src/bench.py

The benchmark automatically performs the following steps:

generate synthetic datasets;
clear old objects from MinIO;
upload datasets to object storage;
download datasets locally;
execute analytical reads;
store benchmark results.
Benchmark Modes

The benchmark evaluates three reading modes:

Mode	Description
v1	Read original schema only
v2	Read evolved schema only
mixed	Read both schema versions together

The mixed mode represents the central schema evolution scenario of the project.

Dataset Sizes

The benchmark was executed using three dataset sizes:

Label	Rows per Version
S	10,000
M	100,000
L	1,000,000

The mixed mode combines both dataset versions during query execution.

Benchmark Metrics

The benchmark records several metrics during execution:

query runtime;
upload throughput;
download throughput;
listing time;
object count;
rows loaded.

The results are stored in:

results/results.csv
Analysis Notebook

The notebook:

notebooks/analysis.ipynb

contains:

benchmark analysis;
schema evolution validation;
dataset visualizations;
runtime analysis;
throughput analysis;
interpretation of benchmark results.

The notebook also explains how the benchmark behaves when multiple schema versions are queried together.

Demo Video

Google Drive link:

https://drive.google.com/file/d/1kSqRQ2ArWDe0jnBVXeeNsN9MzatAq53J/view?usp=drivesdk

Main Result

The experiment validates that schema evolution can be implemented on S3-compatible object storage while preserving backward compatibility between dataset versions.

The benchmark also shows that mixed-version reads introduce additional processing overhead, especially for larger datasets, while remaining fully functional across all tested configurations.