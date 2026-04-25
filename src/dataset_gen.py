import os
import pandas as pd
import numpy as np


# Get project root directory (ccbd-project/)
BASE_DIR = os.path.dirname(os.path.dirname(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")


def generate_v1(n):
    return pd.DataFrame({
        "ts": pd.to_datetime("2026-01-01") + pd.to_timedelta(
            np.random.randint(0, 60 * 24 * 30, n), unit="m"
        ),
        "user_id": np.random.choice(
            np.arange(1, 1000),
            size=n,
            p=np.random.dirichlet(np.ones(999))
        ),
        "region": np.random.choice(
            ["EU", "US", "ASIA"],
            size=n,
            p=[0.5, 0.3, 0.2]
        ),
        "event_type": np.random.choice(
            ["view", "click", "purchase"],
            size=n,
            p=[0.7, 0.25, 0.05]
        ),
        "value": np.random.exponential(scale=30, size=n)
    })


def generate_v2(n):
    return pd.DataFrame({
        "ts": pd.to_datetime("2026-02-01") + pd.to_timedelta(
            np.random.randint(0, 60 * 24 * 30, n), unit="m"
        ),
        "user_id": np.random.choice(
            np.arange(1, 1000),
            size=n,
            p=np.random.dirichlet(np.ones(999))
        ),
        "region": np.random.choice(
            ["EU", "US", "ASIA"],
            size=n,
            p=[0.5, 0.3, 0.2]
        ),
        "event_type": np.random.choice(
            ["view", "click", "purchase"],
            size=n,
            p=[0.7, 0.25, 0.05]
        ),
        "value": np.random.exponential(scale=30, size=n),
        "device_type": np.random.choice(
            ["mobile", "desktop"],
            size=n,
            p=[0.7, 0.3]
        )
    })


def save_datasets(df_v1, df_v2):
    os.makedirs(os.path.join(DATA_DIR, "v1"), exist_ok=True)
    os.makedirs(os.path.join(DATA_DIR, "v2"), exist_ok=True)

    df_v1.to_parquet(os.path.join(DATA_DIR, "v1", "data.parquet"), index=False)
    df_v2.to_parquet(os.path.join(DATA_DIR, "v2", "data.parquet"), index=False)

    print("Datasets saved successfully in:")
    print(os.path.join(DATA_DIR, "v1", "data.parquet"))
    print(os.path.join(DATA_DIR, "v2", "data.parquet"))


def main():
    n = 10000  # small dataset for testing

    df_v1 = generate_v1(n)
    df_v2 = generate_v2(n)

    save_datasets(df_v1, df_v2)


if __name__ == "__main__":
    main()