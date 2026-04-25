import pandas as pd


V1_PATH = "../data/v1/data.parquet"
V2_PATH = "../data/v2/data.parquet"

EXPECTED_COLUMNS = ["ts", "user_id", "region", "event_type", "value", "device_type"]


def load_v1():
    """Load dataset version 1."""
    return pd.read_parquet(V1_PATH)


def load_v2():
    """Load dataset version 2."""
    return pd.read_parquet(V2_PATH)


def align_schema(df):
    """
    Align dataset schema with expected schema.
    Adds missing columns and reorders columns.
    """
    for col in EXPECTED_COLUMNS:
        if col not in df.columns:
            df[col] = None

    return df[EXPECTED_COLUMNS]


def load_all():
    """
    Load and combine v1 and v2 with unified schema.
    """
    df_v1 = load_v1()
    df_v2 = load_v2()

    df_v1 = align_schema(df_v1)
    df_v2 = align_schema(df_v2)

    df_all = pd.concat([df_v1, df_v2], ignore_index=True)

    return df_all


if __name__ == "__main__":
    df_v1 = load_v1()
    df_v2 = load_v2()
    df_all = load_all()

    print("v1 shape:", df_v1.shape)
    print("v2 shape:", df_v2.shape)
    print("combined shape:", df_all.shape)

    print("\nv1 columns:", list(df_v1.columns))
    print("v2 columns:", list(df_v2.columns))
    print("combined columns:", list(df_all.columns))
