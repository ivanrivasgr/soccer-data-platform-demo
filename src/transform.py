import pandas as pd
import numpy as np
import sys
import os


def transform_tracking_data(df):
    # Support RAW schema used by tests and uploads
    if "timestamp" in df.columns:
        df = df.sort_values(["player_id", "timestamp"])

        df["dx"] = df.groupby("player_id")["x"].diff()
        df["dy"] = df.groupby("player_id")["y"].diff()

        df["distance"] = np.sqrt(df["dx"]**2 + df["dy"]**2)
        df["distance"] = df["distance"].fillna(0)

        # Standardized columns for downstream app / parquet output
        df["ts"] = df["timestamp"]
        df["x_m"] = df["x"]
        df["y_m"] = df["y"]
        df["speed_mps"] = df["speed"]

        return df

    # Support already-transformed schema if ever passed in
    if {"ts", "x_m", "y_m", "speed_mps"}.issubset(df.columns):
        df = df.sort_values(["player_id", "ts"])

        df["dx"] = df.groupby("player_id")["x_m"].diff()
        df["dy"] = df.groupby("player_id")["y_m"].diff()

        df["distance"] = np.sqrt(df["dx"]**2 + df["dy"]**2)
        df["distance"] = df["distance"].fillna(0)

        return df

    raise ValueError("Unsupported schema for transform_tracking_data")


if __name__ == "__main__":
    file_path = sys.argv[1]

    df = pd.read_csv(file_path)
    df = transform_tracking_data(df)

    os.makedirs("data/processed", exist_ok=True)

    df.to_parquet(
        "data/processed/tracking_clean.parquet",
        index=False
    )

    print("Transform completed")