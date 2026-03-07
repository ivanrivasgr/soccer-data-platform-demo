import pandas as pd
import numpy as np
import sys
import os


def transform_tracking_data(df):

    df = df.sort_values(["player_id","timestamp"])

    df["dx"] = df.groupby("player_id")["x"].diff()
    df["dy"] = df.groupby("player_id")["y"].diff()

    df["distance"] = np.sqrt(df["dx"]**2 + df["dy"]**2)

    df["distance"] = df["distance"].fillna(0)

    return df


if __name__ == "__main__":

    file_path = sys.argv[1]

    df = pd.read_csv(file_path)

    df = transform_tracking_data(df)

    os.makedirs("data/processed",exist_ok=True)

    df.to_parquet(
        "data/processed/tracking_clean.parquet",
        index=False
    )

    print("Transform completed")