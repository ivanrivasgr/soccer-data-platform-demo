import pandas as pd
import numpy as np
import sys
import os

file_path = sys.argv[1]

df = pd.read_csv(file_path)

df = df.sort_values(["player_id","timestamp"])

df["dx"] = df.groupby("player_id")["x"].diff()
df["dy"] = df.groupby("player_id")["y"].diff()

df["distance"] = np.sqrt(df["dx"]**2 + df["dy"]**2)

df["distance"] = df["distance"].fillna(0)

df["x_m"] = df["x"]
df["y_m"] = df["y"]

df["ts"] = df["timestamp"]

os.makedirs("data/processed",exist_ok=True)

df.to_parquet(
    "data/processed/tracking_clean.parquet",
    index=False
)

print("Transform completed")