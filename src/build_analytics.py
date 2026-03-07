import pandas as pd
import sys
import os

file_path = sys.argv[1]

df = pd.read_csv(file_path)

df = df.sort_values(["player_id","timestamp"])

df["dx"] = df.groupby("player_id")["x"].diff()
df["dy"] = df.groupby("player_id")["y"].diff()

df["distance"] = (df["dx"]**2 + df["dy"]**2)**0.5

df["distance"] = df["distance"].fillna(0)

metrics = df.groupby(["player_id","role"]).agg(
    total_distance_m=("distance","sum"),
    avg_speed=("speed","mean"),
    max_speed=("speed","max")
).reset_index()

os.makedirs("data/analytics",exist_ok=True)

metrics.to_csv(
    "data/analytics/player_session_metrics.csv",
    index=False
)

print("Analytics metrics generated")