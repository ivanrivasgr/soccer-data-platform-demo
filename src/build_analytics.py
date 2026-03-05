import os
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PROCESSED_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "tracking_clean.parquet")
OUTPUT_PATH = os.path.join(PROJECT_ROOT, "data", "analytics", "player_session_metrics.csv")


def main():

    df = pd.read_parquet(PROCESSED_PATH)

    metrics = df.groupby(["player_id", "session_id"]).agg(

        total_distance_m=("distance_m", "sum"),
        avg_speed=("speed_mps", "mean"),
        max_speed=("speed_mps", "max"),
        samples=("speed_mps", "count")

    ).reset_index()

    metrics.to_csv(OUTPUT_PATH, index=False)

    print("Analytics table created")
    print(metrics.head())


if __name__ == "__main__":
    main()