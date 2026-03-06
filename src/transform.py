import os
import pandas as pd
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if len(sys.argv) > 1:
    input_file = sys.argv[1]
else:
    input_file = os.path.join(PROJECT_ROOT, "data", "raw", "tracking_sample.csv")

QUARANTINE_PATH = os.path.join(PROJECT_ROOT, "data", "quarantine", "tracking_quarantine.csv")
PROCESSED_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "tracking_clean.parquet")


def main():

    df = pd.read_csv(input_file)

    if os.path.exists(QUARANTINE_PATH):
        quarantine = pd.read_csv(QUARANTINE_PATH)
        df_clean = df.drop(quarantine.index)
    else:
        df_clean = df.copy()

    # convert timestamp
    df_clean["ts"] = pd.to_datetime(df_clean["ts"])

    # sort for trajectory calculations
    df_clean = df_clean.sort_values(["player_id", "session_id", "ts"])

    # compute distance between tracking points
    df_clean["prev_x"] = df_clean.groupby(["player_id", "session_id"])["x_m"].shift()
    df_clean["prev_y"] = df_clean.groupby(["player_id", "session_id"])["y_m"].shift()

    df_clean["distance_m"] = (
        (df_clean["x_m"] - df_clean["prev_x"]) ** 2 +
        (df_clean["y_m"] - df_clean["prev_y"]) ** 2
    ) ** 0.5

    df_clean["distance_m"] = df_clean["distance_m"].fillna(0)

    os.makedirs(os.path.dirname(PROCESSED_PATH), exist_ok=True)

    df_clean.to_parquet(PROCESSED_PATH, index=False)

    print("Clean dataset saved")
    print(PROCESSED_PATH)


if __name__ == "__main__":
    main()