import os
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "uploaded_tracking.csv")
QUARANTINE_PATH = os.path.join(PROJECT_ROOT, "data", "quarantine", "tracking_quarantine.csv")
PROCESSED_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "tracking_clean.parquet")


def normalize_columns(df):

    rename_map = {
        "timestamp": "ts",
        "x": "x_m",
        "y": "y_m",
        "speed": "speed_mps"
    }

    df = df.rename(columns=rename_map)

    return df


def main():

    if not os.path.exists(RAW_PATH):
        print("No dataset found, skipping transform")
        exit(0)

    df = pd.read_csv(RAW_PATH)

    df = normalize_columns(df)

    os.makedirs(os.path.dirname(PROCESSED_PATH), exist_ok=True)

    if os.path.exists(QUARANTINE_PATH):
        quarantine = pd.read_csv(QUARANTINE_PATH)
        df = df.drop(quarantine.index)

    df["ts"] = pd.to_datetime(df["ts"])

    df = df.sort_values(["player_id", "session_id", "ts"])

    df["prev_x"] = df.groupby(["player_id", "session_id"])["x_m"].shift()
    df["prev_y"] = df.groupby(["player_id", "session_id"])["y_m"].shift()

    df["distance_m"] = (
        (df["x_m"] - df["prev_x"])**2 +
        (df["y_m"] - df["prev_y"])**2
    ) ** 0.5

    df["distance_m"] = df["distance_m"].fillna(0)

    df.to_parquet(PROCESSED_PATH, index=False)

    print("Clean dataset saved")
    print(PROCESSED_PATH)


if __name__ == "__main__":
    main()