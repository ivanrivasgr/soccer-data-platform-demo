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

    df = pd.read_csv(RAW_PATH)

    df = normalize_columns(df)

    quarantine = pd.read_csv(QUARANTINE_PATH)

    df_clean = df.drop(quarantine.index)

    df_clean["ts"] = pd.to_datetime(df_clean["ts"])

    df_clean = df_clean.sort_values(["player_id", "session_id", "ts"])

    df_clean["prev_x"] = df_clean.groupby(
        ["player_id", "session_id"]
    )["x_m"].shift()

    df_clean["prev_y"] = df_clean.groupby(
        ["player_id", "session_id"]
    )["y_m"].shift()

    df_clean["distance_m"] = (
        (df_clean["x_m"] - df_clean["prev_x"]) ** 2
        + (df_clean["y_m"] - df_clean["prev_y"]) ** 2
    ) ** 0.5

    df_clean["distance_m"] = df_clean["distance_m"].fillna(0)

    os.makedirs(os.path.dirname(PROCESSED_PATH), exist_ok=True)

    df_clean.to_parquet(PROCESSED_PATH, index=False)

    print("Clean dataset saved")
    print(PROCESSED_PATH)


if __name__ == "__main__":
    main()