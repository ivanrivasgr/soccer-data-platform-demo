import os
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

UPLOADED_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "uploaded_tracking.csv")
SAMPLE_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "tracking_sample.csv")
QUARANTINE_PATH = os.path.join(PROJECT_ROOT, "data", "quarantine", "tracking_quarantine.csv")
PROCESSED_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "tracking_clean.parquet")


def resolve_input_path() -> str | None:
    if os.path.exists(UPLOADED_PATH):
        return UPLOADED_PATH
    if os.path.exists(SAMPLE_PATH):
        return SAMPLE_PATH
    return None


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "timestamp": "ts",
        "x": "x_m",
        "y": "y_m",
        "speed": "speed_mps",
    }
    return df.rename(columns=rename_map)


def main() -> None:
    input_path = resolve_input_path()

    if input_path is None:
        print("No dataset found, skipping transform")
        raise SystemExit(0)

    df = pd.read_csv(input_path)
    df = normalize_columns(df).copy()

    required_cols = ["player_id", "session_id", "ts", "x_m", "y_m", "speed_mps"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    os.makedirs(os.path.dirname(PROCESSED_PATH), exist_ok=True)

    # recreate stable row ids to match validate.py
    df["_row_id"] = range(len(df))

    if os.path.exists(QUARANTINE_PATH):
        quarantine = pd.read_csv(QUARANTINE_PATH)
        if "_row_id" in quarantine.columns:
            quarantined_ids = set(quarantine["_row_id"].tolist())
            df = df[~df["_row_id"].isin(quarantined_ids)].copy()

    df["ts"] = pd.to_datetime(df["ts"])
    df = df.sort_values(["player_id", "session_id", "ts"]).copy()

    df["prev_x"] = df.groupby(["player_id", "session_id"])["x_m"].shift()
    df["prev_y"] = df.groupby(["player_id", "session_id"])["y_m"].shift()

    df["distance_m"] = (
        (df["x_m"] - df["prev_x"]) ** 2
        + (df["y_m"] - df["prev_y"]) ** 2
    ) ** 0.5

    df["distance_m"] = df["distance_m"].fillna(0)

    # helper column not needed downstream
    df = df.drop(columns=["_row_id"], errors="ignore")

    df.to_parquet(PROCESSED_PATH, index=False)

    print("Clean dataset saved")
    print(PROCESSED_PATH)


if __name__ == "__main__":
    main()