import os
import pandas as pd
import json

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RAW_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "tracking_sample.csv")
QUARANTINE_PATH = os.path.join(PROJECT_ROOT, "data", "quarantine", "tracking_quarantine.csv")
QUALITY_REPORT = os.path.join(PROJECT_ROOT, "monitoring", "quality_report.json")

SPEED_THRESHOLD = 11  # m/s unrealistic spike


def main():

    df = pd.read_csv(RAW_PATH)

    total_rows = len(df)

    # Missing timestamps
    missing_ts = df["ts"].isna().sum()

    # Duplicates
    duplicates = df.duplicated(subset=["player_id", "ts", "session_id"]).sum()

    # Speed spikes
    speed_spikes = (df["speed_mps"] > SPEED_THRESHOLD).sum()

    # Invalid player IDs
    invalid_players = df[~df["player_id"].str.startswith("P")].shape[0]

    # Quarantine rows
    quarantine_df = df[
        (df["ts"].isna())
        | (df["speed_mps"] > SPEED_THRESHOLD)
        | (~df["player_id"].str.startswith("P"))
    ]

    quarantine_df.to_csv(QUARANTINE_PATH, index=False)

    valid_df = df.drop(quarantine_df.index)

    report = {
        "total_rows": int(total_rows),
        "missing_timestamps": int(missing_ts),
        "duplicate_rows": int(duplicates),
        "speed_spikes": int(speed_spikes),
        "invalid_player_ids": int(invalid_players),
        "rows_quarantined": int(len(quarantine_df)),
        "rows_valid": int(len(valid_df)),
    }

    with open(QUALITY_REPORT, "w") as f:
        json.dump(report, f, indent=2)

    print("Validation complete")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()