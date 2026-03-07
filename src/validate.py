import pandas as pd
import sys


def validate_tracking_data(df):

    required_columns = [
        "session_id",
        "player_id",
        "timestamp",
        "x",
        "y",
        "speed"
    ]

    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")

    return True


if __name__ == "__main__":

    file_path = sys.argv[1]

    df = pd.read_csv(file_path)

    validate_tracking_data(df)

    print("Validation passed")