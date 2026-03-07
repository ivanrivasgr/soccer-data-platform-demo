import pandas as pd
import sys
import os


def normalize_schema(df):
    # RAW schema -> normalized analytics schema
    if "timestamp" in df.columns:
        df = df.rename(columns={
            "timestamp": "ts",
            "x": "x_m",
            "y": "y_m",
            "speed": "speed_mps"
        })

    return df


def build_player_metrics(df):
    df = normalize_schema(df)

    df = df.sort_values(["player_id", "ts"])

    df["prev_x"] = df.groupby("player_id")["x_m"].shift()
    df["prev_y"] = df.groupby("player_id")["y_m"].shift()

    df["distance_m"] = (
        ((df["x_m"] - df["prev_x"]) ** 2 + (df["y_m"] - df["prev_y"]) ** 2) ** 0.5
    )
    df["distance_m"] = df["distance_m"].fillna(0)

    if "role" in df.columns:
        metrics = df.groupby(["player_id", "role"]).agg(
            total_distance_m=("distance_m", "sum"),
            avg_speed=("speed_mps", "mean"),
            max_speed=("speed_mps", "max")
        ).reset_index()
    else:
        metrics = df.groupby(["player_id"]).agg(
            total_distance_m=("distance_m", "sum"),
            avg_speed=("speed_mps", "mean"),
            max_speed=("speed_mps", "max")
        ).reset_index()

    return metrics


def main():
    input_file = sys.argv[1]

    df = pd.read_csv(input_file)
    metrics = build_player_metrics(df)

    os.makedirs("data/analytics", exist_ok=True)

    output_path = "data/analytics/player_session_metrics.csv"
    metrics.to_csv(output_path, index=False)

    print("Analytics metrics generated:")
    print(output_path)


if __name__ == "__main__":
    main()