import os
import random
from datetime import datetime, timedelta
import math
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")

random.seed(42)

ROLES = ["GK", "DEF", "MID", "FWD"]


def _rand_walk(prev_x, prev_y, step_mean=0.8, step_std=0.35):
    step = max(0.0, random.gauss(step_mean, step_std))
    angle = random.uniform(0, 2 * math.pi)
    x = prev_x + step * math.cos(angle)
    y = prev_y + step * math.sin(angle)
    return x, y


def _clip_field(x, y):
    x = min(max(x, 0.0), 105.0)
    y = min(max(y, 0.0), 68.0)
    return x, y


def generate_sessions(n_sessions=4):
    sessions = []
    base_date = datetime(2026, 3, 1, 16, 0, 0)

    for i in range(n_sessions):
        session_id = f"S{i+1:03d}"
        team_id = "TEAM_A" if i % 2 == 0 else "TEAM_B"
        session_type = "match" if i % 2 == 0 else "training"

        start_ts = base_date + timedelta(days=i * 2)
        end_ts = start_ts + timedelta(minutes=90 if session_type == "match" else 75)

        sessions.append({
            "team_id": team_id,
            "session_id": session_id,
            "session_type": session_type,
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat()
        })

    return pd.DataFrame(sessions)


def generate_tracking(sessions_df, n_players=18, hz=1):
    rows = []
    player_ids = [f"P{i+1:03d}" for i in range(n_players)]

    player_roles = {
        pid: random.choice(ROLES)
        for pid in player_ids
    }

    for _, s in sessions_df.iterrows():
        start_ts = datetime.fromisoformat(s["start_ts"])
        end_ts = datetime.fromisoformat(s["end_ts"])
        total_seconds = int((end_ts - start_ts).total_seconds())

        positions = {}
        for pid in player_ids:
            x0 = random.uniform(10, 95)
            y0 = random.uniform(5, 63)
            positions[pid] = (x0, y0)

        for t in range(0, total_seconds, hz):
            ts = start_ts + timedelta(seconds=t)

            for pid in player_ids:
                prev_x, prev_y = positions[pid]

                x, y = _rand_walk(prev_x, prev_y)
                x, y = _clip_field(x, y)
                positions[pid] = (x, y)

                dist = math.sqrt((x - prev_x) ** 2 + (y - prev_y) ** 2)
                speed = dist / hz if hz > 0 else 0.0
                speed = max(0.0, speed + random.gauss(0.0, 0.15))

                rows.append({
                    "session_id": s["session_id"],
                    "player_id": pid,
                    "role": player_roles[pid],
                    "timestamp": ts.isoformat(),
                    "x": round(x, 3),
                    "y": round(y, 3),
                    "speed": round(speed, 3)
                })

    return pd.DataFrame(rows)


def main():
    os.makedirs(RAW_DIR, exist_ok=True)

    sessions = generate_sessions()
    tracking = generate_tracking(sessions)

    sessions_path = os.path.join(RAW_DIR, "sessions.csv")
    tracking_path = os.path.join(RAW_DIR, "tracking_sample.csv")

    sessions.to_csv(sessions_path, index=False)
    tracking.to_csv(tracking_path, index=False)

    print("Generated sample data:")
    print(sessions_path)
    print(tracking_path)


if __name__ == "__main__":
    main()