import os
import random
from datetime import datetime, timedelta
import math
import pandas as pd

# 
# Synthetic tracking data generator
# - realistic-ish soccer tracking rows
# - includes anomalies: duplicates, missing timestamps, speed spikes
# 

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")

random.seed(42)

def _rand_walk(prev_x, prev_y, step_mean=0.8, step_std=0.35):
    """Small random movement step in meters."""
    step = max(0.0, random.gauss(step_mean, step_std))
    angle = random.uniform(0, 2 * math.pi)
    x = prev_x + step * math.cos(angle)
    y = prev_y + step * math.sin(angle)
    return x, y

def _clip_field(x, y):
    """Clip into a standard soccer pitch (approx 105x68)."""
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
        start_ts = base_date + timedelta(days=i*2)
        end_ts = start_ts + timedelta(minutes=90 if session_type == "match" else 75)
        sessions.append({
            "session_id": session_id,
            "team_id": team_id,
            "session_type": session_type,
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat()
        })
    return pd.DataFrame(sessions)

def generate_tracking(sessions_df, n_players=18, hz=1):
    """
    Generate event-level tracking rows.
    - hz=1 => 1 row per second per player
    """
    rows = []
    player_ids = [f"P{i+1:03d}" for i in range(n_players)]

    for _, s in sessions_df.iterrows():
        start_ts = datetime.fromisoformat(s["start_ts"])
        end_ts = datetime.fromisoformat(s["end_ts"])
        total_seconds = int((end_ts - start_ts).total_seconds())

        # initialize player positions
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

                # speed in m/s approximated from step size; introduce noise
                dist = math.sqrt((x - prev_x) ** 2 + (y - prev_y) ** 2)
                speed_mps = dist / hz if hz > 0 else 0.0
                speed_mps = max(0.0, speed_mps + random.gauss(0.0, 0.15))

                # heart rate optional-ish
                hr = int(min(max(random.gauss(150, 12), 90), 205))

                rows.append({
                    "team_id": s["team_id"],
                    "session_id": s["session_id"],
                    "session_type": s["session_type"],
                    "player_id": pid,
                    "ts": ts.isoformat(),
                    "x_m": round(x, 3),
                    "y_m": round(y, 3),
                    "speed_mps": round(speed_mps, 3),
                    "hr_bpm": hr
                })

    df = pd.DataFrame(rows)

    # 
    # Inject anomalies
    # 

    # 1) Duplicates: duplicate ~0.5% of rows
    n_dup = max(1, int(0.005 * len(df)))
    dup_rows = df.sample(n=n_dup, random_state=7)
    df = pd.concat([df, dup_rows], ignore_index=True)

    # 2) Missing timestamps: set ts null for ~0.2% rows
    n_missing = max(1, int(0.002 * len(df)))
    missing_idx = df.sample(n=n_missing, random_state=11).index
    df.loc[missing_idx, "ts"] = None

    # 3) Speed spikes: for ~0.2% rows set speed to unrealistic values
    n_spikes = max(1, int(0.002 * len(df)))
    spike_idx = df.sample(n=n_spikes, random_state=13).index
    df.loc[spike_idx, "speed_mps"] = df.loc[spike_idx, "speed_mps"] + 12.0  # e.g., jump by +12 m/s

    # 4) Random player_id inconsistency: ~0.1% rows
    n_bad_pid = max(1, int(0.001 * len(df)))
    bad_idx = df.sample(n=n_bad_pid, random_state=17).index
    df.loc[bad_idx, "player_id"] = df.loc[bad_idx, "player_id"].apply(lambda p: p.replace("P", "PLAYER_"))

    # Shuffle rows a bit
    df = df.sample(frac=1.0, random_state=21).reset_index(drop=True)
    return df

def main():
    os.makedirs(RAW_DIR, exist_ok=True)

    sessions = generate_sessions(n_sessions=4)
    tracking = generate_tracking(sessions, n_players=18, hz=1)

    sessions_path = os.path.join(RAW_DIR, "sessions.csv")
    tracking_path = os.path.join(RAW_DIR, "tracking_sample.csv")

    sessions.to_csv(sessions_path, index=False)
    tracking.to_csv(tracking_path, index=False)

    print(" Generated sample data:")
    print(f"- {sessions_path}  (rows={len(sessions)})")
    print(f"- {tracking_path}  (rows={len(tracking)})")
    print("\nSample tracking rows:")
    print(tracking.head(5).to_string(index=False))

if __name__ == "__main__":
    main()