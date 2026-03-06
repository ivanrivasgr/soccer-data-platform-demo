import pandas as pd
import numpy as np

np.random.seed(42)

players = [f"P{str(i).zfill(3)}" for i in range(1, 19)]  # 18 players
sessions = ["S001", "S002", "S003", "S004"]

rows = 50000

data = {
    "player_id": np.random.choice(players, rows),
    "session_id": np.random.choice(sessions, rows),
    "timestamp": np.arange(rows),
    "x": np.random.uniform(0, 105, rows),   # soccer field length
    "y": np.random.uniform(0, 68, rows),    # soccer field width
    "speed": np.abs(np.random.normal(3, 1.2, rows))  # realistic running speeds
}

df = pd.DataFrame(data)

df.to_csv("tracking_50k.csv", index=False)

print("CSV generate: tracking_50k.csv")