import pandas as pd
import numpy as np

np.random.seed(42)

# -----------------------------
# TEAM STRUCTURE (4-3-3)
# -----------------------------

players = [
    ("P001","GK"),
    ("P002","DEF"),
    ("P003","DEF"),
    ("P004","DEF"),
    ("P005","DEF"),
    ("P006","MID"),
    ("P007","MID"),
    ("P008","MID"),
    ("P009","FWD"),
    ("P010","FWD"),
    ("P011","FWD")
]

rows_per_player = 900

records = []

# -----------------------------
# GENERATE TRACKING DATA
# -----------------------------

for player_id, role in players:

    x = np.zeros(rows_per_player)
    y = np.zeros(rows_per_player)

    # Starting zones depending on role

    if role == "GK":
        x[0] = np.random.uniform(2,12)
        y[0] = np.random.uniform(20,48)
        speed_mean = 1.5

    elif role == "DEF":
        x[0] = np.random.uniform(10,40)
        y[0] = np.random.uniform(10,58)
        speed_mean = 2.2

    elif role == "MID":
        x[0] = np.random.uniform(35,70)
        y[0] = np.random.uniform(5,63)
        speed_mean = 3.0

    else:  # FWD
        x[0] = np.random.uniform(60,100)
        y[0] = np.random.uniform(15,55)
        speed_mean = 3.6

    for i in range(1, rows_per_player):

        if role == "GK":
            step_x = np.random.normal(0,0.3)
            step_y = np.random.normal(0,0.4)

        elif role == "DEF":
            step_x = np.random.normal(0,0.6)
            step_y = np.random.normal(0,0.7)

        elif role == "MID":
            step_x = np.random.normal(0,0.9)
            step_y = np.random.normal(0,0.9)

        else:
            step_x = np.random.normal(0,1.1)
            step_y = np.random.normal(0,0.8)

        x[i] = np.clip(x[i-1] + step_x,0,105)
        y[i] = np.clip(y[i-1] + step_y,0,68)

        speed = abs(np.random.normal(speed_mean,0.6))

        records.append({
            "player_id": player_id,
            "role": role,
            "timestamp": i,
            "x": x[i],
            "y": y[i],
            "speed": speed
        })

# -----------------------------
# CREATE DATAFRAME
# -----------------------------

df = pd.DataFrame(records)

# -----------------------------
# EXPORT CSV
# -----------------------------

df.to_csv("tracking_50k.csv",index=False)

print("CSV generated: tracking_50k.csv")
print("Total rows:", len(df))