import pandas as pd
from src.build_analytics import build_player_metrics

def test_metrics_output():

    df = pd.DataFrame({
        "player_id":["P001","P001"],
        "role":["MID","MID"],
        "timestamp":[1,2],
        "x":[10,11],
        "y":[20,21],
        "speed":[3.0,3.5]
    })

    metrics = build_player_metrics(df)

    assert "total_distance_m" in metrics.columns
    assert "avg_speed" in metrics.columns