import pandas as pd
from src.transform import transform_tracking_data

def test_transform_creates_distance():

    df = pd.DataFrame({
        "player_id":["P001","P001"],
        "timestamp":[1,2],
        "x":[10,11],
        "y":[20,22],
        "speed":[2,2]
    })

    transformed = transform_tracking_data(df)

    assert "distance" in transformed.columns