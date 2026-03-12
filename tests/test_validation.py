import pandas as pd
from src.validate import validate_tracking_data

def test_validation_pass():

    df = pd.DataFrame({
        "session_id": ["S001","S001"],
        "player_id": ["P001","P001"],
        "timestamp": [1,2],
        "x": [10,11],
        "y": [20,21],
        "speed": [2.0,2.1]
    })

    result = validate_tracking_data(df)

    assert result is True