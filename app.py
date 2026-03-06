import streamlit as st
import pandas as pd
import subprocess
import os

st.set_page_config(page_title="Soccer Tracking Demo", layout="wide")

st.title("⚽ Soccer Tracking Data Pipeline")

st.write("""
Demo of a sports analytics pipeline processing GPS tracking data.

Pipeline stages:
1. Data validation
2. Transformation
3. Player performance metrics
""")

uploaded_file = st.file_uploader("Upload GPS Tracking CSV", type=["csv"])

if uploaded_file:

    os.makedirs("data/raw", exist_ok=True)

    file_path = "data/raw/uploaded_tracking.csv"

    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    st.success("File uploaded")

    if st.button("Run Pipeline"):

        subprocess.run(["python", "src/validate.py"])
        subprocess.run(["python", "src/transform.py"])
        subprocess.run(["python", "src/build_analytics.py"])

        result_path = "data/analytics/player_session_metrics.csv"

        if os.path.exists(result_path):

            df = pd.read_csv(result_path)

            st.success("Pipeline executed successfully")

            total_distance = int(df["total_distance_m"].sum())
            avg_speed = round(df["avg_speed"].mean(),2)
            max_speed = round(df["max_speed"].max(),2)

            col1, col2, col3 = st.columns(3)

            col1.metric("Total Distance (m)", total_distance)
            col2.metric("Average Speed (m/s)", avg_speed)
            col3.metric("Max Speed (m/s)", max_speed)

            st.subheader("Distance by Player")
            st.bar_chart(df.set_index("player_id")["total_distance_m"])

            st.subheader("Player Metrics Table")
            st.dataframe(df)

        else:

            st.error("Pipeline output not found")