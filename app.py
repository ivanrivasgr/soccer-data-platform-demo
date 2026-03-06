import streamlit as st
import subprocess
import pandas as pd
import os
import plotly.express as px
import sys

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

tracking_path = os.path.join(
    PROJECT_ROOT,
    "data",
    "processed",
    "tracking_clean.parquet"
)

st.set_page_config(
    page_title="Soccer Tracking Analytics",
    layout="wide"
)

st.title("⚽ Soccer Tracking Data Pipeline")

st.markdown("""
Demo of a **sports analytics data pipeline** processing **GPS tracking data**.

### Pipeline stages

1️. Data validation  
2️. Data transformation  
3️. Player performance metrics  
4️. Analytics visualization
""")

st.divider()

uploaded_file = st.file_uploader(
    "Upload GPS Tracking CSV",
    type=["csv"]
)

if uploaded_file is not None:

    os.makedirs("data/raw", exist_ok=True)

    file_path = "data/raw/uploaded_tracking.csv"

    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    st.success("CSV uploaded successfully.")

    df = pd.read_csv(file_path)

    st.write("Rows Loaded:", len(df))

    if st.button("⚡ Run Full Pipeline"):

        with st.spinner("Running pipeline..."):

            st.info("Running validation...")
            subprocess.run([sys.executable, "src/validate.py", file_path], check=True)

            st.info("Running transformation...")
            subprocess.run([sys.executable, "src/transform.py", file_path], check=True)

            st.info("Building analytics metrics...")
            subprocess.run([sys.executable, "src/build_analytics.py", file_path], check=True)

        st.success("Pipeline completed!")

result_path = "data/analytics/player_session_metrics.csv"
tracking_path = "data/raw/tracking_clean.parquet"

if os.path.exists(result_path):

    df = pd.read_csv(result_path)

    st.divider()

    st.header("Player Performance Metrics")

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Total Distance (m)",
        round(df["total_distance_m"].sum(), 2)
    )

    col2.metric(
        "Average Speed",
        round(df["avg_speed"].mean(), 2)
    )

    col3.metric(
        "Max Speed",
        round(df["max_speed"].max(), 2)
    )

    st.divider()

    st.subheader("Analytics Table")

    st.dataframe(df, use_container_width=True)

    st.download_button(
        "Download Metrics CSV",
        df.to_csv(index=False),
        file_name="player_metrics.csv",
        mime="text/csv"
    )

    st.divider()

    st.header("Distance by Player")

    fig_distance = px.bar(
        df,
        x="player_id",
        y="total_distance_m",
        title="Total Distance per Player",
        color="player_id"
    )

    st.plotly_chart(fig_distance, use_container_width=True)

    st.header("Speed Distribution")

    fig_speed = px.histogram(
        df,
        x="avg_speed",
        nbins=20,
        title="Average Speed Distribution"
    )

    st.plotly_chart(fig_speed, use_container_width=True)

if os.path.exists(tracking_path):

    tracking = pd.read_parquet(tracking_path)

    if {"x_m", "y_m", "player_id", "ts"}.issubset(tracking.columns):

        st.divider()
        st.header("Player Movement Visualization")

        top_player = tracking["player_id"].value_counts().idxmax()

        player_data = tracking[tracking["player_id"] == top_player].copy()

        player_data = player_data.sort_values("ts")

        st.write(f"Showing movement trajectory for player: **{top_player}**")

        fig_tracking = px.line(
            player_data,
            x="x_m",
            y="y_m",
            title=f"Player Movement Path — {top_player}"
        )

        fig_tracking.update_layout(
            xaxis_title="Field X Position",
            yaxis_title="Field Y Position"
        )

        st.plotly_chart(fig_tracking, width="stretch")