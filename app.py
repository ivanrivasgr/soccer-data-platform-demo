import streamlit as st
import subprocess
import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go
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
tracking_path = "data/processed/tracking_clean.parquet"

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

# -------------------------------------------------------
# TRACKING VISUALIZATION
# -------------------------------------------------------

if os.path.exists(tracking_path):

    tracking = pd.read_parquet(tracking_path)

    if {"x_m", "y_m", "player_id", "ts"}.issubset(tracking.columns):

        st.divider()
        st.header("Player Movement Visualization")

        col1, col2 = st.columns(2)

        player = col1.selectbox(
            "Select player",
            sorted(tracking["player_id"].unique())
        )

        view_mode = col2.selectbox(
            "Visualization type",
            ["Trajectory", "Heatmap", "Points"]
        )

        player_data = tracking[tracking["player_id"] == player].copy()
        player_data = player_data.sort_values("ts")

        # reduce frames for better visualization
        player_data = player_data.iloc[:1500]

        st.write(f"Showing movement data for player: **{player}**")

        # -------------------------------------------------------
        # TRAJECTORY VIEW
        # -------------------------------------------------------

        if view_mode == "Trajectory":

            fig = go.Figure()

            fig.add_trace(go.Scatter(
                x=player_data["x_m"],
                y=player_data["y_m"],
                mode="lines",
                line=dict(width=1.5, color="cyan"),
                name="Trajectory"
            ))

            pivot_data = player_data.iloc[::40]

            fig.add_trace(go.Scatter(
                x=pivot_data["x_m"],
                y=pivot_data["y_m"],
                mode="markers",
                marker=dict(size=6, color="yellow"),
                name="Key positions"
            ))

        # -------------------------------------------------------
        # HEATMAP VIEW
        # -------------------------------------------------------

        elif view_mode == "Heatmap":

            fig = px.density_heatmap(
                player_data,
                x="x_m",
                y="y_m",
                nbinsx=60,
                nbinsy=40,
                color_continuous_scale="YlOrRd"
            )

        # -------------------------------------------------------
        # POINT VIEW
        # -------------------------------------------------------

        else:

            fig = px.scatter(
                player_data,
                x="x_m",
                y="y_m",
                opacity=0.5
            )

        # -------------------------------------------------------
        # DRAW SOCCER FIELD
        # -------------------------------------------------------

        fig.update_layout(
            xaxis=dict(range=[0,105], title="Field X Position"),
            yaxis=dict(range=[0,68], title="Field Y Position"),
            showlegend=False
        )

        # field border
        fig.add_shape(type="rect", x0=0, y0=0, x1=105, y1=68, line=dict(color="white"))

        # midfield line
        fig.add_shape(type="line", x0=52.5, y0=0, x1=52.5, y1=68, line=dict(color="white"))

        # center circle
        fig.add_shape(
            type="circle",
            x0=52.5-9.15,
            y0=34-9.15,
            x1=52.5+9.15,
            y1=34+9.15,
            line=dict(color="white")
        )

        st.plotly_chart(fig, use_container_width=True)