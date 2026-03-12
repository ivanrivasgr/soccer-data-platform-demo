import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import subprocess
import sys

# ── PAGE CONFIG ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Soccer Tracking Analytics",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── CUSTOM CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #0f2027, #203a43, #2c5364);
        border: 1px solid #2c5364;
        border-radius: 10px;
        padding: 16px 20px;
        text-align: center;
    }
    .metric-label { color: #7fb3c8; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 1px; }
    .metric-value { color: #ffffff; font-size: 26px; font-weight: 700; margin-top: 4px; }
    .metric-sub   { color: #7fb3c8; font-size: 11px; margin-top: 2px; }
    .section-header { color: #e8f4f8; font-size: 16px; font-weight: 600; margin: 8px 0 4px 0; }
    [data-testid="stMetricValue"] { font-size: 24px; }
</style>
""", unsafe_allow_html=True)

# ── PATHS ─────────────────────────────────────────────────────────────────────
ROOT          = os.path.dirname(os.path.abspath(__file__))
METRICS_PATH  = os.path.join(ROOT, "data", "analytics", "player_session_metrics.csv")
TRACKING_PATH = os.path.join(ROOT, "data", "processed", "tracking_clean.parquet")
RAW_PATH      = os.path.join(ROOT, "data", "raw", "tracking_sample.csv")

# ── LOAD DATA ─────────────────────────────────────────────────────────────────
@st.cache_data
def load_metrics():
    if os.path.exists(METRICS_PATH):
        df = pd.read_csv(METRICS_PATH)
        df["total_distance_m"] = df["total_distance_m"].round(1)
        df["avg_speed"]        = df["avg_speed"].round(2)
        df["max_speed"]        = df["max_speed"].round(2)
        return df
    return pd.DataFrame()

@st.cache_data
def load_tracking():
    if os.path.exists(TRACKING_PATH):
        return pd.read_parquet(TRACKING_PATH)
    return pd.DataFrame()

@st.cache_data
def load_raw():
    if os.path.exists(RAW_PATH):
        return pd.read_csv(RAW_PATH)
    return pd.DataFrame()

metrics  = load_metrics()
tracking = load_tracking()

# ── SIDEBAR ───────────────────────────────────────────────────────────────────
st.sidebar.markdown("## ⚽ Soccer Tracking")
st.sidebar.markdown("GPS-based player performance analytics platform")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigate",
    ["Dashboard", "Player Movement", "Pipeline", "Data Quality"]
)

st.sidebar.markdown("---")
st.sidebar.markdown("**Data loaded**")
if not metrics.empty:
    st.sidebar.success(f"✅ {len(metrics)} players · {len(metrics['role'].unique())} positions")
else:
    st.sidebar.warning("No metrics found")

# ── ROLE COLORS ───────────────────────────────────────────────────────────────
ROLE_COLORS = {"GK": "#f59e0b", "DEF": "#3b82f6", "MID": "#10b981", "FWD": "#ef4444"}

def role_color(role):
    return ROLE_COLORS.get(role, "#6b7280")

# ─────────────────────────────────────────────────────────────────────────────
# PAGE: DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────
if page == "Dashboard":
    st.title("⚽ Soccer Tracking Analytics")
    st.markdown("GPS player tracking pipeline — validation · transformation · performance metrics")
    st.markdown("---")

    if metrics.empty:
        st.warning("No analytics data found. Run the pipeline first via the Pipeline tab.")
        st.stop()

    # ── KPI ROW ───────────────────────────────────────────────────────────────
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Players Tracked",  len(metrics))
    col2.metric("Avg Distance",     f"{metrics['total_distance_m'].mean():,.0f} m")
    col3.metric("Peak Speed",       f"{metrics['max_speed'].max():.2f} m/s")
    col4.metric("Positions",        len(metrics["role"].unique()))

    st.markdown("---")

    # ── DISTANCE BY PLAYER ────────────────────────────────────────────────────
    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<p class="section-header">Total Distance by Player</p>', unsafe_allow_html=True)
        df_sorted = metrics.sort_values("total_distance_m", ascending=False)
        fig = px.bar(
            df_sorted, x="player_id", y="total_distance_m",
            color="role",
            color_discrete_map=ROLE_COLORS,
            labels={"total_distance_m": "Distance (m)", "player_id": "Player", "role": "Position"},
            template="plotly_dark"
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            legend_title_text="Position", height=340,
            margin=dict(l=0, r=0, t=10, b=0)
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown('<p class="section-header">Distance by Position</p>', unsafe_allow_html=True)
        role_agg = metrics.groupby("role").agg(
            avg_distance=("total_distance_m","mean"),
            avg_speed=("avg_speed","mean"),
            players=("player_id","count")
        ).reset_index()
        fig = px.bar(
            role_agg, x="role", y="avg_distance",
            color="role", color_discrete_map=ROLE_COLORS,
            labels={"avg_distance": "Avg Distance (m)", "role": "Position"},
            template="plotly_dark", text="players"
        )
        fig.update_traces(texttemplate="%{text} players", textposition="outside")
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            showlegend=False, height=340,
            margin=dict(l=0, r=0, t=10, b=0)
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── SPEED ANALYSIS ────────────────────────────────────────────────────────
    col1, col2 = st.columns(2)
    with col1:
        st.markdown('<p class="section-header">Max Speed by Player</p>', unsafe_allow_html=True)
        df_speed = metrics.sort_values("max_speed", ascending=False)
        fig = px.bar(
            df_speed, x="player_id", y="max_speed",
            color="role", color_discrete_map=ROLE_COLORS,
            labels={"max_speed": "Max Speed (m/s)", "player_id": "Player"},
            template="plotly_dark"
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            showlegend=False, height=300,
            margin=dict(l=0, r=0, t=10, b=0)
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown('<p class="section-header">Speed vs Distance — by Position</p>', unsafe_allow_html=True)
        fig = px.scatter(
            metrics, x="total_distance_m", y="avg_speed",
            color="role", color_discrete_map=ROLE_COLORS,
            hover_name="player_id", size="max_speed",
            labels={"total_distance_m": "Total Distance (m)", "avg_speed": "Avg Speed (m/s)"},
            template="plotly_dark"
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            height=300, margin=dict(l=0, r=0, t=10, b=0)
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── TABLE ─────────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown('<p class="section-header">Full Player Metrics Table</p>', unsafe_allow_html=True)

    col1, col2 = st.columns([1, 3])
    role_filter = col1.multiselect("Filter by position", options=sorted(metrics["role"].unique()), default=sorted(metrics["role"].unique()))
    filtered = metrics[metrics["role"].isin(role_filter)].sort_values("total_distance_m", ascending=False)
    st.dataframe(filtered, use_container_width=True, hide_index=True)

    col1, col2 = st.columns([1, 5])
    col1.download_button(
        "⬇ Download CSV", filtered.to_csv(index=False),
        file_name="player_metrics.csv", mime="text/csv"
    )

# ─────────────────────────────────────────────────────────────────────────────
# PAGE: PLAYER MOVEMENT
# ─────────────────────────────────────────────────────────────────────────────
elif page == "Player Movement":
    st.title("📍 Player Movement Visualization")
    st.markdown("GPS trajectory analysis on a full-pitch canvas.")
    st.markdown("---")
 
    if tracking.empty:
        st.warning("Tracking data not found at `data/processed/tracking_clean.parquet`.")
        st.stop()
 
    required = {"x_m", "y_m", "player_id"}
    if not required.issubset(tracking.columns):
        st.error(f"Tracking file missing columns: {required - set(tracking.columns)}")
        st.stop()
 
    time_col  = "ts" if "ts" in tracking.columns else "timestamp" if "timestamp" in tracking.columns else None
    speed_col = "speed_mps" if "speed_mps" in tracking.columns else "speed" if "speed" in tracking.columns else None
 
    col1, col2, col3 = st.columns(3)
    player = col1.selectbox("Player", sorted(tracking["player_id"].unique()))
    view   = col2.selectbox("View type", ["Trajectory", "Heatmap", "Points"])
    n_pts  = col3.slider("Data points", 500, 3000, 1500, step=250)
 
    player_data = tracking[tracking["player_id"] == player].copy()
    if time_col:
        player_data = player_data.sort_values(time_col)
    player_data = player_data.iloc[:n_pts].copy()
 
    st.markdown(f"**{player}** — {len(player_data):,} GPS frames")
 
    # ── DRAW FIELD ────────────────────────────────────────────────────────────
    def draw_field():
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=[0, 105], y=[0, 68],
            mode="markers", marker=dict(opacity=0),
            showlegend=False, hoverinfo="skip"
        ))
        field_color = "#1a4a1a"
 
        def s(type, x0, y0, x1, y1, fill=None, lcolor="white", lwidth=1.5):
            kw = dict(type=type, x0=x0, y0=y0, x1=x1, y1=y1,
                      line=dict(color=lcolor, width=lwidth), layer="below")
            if fill:
                kw["fillcolor"] = fill
            fig.add_shape(**kw)
 
        s("rect", 0, 0, 105, 68, fill=field_color, lwidth=2)
        fig.add_shape(type="line", x0=52.5, y0=0, x1=52.5, y1=68,
                      line=dict(color="white", width=1.5), layer="below")
        s("circle", 52.5-9.15, 34-9.15, 52.5+9.15, 34+9.15)
        s("circle", 52.3, 33.8, 52.7, 34.2, fill="white")
        for x0, x1 in [(0, 16.5), (88.5, 105)]:
            s("rect", x0, 13.84, x1, 54.16)
        for x0, x1 in [(0, 5.5), (99.5, 105)]:
            s("rect", x0, 24.84, x1, 43.16, lwidth=1)
        for x0, x1 in [(-2, 0), (105, 107)]:
            s("rect", x0, 30.34, x1, 37.66, lwidth=2)
 
        
        for px_coord in [11, 94]:
            s("circle", px_coord - 0.3, 33.7, px_coord + 0.3, 34.3, fill="white")
 
        
        import numpy as np
        for cx in [11, 94]:
            theta = (np.linspace(np.pi * 0.28, np.pi * 0.72, 50)
                     if cx < 52.5 else
                     np.linspace(np.pi * 1.28, np.pi * 1.72, 50))
            arc_x = cx + 9.15 * np.cos(theta)
            arc_y = 34 + 9.15 * np.sin(theta)
            fig.add_trace(go.Scatter(
                x=arc_x, y=arc_y, mode="lines",
                line=dict(color="white", width=1.5),
                showlegend=False, hoverinfo="skip"
            ))
 
        return fig
 
    # ── HEATMAP ───────────────────────────────────────────────────────────────
    if view == "Heatmap":
        fig = draw_field()
 
        try:
            
            import numpy as np
            from scipy.stats import gaussian_kde
 
            x_vals = player_data["x_m"].values
            y_vals = player_data["y_m"].values
 
            xi = np.linspace(0, 105, 210)
            yi = np.linspace(0, 68, 136)
            xx, yy = np.meshgrid(xi, yi)
 
            kde = gaussian_kde(np.vstack([x_vals, y_vals]), bw_method=0.12)
            zi  = kde(np.vstack([xx.ravel(), yy.ravel()])).reshape(xx.shape)
 
            fig.add_trace(go.Heatmap(
                z=zi, x=xi, y=yi,
                colorscale="Hot",
                reversescale=True,
                opacity=0.75,
                showscale=False,
                zsmooth="best",       
                name="Density"
            ))
 
        except ImportError:
            # Fallback sin scipy
            fig.add_trace(go.Histogram2dContour(
                x=player_data["x_m"], y=player_data["y_m"],
                colorscale="Hot",
                reversescale=True,
                opacity=0.75,
                showscale=False,
                ncontours=25,
                contours=dict(showlines=False),
                nbinsx=26, nbinsy=17,
                name="Density"
            ))
 
    # ── TRAJECTORY ────────────────────────────────────────────────────────────
    elif view == "Trajectory":
        import numpy as np
        fig = draw_field()
 
        if speed_col and speed_col in player_data.columns:
            
            speeds = player_data[speed_col].values
            max_s  = speeds.max() or 1
 
            step = max(1, len(player_data) // 300)   # ~300 
            for i in range(0, len(player_data) - step, step):
                t   = float(speeds[i]) / max_s
                r   = int(220 * t)
                b   = int(220 * (1 - t))
                clr = f"rgba({r}, 80, {b}, 0.85)"
                fig.add_trace(go.Scatter(
                    x=player_data["x_m"].iloc[i:i + step + 1],
                    y=player_data["y_m"].iloc[i:i + step + 1],
                    mode="lines",
                    line=dict(width=2.5, color=clr),
                    showlegend=False,
                    hoverinfo="skip"
                ))
        else:
            
            fig.add_trace(go.Scatter(
                x=player_data["x_m"], y=player_data["y_m"],
                mode="lines",
                line=dict(width=2, color="rgba(0, 200, 255, 0.7)"),
                showlegend=False
            ))
 
        
        fig.add_trace(go.Scatter(
            x=[player_data["x_m"].iloc[0]],
            y=[player_data["y_m"].iloc[0]],
            mode="markers",
            marker=dict(size=12, color="#00ff88", symbol="circle",
                        line=dict(width=2, color="white")),
            name="Inicio"
        ))
        fig.add_trace(go.Scatter(
            x=[player_data["x_m"].iloc[-1]],
            y=[player_data["y_m"].iloc[-1]],
            mode="markers",
            marker=dict(size=12, color="#ef4444", symbol="x-thin",
                        line=dict(width=2.5, color="white")),
            name="Fin"
        ))
 
    # ── POINTS ────────────────────────────────────────────────────────────────
    else:
        fig = draw_field()
 
        if speed_col and speed_col in player_data.columns:
            
            fig.add_trace(go.Scatter(
                x=player_data["x_m"], y=player_data["y_m"],
                mode="markers",
                marker=dict(
                    size=3,
                    color=player_data[speed_col],
                    colorscale="Turbo",
                    opacity=0.6,
                    showscale=True,
                    colorbar=dict(title="m/s", thickness=10, len=0.5)
                ),
                name="Positions"
            ))
        else:
            fig.add_trace(go.Scatter(
                x=player_data["x_m"], y=player_data["y_m"],
                mode="markers",
                marker=dict(size=3, color="cyan", opacity=0.5),
                name="Positions"
            ))
 
    # ── COMMON LAYOUT ──────────────────────────────────────────────────────────
    fig.update_layout(
        xaxis=dict(range=[-3, 108], showgrid=False, zeroline=False, title=""),
        yaxis=dict(range=[-3, 71],  showgrid=False, zeroline=False, title="",
                   scaleanchor="x", scaleratio=1),
        plot_bgcolor="#1a4a1a", paper_bgcolor="rgba(0,0,0,0)",
        showlegend=(view == "Trajectory"),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.01,
            xanchor="left",  x=0,
            font=dict(size=11), bgcolor="rgba(0,0,0,0)"
        ),
        height=520,
        margin=dict(l=0, r=0, t=10, b=0)
    )
    st.plotly_chart(fig, use_container_width=True)
 
    # ── SPEED OVER TIME ───────────────────────────────────────────────────────
    if speed_col and speed_col in player_data.columns:
        st.markdown('<p class="section-header">Speed Over Time</p>', unsafe_allow_html=True)
        x_axis = time_col if time_col else player_data.index
        fig2 = px.line(player_data, x=x_axis, y=speed_col,
                       labels={x_axis: "Time", speed_col: "Speed (m/s)"},
                       template="plotly_dark", color_discrete_sequence=["#00c8ff"])
        fig2.update_layout(
            plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
            height=220, margin=dict(l=0, r=0, t=10, b=0)
        )
        st.plotly_chart(fig2, use_container_width=True)

# ─────────────────────────────────────────────────────────────────────────────
# PAGE: PIPELINE
# ─────────────────────────────────────────────────────────────────────────────
elif page == "Pipeline":
    st.title("⚙️ Data Pipeline")
    st.markdown("Run the full ETL pipeline on new GPS tracking data.")
    st.markdown("---")

    # Architecture diagram
    st.markdown("### Pipeline Architecture")
    col1, col2, col3, col4 = st.columns(4)
    col1.info("**① Validate**\n\nSchema checks\nNull detection\nOutlier flags")
    col2.success("**② Transform**\n\nType casting\nCoordinate norm\nParquet output")
    col3.warning("**③ Analytics**\n\nDistance calc\nSpeed metrics\nRole aggregation")
    col4.error("**④ Monitor**\n\nQuality report\nQuarantine log\nRow counts")

    st.markdown("---")
    st.markdown("### Run Pipeline")

    uploaded = st.file_uploader("Upload GPS Tracking CSV", type=["csv"])

    if uploaded:
        os.makedirs("data/raw", exist_ok=True)
        file_path = "data/raw/uploaded_tracking.csv"
        with open(file_path, "wb") as f:
            f.write(uploaded.getbuffer())

        df_preview = pd.read_csv(file_path)
        st.success(f"✅ Uploaded — {len(df_preview):,} rows · {len(df_preview.columns)} columns")
        with st.expander("Preview first 10 rows"):
            st.dataframe(df_preview.head(10), use_container_width=True)

        if st.button("▶ Run Full Pipeline", type="primary"):
            steps = [
                ("Validation",   ["src/validate.py",       file_path]),
                ("Transformation",["src/transform.py",     file_path]),
                ("Analytics",    ["src/build_analytics.py", file_path]),
            ]
            progress = st.progress(0)
            for i, (label, cmd) in enumerate(steps):
                with st.spinner(f"Running {label}..."):
                    result = subprocess.run([sys.executable] + cmd, capture_output=True, text=True)
                    if result.returncode == 0:
                        st.success(f"✅ {label} complete")
                    else:
                        st.error(f"❌ {label} failed")
                        st.code(result.stderr)
                        break
                progress.progress((i+1)/len(steps))
            else:
                st.balloons()
                st.success("Pipeline complete! Navigate to Dashboard to view results.")
                st.cache_data.clear()
    else:
        st.info("Upload a CSV to run the pipeline, or navigate to Dashboard to view existing results.")

        with st.expander("Expected CSV format"):
            st.code("""player_id,ts,x_m,y_m,speed
P001,0,10.5,34.2,2.1
P001,1,10.8,34.5,2.3
...""")

# ─────────────────────────────────────────────────────────────────────────────
# PAGE: DATA QUALITY
# ─────────────────────────────────────────────────────────────────────────────
elif page == "Data Quality":
    st.title("🔍 Data Quality Report")
    st.markdown("Validation results, quarantine log, and pipeline health metrics.")
    st.markdown("---")

    import json
    quality_path = os.path.join(ROOT, "monitoring", "quality_report.json")

    if os.path.exists(quality_path):
        with open(quality_path) as f:
            report = json.load(f)

        st.markdown("### Pipeline Run Summary")
        cols = st.columns(len(report) if len(report) <= 5 else 5)
        for i, (key, val) in enumerate(report.items()):
            if i < 5:
                cols[i].metric(key.replace("_", " ").title(), val)

        with st.expander("Full quality report JSON"):
            st.json(report)
    else:
        st.info("No quality report found. Run the pipeline to generate one.")

    # Quarantine log
    quarantine_path = os.path.join(ROOT, "data", "quarantine", "tracking_quarantine.csv")
    if os.path.exists(quarantine_path):
        st.markdown("---")
        st.markdown("### Quarantine Log")
        qdf = pd.read_csv(quarantine_path)
        st.metric("Quarantined rows", len(qdf))
        if not qdf.empty:
            st.dataframe(qdf.head(50), use_container_width=True, hide_index=True)
    else:
        st.info("No quarantine data found.")

    # Metrics health
    if not metrics.empty:
        st.markdown("---")
        st.markdown("### Metrics Health Check")
        col1, col2 = st.columns(2)
        with col1:
            nulls = metrics.isnull().sum()
            st.markdown("**Null counts per column**")
            st.dataframe(nulls.rename("nulls").reset_index().rename(columns={"index":"column"}),
                         use_container_width=True, hide_index=True)
        with col2:
            st.markdown("**Descriptive statistics**")
            st.dataframe(metrics.describe().round(2), use_container_width=True)