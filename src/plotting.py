import pandas as pd
import plotly.express as px
import plotly.graph_objects as go  # if you ever want finer control
from src.config import TZ, COUNTRY_CODE, DAYS_BACK, SITE_TITLE, SITE_TAGLINE, OUTPUT_DIR, OUTPUT_FILE


# -------------- Figures -------------------
def make_actual_load_plot(df: pd.DataFrame, title: str):
    fig = px.line(df, x="Date", y="Load (MW)", title=title, template="plotly_white")
    fig.update_layout(
        hovermode="x unified",
        xaxis=dict(
            title=f"Date ({TZ})",
            rangeslider=dict(visible=True),
            rangeselector=dict(
                buttons=[
                    dict(count=24,  label="1d", step="hour", stepmode="backward"),
                    dict(count=72,  label="3d", step="hour", stepmode="backward"),
                    dict(count=168, label="7d", step="hour", stepmode="backward"),
                    dict(step="all", label="All"),
                ]
            ),
        ),
        yaxis=dict(title="Load (MW)"),
        margin=dict(l=60, r=30, t=60, b=40),
    )
    return fig

def make_daily_avg_bar_plot(df: pd.DataFrame, title: str):
    # Aggregate hourly → daily average
    daily = (
        df.assign(day=df["Date"].dt.floor("D"))
          .groupby("day", as_index=False)["Load (MW)"].mean()
          .rename(columns={"day": "Date", "Load (MW)": "Daily Avg (MW)"})
    )

    fig = px.bar(
        daily,
        x="Date",
        y="Daily Avg (MW)",
        title=title,
        template="plotly_white",
    )
    fig.update_layout(
        hovermode="x unified",
        xaxis=dict(title="Date"),
        yaxis=dict(title="Daily average load (MW)"),
        margin=dict(l=60, r=30, t=60, b=40),
    )
    fig.update_traces(hovertemplate="Date: %{x|%Y-%m-%d}<br>Daily avg: %{y:.0f} MW<extra></extra>")
    return fig