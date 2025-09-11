import pandas as pd
import plotly.express as px
from config import TZ, COUNTRY_CODE, DAYS_BACK, SITE_TITLE, SITE_TAGLINE, OUTPUT_DIR, OUTPUT_FILE, INITIAL_DAYS


# The load shown a specific hour is the average load over that hour: 
# e.g the load shown at 01:00 is the average load from 01:00 to 01:59
def make_actual_load_plot(df: pd.DataFrame, title: str, initial_days: int = INITIAL_DAYS):
    
    fig = px.line(df, x="Date", y="Load (MW)", title=title, template="plotly_white")

    # Compute initial viewport (last `initial_days` days)
    end  = df["Date"].max()
    start = end - pd.Timedelta(days=initial_days)

    fig.update_layout(
        hovermode="x unified",
        xaxis=dict(
            title=f"Date ({TZ})",
            range=[start, end],                 
            rangeslider=dict(visible=True),
            rangeselector=dict(
                buttons=[
                    dict(count=24, step="hour", stepmode="backward", label="1d"),
                    dict(count=3,  step="day",  stepmode="backward", label="3d"),
                    dict(count=7,  step="day",  stepmode="backward", label="7d"),
                    dict(count=1,  step="month",stepmode="backward", label="1m"),
                    dict(count=3,  step="month",stepmode="backward", label="3m"),
                    dict(count=1,  step="year", stepmode="backward", label="1y"),
                    dict(step="year", stepmode="todate", label="YTD"),
                    dict(step="all", label="All"),
                ]
            ),
        ),
        yaxis=dict(title="Load (MW)"),
        margin=dict(l=60, r=30, t=60, b=40),
    )
    return fig


#TODO: Should look into removing/ changing this, not sure if daily avg is useful
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