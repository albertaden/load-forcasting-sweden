import pandas as pd
import plotly.express as px
from config import (
    TZ, COUNTRY_CODE, DAYS_BACK, BACKFILL_DAYS, INITIAL_HISTORY_DAYS,
    SITE_TITLE, SITE_TAGLINE, OUTPUT_DIR, OUTPUT_FILE, PARQUET_FILE, 
    DATA_DIR, ZONE_CODES, TARGET_ZONES, INITIAL_DAYS
)


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

#########################################################################################

# TODO: Add automatic switching between summer time and winter time
# TODO: Somewhere before plotting, add 1-hour shift to the data so that the hour shown is the hour when the avarage load was measured
# (e.g. the load shown at 01:00 is the average load from 01:00 to 01:59) CHANGE THIS
def make_all_zones_plot(df_long: pd.DataFrame, title: str, tz_label: str, initial_days: int = INITIAL_DAYS, order: list[str] = TARGET_ZONES) -> px.line:
    
    """
    
    df_long: columns ["Date","Load (MW)","zone"] in display TZ.
    Includes 'SE_total' and 'SE1'..'SE4' in the same frame.
    
    Returns: plotly Figure object.
    
    """
    
    if "zone" in df_long.columns:
        df_long = df_long.copy()
        df_long["zone"] = pd.Categorical(df_long["zone"], categories=order, ordered=True)
        
    fig = px.line(
        df_long,
        x="Date",
        y="Load (MW)",
        color="zone",
        category_orders={"zone": order},
        title=title,
        template="plotly_white",
    )

    # Initial viewport = last N days (data still contains the full range)
    end = df_long["Date"].max()
    start = end - pd.Timedelta(days=initial_days)

    fig.update_layout(
        hovermode="x unified",
        xaxis=dict(
            title=f"Date ({tz_label})",
            range=[start, end],
            rangeslider=dict(visible=True),
            rangeselector=dict(
                buttons=[
                    dict(count=24, step="hour",  stepmode="backward", label="1d"),
                    dict(count=3,  step="day",   stepmode="backward", label="3d"),
                    dict(count=7,  step="day",   stepmode="backward", label="7d"),
                    dict(count=1,  step="month", stepmode="backward", label="1m"),
                    dict(count=3,  step="month", stepmode="backward", label="3m"),
                    dict(count=1,  step="year", stepmode="backward", label="1y"),                    
                    dict(step="year", stepmode="todate", label="YTD"),
                    dict(step="all", label="All"),
                ]
            ),
        ),
        yaxis=dict(title="Load (MW)"),
        margin=dict(l=60, r=30, t=60, b=40),
        legend_title_text="Zone",
    )
    
    # Force step-like plotting
    fig.update_traces(line_shape="hv")

    # Make SE_total a bit more prominent
    for tr in fig.data:
        if getattr(tr, "name", "") == "SE_total":
            tr.update(line=dict(width=3))
        else:
            tr.update(line=dict(width=2))
            
    return fig

#########################################################################################