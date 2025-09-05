#!/usr/bin/env python3
import os
from pathlib import Path
import pandas as pd
from entsoe import EntsoePandasClient
import plotly.express as px
import os
from dotenv import load_dotenv

load_dotenv()  # reads from .env

# -------- Settings --------
TZ = "Europe/Stockholm"
COUNTRY_CODE = "SE"          # Sweden
DAYS_BACK = 7                # how many days back from "now" to plot
OUTPUT_DIR = Path("docs")    # GitHub Pages: set Pages to serve from /docs on main
OUTPUT_FILE = OUTPUT_DIR / "index.html"
TITLE = "Actual Total Load – Sweden"

def get_time_range(days_back: int):
    now = pd.Timestamp.now(tz=TZ).floor("h")
    start = now - pd.Timedelta(days=days_back)
    end = now
    return start, end

def fetch_load_df(client: EntsoePandasClient, country_code: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    s = client.query_load(country_code, start=start, end=end)

    if isinstance(s, pd.DataFrame):
        df = s.copy()
    else:
        df = s.to_frame()

    df.index = df.index.tz_convert("Europe/Stockholm")
    df.reset_index(inplace=True)
    df.rename(columns={"index": "Date"}, inplace=True)

    # Rename any second column to "Load (MW)"
    for col in df.columns:
        if col != "Date":
            df.rename(columns={col: "Load (MW)"}, inplace=True)

    return df

def make_plot(df: pd.DataFrame, title: str):
    fig = px.line(
        df,
        x="Date",
        y="Load (MW)",
        title=title,
        template="plotly_white",
    )
    fig.update_layout(
        hovermode="x unified",
        xaxis=dict(
            rangeslider=dict(visible=True),
            rangeselector=dict(
                buttons=[
                    dict(count=24,  label="1d", step="hour", stepmode="backward"),
                    dict(count=72,  label="3d", step="hour", stepmode="backward"),
                    dict(count=168, label="7d", step="hour", stepmode="backward"),
                    dict(step="all", label="All")
                ]
            ),
            title="Date (Europe/Stockholm)"
        ),
        yaxis=dict(title="Load (MW)"),
        margin=dict(l=60, r=30, t=60, b=40),
    )
    return fig

def main():
    api_key = os.getenv("ENTSOE_API_KEY")
    if not api_key:
        raise SystemExit(
            "Missing ENTSOE_API_KEY. Set it in your environment (do NOT hardcode keys in code)."
        )

    start, end = get_time_range(DAYS_BACK)
    client = EntsoePandasClient(api_key=api_key)

    df = fetch_load_df(client, COUNTRY_CODE, start, end)
    fig = make_plot(df, TITLE)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fig.write_html(
        str(OUTPUT_FILE),
        include_plotlyjs="cdn",   # smaller file; loads Plotly from CDN
        full_html=True,
        config={"displaylogo": False,"responsive": True},
    )
    print(f"Wrote {OUTPUT_FILE.resolve()}")

if __name__ == "__main__":
    main()
