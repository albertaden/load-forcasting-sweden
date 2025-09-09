#!/usr/bin/env python3
import os
from pathlib import Path
import pandas as pd
from entsoe import EntsoePandasClient

from config import (
    TZ, COUNTRY_CODE, DAYS_BACK, BACKFILL_DAYS, INITIAL_HISTORY_DAYS,
    SITE_TITLE, SITE_TAGLINE, OUTPUT_DIR, OUTPUT_FILE, PARQUET_FILE
)
from data_fetch import fetch_load_df, get_time_range, to_display_df, to_storage_df, update_history_parquet
from plotting import make_actual_load_plot, make_daily_avg_bar_plot
from page_builder import build_page

# Load ENTSOE_API_KEY from a local .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


def main():
  
    api_key = os.getenv("ENTSOE_API_KEY")
    
    if not api_key:
      
        raise SystemExit("Missing ENTSOE_API_KEY")

    start, end = get_time_range(DAYS_BACK)
    client = EntsoePandasClient(api_key=api_key)

    # fetch recent
    recent_display = fetch_load_df(client, COUNTRY_CODE, start, end)

    # convert to storage schema (UTC) and update parquet
    recent_store = to_storage_df(recent_display)
    hist_df = update_history_parquet(recent_store, PARQUET_FILE)  # expects ['date','load_mw'] UTC

    # build plotting window from history, then convert to your display schema
    max_dt = hist_df["date"].max()
    cut = max_dt - pd.Timedelta(days=DAYS_BACK)
    plot_store = hist_df[hist_df["date"] >= cut].copy()
    plot_df = to_display_df(plot_store, TZ)   # returns ['Date','Load (MW)']

    fig1 = make_actual_load_plot(plot_df, f"Actual Total Load – {COUNTRY_CODE}")
    fig2 = make_daily_avg_bar_plot(plot_df, "Daily Average Load")

    fig1_html = fig1.to_html(include_plotlyjs="cdn", full_html=False,
                             config={"displaylogo": False, "responsive": True})
    fig2_html = fig2.to_html(include_plotlyjs=False, full_html=False,
                             config={"displaylogo": False, "responsive": True})

    sections = [
        {"id": "actual-load", "title": "Actual Total Load",
         "blurb": f"Hourly total load for {COUNTRY_CODE}.", "fig_html": fig1_html},
        {"id": "daily-avg", "title": "Daily Average Load",
         "blurb": "Daily averages shown as bars.", "fig_html": fig2_html},
    ]

    page_html = build_page(sections)
    
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(page_html, encoding="utf-8")
    
    print(f"Wrote {OUTPUT_FILE.resolve()} and updated {PARQUET_FILE.resolve()}")
    
    return plot_df

if __name__ == "__main__":
  
    results = main()
    
    # To sanity check last rows
    print(results.tail())
    
# To test locally, run `python src/main.py` in the root folder and open the generated docs/index.html in a browser.