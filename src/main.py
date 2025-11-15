#!/usr/bin/env python3
import os
from pathlib import Path
import pandas as pd
from entsoe import EntsoePandasClient

from config import (
    TZ, COUNTRY_CODE, DAYS_BACK, BACKFILL_DAYS, INITIAL_HISTORY_DAYS,
    SITE_TITLE, SITE_TAGLINE, OUTPUT_DIR, OUTPUT_FILE, PARQUET_FILE, 
    DATA_DIR, ZONE_CODES, TARGET_ZONES
)

from data_fetch import fetch_load_df, get_time_range, to_display_df, to_storage_df, update_history_parquet_multi
from plotting import make_actual_load_plot, make_all_zones_plot
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

    # Decide how much to fetch: first run = long history, otherwise small backfill
    if not PARQUET_FILE.exists():
        start, end = get_time_range(INITIAL_HISTORY_DAYS)
    else:
        start, end = get_time_range(BACKFILL_DAYS)

    client = EntsoePandasClient(api_key=api_key)

    # Fetch each zone and convert to UTC storage schema
    storage_chunks = []
    
    for zl in TARGET_ZONES:
        
        code = ZONE_CODES[zl]
        display_df = fetch_load_df(client, code, start, end)  # -> ["Date","Load (MW)"]
        storage_chunks.append(to_storage_df(display_df, zone_label=zl))  # -> ["date","zone","load_mw"]

    recent_store = pd.concat(storage_chunks, ignore_index=True)

    # Update parquet history and dedupe
    hist_df = update_history_parquet_multi(recent_store, PARQUET_FILE)

    # Convert back to display schema in TZ (all zones)
    plot_df = to_display_df(hist_df.copy(), TZ)  # -> ["Date","Load (MW)","zone"]
    
    # Store max load per year for analysis
    max_loads = []

    for year in plot_df["Date"].dt.year.unique():
        yearly_data = plot_df[plot_df["Date"].dt.year == year]
        max_load = yearly_data.sort_values(by="Load (MW)", ascending=False).iloc[0]
        max_loads.append({
            "year": year,
            "Date": max_load["Date"],
            "Load (MW)": max_load["Load (MW)"],
        })

    yearly_peaks = pd.DataFrame(max_loads).sort_values("year")

    yearly_peaks["Date"] = yearly_peaks["Date"].dt.strftime("%Y-%m-%d %H:%M")
    
    # Looks good!
    print("Yearly peak loads:")
    print(yearly_peaks.to_string(index=False))

    # Build combined multi-line plot
    fig = make_all_zones_plot(plot_df, "", tz_label=TZ, initial_days=DAYS_BACK)

    # Export plot to HTML snippet & Convert yearly peaks to HTML table
    fig_html = fig.to_html(include_plotlyjs="cdn", full_html=False,
                           config={"displaylogo": False, "responsive": True})
    
    peaks_table_html = yearly_peaks.to_html(index=False,classes="peak-table",border=0,justify="center",)

    sections = [
        {"id": "zones-all", "title": "Actual load in bidding zones SE1–SE4 & total load for Sweden",
         "blurb": f"Actual measured load data from 2015-01-01 to today. Plot is upated automatically every other hour.",
         "fig_html": fig_html},
            {
        "id": "peak-loads",
        "title": "Yearly Peak Loads",
        "blurb": "Highest hourly load observed per year.",
        "fig_html": peaks_table_html},
    ]

    page_html = build_page(sections)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(page_html, encoding="utf-8")
    
    print(f" Wrote {OUTPUT_FILE.resolve()} and updated {PARQUET_FILE.resolve()}")
    
    return plot_df

if __name__ == "__main__":
  
    results = main()
    
    # To sanity check last rows
    print(results.tail())
    
# To test locally, run `python src/main.py` in the root folder and open the generated docs/index.html in a browser.