#!/usr/bin/env python3
import os
from pathlib import Path
import pandas as pd
from entsoe import EntsoePandasClient

from src.config import TZ, COUNTRY_CODE, DAYS_BACK, SITE_TITLE, SITE_TAGLINE, OUTPUT_DIR, OUTPUT_FILE
from src.data_fetch import fetch_load_df, get_time_range
from src.plotting import make_actual_load_plot, make_daily_avg_bar_plot
from src.page_builder import build_page

# Optional: load ENTSOE_API_KEY from a local .env (gitignore this file!)
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

    df = fetch_load_df(client, COUNTRY_CODE, start, end)

    fig1 = make_actual_load_plot(df, f"Actual Total Load – {COUNTRY_CODE}")
    fig2 = make_daily_avg_bar_plot(df, "Daily Average Load")

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

    print(f" Wrote {OUTPUT_FILE.resolve()}")

if __name__ == "__main__":
    main()