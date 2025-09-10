#!/usr/bin/env python3
"""
Minimal ENTSO-E backfill → Parquet (UTC). Currently implements:
- load  (Actual Total Load)

Designed to be easy to extend later (see FETCHERS and the TODOs).
"""

import os
import argparse
from pathlib import Path
import pandas as pd
from entsoe import EntsoePandasClient

# Request in your familiar TZ; store in UTC 
QUERY_TZ = "Europe/Stockholm"
STORE_TZ = "UTC"

# ---------- helpers ----------
def month_ranges(start: pd.Timestamp, end: pd.Timestamp):
    
    """Yield [a, b) month windows from start (inclusive) to end (exclusive)."""
    
    for a in pd.date_range(start, end, freq="MS", tz=start.tz):
        
        b = min(a + pd.offsets.MonthBegin(1), end)
        
        if a < b:
            yield a, b

def _to_tidy(df_or_s, value_name: str) -> pd.DataFrame:
    
    """Normalize Series/DataFrame to ['date','<value_name>'] with UTC datetimes."""
    
    if isinstance(df_or_s, pd.Series):
        df = df_or_s.to_frame(name=value_name)
    else:
        df = df_or_s.copy()

    # ensure tz-aware index, convert to UTC
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
        
    df.index = df.index.tz_convert(STORE_TZ)

    # pick / coerce numeric value column
    if value_name not in df.columns:
        
        num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
        if len(num_cols) == 1:
            
            df.rename(columns={num_cols[0]: value_name}, inplace=True)
            
        elif len(num_cols) > 1:
            
            df[value_name] = df[num_cols].sum(axis=1)
            
        else:
            value_cols = [c for c in df.columns if c != (df.index.name or "index")]
            
            if not value_cols:
                
                raise ValueError("No value column in response.")
            
            df.rename(columns={value_cols[0]: value_name}, inplace=True)

    tidy = (
        df.rename_axis("date").reset_index()[["date", value_name]]
          .dropna(subset=["date", value_name])
          .copy()
    )
    
    tidy["date"] = pd.to_datetime(tidy["date"], utc=True)
    tidy[value_name] = pd.to_numeric(tidy[value_name], errors="coerce")
    
    return tidy.dropna(subset=[value_name]).sort_values("date").reset_index(drop=True)

# ---------- dataset fetchers (extend here later) ----------
def fetch_load(client: EntsoePandasClient, *, country: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    
    """Actual Total Load → ['date','load_mw']"""
    
    res = client.query_load(country, start=start, end=end)
    
    return _to_tidy(res, value_name="load_mw")

FETCHERS = {
    "load": fetch_load,
    # TODO (later):
    # "load_forecast": lambda client, **kw: _to_tidy(client.query_load_forecast(kw["country"], kw["start"], kw["end"]), "value"),
    # "day_ahead_price": lambda client, **kw: _to_tidy(client.query_day_ahead_prices(kw["country"], kw["start"], kw["end"]), "eur_mwh"),
    # "generation": lambda client, **kw: _to_tidy(client.query_generation(kw["country"], kw["start"], kw["end"], psr_type=kw.get("psr")), "mw"),
    # "crossborder": lambda client, **kw: _to_tidy(client.query_crossborder_flows(kw["in_domain"], kw["out_domain"], kw["start"], kw["end"]), "mw"),
}

# ---------- CLI / main ----------

def main():
    
    ap = argparse.ArgumentParser(description="Backfill ENTSO-E dataset to a Parquet file (UTC).")
    ap.add_argument("--dataset", default="load", help="Currently supported: load")
    ap.add_argument("--country", required=True, help="ENTSO-E country/bidding-zone code (e.g. SE, NO, DE)")
    ap.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD (exclusive)")
    ap.add_argument("--out", required=True, help="Parquet output file, e.g. data/load_SE.parquet")
    args = ap.parse_args()

    if args.dataset not in FETCHERS:
        raise SystemExit(f"Dataset '{args.dataset}' not implemented yet. Supported now: load")

    api_key = os.getenv("ENTSOE_API_KEY")
    
    if not api_key:
        raise SystemExit("Missing ENTSOE_API_KEY in environment")

    start = pd.Timestamp(args.start, tz=QUERY_TZ)
    end = pd.Timestamp(args.end, tz=QUERY_TZ)
    
    if end <= start:
        raise SystemExit("end must be after start")

    client = EntsoePandasClient(api_key=api_key)
    fetcher = FETCHERS[args.dataset]

    frames = []
    
    for a, b in month_ranges(start, end):
        print(f"Fetching {args.dataset} {args.country} {a.strftime('%Y-%m')} ...")
        frames.append(fetcher(client, country=args.country, start=a, end=b))

    full = (pd.concat(frames, ignore_index=True)
              .drop_duplicates(subset=["date"], keep="last")
              .sort_values("date")
              .reset_index(drop=True))

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    full.to_parquet(out_path, index=False)
    
    print(f"Wrote {len(full):,} rows to {out_path.resolve()} "
          f"({full['date'].min()} → {full['date'].max()})")

if __name__ == "__main__":
    main()
    
# Example usage in a terminal:

# Set your ENTSOE_API_KEY environment variable first, e.g. in bash:
# $env:ENTSOE_API_KEY="your_key_here"

# Then run the script, e.g. to backfill Swedish load from 2015-01-01 to 2025-09-08:
# python scripts/historical_data_fetch.py --dataset load --country SE --start 2015-01-01 --end 2025-09-10 --out data/load_se.parquet

