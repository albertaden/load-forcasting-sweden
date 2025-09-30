#!/usr/bin/env python3
# Backfill ENTSO-E Actual Total Load for Sweden total + zones → one Parquet (UTC)
# Output schema: ['date','zone','load_mw']

import os
import argparse
from pathlib import Path
import pandas as pd
from entsoe import EntsoePandasClient

# Request in your familiar TZ; store in UTC
QUERY_TZ = "Europe/Stockholm"
STORE_TZ = "UTC"

# Default mapping: aliases that entsoe-py usually accepts.
# If your entsoe-py needs EIC codes instead, run with --use-eic
ZONE_CODES_ALIAS = {
    "SE_total": "SE",
    "SE1": "SE_1",
    "SE2": "SE_2",
    "SE3": "SE_3",
    "SE4": "SE_4",
}

# EIC mapping (use with --use-eic if aliases don’t work)
ZONE_CODES_EIC = {
    "SE_total": "10YSE-1--------K",
    "SE1": "10Y1001A1001A44P",
    "SE2": "10Y1001A1001A45N",
    "SE3": "10Y1001A1001A46L",
    "SE4": "10Y1001A1001A47J",
}

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

    # pick / coerce numeric value column if needed
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
        df.rename_axis("date")
          .reset_index()[["date", value_name]]
          .dropna(subset=["date", value_name])
          .copy()
    )
    
    tidy["date"] = pd.to_datetime(tidy["date"], utc=True)
    tidy[value_name] = pd.to_numeric(tidy[value_name], errors="coerce")
    
    return tidy.dropna(subset=[value_name]).sort_values("date").reset_index(drop=True)

def fetch_load_chunk(client: EntsoePandasClient, area_code: str,
                     start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    
    """One area for one month window → ['date','load_mw'] UTC."""
    
    res = client.query_load(area_code, start=start, end=end)
    
    return _to_tidy(res, value_name="load_mw")

def parse_zones_arg(zones_csv: str, mapping: dict[str, str]) -> list[tuple[str, str]]:
    
    """Parse 'SE_total,SE1,SE2,SE3,SE4' into [(label, area_code), ...]."""
    
    labels = [z.strip() for z in zones_csv.split(",") if z.strip()]
    unknown = [z for z in labels if z not in mapping]
    
    if unknown:
        raise SystemExit(f"Unknown zone labels: {unknown}. Allowed: {list(mapping.keys())}")
    
    return [(z, mapping[z]) for z in labels]

def main():
    
    ap = argparse.ArgumentParser(
        description="Backfill ENTSO-E Actual Total Load for Sweden zones into one Parquet (UTC)."
    )
    ap.add_argument("--start", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD (exclusive)")
    ap.add_argument("--out", required=True, help="Parquet output file, e.g. data/load_se_zones.parquet")
    ap.add_argument(
        "--zones",
        default="SE_total,SE1,SE2,SE3,SE4",
        help="Comma-separated zone labels to fetch (default: SE_total,SE1,SE2,SE3,SE4)",
    )
    ap.add_argument(
        "--use-eic",
        action="store_true",
        help="Use EIC area codes instead of alias codes (if your entsoe-py requires it).",
    )
    args = ap.parse_args()

    api_key = os.getenv("ENTSOE_API_KEY")
    if not api_key:
        raise SystemExit("Missing ENTSOE_API_KEY in environment")

    start = pd.Timestamp(args.start, tz=QUERY_TZ)
    end = pd.Timestamp(args.end, tz=QUERY_TZ)
    if end <= start:
        raise SystemExit("end must be after start")

    mapping = ZONE_CODES_EIC if args.use_eic else ZONE_CODES_ALIAS
    zones = parse_zones_arg(args.zones, mapping)

    client = EntsoePandasClient(api_key=api_key)

    frames = []
    
    for a, b in month_ranges(start, end):
        ym = a.strftime("%Y-%m")
        
        for label, area in zones:
            print(f"Fetching {label} ({area}) {ym} ...", flush=True)
            df = fetch_load_chunk(client, area, a, b)
            
            if not df.empty:
                df["zone"] = label
                frames.append(df)

    if not frames:
        raise SystemExit("No data fetched. Check your API key, zones, and date range.")

    full = (
        pd.concat(frames, ignore_index=True)
          .drop_duplicates(subset=["date", "zone"], keep="last")
          .sort_values(["zone", "date"])
          .reset_index(drop=True)
    )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    full.to_parquet(out_path, index=False)

    print(
        f"Wrote {len(full):,} rows across {full['zone'].nunique()} zones "
        f"to {out_path.resolve()} ({full['date'].min()} → {full['date'].max()})"
    )

if __name__ == "__main__":
    main()
    
# Example usage in a terminal:

# Set your ENTSOE_API_KEY environment variable first, e.g. in bash:
# $env:ENTSOE_API_KEY="your_key_here"

# Then run the script, e.g. to backfill Swedish load from 2015-01-01 to 2025-09-11:
# python scripts/historical_data_fetch.py --start 2015-01-01 --end 2025-09-11 --out data/load_se_zones.parquet

