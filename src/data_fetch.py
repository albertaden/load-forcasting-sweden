import pandas as pd
from entsoe import EntsoePandasClient
from pathlib import Path
from config import (
    TZ, COUNTRY_CODE, DAYS_BACK, BACKFILL_DAYS, INITIAL_HISTORY_DAYS,
    SITE_TITLE, SITE_TAGLINE, OUTPUT_DIR, OUTPUT_FILE, PARQUET_FILE, 
    DATA_DIR, ZONE_CODES, TARGET_ZONES
)

#########################################################################################

def get_time_range(days_back: int):
    now = pd.Timestamp.now(tz=TZ).floor("h")
    start = now - pd.Timedelta(days=days_back)
    end = now
    return start, end

#########################################################################################

def fetch_load_df(client: EntsoePandasClient, country_code: str,
                  start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    """

    Return exactly two columns: Date (tz-aware) and Load (MW).
    Note that the load data is typically available with a delay of 1 hour.
    
    Can take a country code like "SE", "DE", "FR", but also bidding zone codes like "SE_1", "SE_2", etc.
    (The library will convert to the required ENTSO-E format internally.)
    
    (The data is published and retrivable 1 hour after the time period has passed) 
    
    """
    data = client.query_load(country_code, start=start, end=end)

    if isinstance(data, pd.DataFrame):
        df = data.copy()
    else:
        df = data.to_frame()

    # Ensure tz-aware, convert to display TZ, and make Date a column
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
        
    df.index = df.index.tz_convert(TZ)
    df = df.rename_axis("Date").reset_index()

    # Rename the (single) value column to Load (MW)
    value_cols = [c for c in df.columns if c != "Date"]
    
    if not value_cols:
        raise ValueError("No value column returned from ENTSO-E.")
    df.rename(columns={value_cols[0]: "Load (MW)"}, inplace=True)

    # Enforce dtypes and return only the two columns
    df["Date"] = pd.to_datetime(df["Date"])
    df["Load (MW)"] = pd.to_numeric(df["Load (MW)"], errors="coerce")

    return df.sort_values("Date")[["Date", "Load (MW)"]].reset_index(drop=True)

#########################################################################################

def to_storage_df(display_df: pd.DataFrame, zone_label: str) -> pd.DataFrame:
    
    """Convert your display schema → storage schema with UTC + zone."""
    
    out = display_df.copy()
    out["date"] = out["Date"].dt.tz_convert("UTC")
    out["load_mw"] = out["Load (MW)"]
    out["zone"] = zone_label
    
    return out[["date", "zone", "load_mw"]]

#########################################################################################

def to_display_df(storage_df: pd.DataFrame, tz: str) -> pd.DataFrame:
    
    """Convert storage schema back to display schema in tz."""
    
    out = storage_df.copy()
    out["Date"] = out["date"].dt.tz_convert(tz)
    out["Load (MW)"] = out["load_mw"]
    
    return out[["Date", "Load (MW)", "zone"]].sort_values(["zone", "Date"]).reset_index(drop=True)

#########################################################################################

def update_history_parquet_multi(new_df: pd.DataFrame, parquet_path: Path) -> pd.DataFrame:
    
    """Append + dedupe on ['date','zone'] and write back. Returns full history."""
    
    if parquet_path.exists():
        hist = pd.read_parquet(parquet_path)
        hist["date"] = pd.to_datetime(hist["date"], utc=True)
        combo = pd.concat([hist, new_df], ignore_index=True)
    else:
        combo = new_df.copy()

    combo = (
        combo.dropna(subset=["date","zone"])
             .sort_values(["zone","date"])
             .drop_duplicates(subset=["date","zone"], keep="last")
             .reset_index(drop=True)
    )

    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    combo.to_parquet(parquet_path, index=False)
    
    return combo
