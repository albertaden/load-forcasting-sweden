import pandas as pd
from entsoe import EntsoePandasClient
from config import TZ, COUNTRY_CODE, DAYS_BACK, SITE_TITLE, SITE_TAGLINE, OUTPUT_DIR, OUTPUT_FILE

# -------------- Data helpers --------------
def get_time_range(days_back: int):
    now = pd.Timestamp.now(tz=TZ).floor("h")
    start = now - pd.Timedelta(days=days_back)
    end = now
    return start, end

def fetch_load_df(client: EntsoePandasClient, country_code: str,
                  start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    """Return exactly two columns: Date (tz-aware) and Load (MW)."""
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