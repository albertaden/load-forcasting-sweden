from pathlib import Path

# Data settings
TZ = "Europe/Stockholm"
COUNTRY_CODE = "SE"
DAYS_BACK = 14
BACKFILL_DAYS = 3            # re-fetch recent days to capture revisions
INITIAL_HISTORY_DAYS = 60    # used only on first run (no parquet yet)
INITIAL_DAYS = 14            # initial viewport in plot

# Bidding zones (values are what entsoe-py accepts)
# entsoe-py usually recognizes: "SE", "SE_1", "SE_2", "SE_3", "SE_4"
ZONE_CODES = {
    "SE_total": "SE",
    "SE1": "SE_1",
    "SE2": "SE_2",
    "SE3": "SE_3",
    "SE4": "SE_4",
}
TARGET_ZONES = ["SE_total", "SE1", "SE2", "SE3", "SE4"]

# Site settings
SITE_TITLE = "Sweden Load Dashboard"
SITE_TAGLINE = "Interactive charts generated from ENTSO-E data"

# Output
OUTPUT_DIR = Path("docs")
OUTPUT_FILE = OUTPUT_DIR / "index.html"

# Storage
DATA_DIR = Path("data")
PARQUET_FILE = DATA_DIR / f"load_{COUNTRY_CODE.lower()}_zones.parquet"