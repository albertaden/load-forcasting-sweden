from pathlib import Path

# Data settings
TZ = "Europe/Stockholm"
COUNTRY_CODE = "SE"
DAYS_BACK = 14
BACKFILL_DAYS = 3            # re-fetch recent days to capture revisions
INITIAL_HISTORY_DAYS = 60    # used only on first run (no parquet yet)
INITIAL_DAYS = 14            # initial viewport in plot

# Site settings
SITE_TITLE = "Sweden Load Dashboard"
SITE_TAGLINE = "Interactive charts generated from ENTSO-E data"

# Output
OUTPUT_DIR = Path("docs")
OUTPUT_FILE = OUTPUT_DIR / "index.html"

# Storage
DATA_DIR = Path("data")
PARQUET_FILE = DATA_DIR / f"load_{COUNTRY_CODE.lower()}.parquet"