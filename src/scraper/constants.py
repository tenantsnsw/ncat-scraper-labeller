"""
Constants and directory setup for NCAT scraper.
Shared constants (PROJECT_DIR, BASE_DATA_DIR, RAW_DATA_DIR,
PROCESSED_DATA_DIR, SLACK_WEBHOOK_URL, LOG_DIR, SLACK_BOT_TOKEN,
SLACK_CHANNEL) are imported from src/core/constants.py.
Scraper-specific config is read from src/scraper/config/config.yaml.
"""

import sys
from pathlib import Path

import yaml

# Ensure src/ is on sys.path so core is importable
sys.path.insert(0, str(Path(__file__).parents[1]))

from core.constants import (  # noqa: F401 E402
    PROJECT_DIR,
    BASE_DATA_DIR,
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR,
    LOG_DIR,
    SLACK_WEBHOOK_URL,
    SLACK_BOT_TOKEN,
    SLACK_CHANNEL,
)

# Config directory
CONFIG_DIR = Path(__file__).parent / "config"

# Load scraper-specific config
_config_path = CONFIG_DIR / "config.yaml"
config_yaml = {}
if _config_path.exists():
    with open(_config_path, "r") as f:
        config_yaml = yaml.safe_load(f) or {}

_downloads_dir = (config_yaml.get("downloads_dir") or "").strip()
if _downloads_dir:
    DOWNLOADS_DIR = Path(_downloads_dir)
else:
    DOWNLOADS_DIR = Path.home() / "Downloads"

# API call for NCAT Online Registry court listings
_api_query_format = (config_yaml.get("api_query_format") or "").strip()
API_QUERY_FORMAT = _api_query_format or (
    "https://api.onlineregistry.justice.nsw.gov.au/courtlistsearch/listings?"
    "callback=JSON_CALLBACK&startDate={date}&endDate={date}&jurisdiction=NCAT&court="
    "NCAT%20Consumer%20and%20Commercial%20Division%2CNCAT%20Appeal%20Panel&count=1000"
    "&offset=0&sortField=date,time,location&sortOrder=ASC&format=csv"
)

# Downloads and metadata/logs
LISTINGS_FILE = DOWNLOADS_DIR / "listings.csv"
DATA_LIST_CSV = PROJECT_DIR / "metadata" / "scraper_listings_data.csv"
RUN_CONTROL_CSV = PROJECT_DIR / "logs" / "scraper_run_logs.csv"

SLACK_POST_DAYS = config_yaml.get(
    "slack_post_days", [0, 3]
)  # Default to Monday and Thursday

# TODO: Check if UA1–UA5 and UA are used; may be removed if unused.
# User agents
UA1 = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/42.0.2311.135 Safari/537.36 Edge/12.246"
)
UA2 = (
    "Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/51.0.2704.64 Safari/537.36"
)
UA3 = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) "
    "AppleWebKit/601.3.9 (KHTML, like Gecko) "
    "Version/9.0.2 Safari/601.3.9"
)
UA4 = (
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/47.0.2526.111 Safari/537.36"
)
UA5 = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:15.0) Gecko/20100101 Firefox/15.0."
UA = [UA1, UA2, UA3, UA4, UA5]

# This is just used to initiate captcha
BASE_URL = config_yaml.get(
    "base_url", "https://onlineregistry.lawlink.nsw.gov.au/content/court-lists?#/"
)

# NCAT Online Listings Registry and Run Control Constants
try:
    DAYS_AVAILABLE_BACKWARDS = int(config_yaml.get("days_available_backwards", 7))
except Exception:
    DAYS_AVAILABLE_BACKWARDS = 7
try:
    DAYS_AVAILABLE_FORWARDS = int(config_yaml.get("days_available_forwards", 21))
except Exception:
    DAYS_AVAILABLE_FORWARDS = 21
try:
    DAYS_BETWEEN_RUNS = int(config_yaml.get("days_between_runs", 1))
except Exception:
    DAYS_BETWEEN_RUNS = 1

# ── process_data constants ──────────────────────────────────────────────────
REPLACE_SPACES = {" +": " "}

REPLACE_SPECIAL = {"_": " ", r"\.": " ", r"\-": " ", r" \- ": " ", '"': ""}

FIX_APPOST_S = {
    "'S ": "'s ",
    "'S$": "'s",
    "\u2019S ": "'s ",
    "\u2019S$": "'s",
}

PRIMARY_LISTING_TYPE_RANK = [
    "Termination Conciliation (Group)",
    "Non Termination Conciliation (Group)",
    "Home Building Directions",
    "Strata & Community Directions",
    "Retail Lease Directions",
    "Non Tenancy Conciliation (Group)",
]
