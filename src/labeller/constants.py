"""
Constants and directory setup for NCAT Scraper labeller.
Shared constants (PROJECT_DIR, BASE_DATA_DIR, RAW_DATA_DIR,
PROCESSED_DATA_DIR, SLACK_WEBHOOK_URL) are imported from src/core/constants.py.
Labeller-specific config is read from src/labeller/config/config.yaml.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

# Ensure src/ is on sys.path so core is importable
sys.path.insert(0, str(Path(__file__).parents[1]))

from core.constants import (  # noqa: F401, E402
    PROJECT_DIR,
    BASE_DATA_DIR,
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR,
    LOG_DIR,
    SLACK_WEBHOOK_URL,
)

SCRIPT_DIR = Path(__file__).parent.absolute()
CONFIG_DIR = SCRIPT_DIR / "config"

# Load labeller-specific config
CONFIG_YAML_PATH = CONFIG_DIR / "config.yaml"
config_yaml = {}
if CONFIG_YAML_PATH.exists():
    with open(CONFIG_YAML_PATH, "r") as f:
        config_yaml = yaml.safe_load(f) or {}

_nrsch_base = (config_yaml.get("nrsch_data_dir") or "").strip()
if _nrsch_base:
    NRSCH_DATA_DIR = Path(_nrsch_base)
else:
    NRSCH_DATA_DIR = PROJECT_DIR.parents[1] / "NRSCH"

# Data directories (labeller-specific)
INTERIM_DATA_DIR = BASE_DATA_DIR / "interim"
OBJECTS_DIR = BASE_DATA_DIR / "objects"
PKL_DIR = OBJECTS_DIR / "pkl"

# NRSCH
LATEST_NRSCH_DATA = NRSCH_DATA_DIR / "processed" / "chps.csv"

# Google Sheets (secrets — from config.yaml only, not config_template.yaml)
GOOGLE_CREDENTIALS_PATH = (config_yaml.get("google_credentials_path") or "").strip()
GOOGLE_SHEET_ID = (config_yaml.get("google_sheet_id") or "").strip()
GID_ALL = str(config_yaml.get("gid_all") or "").strip()
GID_MONTH = str(config_yaml.get("gid_month") or "").strip()
GID_PROVIDERS = str(config_yaml.get("gid_providers") or "").strip()

# Slack (labeller-specific)
SLACK_BOT_TOKEN = (config_yaml.get("slack_bot_token") or "").strip()
SLACK_CHANNEL = (config_yaml.get("slack_channel") or "").strip()
SLACK_MAX_PROVIDER_CHARS = int(config_yaml.get("slack_max_provider_chars") or 30)

# Boolean replacement mapping for loading CSV data
# YAML null → pd.NA; YAML .nan key → np.nan
_bool_raw = config_yaml.get("bool_replace_dict", {})
BOOL_REPLACE_DICT = {
    (np.nan if (isinstance(k, float) and np.isnan(k)) else k): (
        pd.NA if v is None else v
    )
    for k, v in _bool_raw.items()
}

# Ensure required data directories exist
for _d in [
    RAW_DATA_DIR,
    PROCESSED_DATA_DIR,
    OBJECTS_DIR,
    PKL_DIR,
    LOG_DIR,
    PROCESSED_DATA_DIR / "backups",
]:
    _d.mkdir(parents=True, exist_ok=True)
