"""
Shared constants for the NCAT Scraper project.
Used by both src/scraper/ and src/labeller/ modules.
Reads shared configuration from src/core/config/config.yaml.
"""

from pathlib import Path

import yaml

_CONFIG_PATH = Path(__file__).parent / "config" / "config.yaml"
_config = {}
if _CONFIG_PATH.exists():
    with open(_CONFIG_PATH, "r") as f:
        _config = yaml.safe_load(f) or {}

# g:\Shared drives\Data\Analysis\NCAT Scraper\
PROJECT_DIR = Path(__file__).parents[2]

_custom_base = (_config.get("base_data_dir") or "").strip()
if _custom_base:
    BASE_DATA_DIR = Path(_custom_base)
else:
    BASE_DATA_DIR = PROJECT_DIR.parents[1] / "NCAT Scraper"

RAW_DATA_DIR = BASE_DATA_DIR / "raw"
PROCESSED_DATA_DIR = BASE_DATA_DIR / "processed"

LOG_DIR = PROJECT_DIR / "logs"

SLACK_WEBHOOK_URL = (_config.get("slack_webhook_url") or "").strip()
SLACK_BOT_TOKEN = (_config.get("slack_bot_token") or "").strip()
SLACK_CHANNEL = (_config.get("slack_channel") or "").strip()

# Ensure shared directories exist
for _d in [RAW_DATA_DIR, PROCESSED_DATA_DIR, LOG_DIR]:
    _d.mkdir(parents=True, exist_ok=True)
