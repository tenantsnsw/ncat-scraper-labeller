"""
Loguru-based logger setup shared across all NCAT Scraper modules.

Log files are written daily to the project logs/ directory.

Usage:
    from logger_config import get_logger, post_log_update
    log = get_logger(__file__)
    log.info("Loaded 1,200 listings")
"""

import sys
import datetime
import traceback
from pathlib import Path

from loguru import logger as _base
from slack_sdk.webhook import WebhookClient
from slack_sdk import WebClient

sys.path.insert(0, str(Path(__file__).parents[1]))

from core.constants import (  # noqa: E402
    LOG_DIR,
    SLACK_WEBHOOK_URL,
    SLACK_BOT_TOKEN,
    SLACK_CHANNEL,
)

# --- Sink configuration (runs once at import) ---
_base.remove()
_base.configure(extra={"script": "?"})
_slack_warned = False

_log_file = LOG_DIR / f"log_{datetime.date.today():%Y%m%d}.log"
_FMT = (
    "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | "
    "{extra[script]: <25} | {message}"
)
_base.add(sys.stderr, format=_FMT, level="DEBUG", colorize=True)
_base.add(str(_log_file), format=_FMT, level="DEBUG", mode="a")


def get_logger(script_path: str):
    """Return a loguru logger bound with the calling script's stem name."""
    script_name = Path(script_path).stem
    return _base.bind(script=script_name)


def post_log_update(msg: str) -> None:
    """Post *msg* to Slack and upload today's log file as an attachment."""
    global _slack_warned
    if not msg:
        return
    if not SLACK_WEBHOOK_URL:
        if not _slack_warned:
            _base.bind(script="logger_config").warning(
                "SLACK_WEBHOOK_URL not configured — skipping Slack post"
            )
            _slack_warned = True
        return
    webhook = WebhookClient(SLACK_WEBHOOK_URL)
    response = webhook.send(text=msg)
    assert response.status_code == 200, response.body
    assert response.body == "ok", response.body

    if SLACK_BOT_TOKEN and SLACK_CHANNEL and _log_file.exists():
        web = WebClient(token=SLACK_BOT_TOKEN)
        web.files_upload_v2(
            channel=SLACK_CHANNEL,
            file=str(_log_file),
            filename=_log_file.name,
            initial_comment=f"Log file for {_log_file.stem}",
        )


def format_traceback() -> str:
    """Return the current exception traceback as a string."""
    return traceback.format_exc()
