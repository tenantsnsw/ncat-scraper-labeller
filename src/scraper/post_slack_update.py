"""Post a Slack update with the last 7 days of run data.

Uses core.logger_config.post_log_update so that today's
log file is automatically attached alongside the message.
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parents[1]))

from core.logger_config import (  # noqa: E402
    get_logger,
    post_log_update,
    format_traceback,
)

import constants  # noqa: E402

log = get_logger(__file__)


def run():
    """Build and post the scraper summary to Slack."""
    run_control = pd.read_csv(
        constants.RUN_CONTROL_CSV, parse_dates=["Date"]
    )
    seven_days_ago = (
        pd.to_datetime("today") - pd.Timedelta(days=7)
    )
    recent = run_control[
        run_control["Date"] >= seven_days_ago
    ]
    df_string = recent.to_string(index=False)

    msg = (
        "NCAT Scraper Data Update\n"
        "Last 7 days of run data:\n"
        + df_string
    )
    log.info(msg)
    post_log_update(msg)
    return msg


if __name__ == "__main__":
    try:
        run()
    except Exception:
        log.error(
            "CRITICAL ERROR\n" + format_traceback()
        )
        post_log_update(
            "CRITICAL ERROR\n"
            + f"{Path(__file__).as_posix()}\n"
            + format_traceback()
        )
        sys.exit(1)
