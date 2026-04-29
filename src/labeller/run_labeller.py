"""
NCAT Scraper labeller orchestrator.
Runs 5 steps sequentially, collects summaries, posts a Slack update.

Task Scheduler entry point:
    python "g:\\Shared drives\\Data\\Analysis\\NCAT Scraper\\src\\labeller\\run_labeller.py" # noqa: E501
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))  # labeller submodule imports
sys.path.insert(0, str(Path(__file__).parents[1]))  # core imports

from core.logger_config import (  # noqa: E402
    get_logger,
    post_log_update,
    format_traceback,
)
import update_applicant_labels as ual  # noqa: E402
import update_providers as up  # noqa: E402
import update_jaccard_features as ujf  # noqa: E402
import prepare_to_label as ptl  # noqa: E402
import update_gsheets as ugs  # noqa: E402

log = get_logger(__file__)


def main():
    log.info("ncat_labeller started")

    try:
        s1 = ual.run()
    except Exception as e:
        log.error("update_applicant_labels failed\n" + format_traceback())
        post_log_update(
            "NCAT Labeller — FAILED at update_applicant_labels\n" + format_traceback()
        )
        sys.exit(e)

    try:
        s2, actions = up.run()
    except Exception as e:
        log.error("update_providers failed\n" + format_traceback())
        post_log_update(
            "NCAT Labeller — FAILED at update_providers\n" + format_traceback()
        )
        sys.exit(e)

    try:
        s3 = ujf.run()
    except Exception as e:
        log.error("update_jaccard_features failed\n" + format_traceback())
        post_log_update(
            "NCAT Labeller — FAILED at update_jaccard_features\n" + format_traceback()
        )
        sys.exit(e)

    try:
        s4 = ptl.run()
    except Exception as e:
        log.error("prepare_to_label failed\n" + format_traceback())
        post_log_update(
            "NCAT Labeller — FAILED at prepare_to_label\n" + format_traceback()
        )
        sys.exit(e)

    try:
        s5 = ugs.run()
    except Exception as e:
        log.error("update_gsheets failed\n" + format_traceback())
        post_log_update(
            "NCAT Labeller — FAILED at update_gsheets\n" + format_traceback()
        )
        sys.exit(e)

    summary = "\n".join([s1, s2, s3, s4, s5])
    log.success("All steps complete:\n" + summary)

    if actions:
        action_block = "\n\n".join(f"[ ] {a}" for a in actions)
        log.warning(f"ACTIONS REQUIRED\n================\n{action_block}")
        post_log_update(
            "NCAT Labeller\n" + summary + f"\n\nACTIONS REQUIRED\n{action_block}"
        )
    else:
        post_log_update("NCAT Labeller\n" + summary)


if __name__ == "__main__":
    main()
