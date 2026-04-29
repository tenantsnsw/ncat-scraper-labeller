import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

import label_loading_saving as ls
from core.logger_config import get_logger, post_log_update, format_traceback

from functools import reduce
import argparse
import constants
import pandas as pd
import time

log = get_logger(__file__)


# import constants
def join_df_list(df_list, on, how="outer"):
    joined_df = reduce(
        lambda df1, df2: pd.merge(
            df1,
            df2,
            on=on,
            how=how,
        ),
        df_list,
    )
    return joined_df


def load_all_to_label():
    sheet_id = constants.GOOGLE_SHEET_ID
    gid_all = constants.GID_ALL
    gid_month = constants.GID_MONTH
    gsheets_base = "https://docs.google.com/spreadsheets/d/"
    file_all = gsheets_base + f"{sheet_id}/export?gid={gid_all}&format=csv"
    file_month = gsheets_base + f"{sheet_id}/export?gid={gid_month}&format=csv"
    to_label_all = ls.load_to_label(filename=file_all)
    to_label_all = to_label_all.loc[to_label_all["Applicant Label"] != ""]
    to_label_month = ls.load_to_label(filename=file_month)
    to_label_month = to_label_month.loc[to_label_month["Applicant Label"] != ""]
    labelled = {"To Label All": to_label_all, "To Label Month": to_label_month}
    labelled = join_df_list(
        labelled.values(), on=["Normalised Applicant", "Applicant Label"]
    )
    return labelled


def check_conflicts(labelled):
    n_conflicting = len(labelled[labelled.duplicated(subset="Normalised Applicant")])
    if n_conflicting > 0:
        conflict_table = labelled.loc[
            labelled.duplicated(subset="Normalised Applicant"),
            ["Normalised Applicant", "Applicant Label"],
        ].to_string()
        log.error(
            f"{n_conflicting} contradictory label(s) found:\n{conflict_table}"
        )
        raise ValueError(
            f"Fix contradictory label(s):\n{conflict_table}"
        )
    return False


def run() -> str:
    t0 = time.monotonic()
    applicant_labels = ls.load_applicant_labels(backup=True)
    prev_count = (applicant_labels["Applicant Label"] != "").sum()
    labelled = load_all_to_label()
    check_conflicts(labelled)
    applicant_labels = ls.update_applicant_labels(
        applicant_labels=applicant_labels, labelled=labelled
    )
    curr_count = (applicant_labels["Applicant Label"] != "").sum()
    ls.save_applicant_labels(applicant_labels)
    elapsed = round(time.monotonic() - t0)
    summary = (
        f"update_applicant_labels: {prev_count} → {curr_count} labelled"
        f" applicants, {elapsed}s elapsed"
    )
    log.success(summary)
    return summary


def load_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-v", "--verbose", help="increase output verbosity", action="store_true"
    )
    parser.add_argument(
        "-g", "--debug", help="prints out debug information", action="store_true"
    )
    known_flags = {"-v", "--verbose", "-g", "--debug"}
    argv = [a for a in sys.argv[1:] if a in known_flags]
    return parser.parse_args(argv)


if __name__ == "__main__":

    args = load_args()
    verbose = args.verbose
    debug = args.debug

    try:
        # Load local applicant labels CSV, writing a timestamped backup first
        applicant_labels = ls.load_applicant_labels(backup=True)
        log.info(
            "Previous Labelled Applicants: "
            + str((applicant_labels["Applicant Label"] != "").sum())
        )

        if not debug:

            # Load labelled rows from both Google Sheets tabs
            labelled = load_all_to_label()
            # Raise error if the same applicant has two different labels
            check_conflicts(labelled)
            # Merge new labels from Google Sheets into the local labels file
            applicant_labels = ls.update_applicant_labels(
                applicant_labels=applicant_labels,
                labelled=labelled,
            )

            log.info(
                "Current Labelled Applicants: "
                + str((applicant_labels["Applicant Label"] != "").sum())
                + "\nApplicant Labels Updated"
            )
            # Overwrite the local applicant labels CSV with updated labels
            ls.save_applicant_labels(applicant_labels)

    except Exception as e:
        log.error("CRITICAL ERROR\n" + str(e) + "\n" + format_traceback())
        post_log_update(
            "CRITICAL ERROR\n"
            + f"{Path(__file__).as_posix()}\n"
            + format_traceback()
        )
        sys.exit(e)
