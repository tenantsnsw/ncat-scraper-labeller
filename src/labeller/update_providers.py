import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

import label_loading_saving as ls
import datetime as dt
from core.logger_config import get_logger, post_log_update, format_traceback
import constants

import time

log = get_logger(__file__)


def load_gsheet_providers(backup=False):
    sheet_id = constants.GOOGLE_SHEET_ID
    gid_providers = constants.GID_PROVIDERS
    gsheets_base = "https://docs.google.com/spreadsheets/d/"
    providers_filename = (
        gsheets_base + f"{sheet_id}/export?gid={gid_providers}&format=csv"
    )
    providers = ls.load_providers(filename=providers_filename, backup=True)
    if backup:
        now = dt.datetime.today().strftime("%Y%m%d%H%M%S")
        providers.to_csv(
            constants.PROCESSED_DATA_DIR / "backups" / f"providers{now}.csv",
            index=False,
        )
    return providers


def run() -> tuple[str, list[str]]:
    t0 = time.monotonic()
    providers = load_gsheet_providers(backup=True)
    nrsch_data = ls.load_nrsch_data()
    updated_providers = ls.update_providers(providers, nrsch_data)
    ls.save_providers(updated_providers)

    ungrouped = updated_providers[updated_providers["Provider Group"].isna()]
    actions = []
    if len(ungrouped) > 0:
        names = ", ".join(ungrouped["Provider Official Name"].to_list())
        sheet_url = (
            f"https://docs.google.com/spreadsheets/d/{constants.GOOGLE_SHEET_ID}"
            f"/edit#gid={constants.GID_PROVIDERS}"
        )
        log.info(f"NEW PROVIDERS TO GROUP: {names}")
        actions.append(
            f"Assign Provider Group for {len(ungrouped)} provider(s) "
            f"in Google Sheet:\n{names}\n{sheet_url}"
        )

    elapsed = round(time.monotonic() - t0)
    summary = (
        f"update_providers: {len(updated_providers)} providers saved, "
        f"{len(ungrouped)} ungrouped, {elapsed}s elapsed"
    )
    log.success(summary)
    return summary, actions


if __name__ == "__main__":

    # Settings
    verbose = True  # Prints more information
    debug = False  # Used for debugging only

    try:
        # Load gsheet
        providers = load_gsheet_providers(backup=True)
        # Load Local
        # providers = ls.load_providers()
        nrsch_data = ls.load_nrsch_data()
        if not debug:

            updated_providers = ls.update_providers(providers, nrsch_data)
            ls.save_providers(updated_providers)
            if len(updated_providers[updated_providers["Provider Group"].isna()]) > 0:
                log.info(
                    "NEW PROVIDERS TO GROUP: "
                    + ", ".join(
                        updated_providers.loc[
                            updated_providers["Provider Group"].isna(),
                            "Provider Official Name",
                        ].to_list()
                    )
                )
    except Exception as e:
        log.error("CRITICAL ERROR\n" + str(e) + "\n" + format_traceback())
        post_log_update(
            "CRITICAL ERROR\n" + f"{Path(__file__).as_posix()}\n" + format_traceback()
        )
        sys.exit(e)
