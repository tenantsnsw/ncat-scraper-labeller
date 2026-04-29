import argparse
import datetime as dt
import os
import shutil
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parents[1]))

from core.logger_config import get_logger  # noqa: E402

import constants  # noqa: E402
import scraper_methods as sm  # noqa: E402

logger = get_logger(__file__)


def clear_downloads(verbose=True, debug=False):
    if os.path.exists(constants.LISTINGS_FILE):
        os.remove(constants.LISTINGS_FILE)
        if verbose:
            logger.info("Listings file deleted")
    else:
        if verbose:
            logger.info("Downloads director is clear")


def build_api_query(date_string):
    return constants.API_QUERY_FORMAT.format(date=date_string)


# Data list methods


def load_data_list():
    return pd.read_csv(constants.DATA_LIST_CSV)


def save_data_list(data_list):
    data_list.to_csv(constants.DATA_LIST_CSV, index=False)


def rebuild_data_list(start, end):
    # Create some dummy dates - This is only for rebuilding the data set
    all_dates = pd.date_range(start, end)
    data_list = pd.DataFrame(data={"Date": all_dates})
    data_list["Date"] = data_list["Date"].dt.strftime("%Y-%m-%d")
    data_list["File Name"] = data_list["Date"].apply(
        lambda x: str(constants.RAW_DATA_DIR / f"listings_{x}.csv")
    )


def recalculate_data_list(data_list, force_download=False):
    data_list["File Exists"] = data_list["File Name"].apply(lambda x: os.path.exists(x))

    available_days = pd.date_range(
        start=pd.to_datetime("today").normalize() + pd.DateOffset(days=-7),
        end=pd.to_datetime("today").normalize() + pd.DateOffset(days=3 * 7),
    ).strftime("%Y-%m-%d")

    data_list["Available"] = data_list["Date"].isin(available_days)
    data_list["Weekday"] = pd.to_datetime(data_list["Date"]).dt.day_of_week < 5
    data_list["Days Till Unavailable"] = data_list["Date"].apply(
        calculate_days_till_unavailable
    )
    data_list["Days Till Available"] = data_list["Date"].apply(
        calculate_days_till_available
    )
    data_list["Days Old"] = (
        pd.to_datetime("today").normalize() - pd.to_datetime(data_list["Last Updated"])
    ).dt.days
    if force_download:
        data_list["To Download"] = data_list["Available"]
    else:
        data_list["To Download"] = data_list["Available"] * (data_list["Days Old"] > 0)
    return data_list


def read_last_update_date(file_name):
    if os.path.exists(file_name):
        return dt.datetime.fromtimestamp(Path(file_name).stat().st_mtime).strftime(
            "%Y-%m-%d"
        )
    else:
        return ""


def calculate_days_till_unavailable(date_string):
    days = (
        (pd.to_datetime(date_string) - pd.to_datetime("today").normalize()).days
        + constants.DAYS_AVAILABLE_BACKWARDS
        + 1
    )
    if days >= 0:
        return days
    else:
        return 0


def calculate_days_till_available(date_string):
    days = (
        pd.to_datetime(date_string) - pd.to_datetime("today").normalize()
    ).days - constants.DAYS_AVAILABLE_FORWARDS
    if days >= 0:
        return days
    else:
        return 0


def add_next_day_to_data_list(data_list, force_download=False):
    new_row = data_list.loc[len(data_list) - 1].copy()
    new_row["Date"] = (
        pd.to_datetime(new_row["Date"]) + pd.DateOffset(days=1)
    ).strftime("%Y-%m-%d")
    new_row["File Name"] = str(
        constants.RAW_DATA_DIR / f"listings_{new_row['Date']}.csv"
    )

    new_row = pd.DataFrame([new_row.values], columns=new_row.index)

    data_list = pd.concat([data_list, new_row], ignore_index=True)
    data_list = recalculate_data_list(data_list, force_download=force_download)
    return data_list


def add_days_to_data_list(data_list, days_forward=14, force_download=False):
    while data_list["Days Till Available"].max() < days_forward:
        data_list = add_next_day_to_data_list(data_list, force_download=force_download)
    return data_list


def number_of_listings(file_name):
    if os.path.exists(file_name):
        return len(pd.read_csv(file_name))
    else:
        return None


# Run Controls


def calculate_next_run(run_control):
    last_successful_run = pd.to_datetime(
        run_control[run_control["Success"]]["Date"], format="%Y-%m-%d"
    ).max()
    next_run = last_successful_run + pd.DateOffset(days=constants.DAYS_BETWEEN_RUNS)
    while (
        dt.datetime.weekday(next_run) >= 5
    ):  # If next run lands on Sat or Sun, make it Friday
        next_run = next_run + pd.DateOffset(days=-1)
    return next_run.strftime("%Y-%m-%d")


def run_now(
    run_control,
    verbose=True,
    debug=False,
    force=False,
):
    next_run_date = pd.to_datetime(calculate_next_run(run_control))
    today = pd.to_datetime("today").normalize()
    days_until_run = (next_run_date - today).days
    run_now_bool = days_until_run <= 0 or force

    if verbose:
        logger.info(f"Next scheduled run: {next_run_date.strftime('%Y-%m-%d')}")
        logger.info(f"Today: {today.strftime('%Y-%m-%d')}")
        logger.info(f"Days until scheduled run: {days_until_run}")
        if force:
            logger.info("Force flag enabled - running regardless of schedule")
        else:
            logger.info(f"Running now?: {run_now_bool}")
    return run_now_bool


def load_run_control():
    return pd.read_csv(constants.RUN_CONTROL_CSV)


def save_run_control(run_control):
    run_control.to_csv(constants.RUN_CONTROL_CSV, index=False)


def calculate_min_day_available():
    min_day = pd.to_datetime("today") + pd.DateOffset(
        days=-constants.DAYS_AVAILABLE_BACKWARDS
    )
    min_day = min_day.strftime("%Y-%m-%d")
    return min_day


def calculate_max_day_available():
    max_day = pd.to_datetime("today") + pd.DateOffset(
        days=constants.DAYS_AVAILABLE_FORWARDS
    )
    max_day = max_day.strftime("%Y-%m-%d")
    return max_day


def today_as_string():
    return pd.to_datetime("today").strftime("%Y-%m-%d")


def calculate_days_between_runs(run_control):
    last_run_attempt = run_control["Date"].iloc[-1]
    days_since_last_run = (
        pd.to_datetime("today") - pd.to_datetime(last_run_attempt)
    ).days
    return days_since_last_run


def log_run_attempt(run_control, success=False):
    if run_control["Date"].isin([today_as_string()]).any():
        run_control.loc[run_control["Date"] == today_as_string(), "Success"] = success
    else:
        run_control.loc[len(run_control)] = [
            today_as_string(),
            success,
            calculate_min_day_available(),
            calculate_max_day_available(),
            calculate_days_between_runs(run_control),
        ]
    return run_control


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Scrape NCAT court listings.")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_false",
        help="Enable debug output",
    )
    parser.add_argument(
        "-f",
        "--force",
        action="store_true",
        help="Force download regardless of schedule",
    )
    parser.add_argument(
        "--no-headless",
        action="store_false",
        dest="headless",
        help="Disable headless mode (show browser window)",
    )
    args = parser.parse_args()

    verbose = args.verbose
    debug = args.debug
    force = args.force
    headless = args.headless

    # Need a readable log for each time this is run - external logging method
    # log = init_log()

    run_control = load_run_control()

    if run_now(run_control, verbose=verbose, debug=debug, force=force):

        run_control = log_run_attempt(run_control, success=False)

        data_list = recalculate_data_list(load_data_list(), force_download=force)

        data_list = add_days_to_data_list(data_list, force_download=force)

        driver = sm.init_driver(headless=headless)

        # Skipping the init captcha part as making api requests directly currently works
        # init_captcha(driver, verbose=True) # Arrive at the captcha page
        # This should wait for a long time for the user to do captcha
        # wait_for_captcha(driver, verbose=True)

        clear_downloads(verbose=verbose, debug=debug)
        # Remove any existing files called 'listings.csv' to simplify moving data
        try:
            to_download = data_list[data_list["To Download"]]
            if len(to_download) > 0:
                date_range = (
                    f"{to_download['Date'].iloc[0]} to "
                    f"{to_download['Date'].iloc[-1]}"
                )
                logger.info(f"Downloading listings for date range: {date_range}")
            for row_index, row in to_download.iterrows():
                # Clears the downloads folder downloads folder of any listings.csv files
                clear_downloads(verbose=verbose, debug=debug)
                sm.api_call(
                    driver, build_api_query(row["Date"]), verbose=verbose, debug=debug
                )
                sm.rand_wait(2, verbose=verbose, debug=debug)
                file_name = str(constants.RAW_DATA_DIR / f"listings_{row['Date']}.csv")
                shutil.move(constants.LISTINGS_FILE, file_name)
                data_list.loc[row_index, "Last Updated"] = today_as_string()
                data_list.loc[row_index, "File Exists"] = os.path.exists(file_name)
                data_list.loc[row_index, "Number Of Listings"] = number_of_listings(
                    file_name
                )

            run_control = log_run_attempt(run_control, success=True)

        except Exception as e:
            run_control = log_run_attempt(run_control, success=False)
            if verbose:
                logger.error(f"Last row data: {row}")
                logger.error(f"Exception raised: {e}")

        driver.quit()
        save_data_list(data_list)
        save_run_control(run_control)
