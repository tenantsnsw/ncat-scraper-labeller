import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[1]))

from oauth2client.service_account import ServiceAccountCredentials
import label_loading_saving as ls
from core.logger_config import get_logger, post_log_update, format_traceback

import time
import numpy as np
import constants
from tqdm import tqdm
import gspread

log = get_logger(__file__)


def format_batch_update(df, splits=100):
    def a1_range_notation(row_start, row_end, col_start, col_end):
        start_cell = f"{chr(ord('a') + col_start - 1).upper()}{row_start}"
        end_cell = f"{chr(ord('a') + col_end - 1).upper()}{row_end}"
        return f"{start_cell}:{end_cell}"

    n_cols = df.shape[1]
    row_start = 1
    col_start = 1
    col_end = n_cols
    row_end = 1
    df_list = np.array_split(df, splits)
    batch_update_data = [
        {
            "range": a1_range_notation(row_start, row_end, col_start, col_end),
            "values": [df.columns.tolist()],
        }
    ]
    for df_part in df_list:
        row_start = row_end + 1
        row_end = row_start + len(df_part) - 1
        batch_update_data.append(
            {
                "range": a1_range_notation(row_start, row_end, col_start, col_end),
                "values": df_part.values.tolist(),
            }
        )
    return batch_update_data


def update_gsheet(spreadsheet, update_sheet_name, sheet) -> bool:
    log.info(f"Updating {update_sheet_name}")
    all_column_names = spreadsheet.worksheet(update_sheet_name).row_values(1)
    cols_to_search = [chr(ord("@") + j) for j in range(1, 27)][
        : len(all_column_names)
    ]
    column_to_clear = {
        all_column_names[j]: cols_to_search[j]
        for j in range(len(all_column_names))
    }
    column_names_to_keep = ["Reduced Applicant Label"]
    column_to_clear = {
        k: v
        for k, v in column_to_clear.items()
        if k not in column_names_to_keep
    }
    spreadsheet.worksheet(update_sheet_name).batch_clear(
        [v + ":" + v for k, v in column_to_clear.items()]
    )
    batches = format_batch_update(sheet, splits=10)
    batch_update_success = True
    for val in tqdm(batches, total=len(batches)):
        try:
            spreadsheet.worksheet(update_sheet_name).update(
                val["values"], val["range"]
            )
        except Exception as exc:
            batch_update_success = False
            log.error(f"Batch update error: {exc}")
    if update_sheet_name in ("To Label - All", "To Label - This Month"):
        spreadsheet.worksheet(update_sheet_name).add_validation(
            "B2:B",
            gspread.utils.ValidationConditionType.one_of_range,
            values=["=provider_group"],
            showCustomUi=True,
        )
    if batch_update_success:
        log.info(f"Update to {update_sheet_name} successful")
    return batch_update_success


def run() -> str:
    t0 = time.monotonic()
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.file",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        constants.GOOGLE_CREDENTIALS_PATH, scope
    )
    client = gspread.authorize(credentials)
    spreadsheet = client.open("Social Housing NCAT Labelling")

    providers = ls.load_providers()
    to_update = {
        # "To Label - All": ls.to_label_column_saving(ls.load_to_label(...)),
        # "To Label - This Month": ...,
        "Providers": ls.providers_column_saving(providers).fillna(""),
    }

    results = []
    for sheet_name, sheet_df in to_update.items():
        ok = update_gsheet(spreadsheet, sheet_name, sheet_df)
        results.append(f"{sheet_name}: {'ok' if ok else 'FAILED'}")

    elapsed = round(time.monotonic() - t0)
    summary = (
        "update_gsheets: " + ", ".join(results) + f", {elapsed}s elapsed"
    )
    log.success(summary)
    return summary


if __name__ == "__main__":
    try:
        # Settings
        verbose = True
        connect_gsheet_bool = True
        update_gsheet_bool = True

        if connect_gsheet_bool:
            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive.file",
                "https://www.googleapis.com/auth/drive",
            ]
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                constants.GOOGLE_CREDENTIALS_PATH, scope
            )
            client = gspread.authorize(credentials)
            spreadsheet = client.open("Social Housing NCAT Labelling")

        to_label_month = ls.load_to_label(
            filename=constants.PROCESSED_DATA_DIR / "to_label_month.csv",
        )
        to_label_month = ls.to_label_column_saving(to_label_month)
        to_label_all = ls.load_to_label(
            filename=constants.PROCESSED_DATA_DIR / "to_label_all.csv"
        )
        to_label_all = ls.to_label_column_saving(to_label_all)
        providers = ls.load_providers()
        provider = ls.providers_column_saving(providers)
        to_update = {
            # "To Label - All": to_label_all,
            # "To Label - This Month": to_label_month,
            "Providers": providers.fillna(""),
        }

        if update_gsheet_bool:
            for update_sheet_name, sheet in to_update.items():
                log.info(f"Updating {update_sheet_name}")
                all_column_names = spreadsheet.worksheet(update_sheet_name).row_values(
                    1
                )
                cols_to_search = [chr(ord("@") + j) for j in range(1, 27)][
                    : len(all_column_names)
                ]
                column_to_clear = {
                    all_column_names[j]: cols_to_search[j]
                    for j in range(len(all_column_names))
                }
                column_names_to_keep = ["Reduced Applicant Label"]
                column_to_clear = {
                    k: v
                    for k, v in column_to_clear.items()
                    if k not in column_names_to_keep
                }
                spreadsheet.worksheet(update_sheet_name).batch_clear(
                    [v + ":" + v for k, v in column_to_clear.items()]
                )
                batches = {}
                batches[update_sheet_name] = format_batch_update(sheet, splits=10)

                for val in tqdm(
                    batches[update_sheet_name], total=len(batches[update_sheet_name])
                ):
                    batch_update_success = True
                    try:
                        values = val["values"]
                        range_a1 = val["range"]
                        spreadsheet.worksheet(update_sheet_name).update(
                            values, range_a1
                        )
                    except Exception as e:
                        batch_update_success = False
                        print(e)
                # This adds data validation to the "Applicant Label" column
                if (
                    update_sheet_name == "To Label - All"
                    or update_sheet_name == "To Label - This Month"
                ):
                    spreadsheet.worksheet(update_sheet_name).add_validation(
                        "B2:B",
                        gspread.utils.ValidationConditionType.one_of_range,
                        values=["=provider_group"],
                        showCustomUi=True,
                    )
                if batch_update_success:
                    log.info(f"Update to {update_sheet_name} successful")

        log.info("update_gsheets.py completed")
    except Exception as e:
        log.error("CRITICAL ERROR\n" + str(e) + "\n" + format_traceback())
        post_log_update(
            "CRITICAL ERROR\n"
            + f"{Path(__file__).as_posix()}\n"
            + format_traceback()
        )
        sys.exit(e)
