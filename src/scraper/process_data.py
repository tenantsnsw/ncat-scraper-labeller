"""
Incrementally processes raw NCAT court listing CSVs into the combined
processed/listings.csv.

Runs after scrape_court_listings.py in the scraper pipeline.
Only raw files whose date is later than the latest date already in
listings.csv are processed.  Case-level aggregates (Primary Listing Type,
Case Open Date) are recomputed over the full combined dataset.
"""

import sys
import re
import hashlib
from pathlib import Path

import pandas as pd

# sys.path setup — must come before importing labeller modules.
# labeller/ first so its bare `import constants` finds labeller's constants.
# src/ second so `import scraper.constants` (namespace package) works.
_src_dir = Path(__file__).parents[1]
sys.path.insert(0, str(_src_dir / "labeller"))
sys.path.insert(0, str(_src_dir))

from core.logger_config import get_logger  # noqa: E402

import scraper.constants  # noqa: E402
import label_loading_saving as ls  # type: ignore  # noqa: E402

log = get_logger(__file__)

# Columns used to generate UIDs — matches the archive process_data.py ordering
_UID_INPUT_COLS = [
    "Date",
    "Time",
    "Case Number",
    "Address",
    "Listing Type",
    "Presiding Officers",
    "Location",
    "Court Room",
    "Applicant",
    "Respondent",
    "Postcode",
    "Old Data",
    "List",
    "Year",
]

# Final column order — matches current processed/listings.csv schema
_FINAL_COLS = [
    "UID",
    "Date",
    "Time",
    "Case Number",
    "Address",
    "Listing Type",
    "Presiding Officers",
    "Location",
    "Court Room",
    "Applicant",
    "Respondent",
    "Postcode",
    "Old Data",
    "List",
    "Year",
    "Normalised Applicant",
    "Applicant Label",
    "Social Housing Provider",
    "Entity Classification",
    "Primary Listing Type",
    "Case Open Date",
]


def _load_normalise_dict() -> dict:
    path = Path(__file__).parent / "config" / "normalise_dict.csv"
    df = pd.read_csv(path)
    return dict(zip(df["pattern"], df["replacement"]))


def _unique_name_from_str(string: str, last_idx: int = 12) -> str:
    m = hashlib.md5()
    m.update(string.encode("utf-8"))
    return str(m.hexdigest())[0:last_idx]


def _create_uid(df: pd.DataFrame) -> pd.Series | None:
    """Hash _UID_INPUT_COLS to produce a 12-char UID per row."""
    work = df[_UID_INPUT_COLS].copy()
    work["Old Data"] = work["Old Data"].apply(str)
    work["Year"] = work["Year"].apply(str)
    uid = work.fillna("").agg("_".join, axis=1).apply(_unique_name_from_str)
    if uid.duplicated().any():
        log.error("UID collision detected — UID generation failed")
        return None
    return uid


def _date_from_file(path: Path) -> pd.Timestamp:
    return pd.to_datetime(path.stem.split("_")[-1], format="%Y-%m-%d")


def _clean_one_file(path: Path) -> pd.DataFrame | None:
    """Read one raw listings CSV and return a cleaned DataFrame."""
    df = pd.read_csv(path)
    if len(df) == 0:
        return None

    df.columns = df.columns.str.title()

    file_date = _date_from_file(path)
    year = file_date.year

    # Resolve Date: parse "DD Mon" from data, fall back to file date on mismatch
    df["File Date"] = file_date.strftime("%Y-%m-%d")
    df["Year"] = year
    df["Inferred Date"] = df["Date"] + " " + df["Year"].astype(str)
    if len(df["Date"].unique()) > 1:
        log.warning(f"{path.name} Multiple dates in file — using file date")
        df["Date"] = df["File Date"]
    elif not (
        pd.to_datetime(df["Inferred Date"], format="%d %b %Y")
        == pd.to_datetime(df["File Date"])
    ).all():
        log.warning(f"{path.name} Date mismatch — using inferred date from data")
        df["Date"] = pd.to_datetime(df["Inferred Date"], format="%d %b %Y").dt.strftime(
            "%Y-%m-%d"
        )
    else:
        df["Date"] = df["File Date"]
    df = df.drop(columns=["File Date", "Year", "Inferred Date"])

    # Split Case Name → Applicant / Respondent
    def _split_case_name(case_name):
        if " v " in case_name:
            parts = case_name.split(" v ", 1)
            return parts[0], parts[1]
        log.warning(f"{case_name} Missing ' v ' — cannot split")
        return case_name, case_name

    df[["Applicant", "Respondent"]] = pd.DataFrame(
        df["Case Name"].apply(_split_case_name).tolist(), index=df.index
    )

    df = df.drop(
        columns=[
            "Court",
            "Jurisdiction",
            "Parties",
            "Court House",
            "List Number",
            "Case Name",
        ]
    )

    # Clean Location: strip "NCAT " prefix and " (CCD)" suffix
    pattern = "|".join([r"NCAT ", r" \(CCD\)"])
    df["Location"] = df["Location"].str.replace(pattern, "", regex=True)

    # Extract Postcode from Address (last 4 chars if numeric)
    def _extract_postcode(address):
        postcode = str(address)[-4:]
        if postcode.isnumeric():
            return postcode
        return ""

    df["Postcode"] = df["Address"].apply(_extract_postcode)

    return df


def _normalise_names(df: pd.DataFrame) -> pd.DataFrame:
    """Produce 'Normalised Applicant' from 'Applicant'."""
    normalise_dict = _load_normalise_dict()
    col = "Applicant"
    df["Normalised Applicant"] = df[col].str.lower()
    norm = "Normalised Applicant"
    df[norm] = df[norm].replace(scraper.constants.REPLACE_SPACES, regex=True)
    df[norm] = df[norm].replace(normalise_dict, regex=True)
    df[norm] = df[norm].replace(scraper.constants.REPLACE_SPECIAL, regex=True)
    df[norm] = df[norm].replace(scraper.constants.REPLACE_SPACES, regex=True)
    df[norm] = df[norm].str.strip()
    df[norm] = df[norm].str.title()
    df[norm] = df[norm].replace(scraper.constants.FIX_APPOST_S, regex=True)
    return df


def _apply_primary_listing_type(df: pd.DataFrame) -> pd.DataFrame:
    """Assign Primary Listing Type via ranked match across all listing types
    for each Case Number."""
    ranked = scraper.constants.PRIMARY_LISTING_TYPE_RANK

    def _ranked_label(label_list):
        if len("".join(label_list)) > 0:
            for label in ranked:
                if label in label_list:
                    return label
            return "Other"
        return ""

    agg = (
        df[["Case Number", "Listing Type"]]
        .groupby("Case Number")["Listing Type"]
        .agg(list)
        .rename("Case Listing Types")
        .reset_index()
    )
    df = df.merge(agg, how="left")
    df["Primary Listing Type"] = df["Case Listing Types"].apply(_ranked_label)
    df = df.drop(columns="Case Listing Types")
    return df


def _apply_case_open_date(df: pd.DataFrame) -> pd.DataFrame:
    """Assign Case Open Date as the minimum Date for each Case Number."""
    agg = (
        df[["Case Number", "Date"]]
        .groupby("Case Number")["Date"]
        .min()
        .rename("Case Open Date")
        .reset_index()
    )
    df = df.merge(agg, how="left")
    return df


def _refresh_labels(listings: pd.DataFrame) -> None:
    """Re-apply applicant labels from applicant_labels.csv
    to all rows in listings and save."""
    applicant_labels = ls.load_applicant_labels()
    updated = ls.update_data_labels(listings, applicant_labels)

    # Drop then recompute case-level aggregates
    updated = updated.drop(
        columns=["Primary Listing Type", "Case Open Date"],
        errors="ignore",
    )
    updated = _apply_primary_listing_type(updated)
    updated = _apply_case_open_date(updated)
    updated = updated[_FINAL_COLS]
    ls.save_listings(updated)
    log.info("Refreshed applicant labels on existing data.")


def run():
    raw_dir = Path(scraper.constants.RAW_DATA_DIR)

    # Load existing processed listings to find the last processed date
    existing = ls.load_listings()
    max_processed_date = pd.to_datetime(existing["Date"]).max()
    log.info(f"Latest processed date: {max_processed_date.date()}")

    # Find raw files with a date strictly after the latest processed date
    raw_files = sorted(raw_dir.glob("listings_*.csv"))
    new_files = [f for f in raw_files if _date_from_file(f) > max_processed_date]

    if not new_files:
        log.info("No new raw files to process.")
        # Still refresh labels on existing data
        _refresh_labels(existing)
        return (
            f"process_data: no new files (latest processed: "
            f"{max_processed_date.date()})"
        )

    log.info(f"Processing {len(new_files)} new raw file(s)...")

    # Clean each new file
    cleaned = [_clean_one_file(f) for f in new_files]
    cleaned = [df for df in cleaned if df is not None]

    if not cleaned:
        log.info("No data found in new raw files.")
        return f"process_data: {len(new_files)} new file(s), 0 rows after cleaning"

    new_df = pd.concat(cleaned, ignore_index=True)
    new_df["Old Data"] = False
    new_df["List"] = ""
    new_df["Year"] = pd.to_datetime(new_df["Date"]).dt.year

    # Drop exact duplicate rows (API can return the
    # same listing twice in a single response)
    pre_dedup = len(new_df)
    new_df = new_df.drop_duplicates(
        subset=_UID_INPUT_COLS, ignore_index=True
    )
    n_dupes = pre_dedup - len(new_df)
    if n_dupes:
        log.info(f"Dropped {n_dupes} duplicate row(s).")

    # Generate UIDs and assert no collision with existing
    uid = _create_uid(new_df)
    if uid is None:
        raise RuntimeError("UID generation failed — duplicate UIDs in new data")
    collision = uid.isin(existing["UID"])
    if collision.any():
        raise RuntimeError(
            f"UID collision with existing data: " f"{uid[collision].tolist()}"
        )
    new_df.insert(0, "UID", uid)

    # Normalise applicant names
    new_df = _normalise_names(new_df)

    # Ensure Date is datetime before combining (existing
    # data is parsed via parse_dates, new data is string)
    new_df["Date"] = pd.to_datetime(new_df["Date"])

    # Combine with existing
    combined = pd.concat(
        [existing, new_df], ignore_index=True
    )

    # Re-apply applicant labels to all rows so that
    # label changes in applicant_labels.csv propagate
    # to existing listings
    applicant_labels = ls.load_applicant_labels()
    combined = ls.update_data_labels(
        combined, applicant_labels
    )

    # Drop old aggregates then recompute over full dataset
    combined = combined.drop(
        columns=["Primary Listing Type", "Case Open Date"],
        errors="ignore",
    )
    combined = _apply_primary_listing_type(combined)
    combined = _apply_case_open_date(combined)

    # Fix malformed Time values
    combined["Time"] = combined["Time"].str.extract(
        r"(\d{1,2}:\d{1,2}\s[ap]m)", expand=False, flags=re.IGNORECASE
    )

    # Reorder to match schema
    combined = combined[_FINAL_COLS]

    ls.save_listings(combined)

    # Update applicant_labels.csv with any new normalised names
    applicant_labels = ls.update_applicant_labels_from_listings(
        applicant_labels=applicant_labels,
        listings=combined,
    )
    ls.save_applicant_labels(applicant_labels)

    summary = (
        f"process_data: {len(new_files)} new file(s), "
        f"{len(new_df):,} new rows added "
        f"(total: {len(combined):,})"
    )
    log.success(summary)
    return summary


if __name__ == "__main__":
    run()
