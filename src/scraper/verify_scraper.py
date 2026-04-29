"""Verification checks for the scraper module.

Run standalone:
    python src/scraper/verify_scraper.py

Or via the pipeline:
    .\\run_scraper.ps1 -Verify

Column note: applicant_labels.csv stores the column as
"Applicant" on disk, but ls.load_applicant_labels() renames
it to "Normalised Applicant" in memory.  listings.csv
stores it as "Normalised Applicant" on disk.  Both loaded
DataFrames therefore use "Normalised Applicant".
"""

import sys
from pathlib import Path

import pandas as pd
import yaml

MODULE_DIR = Path(__file__).resolve().parent
SRC_DIR = MODULE_DIR.parent

sys.path.insert(0, str(SRC_DIR))
sys.path.insert(0, str(SRC_DIR / "labeller"))

from core.constants import PROJECT_DIR  # noqa: E402
from core.logger_config import get_logger  # noqa: E402
import labeller.label_loading_saving as ls  # type: ignore  # noqa: E402

log = get_logger(__file__)

# Load scraper config for verification thresholds
_cfg_path = MODULE_DIR / "config" / "config.yaml"
_cfg = {}
if _cfg_path.exists():
    with open(_cfg_path) as _f:
        _cfg = yaml.safe_load(_f) or {}

RUN_CONTROL_CSV = PROJECT_DIR / "logs" / "scraper_run_logs.csv"
DAYS_BETWEEN_RUNS = int(_cfg.get("days_between_runs", 1))


class VerificationResult:
    """Collects pass/fail/warn results."""

    def __init__(self):
        self.results = []

    def passed(self, check, detail=""):
        self.results.append(("PASS", check, detail))
        log.info(f"PASS: {check}")

    def failed(self, check, detail=""):
        self.results.append(("FAIL", check, detail))
        log.error(f"FAIL: {check} — {detail}")

    def warned(self, check, detail=""):
        self.results.append(("WARN", check, detail))
        log.warning(f"WARN: {check} — {detail}")

    def summary(self):
        total = len(self.results)
        passes = sum(1 for s, _, _ in self.results if s == "PASS")
        fails = sum(1 for s, _, _ in self.results if s == "FAIL")
        warns = sum(1 for s, _, _ in self.results if s == "WARN")
        log.info(
            f"Verification: {passes}/{total} passed, "
            f"{fails} failed, {warns} warnings"
        )
        return fails == 0


def _check_applicant_label_consistency(v):
    """Check that labelled applicants in listings.csv
    match those in applicant_labels.csv.

    Both DataFrames use "Normalised Applicant" after
    loading (applicant_labels.csv renames from "Applicant"
    on load via ls.load_applicant_labels).
    """
    listings = ls.load_listings(update_labels=False)
    applicant_labels = ls.load_applicant_labels()

    # Build lookup of applicant -> label from
    # applicant_labels.csv (non-empty labels only)
    al_lookup = (
        applicant_labels.loc[
            applicant_labels["Applicant Label"] != "",
            ["Normalised Applicant", "Applicant Label"],
        ]
        .drop_duplicates()
        .set_index("Normalised Applicant")["Applicant Label"]
        .to_dict()
    )

    # Listings rows that have a non-empty Applicant Label
    labelled_listings = listings.loc[
        listings["Applicant Label"] != "",
        ["Normalised Applicant", "Applicant Label"],
    ].drop_duplicates()

    # Check 1: Every labelled listing row has a
    # matching entry in applicant_labels.csv
    missing_from_al = []
    mismatched = []
    for _, row in labelled_listings.iterrows():
        name = row["Normalised Applicant"]
        label = row["Applicant Label"]
        if name not in al_lookup:
            missing_from_al.append(name)
        elif al_lookup[name] != label:
            mismatched.append(
                f"{name}: listings='{label}' " f"vs labels='{al_lookup[name]}'"
            )

    if missing_from_al:
        unique = sorted(set(missing_from_al))
        v.failed(
            "Labelled applicants in listings exist " "in applicant_labels",
            f"{len(unique)} applicant(s) labelled in "
            f"listings.csv but missing from "
            f"applicant_labels.csv: "
            + ", ".join(unique[:10])
            + ("..." if len(unique) > 10 else ""),
        )
    else:
        v.passed("Labelled applicants in listings exist " "in applicant_labels")

    if mismatched:
        v.failed(
            "Applicant labels match between " "listings and applicant_labels",
            f"{len(mismatched)} mismatch(es): "
            + "; ".join(mismatched[:5])
            + ("..." if len(mismatched) > 5 else ""),
        )
    else:
        v.passed("Applicant labels match between " "listings and applicant_labels")

    # Check 2: Every applicant with a label in
    # applicant_labels.csv also has that label in
    # listings.csv (catches stale/empty labels)
    listings_label_lookup = (
        listings[["Normalised Applicant", "Applicant Label"]]
        .drop_duplicates()
        .set_index("Normalised Applicant")["Applicant Label"]
        .to_dict()
    )
    unlabelled_in_listings = []
    for name, al_label in al_lookup.items():
        listings_label = listings_label_lookup.get(name, "")
        if listings_label == "" and al_label != "":
            unlabelled_in_listings.append(name)

    if unlabelled_in_listings:
        v.failed(
            "Applicant labels in applicant_labels " "are applied in listings",
            f"{len(unlabelled_in_listings)} applicant(s) "
            f"labelled in applicant_labels.csv but "
            f"unlabelled in listings.csv: "
            + ", ".join(sorted(unlabelled_in_listings)[:10])
            + ("..." if len(unlabelled_in_listings) > 10 else ""),
        )
    else:
        v.passed("Applicant labels in applicant_labels " "are applied in listings")

    # Check 3: Every labelled applicant in
    # applicant_labels.csv exists in listings.csv
    listings_applicants = set(listings["Normalised Applicant"].unique())
    orphaned = [name for name in al_lookup if name not in listings_applicants]
    if orphaned:
        v.warned(
            "All labelled applicants in " "applicant_labels appear in listings",
            f"{len(orphaned)} applicant(s) in "
            f"applicant_labels.csv not found in "
            f"listings.csv: "
            + ", ".join(sorted(orphaned)[:10])
            + ("..." if len(orphaned) > 10 else ""),
        )
    else:
        v.passed("All labelled applicants in " "applicant_labels appear in listings")


def _check_run_schedule(v):
    """Check if the last run date was within DAYS_BETWEEN_RUNS.

    This verification warns if a run occurred too recently
    (i.e., less than DAYS_BETWEEN_RUNS since the last run).
    """
    run_control = pd.read_csv(RUN_CONTROL_CSV)

    if len(run_control) == 0:
        v.warned(
            "Run schedule verification",
            "No run history found",
        )
        return

    last_run_date = pd.to_datetime(run_control["Date"].iloc[-1])
    today = pd.to_datetime("today").normalize()
    days_since = (today - last_run_date).days

    if days_since < DAYS_BETWEEN_RUNS:
        v.warned(
            "Last run was within DAYS_BETWEEN_RUNS",
            f"Last run: "
            f"{last_run_date.strftime('%Y-%m-%d')}, "
            f"Days since: {days_since}, "
            f"Required: {DAYS_BETWEEN_RUNS}",
        )
    else:
        v.passed(
            "Last run was within DAYS_BETWEEN_RUNS",
            f"Last run: "
            f"{last_run_date.strftime('%Y-%m-%d')}, "
            f"Days since: {days_since}",
        )


def verify():
    """Run all verification checks."""
    v = VerificationResult()
    _check_applicant_label_consistency(v)
    _check_run_schedule(v)
    return v.summary()


if __name__ == "__main__":
    success = verify()
    sys.exit(0 if success else 1)
