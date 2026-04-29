# NCAT Scraper

## Overview

Scrapes daily NCAT court listings and labels applicants as social housing providers.

- **Scraper** (`scraper`)
    - Scrapes daily NCAT court listings from the NCAT website
    - Tracks what data has been downloaded and what still needs to be collected
    - Posts updates to Slack on completion

- **Labeller** (`labeller`)
    - Pulls manual labels from Google Sheets and validates for conflicts
    - Rebuilds MinHash/LSH fuzzy matching index over applicant names
    - Engineers features and trains a Random Forest model to predict provider labels
    - Prepares to-label files and pushes them to Google Sheets for manual review
    - Updates provider registry from NRSCH data



## Folder Structure

- `src/` — Main source code
    - `scraper/` — Court listings scraper
        - `config/` — Configuration templates and Task Scheduler XML
    - `labeller/` — Social housing provider labeller
        - `config/` — Configuration templates and Google service account credentials
- `logs/` — Run logs
- `metadata/` — Data tracking files
- `reports/` — Tableau files and reports
- `run_scraper.ps1` — Scraper pipeline entry point
- `run_labeller.ps1` — Labeller pipeline entry point
- `.gitignore` — Git ignore rules
- `README.md` — This file


## Usage

### Scraper

Scrapes daily NCAT court listings.

<details>
<summary><strong>Setup</strong></summary>

1. **Configuration File:**
    - Copy the template to create your config file:
        ```sh
        cp src/scraper/config/config_template.yaml src/scraper/config/config.yaml
        ```
    - Edit `src/scraper/config/config.yaml` to set your `base_data_dir` and (optionally) `downloads_dir`.
    - If `downloads_dir` is left blank, your system's Downloads folder will be used by default.

2. **Install Requirements:**
    ```sh
    pip install -r src/scraper/requirements.txt
    ```

3. **Task Scheduler (optional):**
    - Open Task Scheduler and import `src/scraper/config/run_ncat_scraper.xml`
    - Update the **WorkingDirectory** in Actions to your repo root path
    - Defaults to running daily at 9 am with an 8-hour random delay

</details>

<details>
<summary><strong>Update</strong></summary>

Run from the repo root:

```sh
.\run_scraper.ps1
```

Or run individual scripts from `src/scraper/`:

```sh
python src/scraper/scrape_court_listings.py
python src/scraper/post_slack_update.py
```

#### downloads_dir in config.yaml

The `downloads_dir` setting determines where the scraper temporarily stores `listings.csv` before it is moved to the raw data directory. Defaults to your system's Downloads folder if left blank.

</details>


### Labeller

Labels NCAT court listing applicants as social housing providers using ML and collaborative review via Google Sheets.

<details>
<summary><strong>Setup</strong></summary>

1. **Configuration File:**
    - Copy the template to create your config file:
        ```sh
        cp src/labeller/config/config_template.yaml src/labeller/config/config.yaml
        ```
    - Edit `src/labeller/config/config.yaml` and fill in all required values:

    | Key | Description |
    |---|---|
    | `base_data_dir` | Root data directory — leave blank to default to `G:\Shared drives\Data\NCAT Scraper\` |
    | `nrsch_data_dir` | NRSCH data directory — leave blank to default to `G:\Shared drives\Data\NRSCH\` |
    | `google_credentials_path` | Path to Google service account JSON file (WARNING: SECRET) |
    | `google_sheet_id` | Google Sheet ID for the labelling workbook (WARNING: SECRET) |
    | `gid_all` | Tab ID for the "To Label - All" sheet (WARNING: SECRET) |
    | `gid_month` | Tab ID for the "To Label - This Month" sheet (WARNING: SECRET) |
    | `gid_providers` | Tab ID for the "Providers" sheet (WARNING: SECRET) |

2. **Install Requirements:**
    ```sh
    pip install -r src/labeller/requirements.txt
    ```

3. **Slack Notifications:**
    - Configure `slack_webhook_url`, `slack_bot_token`, and `slack_channel` in `config.yaml`.
    - The webhook posts summary messages; the bot token uploads the daily log file.

4. **Task Scheduler (optional):**
    - Open Task Scheduler and import `src/labeller/config/run_ncat_labeller.xml`
    - Update the **WorkingDirectory** in Actions to your repo root path
    - Defaults to running Monday and Thursday at 9 am with an 8-hour random delay

</details>

<details>
<summary><strong>Update</strong></summary>

Run from the repo root:

```sh
.\run_labeller.ps1
# or directly:
python src/labeller/run_labeller.py
```

This runs the pipeline in order:
1. `update_applicant_labels.py` — pulls labels from Google Sheets
2. `update_providers.py` — updates provider registry from NRSCH
3. `update_jaccard_features.py` — rebuilds MinHash/LSH index
4. `prepare_to_label.py` — builds ML predictions, writes `to_label_*.csv`
5. `update_gsheets.py` — pushes to-label files to Google Sheets

</details>


## Logs and Metadata

**logs/scraper_run_logs.csv**

Log of scraper run history.

| Column | Description |
|---|---|
| Date | Scraper run date |
| Success | Script execution status |
| Min day | Minimum day available (API) |
| Max day | Maximum day available (API) |
| Days Since Last Run | Days since previous run |

**metadata/scraper_listings_data.csv**

Used by `src/scraper/` to control what data it should download and has already downloaded.

| Column | Description |
|---|---|
| Date | Data date |
| Exists | Data file presence |
| Storage Location | Data storage path |
| Needs Download | Download status |
| Last Updated | Last update timestamp |
| Record Count | Number of records |
