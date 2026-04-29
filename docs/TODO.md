# Backlog

## Labeller

### ~~Slack: attach log file to summary post~~ DONE
- Implemented in `post_log_update()` — webhook sends summary, bot token uploads log file
- Config entries: `slack_webhook_url`, `slack_bot_token`, `slack_channel`
- Tested successfully via `update_gsheets` on 2026-03-23

### Memory optimisation: `build_bag_of_words_features`
- Peak alloc reduced from ~21 GB to ~5 GB by switching to `int16` + immediate `del`
- Further reduction possible by keeping the CountVectorizer output as a sparse matrix instead of converting to dense DataFrame via `.toarray()`
- Would require changes downstream (model training, feature merging) to handle sparse input

### Warnings to clean up
- `DataFrame is highly fragmented` in `label_loading_saving.py:615` — add `features_df = features_df.copy()` before column assignment
- `UndefinedMetricWarning` in `produce_classification_report` — pass `zero_division=0` to `classification_report()`
- `FutureWarning: DataFrame.swapaxes` in `update_gsheets.py` — replace `.swapaxes` with `.transpose` (triggered by gspread internals, may need gspread upgrade)

### Replace `print()` with logger
- Several `print()` calls remain in `prepare_to_label.py` and `update_jaccard_features.py`
- These appear in stdout but not in the log file
- Replace with `log.info()` / `log.debug()` for consistent logging

### Move inline verification to `verify_scraper.py`
- `process_data.py:57-66, 228-236` — UID collision detection (duplicate hash check within new data + collision against existing UIDs). Add as a post-run audit: scan full `listings.csv` for duplicate UIDs
- `process_data.py:88-100` — date consistency checks (multiple dates in file, date mismatch between data and filename). Add as a check: verify all listing dates fall on weekdays and within expected range
- `process_data.py:104-109` — case name splitting validation (prints warning when " v " separator is missing). Add as a check: report listings where Applicant == Respondent (failed split)
- These checks currently run inline during processing; standalone versions in `verify_scraper.py` would allow auditing the full processed dataset independently of a pipeline run

### Move inline verification to `verify_labeller.py`
- `update_applicant_labels.py:50-63` — label conflict detection (`check_conflicts`): raises `ValueError` if the same `Normalised Applicant` has two different `Applicant Label` values from Google Sheets. Add as a standalone audit check in a new `verify_labeller.py`
- `label_loading_saving.py:561-563` — type safety filter: drops non-string `Normalised Applicant` values when loading features. Add as a data quality check: report any non-string entries
- `post_slack_update.py:15-16`, `logger_config.py:64-65` — Slack response assertions (`assert status_code == 200`). Consider adding a config completeness check: verify `SLACK_WEBHOOK_URL`, `SLACK_BOT_TOKEN`, `SLACK_CHANNEL` are configured

### Human-readable elapsed times
- Elapsed times in summaries and profiler output show raw seconds (e.g. `1784s elapsed`)
- Format as `29m 44s` for readability in logs and Slack messages
- Affects: `profile_resources()` in `profiler.py`, `run()` summaries in all 5 scripts