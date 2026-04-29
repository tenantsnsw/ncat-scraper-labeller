# run_scraper.ps1
# Runs the NCAT scraper pipeline.
#
# Usage:
#   .\run_scraper.ps1            # normal run
#   .\run_scraper.ps1 -Verbose   # verbose output from each stage
#   .\run_scraper.ps1 -Verify    # run verification checks after pipeline
#   .\run_scraper.ps1 -Force     # force scrape even if not scheduled

param(
    [switch]$Verbose,
    [switch]$Verify,
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot

function Invoke-Stage {
    param([string]$Name, [string]$Script, [string[]]$Flags)
    Write-Host ""
    Write-Host "=== $Name ===" -ForegroundColor Cyan
    python "$root\$Script" @Flags
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: $Name failed (exit code $LASTEXITCODE). Stopping." -ForegroundColor Red
        Start-Sleep -Seconds 60
        exit $LASTEXITCODE
    }
    Write-Host "OK: $Name completed successfully." -ForegroundColor Green
}

$scrapeFlags = @()
if ($Verbose) { $scrapeFlags += "-v" }
if ($Force)   { $scrapeFlags += "--force" }

Invoke-Stage "Scrape Court Listings" "src\scraper\scrape_court_listings.py" $scrapeFlags
Invoke-Stage "Process Data"          "src\scraper\process_data.py"          $scrapeFlags
Invoke-Stage "Post Slack Update"     "src\scraper\post_slack_update.py"     $scrapeFlags

if ($Verify) {
    Invoke-Stage "Verify" "src\scraper\verify_scraper.py" @()
}

Write-Host ""
Write-Host "=== Pipeline complete ===" -ForegroundColor Green
