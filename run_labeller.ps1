# run_labeller.ps1
# Runs the NCAT labeller.
#
# Usage:
#   .\run_labeller.ps1   # normal run

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

Invoke-Stage "Run Labeller" "src\labeller\run_labeller.py" @()

Write-Host ""
Write-Host "=== Pipeline complete ===" -ForegroundColor Green
