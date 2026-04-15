<#
.SYNOPSIS
    Runs the Grove Sync pipeline: sync → sync-process.
.DESCRIPTION
    Intended to be invoked by Windows Task Scheduler (or manually).
    Runs sync first, then sync-process (always, regardless of sync outcome).
    All output streams to the console in real time and is appended to a daily log file.
#>
param(
    [switch]$DryRun
)

$ErrorActionPreference = "Continue"

# Ensure Python uses UTF-8 for stdout/stderr (Task Scheduler defaults to cp1252)
$env:PYTHONIOENCODING = "utf-8"

# Resolve paths relative to the repo root (one level up from scripts/)
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

# --- Logging setup ---
$logDir = Join-Path $repoRoot "logs"
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }

$logFile = Join-Path $logDir "grove-sync-$(Get-Date -Format 'yyyy-MM-dd').log"

function Write-Log {
    param([string]$Message)
    $entry = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $Message"
    Write-Host $entry
    Add-Content -Path $logFile -Value $entry
}

# --- Pipeline ---
Write-Log "========== Grove Sync Job Start =========="

$extraArgs = @()
if ($DryRun) { $extraArgs += "--dry-run" }

# Step 1: sync
Write-Log "Running: uv run sync $extraArgs"
& uv run sync @extraArgs
$syncExit = $LASTEXITCODE
if ($syncExit -ne 0) {
    Write-Log "WARNING: sync exited with code $syncExit"
} else {
    Write-Log "sync completed successfully"
}

# Step 2: sync-process (always runs)
Write-Log "Running: uv run sync-process $extraArgs"
& uv run sync-process @extraArgs
$processExit = $LASTEXITCODE
if ($processExit -ne 0) {
    Write-Log "WARNING: sync-process exited with code $processExit"
} else {
    Write-Log "sync-process completed successfully"
}

Write-Log "========== Grove Sync Job End =========="
exit 0
