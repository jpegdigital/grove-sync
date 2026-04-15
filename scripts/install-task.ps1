<#
.SYNOPSIS
    Registers (or re-registers) the GroveSyncJob scheduled task.
.DESCRIPTION
    Idempotent: removes any existing task with the same name first.
    Defaults to running every 4 hours. Override with -IntervalHours.
    Must be run elevated (Administrator) to register the task.
.EXAMPLE
    .\install-task.ps1                    # every 4 hours (default)
    .\install-task.ps1 -IntervalHours 6   # every 6 hours
#>
param(
    [int]$IntervalHours = 4,
    [string]$TaskName = "GroveSyncJob"
)

$ErrorActionPreference = "Stop"

# --- Elevation check ---
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole(
    [Security.Principal.WindowsBuiltInRole]::Administrator
)
if (-not $isAdmin) {
    Write-Error "This script must be run as Administrator. Right-click PowerShell → Run as Administrator."
    exit 1
}

# --- Paths ---
$repoRoot   = Split-Path -Parent $PSScriptRoot
$jobScript  = Join-Path $repoRoot "scripts\grove-sync-job.ps1"

if (-not (Test-Path $jobScript)) {
    Write-Error "Runner script not found at: $jobScript"
    exit 1
}

# --- Idempotent: remove existing task ---
$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Write-Host "Removing existing task '$TaskName'..."
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

# --- Build task components ---
$action = New-ScheduledTaskAction `
    -Execute "pwsh.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$jobScript`"" `
    -WorkingDirectory $repoRoot

# Repeat every N hours, for an indefinite duration, starting now
$trigger = New-ScheduledTaskTrigger -Once -At (Get-Date) `
    -RepetitionInterval (New-TimeSpan -Hours $IntervalHours)

# Run whether logged in or not is complex (needs password); keep it simple:
# run only when logged in, don't stop if on batteries, allow start if missed
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2) `
    -MultipleInstances IgnoreNew

$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType Interactive -RunLevel Limited

# --- Enable task history (global setting, requires the event log to be active) ---
$logName = "Microsoft-Windows-TaskScheduler/Operational"
$log = Get-WinEvent -ListLog $logName -ErrorAction SilentlyContinue
if ($log -and -not $log.IsEnabled) {
    $log.IsEnabled = $true
    $log.SaveChanges()
    Write-Host "Enabled Task Scheduler history ($logName)"
} else {
    Write-Host "Task Scheduler history already enabled"
}

# --- Register ---
Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -Principal $principal `
    -Description "Grove Sync: fetch YouTube metadata, score, download, and upload to R2 (every ${IntervalHours}h)" `
    | Out-Null

Write-Host ""
Write-Host "Task '$TaskName' registered successfully." -ForegroundColor Green
Write-Host "  Schedule  : Every $IntervalHours hour(s)"
Write-Host "  Runner    : $jobScript"
Write-Host "  Working dir: $repoRoot"
Write-Host "  Logs      : $repoRoot\logs\"
Write-Host ""
Write-Host "To verify:  Get-ScheduledTask -TaskName $TaskName | Format-List"
Write-Host "To run now: Start-ScheduledTask -TaskName $TaskName"
Write-Host "To remove:  .\uninstall-task.ps1"
