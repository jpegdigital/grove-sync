<#
.SYNOPSIS
    Removes the GroveSyncJob scheduled task.
.DESCRIPTION
    Idempotent: no error if the task doesn't exist.
    Must be run elevated (Administrator).
#>
param(
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

# --- Remove ---
$existing = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existing) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    Write-Host "Task '$TaskName' removed." -ForegroundColor Green
} else {
    Write-Host "Task '$TaskName' does not exist — nothing to remove." -ForegroundColor Yellow
}
