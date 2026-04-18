<#
.SYNOPSIS
  Registers a weekly Windows Scheduled Task to run discovery milestone (deep soak).

.DESCRIPTION
  Creates task "AlphaEngine - Discovery Milestone" running
  scripts\windows\run_discovery_milestone_scheduled.bat from the repo root.

  If you see **Access is denied**, close PowerShell and run it again: **Run as administrator**
  (Task Scheduler often requires elevation to register tasks, depending on machine policy).

.EXAMPLE
  cd C:\wamp64\www\alpha-engine-poc\scripts\windows
  .\register_discovery_milestone_task.ps1

.EXAMPLE
  .\register_discovery_milestone_task.ps1 -DayOfWeek Sunday -Time "03:00" -Force
#>
param(
    [string] $TaskName = "AlphaEngine - Discovery Milestone",
    [ValidateSet(
        "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"
    )]
    [string] $DayOfWeek = "Sunday",
    [string] $Time = "03:00",
    [switch] $Force
)

$ErrorActionPreference = "Stop"

$here = $PSScriptRoot
$repoRoot = (Resolve-Path (Join-Path $here "..\..")).Path
$bat = Join-Path $here "run_discovery_milestone_scheduled.bat"
if (-not (Test-Path $bat)) {
    throw "Missing batch file: $bat"
}

if ($Force) {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
}

$t = $null
foreach ($p in @("H:mm", "HH:mm")) {
    try {
        $t = [datetime]::ParseExact($Time, $p, [System.Globalization.CultureInfo]::InvariantCulture)
        break
    }
    catch { }
}
if (-not $t) {
    throw "Bad -Time '$Time': use 24h like 3:00 or 03:00"
}
$at = Get-Date -Hour $t.Hour -Minute $t.Minute -Second 0

$dayEnum = [System.DayOfWeek] $DayOfWeek
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek $dayEnum -At $at

$arg = "/c `"" + $bat + "`""
$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument $arg -WorkingDirectory $repoRoot

$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable

try {
    $null = Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings `
        -Description "Alpha Engine: run_discovery_milestone.py deep soak (docs/internal/ops/daily-process.md)" `
        -ErrorAction Stop
}
catch {
    Write-Host ""
    Write-Host "Register-ScheduledTask failed: $_" -ForegroundColor Red
    Write-Host ""
    Write-Host "Most common fix: register from an elevated shell -" -ForegroundColor Yellow
    Write-Host "  1. Close this window." -ForegroundColor Yellow
    Write-Host "  2. Start Menu -> Windows PowerShell -> Right-click -> Run as administrator." -ForegroundColor Yellow
    Write-Host "  3. cd $here" -ForegroundColor Yellow
    Write-Host "  4. .\register_discovery_milestone_task.ps1 $(if ($Force) { '-Force' })" -ForegroundColor Yellow
    Write-Host ""
    exit 1
}

$chk = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if (-not $chk) {
    Write-Host "Register-ScheduledTask reported success but task '$TaskName' was not found." -ForegroundColor Red
    exit 1
}

Write-Host "Registered: $TaskName"
Write-Host "  Command: cmd.exe $arg"
Write-Host "  WorkingDirectory: $repoRoot"
Write-Host "  Trigger: Weekly $DayOfWeek at $Time"
Write-Host ('Query: schtasks /Query /TN "' + $TaskName + '" /V /FO LIST')
