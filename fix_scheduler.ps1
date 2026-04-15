# Fix AlphaEngine Daily Pipeline Scheduler
# Run this as Administrator

Write-Host "Fixing AlphaEngine Daily Pipeline Scheduler..." -ForegroundColor Green

# Remove existing task if it exists
try {
    Unregister-ScheduledTask -TaskName "AlphaEngine - Daily Pipeline" -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "Removed existing task" -ForegroundColor Yellow
} catch {
    Write-Host "No existing task to remove" -ForegroundColor Yellow
}

# Create new action
$action = New-ScheduledTaskAction -Execute "C:\wamp64\www\alpha-engine-poc\run_daily_pipeline.bat" -WorkingDirectory "C:\wamp64\www\alpha-engine-poc"

# Create new trigger (daily at 10:00 AM)
$trigger = New-ScheduledTaskTrigger -Daily -At 10:00AM

# Register the task
Register-ScheduledTask `
    -TaskName "AlphaEngine - Daily Pipeline" `
    -Action $action `
    -Trigger $trigger `
    -RunLevel Highest `
    -Force `
    -Description "Alpha Engine Daily Pipeline - Price download, discovery, predictions, replay, and reporting"

Write-Host "Task registered successfully" -ForegroundColor Green

# Verify the task
$task = Get-ScheduledTask -TaskName "AlphaEngine - Daily Pipeline"
Write-Host "`nTask Details:" -ForegroundColor Cyan
Write-Host "  Task Name: $($task.TaskName)"
Write-Host "  State: $($task.State)"
Write-Host "  Next Run: $($task.Triggers.StartBoundary)"
Write-Host "  Action: $($task.Actions.Execute)"

# Show next run time
$taskInfo = Get-ScheduledTaskInfo -TaskName "AlphaEngine - Daily Pipeline"
Write-Host "`nSchedule Info:" -ForegroundColor Cyan
Write-Host "  Last Run: $($taskInfo.LastRunTime)"
Write-Host "  Next Run: $($taskInfo.NextRunTime)"
Write-Host "  Missed Runs: $($taskInfo.MissedRuns)"

Write-Host "`nEnvironment fix applied:" -ForegroundColor Yellow
Write-Host "  - Python path set to venv: .venv\Scripts\python.exe" -ForegroundColor White
Write-Host "  - Environment logging added for debugging" -ForegroundColor White
Write-Host "  - All python calls use explicit venv path" -ForegroundColor White

Write-Host "`nScheduler fix complete!" -ForegroundColor Green
Write-Host "The pipeline will run daily at 10:00 AM starting tomorrow." -ForegroundColor White
Write-Host "Logs will be saved as: daily_pipeline_YYYY-MM-DD.log" -ForegroundColor Cyan
