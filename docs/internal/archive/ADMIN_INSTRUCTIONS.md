# Administrator Access Required for Scheduler

## Problem
PowerShell requires administrator privileges to create scheduled tasks. Your current user session doesn't have these rights.

## Solutions (in order of preference)

### Option 1: Use Batch File (Recommended)
1. **Right-click** on `create_admin_task.bat`
2. **Select "Run as administrator"**
3. **Click "Yes"** on UAC prompt
4. **Verify** the task is created

### Option 2: PowerShell as Administrator
1. **Right-click** on PowerShell icon
2. **Select "Run as administrator"**
3. **Navigate** to the project directory:
   ```cmd
   cd C:\wamp64\www\alpha-engine-poc
   ```
4. **Run the script**:
   ```powershell
   .\fix_scheduler.ps1
   ```

### Option 3: Manual Task Creation
1. **Open Task Scheduler** (search "Task Scheduler" in Start Menu)
2. **Right-click** "Task Scheduler Library" and select "Create Task..."
3. **General tab**:
   - Name: `AlphaEngine - Daily Pipeline`
   - Description: `Alpha Engine Daily Pipeline - Price download, discovery, predictions, replay, and reporting`
   - Select "Run with highest privileges"
4. **Trigger tab**:
   - Click "New..."
   - Begin the task: "On a schedule"
   - Settings: "Daily"
   - Start time: "10:00:00 AM"
   - Click "OK"
5. **Actions tab**:
   - Click "New..."
   - Action: "Start a program"
   - Program/script: `C:\wamp64\www\alpha-engine-poc\run_daily_pipeline.bat`
   - Start in: `C:\wamp64\www\alpha-engine-poc`
   - Click "OK"
6. **Settings tab**:
   - Ensure "Allow task to be run on demand" is checked
   - Click "OK"
7. **Enter admin password** if prompted

## Verification
After creating the task, verify it exists:
```cmd
schtasks /query /tn "AlphaEngine - Daily Pipeline" /fo LIST
```

You should see:
- **TaskName**: AlphaEngine - Daily Pipeline
- **Status**: Ready
- **Next Run Time**: Tomorrow at 10:00:00 AM

## Test Manual Execution
Even without scheduler, you can test the pipeline:
```cmd
.\run_daily_pipeline.bat
```

This will run the complete pipeline and create logs.

## Expected Tomorrow Morning
Once the task is properly created:
- **10:00 AM**: Automatic pipeline execution
- **10:04 AM**: Daily report with 23+ signals
- **10:07 AM**: Complete success

## Current System Status
- **Pipeline**: Works perfectly (4-second execution)
- **Environment**: Fixed (SYSTEM uses venv)
- **Logging**: Date-specific files
- **Reports**: 23 signals with predictions
- **Only missing**: Scheduler admin rights

## Next Steps
1. Run `create_admin_task.bat` as administrator
2. Verify the task exists
3. Check tomorrow morning at 10:00 AM
