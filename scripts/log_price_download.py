#!/usr/bin/env python3
"""
Price Download Task with Enhanced Logging
Wraps the original price download script with structured logging
"""

import sys
import time
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from scripts.enhanced_logging import setup_task_logging

def main():
    """Execute price download with enhanced logging"""
    
    logger = setup_task_logging("price_download")
    
    try:
        logger.start_task("Daily price data download")
        
        # Log system setup
        logger.log_step("System Setup", "Initializing price download environment")
        
        # Import and run the original script
        logger.log_step("Price Download", "Executing daily price download")
        start_time = time.time()
        
        try:
            from dev_scripts.scripts.download_prices_daily import main as download_main
            download_main()
            
            execution_time = round(time.time() - start_time, 2)
            logger.log_metric("execution_time_seconds", str(execution_time))
            logger.log_step("Price Download", f"Completed in {execution_time} seconds")
            
        except ImportError as e:
            logger.log_error("Could not import price download script", e)
            # Fallback: try to run directly
            logger.log_warning("Attempting direct script execution")
            import subprocess
            result = subprocess.run([
                sys.executable, 
                "dev_scripts/scripts/download_prices_daily.py"
            ], capture_output=True, text=True, cwd=Path(__file__).parent.parent)
            
            execution_time = round(time.time() - start_time, 2)
            logger.log_metric("execution_time_seconds", str(execution_time))
            
            if result.returncode == 0:
                logger.log_step("Price Download", f"Completed in {execution_time} seconds")
                logger.log_metric("stdout_lines", str(len(result.stdout.splitlines())))
            else:
                logger.log_error("Script execution failed")
                logger.log_error("stderr", result.stderr)
                raise Exception(f"Script failed with return code {result.returncode}")
        
        # Log completion
        logger.end_task(True, "Price download completed successfully")
        
    except Exception as e:
        logger.log_error("Price download task failed", e)
        logger.end_task(False, f"Failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
