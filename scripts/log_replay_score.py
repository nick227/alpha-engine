#!/usr/bin/env python3
"""
Replay Score Task with Enhanced Logging
Wraps the replay score calculation with structured logging and metrics
"""

import sys
import time
import subprocess
import re
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from scripts.enhanced_logging import setup_task_logging

def main():
    """Execute replay score calculation with enhanced logging"""
    
    logger = setup_task_logging("replay_score")
    
    try:
        logger.start_task("Replay score calculation and analysis")
        
        # Log system setup
        logger.log_step("System Setup", "Initializing replay score environment")
        
        # Run the replay score calculation
        logger.log_step("Replay Score", "Executing paper trading replay")
        start_time = time.time()
        
        try:
            # Set PYTHONPATH and run the original script
            env = subprocess_environment()
            result = subprocess.run([
                sys.executable, 
                "run_paper_trading.py",
                "--replay"
            ], capture_output=True, text=True, cwd=Path(__file__).parent.parent, env=env)
            
            execution_time = round(time.time() - start_time, 2)
            logger.log_metric("execution_time_seconds", str(execution_time))
            
            if result.returncode == 0:
                logger.log_step("Replay Score", f"Completed in {execution_time} seconds")
                logger.log_metric("stdout_lines", str(len(result.stdout.splitlines())))
                
                # Extract metrics from output
                extract_replay_metrics(logger, result.stdout)
                
            else:
                logger.log_error("Replay score execution failed")
                logger.log_error("stderr", result.stderr)
                raise Exception(f"Replay failed with return code {result.returncode}")
                
        except Exception as e:
            logger.log_error("Failed to execute replay score calculation", e)
            raise
        
        # Log completion
        logger.end_task(True, "Replay score calculation completed successfully")
        
    except Exception as e:
        logger.log_error("Replay score task failed", e)
        logger.end_task(False, f"Failed: {str(e)}")
        sys.exit(1)

def extract_replay_metrics(logger, output):
    """Extract relevant metrics from replay output"""
    
    lines = output.splitlines()
    
    for line in lines:
        line = line.strip()
        
        # Look for performance metrics
        if "total return" in line.lower():
            logger.log_metric("total_return", line)
        elif "win rate" in line.lower():
            logger.log_metric("win_rate", line)
        elif "sharpe ratio" in line.lower():
            logger.log_metric("sharpe_ratio", line)
        elif "max drawdown" in line.lower():
            logger.log_metric("max_drawdown", line)
        elif "predictions evaluated" in line.lower():
            logger.log_metric("predictions_evaluated", line)
        elif "strategies tested" in line.lower():
            logger.log_metric("strategies_tested", line)
        
        # Look for numeric values using regex
        # Win rate patterns
        win_rate_match = re.search(r'win rate[:\s]+([\d.]+)%?', line, re.IGNORECASE)
        if win_rate_match:
            logger.log_metric("detected_win_rate_percent", win_rate_match.group(1))
        
        # Return patterns
        return_match = re.search(r'return[:\s]+([+-]?[\d.]+)%?', line, re.IGNORECASE)
        if return_match:
            logger.log_metric("detected_return_percent", return_match.group(1))
        
        # Count patterns
        count_match = re.search(r'(\d+)\s+(predictions|trades|signals)', line, re.IGNORECASE)
        if count_match:
            logger.log_metric("detected_count", f"{count_match.group(1)} {count_match.group(2)}")

def subprocess_environment():
    """Create subprocess environment with PYTHONPATH"""
    import os
    env = os.environ.copy()
    env['PYTHONPATH'] = '.'
    return env

if __name__ == "__main__":
    main()
