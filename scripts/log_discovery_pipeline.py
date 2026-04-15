#!/usr/bin/env python3
"""
Discovery Pipeline Task with Enhanced Logging
Wraps the discovery pipeline with structured logging and metrics
"""

import sys
import time
import subprocess
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from scripts.enhanced_logging import setup_task_logging

def main():
    """Execute discovery pipeline with enhanced logging"""
    
    logger = setup_task_logging("discovery_pipeline")
    
    try:
        logger.start_task("Nightly discovery pipeline with predictions")
        
        # Log system setup
        logger.log_step("System Setup", "Initializing discovery pipeline environment")
        
        # Run the discovery pipeline
        logger.log_step("Discovery Pipeline", "Executing nightly discovery pipeline")
        start_time = time.time()
        
        try:
            # Set PYTHONPATH and run the original script
            env = subprocess_environment()
            result = subprocess.run([
                sys.executable, 
                "dev_scripts/scripts/nightly_discovery_pipeline.py",
                "--run-predictions"
            ], capture_output=True, text=True, cwd=Path(__file__).parent.parent, env=env)
            
            execution_time = round(time.time() - start_time, 2)
            logger.log_metric("execution_time_seconds", str(execution_time))
            
            if result.returncode == 0:
                logger.log_step("Discovery Pipeline", f"Completed in {execution_time} seconds")
                logger.log_metric("stdout_lines", str(len(result.stdout.splitlines())))
                
                # Try to extract some metrics from output
                output_lines = result.stdout.splitlines()
                for line in output_lines:
                    if "candidates found" in line.lower():
                        logger.log_metric("discovery_candidates", line.strip())
                    elif "predictions generated" in line.lower():
                        logger.log_metric("predictions_generated", line.strip())
                    elif "strategies evaluated" in line.lower():
                        logger.log_metric("strategies_evaluated", line.strip())
                
            else:
                logger.log_error("Discovery pipeline execution failed")
                logger.log_error("stderr", result.stderr)
                raise Exception(f"Pipeline failed with return code {result.returncode}")
                
        except Exception as e:
            logger.log_error("Failed to execute discovery pipeline", e)
            raise
        
        # Log completion
        logger.end_task(True, "Discovery pipeline completed successfully")
        
    except Exception as e:
        logger.log_error("Discovery pipeline task failed", e)
        logger.end_task(False, f"Failed: {str(e)}")
        sys.exit(1)

def subprocess_environment():
    """Create subprocess environment with PYTHONPATH"""
    import os
    env = os.environ.copy()
    env['PYTHONPATH'] = '.'
    return env

if __name__ == "__main__":
    main()
