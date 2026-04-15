#!/usr/bin/env python3
"""
Enhanced Logging Utility
Provides structured logging for all scheduled tasks
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

class TaskLogger:
    def __init__(self, task_name: str, base_path: Path = None):
        self.task_name = task_name
        self.base_path = base_path or Path(__file__).parent.parent
        self.logs_path = self.base_path / "logs"
        
        # Ensure logging structure exists
        self.logs_path.mkdir(exist_ok=True)
        (self.logs_path / "daily").mkdir(exist_ok=True)
        (self.logs_path / "system").mkdir(exist_ok=True)
        
        # Setup task-specific logger
        self.logger = self.setup_logger()
        
    def setup_logger(self) -> logging.Logger:
        """Setup structured logger for the task"""
        
        # Create logger
        logger = logging.getLogger(f"task_{self.task_name}")
        logger.setLevel(logging.INFO)
        
        # Clear existing handlers
        logger.handlers.clear()
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Daily log file
        today = datetime.now().strftime("%Y-%m-%d")
        daily_log = self.logs_path / "daily" / f"{today}.log"
        
        # Task-specific log file
        task_log = self.logs_path / "system" / f"{self.task_name}.log"
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        
        # Daily log handler
        daily_handler = logging.FileHandler(daily_log, encoding='utf-8')
        daily_handler.setFormatter(formatter)
        logger.addHandler(daily_handler)
        
        # Task log handler
        task_handler = logging.FileHandler(task_log, encoding='utf-8')
        task_handler.setFormatter(formatter)
        logger.addHandler(task_handler)
        
        return logger
        
    def start_task(self, description: str = ""):
        """Log task start"""
        self.logger.info(f"{'='*60}")
        self.logger.info(f"STARTING TASK: {self.task_name.upper()}")
        self.logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if description:
            self.logger.info(f"Description: {description}")
        self.logger.info(f"{'='*60}")
        
    def end_task(self, success: bool = True, message: str = ""):
        """Log task completion"""
        status = "SUCCESS" if success else "FAILED"
        self.logger.info(f"{'='*60}")
        self.logger.info(f"TASK {self.task_name.upper()} {status}")
        self.logger.info(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        if message:
            self.logger.info(f"Message: {message}")
        self.logger.info(f"{'='*60}")
        
    def log_step(self, step: str, message: str = ""):
        """Log task step"""
        self.logger.info(f"STEP: {step}")
        if message:
            self.logger.info(f"  {message}")
            
    def log_error(self, error: str, exception: Optional[Exception] = None):
        """Log error with optional exception details"""
        self.logger.error(f"ERROR: {error}")
        if exception:
            self.logger.error(f"Exception: {str(exception)}")
            
    def log_metric(self, metric_name: str, value: str):
        """Log performance metric"""
        self.logger.info(f"METRIC: {metric_name} = {value}")
        
    def log_warning(self, warning: str):
        """Log warning"""
        self.logger.warning(f"WARNING: {warning}")

def setup_task_logging(task_name: str) -> TaskLogger:
    """Convenience function to setup task logging"""
    return TaskLogger(task_name)

if __name__ == "__main__":
    # Test the logging system
    logger = setup_task_logging("test_task")
    
    logger.start_task("Testing enhanced logging system")
    logger.log_step("Initialization", "Setting up logging structure")
    logger.log_metric("test_metric", "123")
    logger.log_warning("This is a test warning")
    logger.end_task(True, "Test completed successfully")
