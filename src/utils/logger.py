"""
Logging configuration for IMSS Bienestar ETL pipeline.
"""
import logging
import logging.handlers
from pathlib import Path
from typing import Dict, Any
import sys


def setup_logging(config: Dict[str, Any]):
    """
    Setup logging configuration.
    
    Args:
        config: Logging configuration dictionary
    """
    # Get configuration values with defaults
    level = config.get('level', 'INFO')
    format_string = config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handlers = config.get('handlers', ['console'])
    file_path = config.get('file_path', 'logs/etl.log')
    
    # Create formatter
    formatter = logging.Formatter(format_string)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Setup handlers
    if 'console' in handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    
    if 'file' in handlers:
        # Ensure log directory exists
        log_file = Path(file_path)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Create rotating file handler (10MB max, keep 5 backups)
        file_handler = logging.handlers.RotatingFileHandler(
            file_path,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Set specific logger levels
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    
    # Log the configuration
    logger = logging.getLogger(__name__)
    logger.info(f"Logging configured - Level: {level}, Handlers: {handlers}")


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance.
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)