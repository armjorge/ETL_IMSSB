"""
Configuration management for IMSS Bienestar ETL pipeline.
"""
import json
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)


class ConfigManager:
    """Manages configuration for the ETL pipeline."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to configuration file (JSON or YAML)
        """
        self.config_path = config_path
        self.config = self._load_default_config()
        
        if config_path:
            self._load_config_file(config_path)
    
    def _load_default_config(self) -> Dict[str, Any]:
        """Load default configuration."""
        return {
            'logging': {
                'level': 'INFO',
                'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                'handlers': ['console', 'file'],
                'file_path': 'logs/etl.log'
            },
            'imss_bienestar': {
                'input': {
                    'encoding': 'utf-8',
                    'date_format': '%Y-%m-%d',
                    'decimal_separator': '.',
                    'thousands_separator': ','
                },
                'output': {
                    'base_dir': 'output',
                    'formats': ['csv', 'excel'],
                    'include_timestamp': True,
                    'include_validation_report': True
                },
                'validation': {
                    'required_columns': {
                        'patients': ['curp', 'nombre', 'apellido_paterno'],
                        'services': ['codigo_servicio', 'descripcion', 'costo']
                    },
                    'data_types': {
                        'curp': 'string',
                        'nss': 'string',
                        'edad': 'numeric',
                        'fecha_nacimiento': 'datetime',
                        'costo': 'numeric'
                    }
                }
            },
            'processing': {
                'chunk_size': 10000,
                'max_memory_usage': '1GB',
                'parallel_processing': False,
                'error_threshold': 0.05  # 5% error threshold
            }
        }
    
    def _load_config_file(self, config_path: str):
        """Load configuration from file."""
        try:
            config_file = Path(config_path)
            
            if not config_file.exists():
                logger.warning(f"Configuration file not found: {config_path}")
                return
            
            with open(config_file, 'r', encoding='utf-8') as f:
                if config_path.endswith('.json'):
                    file_config = json.load(f)
                elif config_path.endswith(('.yml', '.yaml')):
                    file_config = yaml.safe_load(f)
                else:
                    logger.error(f"Unsupported configuration file format: {config_path}")
                    return
            
            # Merge with default configuration
            self.config = self._deep_merge(self.config, file_config)
            logger.info(f"Loaded configuration from: {config_path}")
            
        except Exception as e:
            logger.error(f"Error loading configuration file {config_path}: {str(e)}")
    
    def _deep_merge(self, base_dict: Dict, update_dict: Dict) -> Dict:
        """Deep merge two dictionaries."""
        result = base_dict.copy()
        
        for key, value in update_dict.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def get_config(self) -> Dict[str, Any]:
        """Get the complete configuration."""
        return self.config
    
    def get_section(self, section: str) -> Dict[str, Any]:
        """Get a specific configuration section."""
        return self.config.get(section, {})
    
    def get_value(self, key: str, default: Any = None) -> Any:
        """Get a specific configuration value using dot notation."""
        keys = key.split('.')
        value = self.config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except KeyError:
            return default
    
    def save_config(self, output_path: str):
        """Save current configuration to file."""
        try:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                if output_path.endswith('.json'):
                    json.dump(self.config, f, indent=2, default=str)
                elif output_path.endswith(('.yml', '.yaml')):
                    yaml.dump(self.config, f, default_flow_style=False, indent=2)
                else:
                    # Default to JSON
                    json.dump(self.config, f, indent=2, default=str)
            
            logger.info(f"Configuration saved to: {output_path}")
            
        except Exception as e:
            logger.error(f"Error saving configuration to {output_path}: {str(e)}")
            raise