#!/usr/bin/env python3
"""
Configuration system for pattern-based file watcher
Allows external configuration management
"""

import json
import yaml
import os
from pathlib import Path
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)

class PatternConfig:
    """Configuration management for pattern-based watcher"""
    
    def __init__(self, config_file=None):
        self.config_file = config_file or self._find_config_file()
        self.config = self._load_config()
    
    def _find_config_file(self):
        """Find configuration file in standard locations"""
        possible_locations = [
            './pattern_config.yaml',
            './pattern_config.json',
            './config/pattern_config.yaml',
            './config/pattern_config.json',
            os.path.expanduser('~/.etl/pattern_config.yaml'),
        ]
        
        for location in possible_locations:
            if os.path.exists(location):
                logger.info(f"Found config file: {location}")
                return location
        
        # If no config file found, create default
        return self._create_default_config()
    
    def _create_default_config(self):
        """Create default configuration file"""
        config_dir = Path('./config')
        config_dir.mkdir(exist_ok=True)
        
        default_config = {
            'watcher_settings': {
                'watch_path': 'Z:\\',
                'backup_watch_path': './watch_production',
                'poll_interval': 10,
                'process_delay': 5,
                'supported_extensions': ['.csv', '.xlsx', '.xls', '.xlsm']
            },
            'celery_settings': {
                'broker_url': 'redis://localhost:6379/0',
                'result_backend': 'redis://localhost:6379/1',
                'task_ignore_result': True
            },
            'pattern_mappings': {
                'tel_list': {
                    'table': 'dim_numbers',
                    'schema': 'public',
                    'description': 'Telephone numbers and contact information'
                },
                'customer_data': {
                    'table': 'dim_customers',
                    'schema': 'public',
                    'description': 'Customer master data'
                },
                'product_info': {
                    'table': 'dim_products',
                    'schema': 'public',
                    'description': 'Product information and catalog'
                },
                'sales_data': {
                    'table': 'fact_sales',
                    'schema': 'public',
                    'description': 'Sales transaction data'
                },
                'inventory': {
                    'table': 'dim_inventory',
                    'schema': 'public',
                    'description': 'Inventory levels and stock data'
                },
                'transactions': {
                    'table': 'fact_transactions',
                    'schema': 'public',
                    'description': 'Financial transaction records'
                },
                'reports': {
                    'table': 'staging_reports',
                    'schema': 'staging',
                    'description': 'Temporary report staging area'
                }
            },
            'data_quality': {
                'max_file_size_mb': 100,
                'require_headers': True,
                'skip_empty_files': True,
                'encoding_fallbacks': ['utf-8', 'utf-8-sig', 'latin1', 'cp1252']
            },
            'logging': {
                'level': 'INFO',
                'file': './logs/pattern_watcher.log',
                'format': '%(asctime)s - %(levelname)s - %(message)s'
            }
        }
        
        config_file = config_dir / 'pattern_config.yaml'
        
        with open(config_file, 'w') as f:
            yaml.safe_dump(default_config, f, default_flow_style=False, sort_keys=False)
        
        logger.info(f"Created default config file: {config_file}")
        return str(config_file)
    
    def _load_config(self):
        """Load configuration from file"""
        try:
            with open(self.config_file, 'r') as f:
                if self.config_file.endswith('.yaml') or self.config_file.endswith('.yml'):
                    return yaml.safe_load(f)
                elif self.config_file.endswith('.json'):
                    return json.load(f)
                else:
                    raise ValueError(f"Unsupported config file format: {self.config_file}")
        except Exception as e:
            logger.error(f"Failed to load config from {self.config_file}: {e}")
            raise
    
    def get_pattern_mappings(self) -> Dict[str, str]:
        """Get simple pattern -> table mappings"""
        patterns = self.config.get('pattern_mappings', {})
        return {pattern: config['table'] for pattern, config in patterns.items()}
    
    def get_pattern_config(self, pattern: str) -> Dict[str, Any]:
        """Get full configuration for a specific pattern"""
        return self.config.get('pattern_mappings', {}).get(pattern, {})
    
    def get_watcher_settings(self) -> Dict[str, Any]:
        """Get watcher configuration settings"""
        return self.config.get('watcher_settings', {})
    
    def get_celery_settings(self) -> Dict[str, Any]:
        """Get Celery configuration settings"""
        return self.config.get('celery_settings', {})
    
    def get_data_quality_settings(self) -> Dict[str, Any]:
        """Get data quality settings"""
        return self.config.get('data_quality', {})
    
    def get_logging_settings(self) -> Dict[str, Any]:
        """Get logging configuration"""
        return self.config.get('logging', {})
    
    def add_pattern(self, pattern: str, table: str, schema: str = 'public', description: str = ''):
        """Add a new pattern mapping"""
        if 'pattern_mappings' not in self.config:
            self.config['pattern_mappings'] = {}
        
        self.config['pattern_mappings'][pattern] = {
            'table': table,
            'schema': schema,
            'description': description
        }
        
        self.save_config()
        logger.info(f"Added pattern mapping: {pattern} -> {table}")
    
    def remove_pattern(self, pattern: str):
        """Remove a pattern mapping"""
        if pattern in self.config.get('pattern_mappings', {}):
            del self.config['pattern_mappings'][pattern]
            self.save_config()
            logger.info(f"Removed pattern mapping: {pattern}")
        else:
            logger.warning(f"Pattern not found: {pattern}")
    
    def save_config(self):
        """Save current configuration to file"""
        try:
            with open(self.config_file, 'w') as f:
                if self.config_file.endswith('.yaml') or self.config_file.endswith('.yml'):
                    yaml.safe_dump(self.config, f, default_flow_style=False, sort_keys=False)
                elif self.config_file.endswith('.json'):
                    json.dump(self.config, f, indent=2)
            
            logger.info(f"Configuration saved to {self.config_file}")
        except Exception as e:
            logger.error(f"Failed to save config: {e}")
            raise
    
    def validate_config(self) -> bool:
        """Validate configuration settings"""
        errors = []
        
        # Check required sections
        required_sections = ['watcher_settings', 'pattern_mappings']
        for section in required_sections:
            if section not in self.config:
                errors.append(f"Missing required section: {section}")
        
        # Validate pattern mappings
        patterns = self.config.get('pattern_mappings', {})
        for pattern, config in patterns.items():
            if not isinstance(config, dict):
                errors.append(f"Pattern {pattern} config must be a dictionary")
                continue
            
            if 'table' not in config:
                errors.append(f"Pattern {pattern} missing required 'table' field")
        
        # Validate watcher settings
        watcher = self.config.get('watcher_settings', {})
        if 'poll_interval' in watcher and not isinstance(watcher['poll_interval'], int):
            errors.append("poll_interval must be an integer")
        
        if errors:
            for error in errors:
                logger.error(f"Config validation error: {error}")
            return False
        
        logger.info("Configuration validation passed")
        return True

def manage_config():
    """Interactive configuration management"""
    config = PatternConfig()
    
    while True:
        print("\nPattern Configuration Manager")
        print("=" * 40)
        print("1. Show current patterns")
        print("2. Add new pattern")
        print("3. Remove pattern")
        print("4. Show full configuration")
        print("5. Validate configuration")
        print("6. Exit")
        
        choice = input("\nEnter choice (1-6): ").strip()
        
        if choice == '1':
            patterns = config.get_pattern_mappings()
            print("\nCurrent Pattern Mappings:")
            print("-" * 30)
            for pattern, table in patterns.items():
                pattern_config = config.get_pattern_config(pattern)
                desc = pattern_config.get('description', 'No description')
                print(f"{pattern:<20} -> {table}")
                print(f"{'':20}    {desc}")
        
        elif choice == '2':
            pattern = input("Enter pattern name: ").strip()
            table = input("Enter table name: ").strip()
            schema = input("Enter schema (default: public): ").strip() or 'public'
            description = input("Enter description: ").strip()
            
            config.add_pattern(pattern, table, schema, description)
            print(f"Added pattern: {pattern} -> {table}")
        
        elif choice == '3':
            pattern = input("Enter pattern to remove: ").strip()
            config.remove_pattern(pattern)
        
        elif choice == '4':
            print("\nFull Configuration:")
            print("-" * 30)
            print(yaml.safe_dump(config.config, default_flow_style=False))
        
        elif choice == '5':
            if config.validate_config():
                print("✓ Configuration is valid")
            else:
                print("✗ Configuration has errors")
        
        elif choice == '6':
            break
        
        else:
            print("Invalid choice. Please enter 1-6.")

if __name__ == "__main__":
    manage_config()