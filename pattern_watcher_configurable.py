#!/usr/bin/env python3
"""
Configurable Pattern-Based File Watcher
Uses external configuration files for easy management
"""

import os
import sys
import time
import logging
import pandas as pd
import glob
from datetime import datetime
from pathlib import Path

# Import configuration management
try:
    from pattern_config_system import PatternConfig
    CONFIG_SYSTEM_AVAILABLE = True
except ImportError:
    # Fallback if config system not available
    PatternConfig = None
    CONFIG_SYSTEM_AVAILABLE = False
    print("Warning: pattern_config_system not found, using default configuration")

# Try to import yaml for configuration files
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False
    print("Warning: PyYAML not installed, configuration management limited")

class ConfigurablePatternWatcher:
    """Production file watcher with external configuration support"""
    
    def __init__(self, config_file=None):
        # Load configuration first
        if PatternConfig:
            self.config_manager = PatternConfig(config_file)
            self._setup_from_config()
        else:
            self._setup_default_config()
        
        # Setup logging immediately after configuration
        self._setup_logging()
        
        # Initialize watcher state
        self.processed_files = set()
        self.file_timestamps = {}
        self.initial_scan_done = False
        
        self.logger.info("Configurable Pattern-Based File Watcher initialized")
        self.logger.info(f"Pattern mappings: {self.pattern_mappings}")
        self.logger.info(f"Monitoring interval: {self.poll_interval} seconds")
        
        # Create Celery client after logging is setup
        self.celery_app = self.create_celery_client()
        
        # Record existing files without processing them
        self._initial_file_scan()
    
    def _setup_from_config(self):
        """Setup from configuration file"""
        watcher_settings = self.config_manager.get_watcher_settings()
        
        self.watch_path = watcher_settings.get('watch_path', 'Z:\\')
        self.backup_watch_path = watcher_settings.get('backup_watch_path', './watch_production')
        self.poll_interval = watcher_settings.get('poll_interval', 10)
        self.process_delay = watcher_settings.get('process_delay', 5)
        self.supported_extensions = set(watcher_settings.get('supported_extensions', ['.csv', '.xlsx', '.xls', '.xlsm']))
        
        # Data quality settings
        dq_settings = self.config_manager.get_data_quality_settings()
        self.max_file_size_mb = dq_settings.get('max_file_size_mb', 100)
        self.require_headers = dq_settings.get('require_headers', True)
        self.skip_empty_files = dq_settings.get('skip_empty_files', True)
        self.encoding_fallbacks = dq_settings.get('encoding_fallbacks', ['utf-8', 'utf-8-sig', 'latin1', 'cp1252'])
        
        # Pattern mappings
        self.pattern_mappings = self.config_manager.get_pattern_mappings()
        
        # Celery settings
        celery_settings = self.config_manager.get_celery_settings()
        self.celery_broker = celery_settings.get('broker_url', 'redis://localhost:6379/0')
        self.celery_backend = celery_settings.get('result_backend', 'redis://localhost:6379/1')
    
    def _setup_default_config(self):
        """Fallback to default configuration if config system unavailable"""
        self.watch_path = os.environ.get('WATCH_PATH', 'Z:\\')
        self.backup_watch_path = os.environ.get('BACKUP_WATCH_PATH', './watch_production')
        self.poll_interval = int(os.environ.get('POLL_INTERVAL', 10))
        self.process_delay = int(os.environ.get('PROCESS_DELAY', 5))
        self.supported_extensions = {'.csv', '.xlsx', '.xls', '.xlsm'}
        
        # Default pattern mappings
        self.pattern_mappings = {
            'tel_list': 'dim_numbers',
            'customer_data': 'dim_customers',
            'product_info': 'dim_products',
            'sales_data': 'fact_sales',
            'inventory': 'dim_inventory',
            'transactions': 'fact_transactions',
            'reports': 'staging_reports',
        }
        
        # Default data quality settings
        self.max_file_size_mb = 100
        self.require_headers = True
        self.skip_empty_files = True
        self.encoding_fallbacks = ['utf-8', 'utf-8-sig', 'latin1', 'cp1252']
        
        # Default Celery settings
        self.celery_broker = 'redis://localhost:6379/0'
        self.celery_backend = 'redis://localhost:6379/1'
    
    def _setup_logging(self):
        """Setup logging based on configuration"""
        if PatternConfig and hasattr(self, 'config_manager'):
            log_settings = self.config_manager.get_logging_settings()
            log_level = getattr(logging, log_settings.get('level', 'INFO'))
            log_file = log_settings.get('file', './lots/pattern_watcher.log')
            log_format = log_settings.get('format', '%(asctime)s - %(levelname)s - %(message)s')
        else:
            log_level = logging.INFO
            log_file = './logs/pattern_watcher.log'
            log_format = '%(asctime)s - %(levelname)s - %(message)s'
        
        # Setup logging
        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _initial_file_scan(self):
        """Record all existing files without processing them"""
        watch_path = self.get_watch_path()
        existing_count = 0
        
        self.logger.info("Recording existing files (will not process them)...")
        
        try:
            patterns = [os.path.join(watch_path, "**", f"*{ext}") for ext in self.supported_extensions]
            
            for pattern in patterns:
                for filepath in glob.glob(pattern, recursive=True):
                    if os.path.isfile(filepath) and self.is_supported_file(filepath):
                        # Record the file's current timestamp
                        stat_info = os.stat(filepath)
                        self.file_timestamps[filepath] = stat_info.st_mtime
                        existing_count += 1
                        
        except Exception as e:
            self.logger.error(f"Error during initial scan: {e}")
        
        self.initial_scan_done = True
        self.logger.info(f"Initial scan complete: {existing_count} existing files recorded (ignored)")
        self.logger.info("Watcher is now ready - will only process NEW files")
    
    def create_celery_client(self):
        """Create Celery client with configuration"""
        try:
            from celery import Celery
            
            # Force environment variables for Celery
            os.environ['CELERY_BROKER_URL'] = self.celery_broker
            os.environ['CELERY_RESULT_BACKEND'] = self.celery_backend
            
            # Clear conflicting settings
            for key in list(os.environ.keys()):
                if 'CELERY' in key or 'REDIS' in key:
                    if key not in ['CELERY_BROKER_URL', 'CELERY_RESULT_BACKEND']:
                        del os.environ[key]
            
            app = Celery('configurable_pattern_watcher')
            app.conf.update({
                'broker_url': self.celery_broker,
                'result_backend': None,
                'task_ignore_result': True,
                'task_store_eager_result': False,
                'result_expires': None,
                'task_acks_late': False,
                'worker_prefetch_multiplier': 1,
                'task_serializer': 'json',
                'accept_content': ['json'],
                'result_serializer': 'json',
                'broker_connection_retry_on_startup': True,
                'broker_connection_retry': True,
                'broker_connection_max_retries': 3
            })
            
            self.logger.info("Celery client configured successfully")
            return app
            
        except Exception as e:
            self.logger.error(f"Failed to create Celery client: {e}")
            return None
    
    def get_watch_path(self):
        """Get the path to watch for files"""
        if os.path.exists(self.watch_path):
            return self.watch_path
        
        os.makedirs(self.backup_watch_path, exist_ok=True)
        self.logger.info(f"Using backup watch path: {self.backup_watch_path}")
        return self.backup_watch_path
    
    def get_table_name_from_path(self, filepath):
        """
        Determine table name based on file path patterns
        
        Args:
            filepath (str): Full path to the file
            
        Returns:
            str or None: Table name if pattern matches, None if no match
        """
        # Convert to forward slashes for consistent matching
        normalized_path = filepath.replace('\\', '/').lower()
        
        # Check each pattern in the mapping
        for pattern, table_name in self.pattern_mappings.items():
            if pattern.lower() in normalized_path:
                self.logger.info(f"Pattern '{pattern}' found in path -> table '{table_name}'")
                return table_name
        
        # No pattern matched
        self.logger.debug(f"No pattern matched for: {filepath}")
        return None
    
    def is_supported_file(self, filename):
        """Check if file is supported"""
        return Path(filename).suffix.lower() in self.supported_extensions
    
    def validate_file(self, filepath):
        """Validate file before processing"""
        filename = os.path.basename(filepath)
        
        # Check file size
        file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
        if file_size_mb > self.max_file_size_mb:
            self.logger.warning(f"File {filename} exceeds size limit: {file_size_mb:.1f}MB > {self.max_file_size_mb}MB")
            return False
        
        # Check if file is empty
        if self.skip_empty_files and os.path.getsize(filepath) == 0:
            self.logger.warning(f"File {filename} is empty, skipping")
            return False
        
        return True
    
    def read_file_to_dataframe(self, filepath):
        """Read file and convert to DataFrame with enhanced error handling"""
        filename = os.path.basename(filepath)
        file_ext = Path(filepath).suffix.lower()
        
        try:
            if file_ext == '.csv':
                # Try different encodings for CSV
                for encoding in self.encoding_fallbacks:
                    try:
                        df = pd.read_csv(filepath, encoding=encoding)
                        self.logger.debug(f"Successfully read CSV with {encoding} encoding")
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    df = pd.read_csv(filepath, encoding='utf-8', errors='ignore')
                    self.logger.warning(f"Read CSV with error handling for {filename}")
                    
            elif file_ext in ['.xlsx', '.xls', '.xlsm']:
                df = pd.read_excel(filepath, sheet_name=0)
                self.logger.debug(f"Successfully read Excel file: {filename}")
            else:
                raise ValueError(f"Unsupported file type: {file_ext}")
            
            if df.empty:
                self.logger.warning(f"File is empty: {filename}")
                return None
            
            # Validate headers if required
            if self.require_headers and df.columns.isna().any():
                self.logger.warning(f"File {filename} has missing column headers")
                return None
            
            self.logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns from {filename}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading file {filename}: {e}")
            raise
    
    def process_file(self, filepath, table_name):
        """Process a single file to the specified table"""
        if not self.celery_app:
            self.logger.warning(f"Celery not configured, skipping {filepath}")
            return False
        
        filename = os.path.basename(filepath)
        
        try:
            # Validate file first
            if not self.validate_file(filepath):
                return False
            
            # Read file to DataFrame
            df = self.read_file_to_dataframe(filepath)
            if df is None:
                return False
            
            # Get additional configuration for this pattern
            pattern_config = {}
            if PatternConfig and hasattr(self, 'config_manager'):
                for pattern in self.pattern_mappings:
                    if pattern.lower() in filepath.lower():
                        pattern_config = self.config_manager.get_pattern_config(pattern)
                        break
            
            # Convert DataFrame to records
            data_records = df.to_dict('records')
            
            # Create source name with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            source_name = f"{Path(filename).stem}_{timestamp}"
            
            self.logger.info(f"Processing: {filename} -> {table_name} ({len(data_records)} records)")
            
            # Prepare task arguments
            task_kwargs = {
                'table_name': table_name,
                'source_name': source_name
            }
            
            # Add schema if specified in configuration
            if pattern_config.get('schema'):
                task_kwargs['schema'] = pattern_config['schema']
            
            # Send task using the specified table name
            result = self.celery_app.send_task(
                'process_dataframe',
                args=[data_records],
                kwargs=task_kwargs
            )
            
            self.logger.info(f"Task sent successfully: {result.id}")
            
            # Mark as processed
            self.processed_files.add(filepath)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to process file {filename}: {e}")
            return False
    
    def find_and_process_files(self):
        """Find new files and process them based on patterns"""
        if not self.initial_scan_done:
            return  # Skip if initial scan not complete
            
        watch_path = self.get_watch_path()
        processed_count = 0
        skipped_count = 0
        error_count = 0
        
        try:
            # Find all supported files recursively
            patterns = [os.path.join(watch_path, "**", f"*{ext}") for ext in self.supported_extensions]
            
            for pattern in patterns:
                for filepath in glob.glob(pattern, recursive=True):
                    if not os.path.isfile(filepath) or not self.is_supported_file(filepath):
                        continue
                    
                    # Check if file is new or modified
                    stat_info = os.stat(filepath)
                    current_mtime = stat_info.st_mtime
                    
                    # Skip if file existed before watcher started and hasn't been modified
                    if filepath in self.file_timestamps and self.file_timestamps[filepath] == current_mtime:
                        continue
                    
                    # Check pattern matching
                    table_name = self.get_table_name_from_path(filepath)
                    
                    if table_name is None:
                        # No pattern matched - skip this file
                        if filepath not in self.file_timestamps:
                            self.file_timestamps[filepath] = current_mtime
                        skipped_count += 1
                        continue
                    
                    # New or modified file with matching pattern
                    if filepath not in self.file_timestamps:
                        self.logger.info(f"NEW file detected: {os.path.basename(filepath)} (pattern matched)")
                    else:
                        self.logger.info(f"MODIFIED file detected: {os.path.basename(filepath)} (pattern matched)")
                    
                    # Update timestamp
                    self.file_timestamps[filepath] = current_mtime
                    
                    # Verify file is stable (not being written to)
                    time.sleep(2)
                    new_stat = os.stat(filepath)
                    if new_stat.st_mtime != current_mtime:
                        self.logger.info(f"File still being written, skipping: {os.path.basename(filepath)}")
                        continue
                    
                    if new_stat.st_size == 0:
                        self.logger.info(f"File is empty, skipping: {os.path.basename(filepath)}")
                        continue
                    
                    # Add processing delay
                    if self.process_delay > 0:
                        time.sleep(self.process_delay)
                    
                    # Process the file
                    try:
                        success = self.process_file(filepath, table_name)
                        if success:
                            processed_count += 1
                            self.logger.info(f"Successfully processed: {os.path.basename(filepath)} -> {table_name}")
                        else:
                            error_count += 1
                            self.logger.error(f"Failed to process: {os.path.basename(filepath)}")
                    except Exception as e:
                        error_count += 1
                        self.logger.error(f"Error processing {os.path.basename(filepath)}: {e}")
                        
        except Exception as e:
            self.logger.error(f"Error during file scanning: {e}")
        
        # Summary log (only when files are found)
        if processed_count > 0 or error_count > 0:
            self.logger.info(f"Scan complete: {processed_count} processed, {error_count} errors")
        elif skipped_count > 0:
            self.logger.debug(f"Scan complete: {skipped_count} skipped (no pattern match)")
    
    def get_status(self):
        """Get current watcher status"""
        return {
            'total_files_watched': len(self.file_timestamps),
            'total_files_processed': len(self.processed_files),
            'pattern_mappings': self.pattern_mappings,
            'watch_path': self.get_watch_path(),
            'poll_interval': self.poll_interval,
            'celery_configured': self.celery_app is not None,
            'initial_scan_complete': self.initial_scan_done,
            'max_file_size_mb': self.max_file_size_mb,
            'supported_extensions': list(self.supported_extensions),
            'config_file': getattr(self.config_manager, 'config_file', None) if hasattr(self, 'config_manager') else None
        }
    
    def reload_config(self):
        """Reload configuration from file"""
        if PatternConfig and hasattr(self, 'config_manager'):
            self.logger.info("Reloading configuration...")
            self.config_manager = PatternConfig(self.config_manager.config_file)
            self._setup_from_config()
            self.logger.info("Configuration reloaded successfully")
        else:
            self.logger.warning("Configuration reload not available")
    
    def start_watching(self):
        """Start the pattern-based file watcher"""
        self.logger.info("="*60)
        self.logger.info("CONFIGURABLE PATTERN-BASED FILE WATCHER STARTED")
        self.logger.info("="*60)
        self.logger.info(f"Watch path: {self.get_watch_path()}")
        self.logger.info(f"Supported files: {', '.join(self.supported_extensions)}")
        self.logger.info(f"Checking every {self.poll_interval} seconds")
        self.logger.info(f"Max file size: {self.max_file_size_mb}MB")
        
        if hasattr(self, 'config_manager') and self.config_manager:
            self.logger.info(f"Configuration file: {self.config_manager.config_file}")
        
        self.logger.info("Pattern -> Table mappings:")
        for pattern, table in self.pattern_mappings.items():
            self.logger.info(f"  '{pattern}' -> {table}")
        self.logger.info("="*60)
        
        try:
            while True:
                self.find_and_process_files()
                time.sleep(self.poll_interval)
                
        except KeyboardInterrupt:
            self.logger.info("File watcher stopped by user")
        except Exception as e:
            self.logger.error(f"File watcher error: {e}")
            raise

def show_usage():
    """Show usage information"""
    print("Configurable Pattern-Based File Watcher")
    print("=" * 50)
    print("Usage: python configurable_pattern_watcher.py [options]")
    print()
    print("Options:")
    print("  (no args)          Start the file watcher")
    print("  --status           Show current configuration")
    print("  --config <file>    Use specific config file")
    print("  --manage-config    Interactive configuration management")
    print("  --help             Show this help message")
    print()
    print("Configuration:")
    print("  The watcher uses YAML or JSON configuration files.")
    print("  Default locations searched:")
    print("    ./pattern_config.yaml")
    print("    ./config/pattern_config.yaml")
    print("    ~/.etl/pattern_config.yaml")
    print()
    print("  If no config file is found, a default one will be created.")

def show_status(config_file=None):
    """Show current configuration status"""
    try:
        if not CONFIG_SYSTEM_AVAILABLE and config_file:
            print("Warning: Configuration system not available, using defaults")
        
        watcher = ConfigurablePatternWatcher(config_file)
        status = watcher.get_status()
        
        print("Current Configuration:")
        print("=" * 40)
        print(f"Config File: {status.get('config_file', 'Default/Environment')}")
        print(f"Watch Path: {status['watch_path']}")
        print(f"Poll Interval: {status['poll_interval']} seconds")
        print(f"Max File Size: {status['max_file_size_mb']}MB")
        print(f"Supported Extensions: {', '.join(status['supported_extensions'])}")
        print(f"Celery Configured: {status['celery_configured']}")
        print(f"Initial Scan Complete: {status['initial_scan_complete']}")
        print(f"Files Watched: {status['total_files_watched']}")
        print(f"Files Processed: {status['total_files_processed']}")
        print()
        print("Pattern Mappings:")
        for pattern, table in status['pattern_mappings'].items():
            print(f"  {pattern:<20} -> {table}")
            
    except Exception as e:
        print(f"Error loading configuration: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main function"""
    config_file = None
    
    # Parse command line arguments
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        arg = args[i].lower()
        
        if arg in ['--help', '-h', 'help']:
            show_usage()
            return
        elif arg in ['--status', '-s', 'status']:
            show_status(config_file)
            return
        elif arg == '--config':
            if i + 1 < len(args):
                config_file = args[i + 1]
                i += 1
            else:
                print("Error: --config requires a file path")
                return
        elif arg == '--manage-config':
            if PatternConfig:
                from pattern_config_system import manage_config
                manage_config()
            else:
                print("Configuration management not available (missing pattern_config_system)")
            return
        else:
            print(f"Unknown argument: {args[i]}")
            show_usage()
            return
        i += 1
    
    # Start normal pattern-based watching
    try:
        watcher = ConfigurablePatternWatcher(config_file)
        watcher.start_watching()
    except KeyboardInterrupt:
        print("\nWatcher stopped by user")
    except Exception as e:
        print(f"Watcher failed to start: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()