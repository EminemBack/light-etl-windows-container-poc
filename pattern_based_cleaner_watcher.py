#!/usr/bin/env python3
"""
Pattern-Based File Watcher
Maps directory patterns to specific table names for production ETL processing
"""

import os
import sys
import time
import logging
import pandas as pd
import glob
from datetime import datetime
from pathlib import Path

# Force localhost settings
os.environ['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
os.environ['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/1'

# Clear any conflicting Celery settings
for key in list(os.environ.keys()):
    if 'CELERY' in key or 'REDIS' in key:
        if key not in ['CELERY_BROKER_URL', 'CELERY_RESULT_BACKEND']:
            del os.environ[key]

# Configuration
WATCH_PATH = os.environ.get('WATCH_PATH', r'Z:\\')
BACKUP_WATCH_PATH = os.environ.get('BACKUP_WATCH_PATH', r'.\watch_production')
POLL_INTERVAL = int(os.environ.get('POLL_INTERVAL', 10))
PROCESS_DELAY = int(os.environ.get('PROCESS_DELAY', 5))
SUPPORTED_EXTENSIONS = {'.csv', '.xlsx', '.xls', '.xlsm'}

# PATTERN MAPPING: directory pattern -> table name
PATTERN_TABLE_MAPPING = {
    'tel_list': 'dim_numbers',
    'customer_data': 'dim_customers',
    'product_info': 'dim_products',
    'sales_data': 'fact_sales',
    'inventory': 'dim_inventory',
    'transactions': 'fact_transactions',
    'reports': 'staging_reports',
    # Add more patterns as needed
    # 'pattern_in_path': 'target_table_name'
}

# Logging setup - production level
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('./pattern_watcher.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class PatternBasedWatcher:
    """Production file watcher that maps file paths to specific tables based on patterns"""
    
    def __init__(self):
        self.processed_files = set()
        self.file_timestamps = {}
        self.celery_app = self.create_celery_client()
        self.initial_scan_done = False
        
        logger.info("Pattern-Based File Watcher initialized")
        logger.info(f"Pattern mappings: {PATTERN_TABLE_MAPPING}")
        logger.info(f"Monitoring interval: {POLL_INTERVAL} seconds")
        
        # Record existing files without processing them
        self._initial_file_scan()
        
    def _initial_file_scan(self):
        """Record all existing files without processing them"""
        watch_path = self.get_watch_path()
        existing_count = 0
        
        logger.info("Recording existing files (will not process them)...")
        
        try:
            patterns = [os.path.join(watch_path, "**", f"*{ext}") for ext in SUPPORTED_EXTENSIONS]
            
            for pattern in patterns:
                for filepath in glob.glob(pattern, recursive=True):
                    if os.path.isfile(filepath) and self.is_supported_file(filepath):
                        # Record the file's current timestamp
                        stat_info = os.stat(filepath)
                        self.file_timestamps[filepath] = stat_info.st_mtime
                        existing_count += 1
                        
        except Exception as e:
            logger.error(f"Error during initial scan: {e}")
        
        self.initial_scan_done = True
        logger.info(f"Initial scan complete: {existing_count} existing files recorded (ignored)")
        logger.info("Watcher is now ready - will only process NEW files")
        
    def create_celery_client(self):
        """Create Celery client with production configuration"""
        try:
            from celery import Celery
            
            app = Celery('pattern_watcher')
            app.conf.update({
                'broker_url': 'redis://localhost:6379/0',
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
            
            logger.info("Celery client configured successfully")
            return app
            
        except Exception as e:
            logger.error(f"Failed to create Celery client: {e}")
            return None
    
    def get_watch_path(self):
        """Get the path to watch for files"""
        if os.path.exists(WATCH_PATH):
            return WATCH_PATH
        
        os.makedirs(BACKUP_WATCH_PATH, exist_ok=True)
        logger.info(f"Using backup watch path: {BACKUP_WATCH_PATH}")
        return BACKUP_WATCH_PATH
    
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
        for pattern, table_name in PATTERN_TABLE_MAPPING.items():
            if pattern.lower() in normalized_path:
                logger.info(f"Pattern '{pattern}' found in path -> table '{table_name}'")
                return table_name
        
        # No pattern matched
        logger.debug(f"No pattern matched for: {filepath}")
        return None
    
    def is_supported_file(self, filename):
        """Check if file is supported"""
        return Path(filename).suffix.lower() in SUPPORTED_EXTENSIONS
    
    def read_file_to_dataframe(self, filepath):
        """Read file and convert to DataFrame"""
        filename = os.path.basename(filepath)
        file_ext = Path(filepath).suffix.lower()
        
        try:
            if file_ext == '.csv':
                # Try different encodings for CSV
                for encoding in ['utf-8', 'utf-8-sig', 'latin1', 'cp1252']:
                    try:
                        df = pd.read_csv(filepath, encoding=encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    df = pd.read_csv(filepath, encoding='utf-8', errors='ignore')
                    
            elif file_ext in ['.xlsx', '.xls', '.xlsm']:
                df = pd.read_excel(filepath, sheet_name=0)
            else:
                raise ValueError(f"Unsupported file type: {file_ext}")
            
            if df.empty:
                logger.warning(f"File is empty: {filename}")
                return None
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading file {filename}: {e}")
            raise
    
    def process_file(self, filepath, table_name):
        """Process a single file to the specified table"""
        if not self.celery_app:
            logger.warning(f"Celery not configured, skipping {filepath}")
            return False
        
        filename = os.path.basename(filepath)
        
        try:
            # Read file to DataFrame
            df = self.read_file_to_dataframe(filepath)
            if df is None:
                return False
            
            # Convert DataFrame to records
            data_records = df.to_dict('records')
            
            # Create source name with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            source_name = f"{Path(filename).stem}_{timestamp}"
            
            logger.info(f"Processing: {filename} -> {table_name} ({len(data_records)} records)")
            
            # Send task using the specified table name
            result = self.celery_app.send_task(
                'process_dataframe',
                args=[data_records],
                kwargs={
                    'table_name': table_name,
                    'source_name': source_name
                }
            )
            
            logger.info(f"Task sent successfully: {result.id}")
            
            # Mark as processed
            self.processed_files.add(filepath)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to process file {filename}: {e}")
            return False
    
    def find_and_process_files(self):
        """Find new files and process them based on patterns"""
        if not self.initial_scan_done:
            return  # Skip if initial scan not complete
            
        watch_path = self.get_watch_path()
        processed_count = 0
        skipped_count = 0
        
        try:
            # Find all supported files recursively
            patterns = [os.path.join(watch_path, "**", f"*{ext}") for ext in SUPPORTED_EXTENSIONS]
            
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
                        logger.info(f"NEW file detected: {os.path.basename(filepath)} (pattern matched)")
                    else:
                        logger.info(f"MODIFIED file detected: {os.path.basename(filepath)} (pattern matched)")
                    
                    # Update timestamp
                    self.file_timestamps[filepath] = current_mtime
                    
                    # Verify file is stable (not being written to)
                    time.sleep(2)
                    new_stat = os.stat(filepath)
                    if new_stat.st_mtime != current_mtime:
                        logger.info(f"File still being written, skipping: {os.path.basename(filepath)}")
                        continue
                    
                    if new_stat.st_size == 0:
                        logger.info(f"File is empty, skipping: {os.path.basename(filepath)}")
                        continue
                    
                    # Add processing delay
                    if PROCESS_DELAY > 0:
                        time.sleep(PROCESS_DELAY)
                    
                    # Process the file
                    success = self.process_file(filepath, table_name)
                    if success:
                        processed_count += 1
                        logger.info(f"Successfully processed: {os.path.basename(filepath)} -> {table_name}")
                    else:
                        logger.error(f"Failed to process: {os.path.basename(filepath)}")
                        
        except Exception as e:
            logger.error(f"Error during file scanning: {e}")
        
        # Summary log (only when files are found)
        if processed_count > 0:
            logger.info(f"Scan complete: {processed_count} processed")
        elif skipped_count > 0:
            logger.debug(f"Scan complete: {skipped_count} skipped (no pattern match)")
    
    def get_status(self):
        """Get current watcher status"""
        return {
            'total_files_watched': len(self.file_timestamps),
            'total_files_processed': len(self.processed_files),
            'pattern_mappings': PATTERN_TABLE_MAPPING,
            'watch_path': self.get_watch_path(),
            'poll_interval': POLL_INTERVAL,
            'celery_configured': self.celery_app is not None,
            'initial_scan_complete': self.initial_scan_done
        }
    
    def start_watching(self):
        """Start the pattern-based file watcher"""
        logger.info("="*60)
        logger.info("PATTERN-BASED FILE WATCHER STARTED")
        logger.info("="*60)
        logger.info(f"Watch path: {self.get_watch_path()}")
        logger.info(f"Supported files: {', '.join(SUPPORTED_EXTENSIONS)}")
        logger.info(f"Checking every {POLL_INTERVAL} seconds")
        logger.info("Pattern -> Table mappings:")
        for pattern, table in PATTERN_TABLE_MAPPING.items():
            logger.info(f"  '{pattern}' -> {table}")
        logger.info("="*60)
        
        try:
            while True:
                self.find_and_process_files()
                time.sleep(POLL_INTERVAL)
                
        except KeyboardInterrupt:
            logger.info("File watcher stopped by user")
        except Exception as e:
            logger.error(f"File watcher error: {e}")
            raise

def show_usage():
    """Show usage information"""
    print("Pattern-Based File Watcher")
    print("=" * 40)
    print("Usage: python pattern_based_watcher.py [options]")
    print()
    print("Options:")
    print("  (no args)     Start the file watcher")
    print("  --status      Show current configuration")
    print("  --help        Show this help message")
    print()
    print("Environment Variables:")
    print("  WATCH_PATH           Path to monitor (default: Z:\\)")
    print("  POLL_INTERVAL        Seconds between checks (default: 10)")
    print("  PROCESS_DELAY        Delay before processing (default: 5)")
    print()
    print("Pattern Mappings:")
    for pattern, table in PATTERN_TABLE_MAPPING.items():
        print(f"  {pattern:<20} -> {table}")

def show_status():
    """Show current configuration status"""
    watcher = PatternBasedWatcher()
    status = watcher.get_status()
    
    print("Current Configuration:")
    print("=" * 40)
    print(f"Watch Path: {status['watch_path']}")
    print(f"Poll Interval: {status['poll_interval']} seconds")
    print(f"Celery Configured: {status['celery_configured']}")
    print(f"Initial Scan Complete: {status['initial_scan_complete']}")
    print(f"Files Watched: {status['total_files_watched']}")
    print(f"Files Processed: {status['total_files_processed']}")
    print()
    print("Pattern Mappings:")
    for pattern, table in status['pattern_mappings'].items():
        print(f"  {pattern:<20} -> {table}")

def main():
    """Main function"""
    
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        
        if arg in ['--help', '-h', 'help']:
            show_usage()
            return
        elif arg in ['--status', '-s', 'status']:
            show_status()
            return
        else:
            print(f"Unknown argument: {sys.argv[1]}")
            show_usage()
            return
    
    # Start normal pattern-based watching
    try:
        watcher = PatternBasedWatcher()
        watcher.start_watching()
    except KeyboardInterrupt:
        logger.info("Watcher stopped by user")
    except Exception as e:
        logger.error(f"Watcher failed to start: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()