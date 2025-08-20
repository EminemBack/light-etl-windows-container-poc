#!/usr/bin/env python3
"""
Working File Watcher - Using Ultimate Fix Configuration
This uses the EXACT same Celery config that just worked in ultimate_fix_watcher.py
"""

import os
import sys
import time
import logging
import pandas as pd
import glob
from datetime import datetime
from pathlib import Path

# CRITICAL: Force localhost settings (same as ultimate_fix_watcher.py)
os.environ['CELERY_BROKER_URL'] = 'redis://localhost:6379/0'
os.environ['CELERY_RESULT_BACKEND'] = 'redis://localhost:6379/1'

# Clear any conflicting Celery settings
for key in list(os.environ.keys()):
    if 'CELERY' in key or 'REDIS' in key:
        if key not in ['CELERY_BROKER_URL', 'CELERY_RESULT_BACKEND']:
            del os.environ[key]

# Configuration
WATCH_PATH = os.environ.get('WATCH_PATH', r'Z:\\')
BACKUP_WATCH_PATH = os.environ.get('BACKUP_WATCH_PATH', r'.\watch_test')
POLL_INTERVAL = int(os.environ.get('POLL_INTERVAL', 10))
PROCESS_DELAY = int(os.environ.get('PROCESS_DELAY', 5))
SUPPORTED_EXTENSIONS = {'.csv', '.xlsx', '.xls', '.xlsm'}

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('./working_watcher.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class WorkingFileWatcher:
    """File watcher using the proven working Celery configuration"""
    
    def __init__(self):
        self.processed_files = set()
        self.file_timestamps = {}
        self.celery_app = self.create_working_celery()
        
    def create_working_celery(self):
        """Create Celery with EXACT same config as ultimate_fix_watcher.py"""
        logger.info("Creating Celery with proven working configuration...")
        
        try:
            from celery import Celery
            
            # Use EXACT same configuration that worked
            app = Celery('working_watcher')
            
            # EXACT same configuration from ultimate_fix_watcher.py
            app.conf.update({
                'broker_url': 'redis://localhost:6379/0',
                'result_backend': None,  # Completely disable backend
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
            
            logger.info("PASS -- Working Celery configuration applied")
            logger.info(f"  Broker: {app.conf.broker_url}")
            logger.info(f"  Backend: {app.conf.result_backend}")
            
            return app
            
        except Exception as e:
            logger.error(f"Failed to create Celery: {e}")
            return None
    
    def get_watch_path(self):
        """Get the path to watch for files"""
        if os.path.exists(WATCH_PATH):
            logger.info(f"Using main watch path: {WATCH_PATH}")
            return WATCH_PATH
        
        os.makedirs(BACKUP_WATCH_PATH, exist_ok=True)
        logger.info(f"Using backup watch path: {BACKUP_WATCH_PATH}")
        return BACKUP_WATCH_PATH
    
    def is_supported_file(self, filename):
        """Check if file is supported"""
        return Path(filename).suffix.lower() in SUPPORTED_EXTENSIONS
    
    def read_file_to_dataframe(self, filepath):
        """Read file and convert to DataFrame"""
        filename = os.path.basename(filepath)
        file_ext = Path(filepath).suffix.lower()
        
        logger.info(f"Reading file: {filename}")
        
        try:
            if file_ext == '.csv':
                # Try different encodings for CSV
                for encoding in ['utf-8', 'utf-8-sig', 'latin1', 'cp1252']:
                    try:
                        df = pd.read_csv(filepath, encoding=encoding)
                        logger.info(f"Successfully read CSV with {encoding} encoding")
                        break
                    except UnicodeDecodeError:
                        continue
                else:
                    df = pd.read_csv(filepath, encoding='utf-8', errors='ignore')
                    logger.warning(f"Read CSV with error handling")
                    
            elif file_ext in ['.xlsx', '.xls', '.xlsm']:
                df = pd.read_excel(filepath, sheet_name=0)
                logger.info(f"Successfully read Excel file")
            else:
                raise ValueError(f"Unsupported file type: {file_ext}")
            
            if df.empty:
                logger.warning(f"File is empty: {filename}")
                return None
            
            logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns from {filename}")
            return df
            
        except Exception as e:
            logger.error(f"Error reading file {filename}: {e}")
            raise
    
    def process_file(self, filepath):
        """Process a single file using working Celery configuration"""
        if not self.celery_app:
            logger.warning(f"Celery not configured, skipping {filepath}")
            return False
        
        filename = os.path.basename(filepath)
        logger.info(f"Processing file: {filename}")
        
        try:
            # Read file to DataFrame
            df = self.read_file_to_dataframe(filepath)
            if df is None:
                logger.warning(f"Skipping empty file: {filename}")
                return False
            
            # Convert DataFrame to records
            data_records = df.to_dict('records')
            
            # Create names
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            source_name = f"{Path(filename).stem}_{timestamp}"
            table_name = Path(filename).stem.lower()
            table_name = ''.join(c if c.isalnum() else '_' for c in table_name)
            table_name = f"working_{table_name}"
            
            logger.info(f"Sending DataFrame processing task:")
            logger.info(f"   Records: {len(data_records)}")
            logger.info(f"   Source: {source_name}")
            logger.info(f"   Table: {table_name}")
            
            # Send task using proven working configuration
            result = self.celery_app.send_task(
                'process_dataframe',
                args=[data_records],
                kwargs={
                    'table_name': table_name,
                    'source_name': source_name
                }
            )
            
            logger.info(f"PASS -- Task sent successfully: {result.id}")
            
            # Mark as processed
            self.processed_files.add(filepath)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to process file {filename}: {e}")
            return False
    
    def find_new_files(self):
        """Find new or modified files"""
        watch_path = self.get_watch_path()
        new_files = []
        
        try:
            patterns = [os.path.join(watch_path, "**", f"*{ext}") for ext in SUPPORTED_EXTENSIONS]
            
            for pattern in patterns:
                for filepath in glob.glob(pattern, recursive=True):
                    if os.path.isfile(filepath) and self.is_supported_file(filepath):
                        
                        # Check if new or modified
                        stat_info = os.stat(filepath)
                        current_mtime = stat_info.st_mtime
                        
                        if filepath not in self.file_timestamps:
                            # New file
                            logger.info(f"New file detected: {os.path.basename(filepath)}")
                            self.file_timestamps[filepath] = current_mtime
                            
                            # Verify file is stable
                            time.sleep(2)
                            new_stat = os.stat(filepath)
                            if new_stat.st_mtime == current_mtime and new_stat.st_size > 0:
                                new_files.append(filepath)
                                
                        elif self.file_timestamps[filepath] != current_mtime:
                            # Modified file
                            logger.info(f"Modified file detected: {os.path.basename(filepath)}")
                            self.file_timestamps[filepath] = current_mtime
                            
                            if filepath not in self.processed_files:
                                time.sleep(2)
                                new_files.append(filepath)
                                
        except Exception as e:
            logger.error(f"Error finding new files: {e}")
        
        return new_files
    
    def run_once(self):
        """Run one cycle of file checking and processing"""
        logger.info("Checking for new files...")
        
        new_files = self.find_new_files()
        
        if new_files:
            logger.info(f"Found {len(new_files)} new files to process")
            
            for filepath in new_files:
                logger.info(f"Processing: {os.path.basename(filepath)}")
                
                # Add delay before processing
                logger.info(f"Waiting {PROCESS_DELAY} seconds before processing...")
                time.sleep(PROCESS_DELAY)
                
                success = self.process_file(filepath)
                if success:
                    logger.info(f"PASS -- Successfully processed: {os.path.basename(filepath)}")
                else:
                    logger.error(f"✗ Failed to process: {os.path.basename(filepath)}")
                
                # Wait between files
                time.sleep(2)
        else:
            logger.info("No new files found")
        
        # Show status
        logger.info(f"Status - Watched: {len(self.file_timestamps)}, Processed: {len(self.processed_files)}")
    
    def start_watching(self):
        """Start the file watcher"""
        logger.info("="*60)
        logger.info("WORKING FILE WATCHER STARTED")
        logger.info("="*60)
        logger.info(f"Watch path: {self.get_watch_path()}")
        logger.info(f"Supported files: {', '.join(SUPPORTED_EXTENSIONS)}")
        logger.info(f"Poll interval: {POLL_INTERVAL} seconds")
        logger.info(f"Using PROVEN working Celery configuration")
        logger.info("="*60)
        
        try:
            while True:
                self.run_once()
                
                logger.info(f"Waiting {POLL_INTERVAL} seconds before next check...")
                time.sleep(POLL_INTERVAL)
                
        except KeyboardInterrupt:
            logger.info("File watcher stopped by user")

def create_test_files():
    """Create test files"""
    test_dir = Path('./watch_test')
    test_dir.mkdir(exist_ok=True)
    
    # Test CSV
    data = {
        'product_id': [1001, 1002, 1003, 1004],
        'product_name': ['Laptop', 'Mouse', 'Keyboard', 'Monitor'],
        'category': ['Electronics', 'Accessories', 'Accessories', 'Electronics'],
        'price': [999.99, 29.99, 79.99, 399.99],
        'in_stock': [True, True, False, True]
    }
    
    df = pd.DataFrame(data)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_file = test_dir / f'test_products_{timestamp}.csv'
    # df.to_csv(csv_file, index=False)
    
    logger.info(f"Created test file: {csv_file}")
    return [str(csv_file)]

def main():
    """Main function"""
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'test':
            logger.info("Running test mode ... commented")
            
            # Create test files
            # test_files = create_test_files()
            
            # Create watcher and process files
            # watcher = WorkingFileWatcher()
            
            # for test_file in test_files:
            #     logger.info(f"Testing with: {os.path.basename(test_file)}")
            #     success = watcher.process_file(test_file)
            #     if success:
            #         logger.info("PASS -- Test completed successfully!")
            #     else:
            #         logger.error("✗ Test failed!")
            # return
        
        elif sys.argv[1] == 'create':
            create_test_files()
            return
    
    # Start normal file watching
    watcher = WorkingFileWatcher()
    watcher.start_watching()

if __name__ == "__main__":
    main()