# etl-worker/etl_processor/enhanced_tasks.py
import logging
import os
from datetime import datetime
from celery import Task
from etl_processor.celery_app import app
from etl_processor.file_access import WindowsFileAccess
import pandas as pd

logger = logging.getLogger(__name__)

class CallbackTask(Task):
    """Enhanced task with callbacks and auto-processing support"""
    def on_success(self, retval, task_id, args, kwargs):
        logger.info(f'Task {task_id} succeeded: {retval}')
        
        # Notify file server of completion
        if kwargs.get('auto_triggered'):
            self.notify_processing_complete(args[0], 'success', retval)
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error(f'Task {task_id} failed: {exc}')
        
        # Notify file server of failure
        if kwargs.get('auto_triggered'):
            self.notify_processing_complete(args[0], 'failure', str(exc))
    
    def notify_processing_complete(self, filename, status, details):
        """Notify file server that processing is complete"""
        try:
            import requests
            fileserver_url = os.getenv('FILESERVER_URL', 'http://host.docker.internal:5000')
            
            response = requests.post(
                f"{fileserver_url}/processing_complete",
                json={
                    'filename': filename,
                    'status': status,
                    'details': details,
                    'timestamp': datetime.now().isoformat(),
                    'worker_id': self.request.hostname
                },
                timeout=10
            )
            logger.info(f"Notified file server of completion: {filename} - {status}")
            
        except Exception as e:
            logger.warning(f"Failed to notify file server: {e}")

@app.task(base=CallbackTask, name='etl_processor.tasks.process_excel_file')
def process_excel_file(filename, sheet_name=0, auto_triggered=False, filepath=None):
    """
    Enhanced Excel processing with auto-trigger support
    """
    logger.info(f"Processing {filename} (auto_triggered: {auto_triggered})")
    
    try:
        # Initialize file access
        file_access = WindowsFileAccess()
        
        # Get file info first
        files_info = file_access.list_files()
        target_file = None
        
        for file_info in files_info.get('files', []):
            if file_info['name'] == filename:
                target_file = file_info
                break
        
        if not target_file:
            raise FileNotFoundError(f"File {filename} not found in file server")
        
        # Log file details
        logger.info(f"Processing file: {filename}")
        logger.info(f"File size: {target_file.get('size_mb', 'unknown')} MB")
        logger.info(f"Modified: {target_file.get('modified', 'unknown')}")
        
        # Read the Excel file with error handling
        try:
            df = file_access.read_excel(filename, sheet_name=sheet_name)
        except Exception as read_error:
            logger.error(f"Failed to read {filename}: {read_error}")
            # Try reading with basic parameters
            df = file_access.read_excel(filename, sheet_name=0, nrows=None)
        
        # Validate data
        if df.empty:
            raise ValueError(f"File {filename} contains no data")
        
        logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns from {filename}")
        
        # Data quality checks
        null_counts = df.isnull().sum()
        logger.info(f"Null value counts: {null_counts.to_dict()}")
        
        # Basic data cleaning
        original_rows = len(df)
        df = df.dropna(how='all')  # Remove completely empty rows
        cleaned_rows = len(df)
        
        if cleaned_rows < original_rows:
            logger.info(f"Removed {original_rows - cleaned_rows} empty rows")
        
        # ETL Processing Logic
        processed_df = perform_etl_transformations(df, filename)
        
        # Save to database
        if os.getenv('DATABASE_URL'):
            save_to_database(processed_df, filename)
        
        # Archive processed file (optional)
        if auto_triggered:
            archive_processed_file(filename, file_access)
        
        # Prepare result
        result = {
            'filename': filename,
            'original_rows': original_rows,
            'processed_rows': len(processed_df),
            'columns': processed_df.columns.tolist(),
            'processing_time': datetime.now().isoformat(),
            'auto_triggered': auto_triggered,
            'file_size_mb': target_file.get('size_mb', 0),
            'data_quality': {
                'null_counts': null_counts.to_dict(),
                'empty_rows_removed': original_rows - cleaned_rows
            }
        }
        
        logger.info(f"Successfully processed {filename}: {len(processed_df)} rows")
        return result
        
    except Exception as e:
        logger.error(f"Error processing {filename}: {str(e)}")
        raise

def perform_etl_transformations(df, filename):
    """
    Perform your specific ETL transformations here
    Customize this function based on your business logic
    """
    logger.info(f"Starting ETL transformations for {filename}")
    
    # Example transformations (customize as needed)
    processed_df = df.copy()
    
    # 1. Standardize column names
    processed_df.columns = [col.strip().lower().replace(' ', '_') for col in processed_df.columns]
    
    # 2. Add metadata columns
    processed_df['file_source'] = filename
    processed_df['processed_timestamp'] = datetime.now()
    processed_df['processing_batch'] = f"{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # 3. Data type conversions (example)
    for col in processed_df.columns:
        if 'date' in col.lower():
            try:
                processed_df[col] = pd.to_datetime(processed_df[col], errors='coerce')
            except:
                pass
        elif 'amount' in col.lower() or 'price' in col.lower():
            try:
                processed_df[col] = pd.to_numeric(processed_df[col], errors='coerce')
            except:
                pass
    
    # 4. Business logic transformations
    # Add your specific transformations here
    
    logger.info(f"ETL transformations completed for {filename}")
    return processed_df

def save_to_database(df, filename):
    """Save processed data to database"""
    try:
        from sqlalchemy import create_engine
        database_url = os.getenv('DATABASE_URL')
        
        if not database_url:
            logger.warning("No DATABASE_URL configured, skipping database save")
            return
        
        engine = create_engine(database_url)
        
        # Determine table name based on filename or use default
        table_name = 'etl_processed_data'
        
        # Save to database
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            schema='dbo',
            method='multi',
            chunksize=1000
        )
        
        logger.info(f"Saved {len(df)} rows to database table: {table_name}")
        
    except Exception as e:
        logger.error(f"Failed to save to database: {e}")
        raise

def archive_processed_file(filename, file_access):
    """Archive the processed file"""
    try:
        # This would move/copy the file to an archive location
        # Implementation depends on your archival strategy
        logger.info(f"File {filename} marked for archival")
        
        # Example: You could call a file server endpoint to move the file
        # Or implement your own archival logic
        
    except Exception as e:
        logger.warning(f"Failed to archive {filename}: {e}")

@app.task(name='etl_processor.tasks.batch_process_files')
def batch_process_files(file_patterns=None):
    """
    Process multiple files in batch
    Useful for catching up on files that may have been missed
    """
    try:
        file_access = WindowsFileAccess()
        files_info = file_access.list_files()
        
        processed_count = 0
        errors = []
        
        for file_info in files_info.get('files', []):
            filename = file_info['name']
            
            # Apply file patterns if specified
            if file_patterns:
                if not any(pattern in filename for pattern in file_patterns):
                    continue
            
            try:
                # Process each file
                result = process_excel_file.delay(filename, auto_triggered=True)
                processed_count += 1
                logger.info(f"Queued {filename} for processing")
                
            except Exception as e:
                error_msg = f"Failed to queue {filename}: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)
        
        return {
            'queued_files': processed_count,
            'errors': errors,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Batch processing failed: {e}")
        raise

@app.task(name='etl_processor.tasks.health_check')
def health_check():
    """Health check task for monitoring"""
    try:
        file_access = WindowsFileAccess()
        health = file_access.health_check()
        
        return {
            'status': 'healthy',
            'file_server': health,
            'worker_hostname': health_check.request.hostname,
            'timestamp': datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }

# Periodic tasks (optional)
from celery.schedules import crontab

app.conf.beat_schedule = {
    'health-check-every-5-minutes': {
        'task': 'etl_processor.tasks.health_check',
        'schedule': crontab(minute='*/5'),
    },
    'batch-process-missed-files': {
        'task': 'etl_processor.tasks.batch_process_files', 
        'schedule': crontab(hour=2, minute=0),  # Daily at 2 AM
    },
}
app.conf.timezone = 'UTC'