# etl-worker/etl_processor/tasks.py
import logging
from celery import Task
from etl_processor.celery_app import app
from etl_processor.file_access import WindowsFileAccess
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import os

logger = logging.getLogger(__name__)

class CallbackTask(Task):
    """Task with callbacks for success/failure"""
    def on_success(self, retval, task_id, args, kwargs):
        logger.info(f'Task {task_id} succeeded with result: {retval}')
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error(f'Task {task_id} failed with exception: {exc}')

def get_db_engine():
    """Create database engine with Windows Authentication"""
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")
    
    # For Windows Authentication, ensure the connection string is properly formatted
    # Example: mssql+pyodbc://@SERVER/DATABASE?trusted_connection=yes&driver=SQL+Server
    return create_engine(database_url)

@app.task(base=CallbackTask, name='process_excel_file')
def process_excel_file(filename, sheet_name=0):
    """
    Process an Excel file from the Windows file server and save to SQL Server
    """
    logger.info(f"Starting to process {filename}")
    
    try:
        # Initialize file access
        file_access = WindowsFileAccess()
        
        # Read the Excel file
        df = file_access.read_excel(filename, sheet_name=sheet_name)
        
        # Perform your ETL operations here
        logger.info(f"Loaded {len(df)} rows from {filename}")
        
        # Example: Save to SQL Server with Windows Authentication
        engine = get_db_engine()
        
        # Write to database (replace 'your_table_name' with actual table)
        df.to_sql(
            name='etl_staging_table',
            con=engine,
            if_exists='append',  # or 'replace'
            index=False,
            schema='dbo'  # specify schema if needed
        )
        
        logger.info(f"Successfully saved {len(df)} rows to database")
        
        result = {
            'filename': filename,
            'rows_processed': len(df),
            'columns': df.columns.tolist(),
            'processed_at': datetime.now().isoformat()
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Error processing {filename}: {str(e)}")
        raise

@app.task(name='scan_for_new_files')
def scan_for_new_files():
    """
    Scan the Windows file server for new files to process
    """
    try:
        file_access = WindowsFileAccess()
        files = file_access.list_files()
        
        logger.info(f"Found {files.get('count', 0)} files")
        
        # Queue processing tasks for each file
        for file_info in files.get('files', []):
            process_excel_file.delay(file_info['name'])
        
        return files
        
    except Exception as e:
        logger.error(f"Error scanning for files: {str(e)}")
        raise

@app.task(name='test_connection')
def test_connection():
    """Test connection to the file server"""
    try:
        file_access = WindowsFileAccess()
        health = file_access.health_check()
        logger.info(f"File server health: {health}")
        return health
    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        raise