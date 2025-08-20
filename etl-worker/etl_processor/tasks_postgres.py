# etl-worker/etl_processor/tasks_postgres.py
import logging
from celery import Task
from etl_processor.celery_app import app
from etl_processor.file_access import WindowsFileAccess
from etl_processor.database_postgres import get_db_engine, create_tables, insert_sample_data
import pandas as pd
from datetime import datetime
import os
from sqlalchemy import text

logger = logging.getLogger(__name__)

class CallbackTask(Task):
    """Task with callbacks for success/failure"""
    def on_success(self, retval, task_id, args, kwargs):
        logger.info(f'Task {task_id} succeeded with result: {retval}')
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error(f'Task {task_id} failed with exception: {exc}')

@app.task(base=CallbackTask, name='test_postgres_connection')
def test_postgres_connection():
    """Test PostgreSQL connection"""
    logger.info("Testing PostgreSQL connection...")
    
    try:
        from etl_processor.database_postgres import test_connection
        result = test_connection()
        logger.info(f"PostgreSQL test result: {result['status']}")
        return result
    except Exception as e:
        logger.error(f"PostgreSQL connection test failed: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }

@app.task(base=CallbackTask, name='setup_database')
def setup_database():
    """Create tables and insert sample data"""
    logger.info("Setting up database...")
    
    try:
        # Create tables
        tables_created = create_tables()
        if not tables_created:
            return {"status": "error", "message": "Failed to create tables"}
        
        # Insert sample data
        sample_inserted = insert_sample_data()
        if not sample_inserted:
            return {"status": "warning", "message": "Tables created but sample data insertion failed"}
        
        return {
            "status": "success",
            "message": "Database setup completed successfully",
            "tables_created": True,
            "sample_data_inserted": True
        }
        
    except Exception as e:
        logger.error(f"Database setup failed: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }

@app.task(base=CallbackTask, name='process_excel_to_postgres')
def process_excel_to_postgres(filename, sheet_name=0, table_name='processed_data'):
    """
    Process an Excel file from the Windows file server and save to PostgreSQL
    
    Args:
        filename (str): Name of the Excel file to process
        sheet_name (str/int): Sheet name or index to read
        table_name (str): Target table name
    """
    logger.info(f"Starting to process {filename} -> PostgreSQL")
    start_time = datetime.utcnow()
    
    try:
        # Initialize file access
        file_access = WindowsFileAccess()
        
        # Read the Excel file
        logger.info(f"Reading file: {filename}, sheet: {sheet_name}")
        df = file_access.read_excel(filename, sheet_name=sheet_name)
        
        if df.empty:
            logger.warning(f"File {filename} is empty or contains no data")
            return {
                'status': 'warning',
                'filename': filename,
                'message': 'File is empty'
            }
        
        # Clean column names (remove special characters, spaces)
        df.columns = df.columns.str.replace(r'[^a-zA-Z0-9_]', '_', regex=True)
        df.columns = df.columns.str.strip('_')
        df.columns = df.columns.str.lower()  # PostgreSQL prefers lowercase
        
        logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns from {filename}")
        logger.info(f"Columns: {list(df.columns)}")
        
        # Get database engine
        engine = get_db_engine()
        
        # Add metadata columns
        df['etl_source_file'] = filename
        df['etl_processed_at'] = datetime.utcnow()
        df['etl_sheet_name'] = str(sheet_name)
        
        # Write to database
        logger.info(f"Writing to table: {table_name}")
        
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',  # Options: 'fail', 'replace', 'append'
            index=False,
            method='multi',  # Faster bulk insert
            chunksize=1000   # Process in chunks for large files
        )
        
        # Log the processing
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO etl_processing_log 
                (filename, sheet_name, rows_processed, status, processed_at, processing_time_seconds)
                VALUES (:filename, :sheet_name, :rows, :status, :processed_at, :time)
            """), {
                'filename': filename,
                'sheet_name': str(sheet_name),
                'rows': len(df),
                'status': 'success',
                'processed_at': datetime.utcnow(),
                'time': int(processing_time)
            })
            conn.commit()
        
        logger.info(f"Successfully saved {len(df)} rows to {table_name}")
        
        result = {
            'status': 'success',
            'filename': filename,
            'sheet_name': sheet_name,
            'table_name': table_name,
            'rows_processed': len(df),
            'columns': list(df.columns),
            'processed_at': datetime.utcnow().isoformat(),
            'processing_time_seconds': processing_time
        }
        
        return result
        
    except Exception as e:
        # Log the error
        try:
            engine = get_db_engine()
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO etl_processing_log 
                    (filename, sheet_name, status, error_message, processed_at, processing_time_seconds)
                    VALUES (:filename, :sheet_name, :status, :error, :processed_at, :time)
                """), {
                    'filename': filename,
                    'sheet_name': str(sheet_name),
                    'status': 'error',
                    'error': str(e)[:1000],
                    'processed_at': datetime.utcnow(),
                    'time': int(processing_time)
                })
                conn.commit()
        except:
            pass  # Don't fail if logging fails
        
        logger.error(f"Error processing {filename}: {str(e)}")
        raise

@app.task(base=CallbackTask, name='scan_and_process_files')
def scan_and_process_files():
    """
    Scan the Windows file server for Excel files and process them
    """
    logger.info("Scanning for files to process...")
    
    try:
        file_access = WindowsFileAccess()
        files_info = file_access.list_files()
        
        processed_files = []
        failed_files = []
        
        for file_info in files_info.get('files', []):
            filename = file_info['name']
            
            if filename.lower().endswith(('.xlsx', '.xls', '.xlsm')):
                try:
                    logger.info(f"Processing file: {filename}")
                    
                    # Process the file
                    result = process_excel_to_postgres.apply_async(
                        args=[filename],
                        kwargs={'table_name': 'processed_excel_data'}
                    ).get(timeout=300)  # 5 minute timeout per file
                    
                    processed_files.append({
                        'filename': filename,
                        'status': 'success',
                        'rows': result.get('rows_processed', 0)
                    })
                    
                except Exception as e:
                    logger.error(f"Failed to process {filename}: {str(e)}")
                    failed_files.append({
                        'filename': filename,
                        'error': str(e)
                    })
        
        return {
            'status': 'completed',
            'processed_files': processed_files,
            'failed_files': failed_files,
            'total_processed': len(processed_files),
            'total_failed': len(failed_files)
        }
        
    except Exception as e:
        logger.error(f"File scanning failed: {str(e)}")
        raise

@app.task(name='get_processing_stats')
def get_processing_stats():
    """Get ETL processing statistics"""
    try:
        engine = get_db_engine()
        
        with engine.connect() as conn:
            # Get processing stats
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_jobs,
                    COUNT(CASE WHEN status = 'success' THEN 1 END) as successful_jobs,
                    COUNT(CASE WHEN status = 'error' THEN 1 END) as failed_jobs,
                    SUM(rows_processed) as total_rows_processed,
                    AVG(processing_time_seconds) as avg_processing_time
                FROM etl_processing_log
            """))
            
            stats = result.fetchone()
            
            return {
                'total_jobs': stats[0] or 0,
                'successful_jobs': stats[1] or 0,
                'failed_jobs': stats[2] or 0,
                'total_rows_processed': stats[3] or 0,
                'avg_processing_time_seconds': float(stats[4] or 0)
            }
            
    except Exception as e:
        logger.error(f"Error getting stats: {str(e)}")
        return {"error": str(e)}