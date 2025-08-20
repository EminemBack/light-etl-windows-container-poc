# etl-worker/etl_processor/dataframe_tasks.py
"""
Simple DataFrame Processing Task
Process CSV data passed as JSON/dict from any source
"""

import logging
import pandas as pd
from datetime import datetime
from celery import Task
from etl_processor.celery_app import app
from etl_processor.database_postgres import get_db_engine
from sqlalchemy import text

logger = logging.getLogger(__name__)

class DataFrameTask(Task):
    """Simple task for DataFrame processing"""
    def on_success(self, retval, task_id, args, kwargs):
        logger.info(f'DataFrame Task {task_id} completed: {retval.get("rows_processed", 0)} rows')
    
    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error(f'DataFrame Task {task_id} failed: {exc}')

@app.task(base=DataFrameTask, name='process_dataframe')
def process_dataframe(data_records, table_name='processed_data', source_name='dataframe'):
    """
    Process DataFrame data sent as JSON records
    
    Args:
        data_records (list): List of dictionaries (like df.to_dict('records'))
        table_name (str): Target PostgreSQL table name
        source_name (str): Source identifier for tracking
    
    Returns:
        dict: Processing results
    """
    logger.info(f"Processing DataFrame data: {source_name}")
    start_time = datetime.utcnow()
    
    try:
        # Convert records back to DataFrame
        df = pd.DataFrame(data_records)
        
        if df.empty:
            logger.warning(f"DataFrame is empty: {source_name}")
            return {
                'status': 'warning',
                'message': 'DataFrame is empty',
                'source_name': source_name,
                'rows_processed': 0
            }
        
        # Clean column names (PostgreSQL friendly)
        original_columns = df.columns.tolist()
        df.columns = df.columns.str.replace(r'[^a-zA-Z0-9_]', '_', regex=True)
        df.columns = df.columns.str.strip('_').str.lower()
        
        # Remove duplicate column names
        cols = df.columns.tolist()
        df.columns = [f"{col}_{i}" if cols.count(col) > 1 and i > 0 else col 
                     for i, col in enumerate(cols)]
        
        # Add processing metadata
        df['source_name'] = source_name
        df['processed_at'] = datetime.utcnow()
        df['processing_batch'] = f"{source_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"Processing {len(df)} rows, {len(original_columns)} original columns")
        logger.info(f"Original columns: {original_columns}")
        logger.info(f"Final columns: {df.columns.tolist()}")
        
        # Save to PostgreSQL
        engine = get_db_engine()
        
        logger.info(f"Writing to table: {table_name}")
        
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',  # Always append
            index=False,
            method='multi',
            chunksize=1000
        )
        
        # Log the processing
        processing_time = (datetime.utcnow() - start_time).total_seconds()
        
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO etl_processing_log 
                (filename, sheet_name, rows_processed, status, processed_at, processing_time_seconds)
                VALUES (:filename, :sheet_name, :rows, :status, :processed_at, :time)
            """), {
                'filename': source_name,
                'sheet_name': 'DataFrame',
                'rows': len(df),
                'status': 'success',
                'processed_at': datetime.utcnow(),
                'time': int(processing_time)
            })
            conn.commit()
        
        # Get final table count
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            total_count = result.fetchone()[0]
        
        logger.info(f"Successfully processed {len(df)} rows")
        logger.info(f"Total rows in {table_name}: {total_count}")
        
        return {
            'status': 'success',
            'source_name': source_name,
            'table_name': table_name,
            'rows_processed': len(df),
            'total_rows_in_table': total_count,
            'original_columns': original_columns,
            'final_columns': df.columns.tolist(),
            'processing_time_seconds': processing_time,
            'processed_at': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        # Log error to database
        try:
            engine = get_db_engine()
            processing_time = (datetime.utcnow() - start_time).total_seconds()
            
            with engine.connect() as conn:
                conn.execute(text("""
                    INSERT INTO etl_processing_log 
                    (filename, sheet_name, status, error_message, processed_at, processing_time_seconds)
                    VALUES (:filename, :sheet_name, :status, :error, :processed_at, :time)
                """), {
                    'filename': source_name,
                    'sheet_name': 'DataFrame',
                    'status': 'error',
                    'error': str(e)[:1000],
                    'processed_at': datetime.utcnow(),
                    'time': int(processing_time)
                })
                conn.commit()
        except:
            pass  # Don't fail if logging fails
        
        logger.error(f"Error processing DataFrame {source_name}: {str(e)}")
        raise

@app.task(name='test_dataframe_connectivity')
def test_dataframe_connectivity():
    """Simple test task to verify everything is connected"""
    try:
        from etl_processor.database_postgres import test_connection
        result = test_connection()
        logger.info(f"Database connectivity test: {result['status']}")
        return {
            'task': 'test_dataframe_connectivity',
            'timestamp': datetime.utcnow().isoformat(),
            'database_status': result['status'],
            'message': 'DataFrame processing system ready'
        }
    except Exception as e:
        logger.error(f"Connectivity test failed: {str(e)}")
        return {
            'task': 'test_dataframe_connectivity',
            'timestamp': datetime.utcnow().isoformat(),
            'database_status': 'error',
            'error': str(e)
        }