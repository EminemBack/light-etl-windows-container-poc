#!/usr/bin/env python3
"""
Manual ETL Test Script
Run this to test Excel to PostgreSQL data flow without the file watcher
"""

import os
import pandas as pd
from datetime import datetime
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_postgres_connection():
    """Test PostgreSQL connection"""
    try:
        # Add the etl-worker directory to Python path
        import sys
        sys.path.insert(0, './etl-worker')
        
        from etl_processor.database_postgres import test_connection, create_tables
        
        print("Testing PostgreSQL connection...")
        result = test_connection()
        print(f"Connection test result: {result['status']}")
        
        if result['status'] == 'success':
            print(f"✅ Connected to database: {result['database']}")
            print(f"   User: {result['user']}")
            print(f"   Host: {result['host']}:{result['port']}")
            
            # Create tables if they don't exist
            print("Creating tables if they don't exist...")
            create_tables()
            return True
        else:
            print(f"❌ Connection failed: {result['error']}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing connection: {e}")
        return False

def create_sample_excel_data():
    """Create sample Excel file for testing"""
    print("Creating sample Excel file...")
    
    # Sample data
    data = {
        'username': ['jdoe', 'asmith', 'bwilson', 'mjohnson', 'slee'],
        'email': ['john.doe@company.com', 'alice.smith@company.com', 
                 'bob.wilson@company.com', 'mary.johnson@company.com', 'sam.lee@company.com'],
        'full_name': ['John Doe', 'Alice Smith', 'Bob Wilson', 'Mary Johnson', 'Sam Lee'],
        'department': ['IT', 'Finance', 'Operations', 'HR', 'Marketing'],
        'salary': [75000, 85000, 70000, 80000, 72000],
        'hire_date': ['2020-01-15', '2019-03-22', '2021-06-10', '2018-11-05', '2022-02-14'],
        'is_active': [True, True, True, False, True]
    }
    
    df = pd.DataFrame(data)
    
    # Create sample file in current directory
    filename = 'sample_employee_data.xlsx'
    df.to_excel(filename, index=False)
    
    print(f"✅ Created sample file: {filename}")
    print(f"   Rows: {len(df)}")
    print(f"   Columns: {list(df.columns)}")
    
    return filename, df

def manual_etl_process(excel_file, df=None):
    """Manually process Excel file and save to PostgreSQL"""
    try:
        # Add the etl-worker directory to Python path
        import sys
        sys.path.insert(0, './etl-worker')
        
        from etl_processor.database_postgres import get_db_engine
        from sqlalchemy import text
        
        print(f"Starting manual ETL process for: {excel_file}")
        
        # Read Excel file if df not provided
        if df is None:
            print("Reading Excel file...")
            df = pd.read_excel(excel_file)
        
        print(f"Loaded {len(df)} rows, {len(df.columns)} columns")
        print(f"Columns: {list(df.columns)}")
        
        # Clean column names (PostgreSQL friendly)
        df.columns = df.columns.str.replace(r'[^a-zA-Z0-9_]', '_', regex=True)
        df.columns = df.columns.str.strip('_').str.lower()
        
        # Add ETL metadata
        df['etl_source_file'] = excel_file
        df['etl_processed_at'] = datetime.utcnow()
        df['etl_batch_id'] = f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        print("Cleaned column names:", list(df.columns))
        
        # Get database connection
        print("Connecting to PostgreSQL...")
        engine = get_db_engine()
        
        # Write to database
        table_name = 'processed_excel_data'
        print(f"Writing to table: {table_name}")
        
        rows_inserted = df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',  # Change to 'replace' if you want to clear table first
            index=False,
            method='multi',
            chunksize=1000
        )
        
        # Log to processing log
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO etl_processing_log 
                (filename, sheet_name, rows_processed, status, processed_at, processing_time_seconds)
                VALUES (:filename, :sheet_name, :rows, :status, :processed_at, :time)
            """), {
                'filename': excel_file,
                'sheet_name': 'Sheet1',
                'rows': len(df),
                'status': 'success',
                'processed_at': datetime.utcnow(),
                'time': 5  # Placeholder
            })
            conn.commit()
        
        print(f"✅ Successfully processed {len(df)} rows")
        print(f"   Table: {table_name}")
        print(f"   Database: PostgreSQL")
        
        # Verify data was inserted
        print("Verifying data insertion...")
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            total_count = result.fetchone()[0]
            
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM {table_name} 
                WHERE etl_source_file = :filename
            """), {'filename': excel_file})
            file_count = result.fetchone()[0]
        
        print(f"✅ Verification complete:")
        print(f"   Total rows in table: {total_count}")
        print(f"   Rows from this file: {file_count}")
        
        return True
        
    except Exception as e:
        print(f"❌ ETL process failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_celery_task():
    """Test the Celery task directly"""
    try:
        print("Testing Celery task execution...")
        
        # Add the etl-worker directory to Python path
        import sys
        sys.path.insert(0, './etl-worker')
        
        # This requires the Celery worker to be running
        from etl_processor.tasks_postgres import process_excel_to_postgres
        
        # Create sample file
        filename, _ = create_sample_excel_data()
        
        # Trigger task
        print("Triggering Celery task...")
        result = process_excel_to_postgres.delay(filename, table_name='celery_test_data')
        
        print(f"Task ID: {result.task_id}")
        print("Waiting for task completion...")
        
        # Wait for result (timeout after 60 seconds)
        task_result = result.get(timeout=60)
        
        print(f"✅ Celery task completed successfully!")
        print(f"   Result: {task_result}")
        
        return True
        
    except Exception as e:
        print(f"❌ Celery task failed: {e}")
        return False

def main():
    """Main test function"""
    print("=" * 60)
    print("Manual ETL Test - PostgreSQL")
    print("=" * 60)
    
    # Step 1: Test PostgreSQL connection
    if not test_postgres_connection():
        print("❌ PostgreSQL connection failed. Check your database configuration.")
        return False
    
    print("\n" + "-" * 40)
    
    # Step 2: Create sample data and run manual ETL
    filename, df = create_sample_excel_data()
    
    if manual_etl_process(filename, df):
        print("\n✅ Manual ETL process completed successfully!")
    else:
        print("\n❌ Manual ETL process failed!")
        return False
    
    print("\n" + "-" * 40)
    
    # Step 3: Test with existing Excel file (if any)
    print("Testing with existing Excel files...")
    
    # Look for Excel files in common locations
    test_paths = [
        '.',  # Current directory
        './shared_data',
        'Z:\\',  # Your shared drive
        'C:\\temp',
    ]
    
    found_files = []
    for path in test_paths:
        if os.path.exists(path):
            for file in os.listdir(path):
                if file.endswith(('.xlsx', '.xls', '.xlsm')):
                    full_path = os.path.join(path, file)
                    if os.path.getsize(full_path) < 50 * 1024 * 1024:  # Less than 50MB
                        found_files.append(full_path)
    
    if found_files:
        test_file = found_files[0]
        print(f"Found Excel file: {test_file}")
        
        try:
            if manual_etl_process(test_file):
                print(f"✅ Successfully processed existing file: {os.path.basename(test_file)}")
        except Exception as e:
            print(f"❌ Failed to process {test_file}: {e}")
    
    print("\n" + "=" * 60)
    print("Test Summary:")
    print("✅ PostgreSQL connection: Working")
    print("✅ Manual ETL process: Working")
    print("✅ Data insertion: Working")
    print("\nNext steps:")
    print("1. Check if Celery workers are running: docker-compose logs etl-worker")
    print("2. Test file watcher service: python file_watcher_service.py")
    print("3. Copy Excel files to Z:\\ drive to trigger automatic processing")
    print("=" * 60)
    
    return True

if __name__ == "__main__":
    # Set environment variables for database connection
    os.environ.setdefault('DB_HOST', 'localhost')
    os.environ.setdefault('DB_PORT', '5432')
    os.environ.setdefault('DB_NAME', 'etl_database')
    os.environ.setdefault('DB_USER', 'etl_user')
    os.environ.setdefault('DB_PASSWORD', 'SecurePassword123!')
    
    main()