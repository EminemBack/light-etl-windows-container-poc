#!/usr/bin/env python3
"""
Simple ETL Test - Fixed for etl-worker directory
Test Excel to PostgreSQL data flow
"""

import sys
import os
from pathlib import Path

# Add etl-worker to Python path
current_dir = Path.cwd()
etl_worker_path = current_dir / 'etl-worker'
if etl_worker_path.exists():
    sys.path.insert(0, str(etl_worker_path))
else:
    print(f"❌ ETL worker directory not found: {etl_worker_path}")
    print("Make sure you're running this from the project root directory")
    sys.exit(1)

import pandas as pd
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Setup environment variables
os.environ.setdefault('DB_HOST', 'localhost')
os.environ.setdefault('DB_PORT', '5432')
os.environ.setdefault('DB_NAME', 'etl_database')
os.environ.setdefault('DB_USER', 'etl_user')
os.environ.setdefault('DB_PASSWORD', 'SecurePassword123!')

def test_database_connection():
    """Test PostgreSQL connection and create tables"""
    print("🔍 Testing PostgreSQL connection...")
    
    try:
        from etl_processor.database_postgres import test_connection, create_tables
        
        # Test connection
        result = test_connection()
        
        if result['status'] == 'success':
            print("✅ PostgreSQL connection successful!")
            print(f"   Database: {result['database']}")
            print(f"   User: {result['user']}")
            print(f"   Host: {result['host']}:{result['port']}")
            
            # Create tables
            print("🔧 Creating tables...")
            create_tables()
            print("✅ Tables created/verified")
            
            return True
        else:
            print(f"❌ Connection failed: {result.get('error')}")
            return False
            
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def create_sample_data():
    """Create sample Excel data"""
    print("📝 Creating sample data...")
    
    data = {
        'employee_id': [1001, 1002, 1003, 1004, 1005],
        'username': ['emp001', 'emp002', 'emp003', 'emp004', 'emp005'],
        'email': [
            'emp001@company.com', 'emp002@company.com', 'emp003@company.com',
            'emp004@company.com', 'emp005@company.com'
        ],
        'full_name': [
            'John Smith', 'Jane Doe', 'Bob Johnson', 'Alice Wilson', 'Charlie Brown'
        ],
        'department': ['IT', 'Finance', 'HR', 'Operations', 'Marketing'],
        'salary': [75000, 85000, 70000, 80000, 72000],
        'hire_date': ['2020-01-15', '2019-03-22', '2021-06-10', '2018-11-05', '2022-02-14'],
        'is_active': [True, True, True, False, True]
    }
    
    df = pd.DataFrame(data)
    
    # Save to Excel
    filename = f'test_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
    df.to_excel(filename, index=False)
    
    print(f"✅ Created sample file: {filename}")
    print(f"   Rows: {len(df)}, Columns: {len(df.columns)}")
    
    return filename, df

def process_excel_to_postgres(filename, df):
    """Process Excel data and save to PostgreSQL"""
    print(f"🔄 Processing {filename} to PostgreSQL...")
    
    try:
        from etl_processor.database_postgres import get_db_engine
        from sqlalchemy import text
        
        # Clean column names for PostgreSQL
        df_clean = df.copy()
        df_clean.columns = df_clean.columns.str.lower().str.replace(' ', '_')
        
        # Add ETL metadata
        df_clean['etl_source_file'] = filename
        df_clean['etl_processed_at'] = datetime.utcnow()
        df_clean['etl_batch_id'] = f"manual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        print(f"   Cleaned columns: {list(df_clean.columns)}")
        
        # Get database engine
        engine = get_db_engine()
        
        # Insert data
        table_name = 'employee_data'
        print(f"   Writing to table: {table_name}")
        
        df_clean.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        
        # Log the processing
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO etl_processing_log 
                (filename, sheet_name, rows_processed, status, processed_at, processing_time_seconds)
                VALUES (:filename, :sheet_name, :rows, :status, :processed_at, :time)
            """), {
                'filename': filename,
                'sheet_name': 'Sheet1',
                'rows': len(df_clean),
                'status': 'success',
                'processed_at': datetime.utcnow(),
                'time': 2
            })
            conn.commit()
        
        # Verify insertion
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            total_count = result.fetchone()[0]
            
            result = conn.execute(text(f"""
                SELECT COUNT(*) FROM {table_name} 
                WHERE etl_source_file = :filename
            """), {'filename': filename})
            file_count = result.fetchone()[0]
        
        print(f"✅ Data successfully inserted!")
        print(f"   Total rows in {table_name}: {total_count}")
        print(f"   Rows from this file: {file_count}")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to process data: {e}")
        import traceback
        traceback.print_exc()
        return False

def check_data_in_database():
    """Check what data is currently in the database"""
    print("🔍 Checking current database contents...")
    
    try:
        from etl_processor.database_postgres import get_db_engine
        from sqlalchemy import text
        
        engine = get_db_engine()
        
        with engine.connect() as conn:
            # Get all tables
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """))
            tables = [row[0] for row in result.fetchall()]
            
            print(f"   Found tables: {tables}")
            
            # Check data in each table
            for table in tables:
                try:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    count = result.fetchone()[0]
                    print(f"   {table}: {count} rows")
                    
                    if count > 0 and count < 10:
                        # Show sample data for small tables
                        result = conn.execute(text(f"SELECT * FROM {table} LIMIT 3"))
                        rows = result.fetchall()
                        columns = result.keys()
                        print(f"      Sample data:")
                        for row in rows:
                            sample_row = dict(zip(columns, row))
                            print(f"      - {sample_row}")
                
                except Exception as e:
                    print(f"   {table}: Error reading - {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to check database: {e}")
        return False

def main():
    """Main test function"""
    print("=" * 60)
    print("Simple ETL Test - Excel to PostgreSQL")
    print("=" * 60)
    
    # Step 1: Test database connection
    if not test_database_connection():
        print("\n❌ Database connection failed!")
        print("\nMake sure PostgreSQL is running:")
        print("  docker-compose up -d postgres")
        return False
    
    print("\n" + "-" * 40)
    
    # Step 2: Check current database state
    check_data_in_database()
    
    print("\n" + "-" * 40)
    
    # Step 3: Create and process sample data
    filename, df = create_sample_data()
    
    if process_excel_to_postgres(filename, df):
        print(f"\n✅ ETL test completed successfully!")
    else:
        print(f"\n❌ ETL test failed!")
        return False
    
    print("\n" + "-" * 40)
    
    # Step 4: Verify results
    print("🔍 Final verification...")
    check_data_in_database()
    
    print("\n" + "=" * 60)
    print("✅ Test Summary:")
    print("  ✅ PostgreSQL connection: Working")
    print("  ✅ Table creation: Working")
    print("  ✅ Excel data processing: Working")
    print("  ✅ Data insertion: Working")
    print("\nYour ETL system is ready for real Excel files!")
    print("=" * 60)
    
    return True

if __name__ == "__main__":
    success = main()
    
    if not success:
        print("\n🔧 Troubleshooting tips:")
        print("1. Make sure PostgreSQL is running: docker-compose up -d postgres")
        print("2. Check database credentials in .env file")
        print("3. Verify you're in the project root directory")
        print("4. Run: python setup_paths.py")
    
    sys.exit(0 if success else 1)