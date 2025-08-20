# etl-worker/etl_processor/database_postgres.py
import os
import logging
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, DateTime, Boolean
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

logger = logging.getLogger(__name__)

def get_db_engine():
    """Create PostgreSQL database engine"""
    
    # Database connection parameters
    db_host = os.getenv('DB_HOST', 'postgres')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'etl_database')
    db_user = os.getenv('DB_USER', 'etl_user')
    db_password = os.getenv('DB_PASSWORD', 'SecurePassword123!')
    
    # Create connection string
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    logger.info(f"Connecting to PostgreSQL: {db_host}:{db_port}/{db_name}")
    
    try:
        engine = create_engine(
            connection_string,
            # pool_pre_ping=True,
            # pool_recycle=3600,
            echo=False,  # Set to True for SQL debugging
        )
        
        # Test the connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            logger.info("✅ PostgreSQL connection successful")
            logger.info(f"PostgreSQL version: {version[:50]}...")
            
        return engine
        
    except SQLAlchemyError as e:
        logger.error(f"PostgreSQL connection failed: {str(e)}")
        raise

def create_tables():
    """Create initial tables for ETL processing"""
    
    try:
        engine = get_db_engine()
        metadata = MetaData()
        
        # Create a table to store user data (equivalent to ad_users)
        users_table = Table(
            'users',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('username', String(100), unique=True, nullable=False),
            Column('email', String(200)),
            Column('full_name', String(200)),
            Column('department', String(100)),
            Column('is_active', Boolean, default=True),
            Column('created_at', DateTime, default=datetime.utcnow),
            Column('updated_at', DateTime, default=datetime.utcnow, onupdate=datetime.utcnow),
            Column('etl_source_file', String(255)),
            Column('etl_processed_at', DateTime, default=datetime.utcnow)
        )
        
        # Create ETL processing log table
        etl_log_table = Table(
            'etl_processing_log',
            metadata,
            Column('id', Integer, primary_key=True),
            Column('filename', String(255), nullable=False),
            Column('sheet_name', String(100)),
            Column('rows_processed', Integer),
            Column('status', String(50)),
            Column('error_message', String(1000)),
            Column('processed_at', DateTime, default=datetime.utcnow),
            Column('processing_time_seconds', Integer)
        )
        
        # Create tables
        metadata.create_all(engine)
        logger.info("✅ Database tables created successfully")
        
        return True
        
    except Exception as e:
        logger.error(f"Error creating tables: {str(e)}")
        return False

def test_connection():
    """Test PostgreSQL connection and return info"""
    try:
        engine = get_db_engine()
        
        with engine.connect() as conn:
            # Get database info
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            
            result = conn.execute(text("SELECT current_database()"))
            db_name = result.fetchone()[0]
            
            result = conn.execute(text("SELECT current_user"))
            current_user = result.fetchone()[0]
            
            # Check if tables exist
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """))
            tables = [row[0] for row in result.fetchall()]
            
            # Test users table (equivalent to ad_users)
            try:
                if 'users' in tables:
                    result = conn.execute(text("SELECT COUNT(*) FROM users"))
                    user_count = result.fetchone()[0]
                    table_test = f"✅ users table accessible with {user_count} records"
                else:
                    table_test = "ℹ️  users table not found - will be created"
            except Exception as e:
                table_test = f"⚠️  users table error: {str(e)}"
            
            return {
                "status": "success",
                "database_type": "PostgreSQL",
                "database": db_name,
                "user": current_user,
                "version": version[:100] + "..." if len(version) > 100 else version,
                "tables": tables,
                "table_test": table_test,
                "host": os.getenv('DB_HOST', 'postgres'),
                "port": os.getenv('DB_PORT', '5432')
            }
            
    except Exception as e:
        return {
            "status": "error",
            "database_type": "PostgreSQL",
            "error": str(e)
        }

def insert_sample_data():
    """Insert sample user data for testing"""
    try:
        engine = get_db_engine()
        
        sample_users = [
            {
                'username': 'jdoe', 
                'email': 'john.doe@kinrossgold.com', 
                'full_name': 'John Doe',
                'department': 'IT',
                'etl_source_file': 'sample_data.xlsx'
            },
            {
                'username': 'asmith', 
                'email': 'alice.smith@kinrossgold.com', 
                'full_name': 'Alice Smith',
                'department': 'Finance',
                'etl_source_file': 'sample_data.xlsx'
            },
            {
                'username': 'bwilson', 
                'email': 'bob.wilson@kinrossgold.com', 
                'full_name': 'Bob Wilson',
                'department': 'Operations',
                'etl_source_file': 'sample_data.xlsx'
            }
        ]
        
        with engine.connect() as conn:
            for user in sample_users:
                # Check if user already exists
                result = conn.execute(
                    text("SELECT COUNT(*) FROM users WHERE username = :username"),
                    {"username": user['username']}
                )
                
                if result.fetchone()[0] == 0:
                    # Insert new user
                    conn.execute(text("""
                        INSERT INTO users (username, email, full_name, department, etl_source_file, etl_processed_at)
                        VALUES (:username, :email, :full_name, :department, :etl_source_file, :etl_processed_at)
                    """), {
                        **user,
                        'etl_processed_at': datetime.utcnow()
                    })
            
            conn.commit()
        
        logger.info("✅ Sample data inserted successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error inserting sample data: {str(e)}")
        return False