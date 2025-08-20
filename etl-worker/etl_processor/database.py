# etl-worker/etl_processor/database.py
import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import urllib.parse

logger = logging.getLogger(__name__)

def get_db_engine(use_windows_auth=False):
    """
    Create database engine with flexible authentication options
    
    Args:
        use_windows_auth (bool): If True, use Windows authentication
                                If False, use SQL Server authentication
    """
    
    if use_windows_auth:
        # Windows Authentication (requires special setup for containers)
        server = os.getenv('DB_SERVER', 'localhost')
        database = os.getenv('DB_DATABASE', 'your_database')
        driver = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
        
        # URL encode the driver name
        driver_encoded = urllib.parse.quote_plus(driver)
        
        connection_string = (
            f"mssql+pyodbc://@{server}/{database}"
            f"?trusted_connection=yes&driver={driver_encoded}"
        )
    else:
        # SQL Server Authentication (recommended for containers)
        server = os.getenv('DB_SERVER', 'localhost')
        database = os.getenv('DB_DATABASE', 'your_database')
        username = os.getenv('DB_USERNAME')
        password = os.getenv('DB_PASSWORD')
        driver = os.getenv('DB_DRIVER', 'ODBC Driver 17 for SQL Server')
        
        if not username or not password:
            raise ValueError("DB_USERNAME and DB_PASSWORD must be set for SQL authentication")
        
        # URL encode components
        username_encoded = urllib.parse.quote_plus(username)
        password_encoded = urllib.parse.quote_plus(password)
        driver_encoded = urllib.parse.quote_plus(driver)
        
        connection_string = (
            f"mssql+pyodbc://{username_encoded}:{password_encoded}@{server}/{database}"
            f"?driver={driver_encoded}"
        )
    
    logger.info(f"Connecting to database: {server}/{database} using this connection_string: {connection_string}")
    
    try:
        engine = create_engine(
            connection_string,
            pool_pre_ping=True,  # Verify connections before use
            pool_recycle=3600,   # Recycle connections every hour
            echo=False  # Set to True for SQL debugging
        )
        
        # Test the connection
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            logger.info("Database connection successful")
        
        return engine
        
    except SQLAlchemyError as e:
        logger.error(f"Database connection failed: {str(e)}")
        raise

def test_connection():
    """Test database connection and return connection info"""
    try:
        engine = get_db_engine(use_windows_auth=True)
        
        with engine.connect() as conn:
            # Get SQL Server version
            result = conn.execute(text("SELECT @@VERSION as version"))
            version = result.fetchone()[0]
            
            # Get current database
            result = conn.execute(text("SELECT DB_NAME() as database_name"))
            db_name = result.fetchone()[0]
            
            # Get current user
            result = conn.execute(text("SELECT SYSTEM_USER as current_user"))
            current_user = result.fetchone()[0]
            
            return {
                "status": "success",
                "database": db_name,
                "user": current_user,
                "version": version[:100] + "..." if len(version) > 100 else version
            }
            
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }

# For backwards compatibility
def get_db_engine_legacy():
    """Legacy function - uses environment variable DATABASE_URL"""
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set")
    
    return create_engine(database_url)