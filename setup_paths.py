#!/usr/bin/env python3
"""
Setup Python paths for ETL project
This script ensures all imports work correctly
"""

import sys
import os
from pathlib import Path

def setup_python_paths():
    """Add necessary paths to Python sys.path"""
    
    # Get current directory
    current_dir = Path.cwd()
    
    # Add etl-worker directory to Python path
    etl_worker_path = current_dir / 'etl-worker'
    if etl_worker_path.exists():
        if str(etl_worker_path) not in sys.path:
            sys.path.insert(0, str(etl_worker_path))
            print(f"✅ Added to Python path: {etl_worker_path}")
    else:
        print(f"❌ ETL worker directory not found: {etl_worker_path}")
        return False
    
    # Also add current directory
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
        print(f"✅ Added to Python path: {current_dir}")
    
    return True

def test_imports():
    """Test if all required modules can be imported"""
    print("\n🔍 Testing imports...")
    
    try:
        from etl_processor.database_postgres import test_connection
        print("✅ etl_processor.database_postgres")
    except ImportError as e:
        print(f"❌ etl_processor.database_postgres: {e}")
        return False
    
    try:
        from etl_processor.file_access import WindowsFileAccess
        print("✅ etl_processor.file_access")
    except ImportError as e:
        print(f"❌ etl_processor.file_access: {e}")
        return False
    
    try:
        from etl_processor.celery_app import app
        print("✅ etl_processor.celery_app")
    except ImportError as e:
        print(f"❌ etl_processor.celery_app: {e}")
        return False
    
    try:
        from etl_processor.tasks_postgres import process_excel_to_postgres
        print("✅ etl_processor.tasks_postgres")
    except ImportError as e:
        print(f"❌ etl_processor.tasks_postgres: {e}")
        return False
    
    return True

def setup_environment():
    """Setup environment variables"""
    print("\n🔧 Setting up environment variables...")
    
    env_vars = {
        'DB_HOST': 'localhost',
        'DB_PORT': '5432',
        'DB_NAME': 'etl_database',
        'DB_USER': 'etl_user',
        'DB_PASSWORD': 'SecurePassword123!',
        'PYTHONPATH': str(Path.cwd() / 'etl-worker'),
        'CELERY_BROKER_URL': 'redis://localhost:6379/0',
        'FILESERVER_URL': 'http://localhost:5000'
    }
    
    for key, value in env_vars.items():
        os.environ.setdefault(key, value)
        print(f"✅ {key}={value}")

def main():
    """Main setup function"""
    print("=" * 50)
    print("Python Path Setup for ETL Project")
    print("=" * 50)
    
    # Setup paths
    if not setup_python_paths():
        print("❌ Failed to setup Python paths")
        return False
    
    # Setup environment
    setup_environment()
    
    # Test imports
    if not test_imports():
        print("❌ Import tests failed")
        return False
    
    print("\n✅ Python path setup completed successfully!")
    print("\nYou can now run:")
    print("  python manual_etl_test.py")
    print("  python db_check_script.py")
    print("  python file_watcher_test.py")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)