#!/usr/bin/env python3
"""
Test connection between Windows file server and Linux containers
"""

import requests
import sys
import time

def test_connection():
    print("="*60)
    print("Testing Simple ETL Setup")
    print("="*60)
    
    # Test 1: File Server on Windows Host
    print("\n1. Testing File Server (Windows Host)...")
    fileserver_url = "http://localhost:5000"
    
    try:
        response = requests.get(f"{fileserver_url}/health", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(f"✓ File server is running")
            print(f"  Path: {data.get('shared_path')}")
            print(f"  Path exists: {data.get('path_exists')}")
            
            # List files
            response = requests.get(f"{fileserver_url}/list", timeout=5)
            if response.status_code == 200:
                files = response.json()
                print(f"✓ Found {files.get('count', 0)} Excel files")
                for f in files.get('files', [])[:3]:
                    print(f"  - {f['name']}")
        else:
            print(f"✗ File server returned status {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("✗ Cannot connect to file server at http://localhost:5000")
        print("\nMake sure the file server is running on Windows:")
        print("  1. Open a command prompt")
        print("  2. Navigate to the project folder")
        print("  3. Run: python simple_fileserver.py")
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False
    
    # Test 2: Redis
    print("\n2. Testing Redis...")
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        print("✓ Redis is accessible")
    except:
        print("✗ Redis not accessible")
        print("  Make sure Docker containers are running:")
        print("  docker-compose up -d")
    
    # Test 3: Flower
    print("\n3. Testing Flower...")
    try:
        response = requests.get("http://localhost:5555/dashboard", timeout=5)
        if response.status_code == 200:
            print("✓ Flower is accessible at http://localhost:5555")
    except:
        print("✗ Flower not accessible")
    
    # Test 4: Test from container
    print("\n4. To test file access from ETL worker container:")
    print("   docker-compose exec etl-worker python -c \"")
    print("   from etl_processor.file_access import WindowsFileAccess")
    print("   file_access = WindowsFileAccess('http://host.docker.internal:5000')")
    print("   health = file_access.health_check()")
    print("   print(health)\"")
    
    print("\n" + "="*60)
    print("Setup Complete!")
    print("="*60)
    print("\nYour services:")
    print("  File Server (Windows):  http://localhost:5000")
    print("  Flower (Docker):        http://localhost:5555")
    print("  Redis (Docker):         localhost:6379")
    
    return True

if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)