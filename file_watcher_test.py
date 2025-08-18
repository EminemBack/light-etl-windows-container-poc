#!/usr/bin/env python3
"""
File Watcher Test Script
Test the file watcher service and ETL triggering
"""

import os
import time
import requests
import pandas as pd
from datetime import datetime
import json
from requests.auth import HTTPBasicAuth

def test_file_watcher_service():
    """Test if the file watcher service is running"""
    print("Testing File Watcher Service...")
    
    try:
        response = requests.get("http://localhost:5000/health", timeout=10)
        
        if response.status_code == 200:
            health_data = response.json()
            print("✅ File watcher service is running")
            print(f"   Status: {health_data.get('status')}")
            print(f"   Service type: {health_data.get('service_type')}")
            print(f"   Watcher running: {health_data.get('watcher_running')}")
            print(f"   ETL trigger enabled: {health_data.get('etl_trigger_enabled')}")
            print(f"   Poll interval: {health_data.get('poll_interval')} seconds")
            print(f"   Active path: {health_data.get('active_path')}")
            
            processing_status = health_data.get('processing_status', {})
            print(f"   Queued files: {processing_status.get('queued_files', 0)}")
            print(f"   Processed files: {processing_status.get('processed_files', 0)}")
            
            return True
        else:
            print(f"❌ File watcher returned status: {response.status_code}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to file watcher service at http://localhost:5000")
        print("\nTo start the file watcher service:")
        print("1. Open command prompt as Administrator")
        print("2. Run: python file_watcher_service.py")
        return False
    except Exception as e:
        print(f"❌ Error testing file watcher: {e}")
        return False

def test_celery_workers():
    """Test if Celery workers are running"""
    print("\nTesting Celery Workers...")
    
    try:
        # Test Redis connection
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        print("✅ Redis is accessible")
        
        # Check worker status via Flower
        try:
            username, password = 'admin', 'password123'
            response = requests.get(
                "http://localhost:5555/api/workers?refresh=1",
                auth=HTTPBasicAuth(username, password),
                timeout=5
            )
            if response.status_code == 200:
                workers = response.json()
                print(f"✅ Found {len(workers)} Celery workers:")
                for worker_name, worker_info in workers.items():
                    status = worker_info.get('status', 'unknown')
                    print(f"   - {worker_name}: {status}")
                return len(workers) > 0
            else:
                print("❌ Flower API not accessible")
                return False
        except:
            print("⚠️  Flower not accessible, checking Redis directly...")
            
            # Check Redis for active workers
            active_queues = r.keys('_kombu.binding.*')
            if active_queues:
                print(f"✅ Found Redis queue bindings: {len(active_queues)}")
                return True
            else:
                print("❌ No active queue bindings found")
                return False
                
    except Exception as e:
        print(f"❌ Error testing Celery workers: {e}")
        return False

def create_test_file():
    """Create a test Excel file for processing"""
    print("\nCreating test Excel file...")
    
    # Get the watch path from the file watcher
    try:
        response = requests.get("http://localhost:5000/health")
        if response.status_code == 200:
            health_data = response.json()
            active_path = health_data.get('active_path', '.')
        else:
            active_path = '.'
    except:
        active_path = '.'
    
    # Create test data
    test_data = {
        'employee_id': [1001, 1002, 1003, 1004, 1005],
        'username': ['emp001', 'emp002', 'emp003', 'emp004', 'emp005'],
        'email': ['emp001@test.com', 'emp002@test.com', 'emp003@test.com', 
                 'emp004@test.com', 'emp005@test.com'],
        'full_name': ['Employee One', 'Employee Two', 'Employee Three',
                     'Employee Four', 'Employee Five'],
        'department': ['IT', 'Finance', 'HR', 'Operations', 'Marketing'],
        'salary': [50000, 60000, 55000, 65000, 58000],
        'hire_date': ['2023-01-15', '2023-02-20', '2023-03-10', 
                     '2023-04-05', '2023-05-12'],
        'status': ['Active', 'Active', 'Active', 'Inactive', 'Active']
    }
    
    df = pd.DataFrame(test_data)
    
    # Create filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"test_employees_{timestamp}.xlsx"
    
    # Try to save to the active path, fallback to current directory
    try:
        filepath = os.path.join(active_path, filename)
        df.to_excel(filepath, index=False)
        print(f"✅ Created test file: {filepath}")
        return filepath
    except:
        # Fallback to current directory
        df.to_excel(filename, index=False)
        print(f"✅ Created test file: {filename} (in current directory)")
        return filename

def test_manual_etl_trigger():
    """Test manual ETL triggering"""
    print("\nTesting manual ETL trigger...")
    
    # Create test file
    filepath = 'Z:\\ad_users.csv'
    filename = os.path.basename(filepath)
    
    try:
        # Trigger ETL manually
        response = requests.post(f"http://localhost:5000/trigger_etl/{filename}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Manual ETL triggered successfully")
            print(f"   Message: {result.get('message')}")
            print(f"   Timestamp: {result.get('timestamp')}")
            
            # Wait a bit and check processing status
            print("Waiting 10 seconds for processing...")
            time.sleep(10)
            
            # Check if file was processed
            response = requests.get("http://localhost:5000/processing_history")
            if response.status_code == 200:
                history = response.json()
                events = history.get('events', [])
                
                # Look for our file in recent events
                our_events = [e for e in events if filename in e.get('filename', '')]
                if our_events:
                    print(f"✅ Found {len(our_events)} processing events for our file:")
                    for event in our_events[-3:]:  # Show last 3 events
                        print(f"   - {event.get('status')}: {event.get('details')}")
                else:
                    print("⚠️  No processing events found for our file yet")
            
            return True
        else:
            print(f"❌ Manual ETL trigger failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error testing manual ETL trigger: {e}")
        return False

def test_file_detection():
    """Test if the file watcher detects new files"""
    print("\nTesting automatic file detection...")
    
    # Get current file list
    try:
        response = requests.get("http://localhost:5000/list")
        if response.status_code == 200:
            initial_files = response.json().get('files', [])
            initial_count = len(initial_files)
            print(f"Initial file count: {initial_count}")
        else:
            print("❌ Could not get initial file list")
            return False
    except Exception as e:
        print(f"❌ Error getting file list: {e}")
        return False
    
    # Create a new test file
    filepath = create_test_file()
    filename = os.path.basename(filepath)
    
    print(f"Waiting 15 seconds for file detection (poll interval is usually 5 seconds)...")
    time.sleep(15)
    
    # Check if file was detected
    try:
        response = requests.get("http://localhost:5000/list")
        if response.status_code == 200:
            current_files = response.json().get('files', [])
            current_count = len(current_files)
            
            if current_count > initial_count:
                print(f"✅ File detection working - count increased from {initial_count} to {current_count}")
                
                # Check if our specific file is listed
                our_file = next((f for f in current_files if f['name'] == filename), None)
                if our_file:
                    print(f"✅ Our test file was detected: {filename}")
                    return True
                else:
                    print(f"⚠️  File count increased but our specific file not found")
                    return False
            else:
                print(f"❌ File not detected - count still {current_count}")
                return False
        else:
            print("❌ Could not get updated file list")
            return False
            
    except Exception as e:
        print(f"❌ Error checking file detection: {e}")
        return False

def run_comprehensive_test():
    """Run all tests in sequence"""
    print("=" * 60)
    print("ETL System Comprehensive Test")
    print("=" * 60)
    
    results = {}
    
    # Test 1: File Watcher Service
    results['file_watcher'] = test_file_watcher_service()
    
    # Test 2: Celery Workers
    results['celery_workers'] = test_celery_workers()
    
    # Test 3: Manual ETL Trigger (if file watcher is working)
    if results['file_watcher']:
        results['manual_etl'] = test_manual_etl_trigger()
    else:
        results['manual_etl'] = False
        print("\nSkipping manual ETL test - file watcher not running")
    
    # Test 4: Automatic File Detection (if file watcher is working)
    if results['file_watcher']:
        results['file_detection'] = test_file_detection()
    else:
        results['file_detection'] = False
        print("\nSkipping file detection test - file watcher not running")
    
    # Summary
    print("\n" + "=" * 60)
    print("Test Results Summary:")
    print("=" * 60)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name.replace('_', ' ').title()}: {status}")
    
    # Recommendations
    print("\nRecommendations:")
    
    if not results['file_watcher']:
        print("1. Start the file watcher service:")
        print("   python file_watcher_service.py")
    
    if not results['celery_workers']:
        print("2. Start Celery workers:")
        print("   docker-compose up -d")
    
    if results['file_watcher'] and results['celery_workers']:
        if not results['manual_etl']:
            print("3. Check ETL configuration and Redis connection")
        if not results['file_detection']:
            print("4. Check file paths and permissions")
    
    print("\n" + "=" * 60)
    
    return all(results.values())

if __name__ == "__main__":
    success = run_comprehensive_test()
    
    if success:
        print("🎉 All tests passed! Your ETL system is working correctly.")
    else:
        print("⚠️  Some tests failed. Please address the issues above.")
        
    print("\nQuick commands for troubleshooting:")
    print("- Check file watcher logs: tail -f logs/fileserver/fileserver_watcher.log")
    print("- Check Docker containers: docker-compose ps")
    print("- Check Celery workers: docker-compose logs etl-worker")
    print("- Restart everything: docker-compose restart")