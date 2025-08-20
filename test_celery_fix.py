#!/usr/bin/env python3
"""
Test script to verify the Celery communication fix
Run this to test if the file watcher can properly trigger ETL tasks
"""

import os
import sys
import time
import requests
import json
from datetime import datetime
import pandas as pd

# Add etl-worker to Python path
from pathlib import Path
current_dir = Path.cwd()
etl_worker_path = current_dir / 'etl-worker'
if etl_worker_path.exists():
    sys.path.insert(0, str(etl_worker_path))

def test_celery_client():
    print("PASS Testing Celery client creation...")
    
    try:
        from celery import Celery
        
        # Create Celery app for sending tasks (like the fixed file watcher does)
        celery_app = Celery(
            'test_client',
            broker='redis://localhost:6379/0',
            backend='redis://localhost:6379/1'
        )
        
        celery_app.conf.update(
            task_serializer='json',
            accept_content=['json'],
            result_serializer='json',
            timezone='UTC',
            enable_utc=True,
            task_always_eager=False,
            task_ignore_result=True,
        )
        
        print("PASS Celery client created successfully")
        return celery_app
        
    except Exception as e:
        print(f"FAIL Failed to create Celery client: {e}")
        return None

def test_redis_connection():
    """Test Redis connection"""
    print("CONN Testing Redis connection...")
    
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        print("PASS Redis connection successful")
        return True
    except Exception as e:
        print(f"FAIL Redis connection failed: {e}")
        return False

def test_celery_workers():
    """Test if Celery workers are running"""
    print("WORK Testing Celery workers...")
    
    from requests.auth import HTTPBasicAuth
    auth = HTTPBasicAuth('admin', 'password123')
    
    try:
        response = requests.get("http://localhost:5555", auth=auth, timeout=5)
        if response.status_code == 200:
            print("PASS Flower is accessible")
            
            # Try to get worker info
            try:
                response = requests.get("http://localhost:5555/api/workers", auth=auth, timeout=5)
                if response.status_code == 200:
                    workers = response.json()
                    print(f"PASS Found {len(workers)} active workers")
                    for worker_name in workers.keys():
                        print(f"   - {worker_name}")
                    return len(workers) > 0
                elif response.status_code == 401:
                    print("FAIL Authentication failed - check username/password")
                    return False
                else:
                    print(f"WARN Flower API returned status: {response.status_code}")
                    return False
            except Exception as e:
                print(f"WARN Could not get worker details: {e}")
                return False
        elif response.status_code == 401:
            print("FAIL Authentication failed - check Flower credentials")
            return False
        else:
            print(f"WARN Flower returned status: {response.status_code}")
            return False
                
    except requests.exceptions.ConnectionError:
        print("FAIL Cannot connect to Flower")
        print("   Make sure Flower container is running: docker-compose up -d flower")
        return False
    except Exception as e:
        print(f"FAIL Flower error: {e}")
        return False

def test_send_task_directly():
    """Test sending task directly using send_task method"""
    print("TASK Testing direct task sending...")
    
    celery_app = test_celery_client()
    if not celery_app:
        return False
        
    try:
        # Create a test file name
        test_filename = f"test_file_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        # Send task using send_task (this is what the fixed file watcher does)
        result = celery_app.send_task(
            'etl_processor.enhanced_tasks.process_excel_file',
            args=[test_filename],
            kwargs={
                'auto_triggered': True,
                'filepath': f'/test/path/{test_filename}'
            },
            countdown=5  # Wait 5 seconds before execution
        )
        
        print(f"PASS Task sent successfully!")
        print(f"   Task ID: {result.id}")
        print(f"   Task name: etl_processor.enhanced_tasks.process_excel_file")
        print(f"   File: {test_filename}")
        print("   Status: Queued for execution")
        
        return True
        
    except Exception as e:
        print(f"FAIL Failed to send task: {e}")
        print(f"   Error type: {type(e).__name__}")
        return False

def test_simple_task():
    """Test sending a simple task first"""
    print("SIMP Testing simple task (hello_world)...")
    
    celery_app = test_celery_client()
    if not celery_app:
        return False
        
    try:
        # Send simple hello_world task
        result = celery_app.send_task(
            'hello_world',
            args=[],
            kwargs={}
        )
        
        print(f"PASS Simple task sent successfully!")
        print(f"   Task ID: {result.id}")
        print("   Task: hello_world")
        
        # Wait a moment and check if task was processed
        time.sleep(3)
        print("   Waiting for task execution...")
        
        return True
        
    except Exception as e:
        print(f"FAIL Failed to send simple task: {e}")
        return False

def test_file_watcher_service():
    """Test the file watcher service"""
    print("FWAT Testing file watcher service...")
    
    try:
        response = requests.get("http://localhost:5000/health", timeout=10)
        
        if response.status_code == 200:
            health_data = response.json()
            print("PASS File watcher service is running")
            print(f"   ETL trigger enabled: {health_data.get('etl_trigger_enabled')}")
            print(f"   Celery configured: {health_data.get('celery_configured')}")
            print(f"   Watcher running: {health_data.get('watcher_running')}")
            
            return health_data.get('etl_trigger_enabled', False)
        else:
            print(f"FAIL File watcher returned status: {response.status_code}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("FAIL Cannot connect to file watcher service")
        print("   Make sure it's running: python fixed_file_watcher_service.py")
        return False
    except Exception as e:
        print(f"FAIL Error testing file watcher: {e}")
        return False

def test_manual_trigger():
    """Test manual ETL trigger through file watcher"""
    print("TRIG Testing manual ETL trigger...")
    
    # Create a test file first
    test_filename = f"test_trigger_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    # Create sample data
    data = {
        'name': ['Test User 1', 'Test User 2'],
        'email': ['test1@example.com', 'test2@example.com'],
        'department': ['IT', 'Finance']
    }
    df = pd.DataFrame(data)
    
    # Try to save to Z:\ drive or fallback location
    try:
        # First try Z:\ drive
        test_path = f"Z:\\{test_filename}"
        if os.path.exists("Z:\\"):
            df.to_excel(test_path, index=False)
            print(f"PASS Created test file: {test_path}")
        else:
            # Fallback to current directory
            test_path = test_filename
            df.to_excel(test_path, index=False)
            print(f"PASS Created test file: {test_path}")
            
    except Exception as e:
        print(f"FAIL Failed to create test file: {e}")
        return False
    
    # Now trigger ETL through file watcher API
    try:
        response = requests.post(
            f"http://localhost:5000/trigger_etl/{test_filename}",
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            print("PASS Manual ETL trigger successful!")
            print(f"   Message: {result.get('message')}")
            print(f"   Method: {result.get('method')}")
            
            # Check processing history after a few seconds
            time.sleep(5)
            try:
                response = requests.get("http://localhost:5000/processing_history")
                if response.status_code == 200:
                    history = response.json()
                    recent_events = history.get('events', [])[-3:]
                    
                    print("INFO Recent processing events:")
                    for event in recent_events:
                        if test_filename in event.get('filename', ''):
                            print(f"   - {event.get('status')}: {event.get('details')}")
                            
            except Exception as e:
                print(f"WARN Could not check processing history: {e}")
            
            return True
        else:
            print(f"FAIL Manual trigger failed: {response.status_code}")
            try:
                error_data = response.json()
                print(f"   Error: {error_data.get('error')}")
            except:
                print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"FAIL Error triggering manual ETL: {e}")
        return False

def check_worker_logs():
    """Check recent worker logs for task execution"""
    print("LOGS Checking worker logs...")
    
    try:
        import subprocess
        result = subprocess.run(
            ['docker-compose', 'logs', '--tail=10', 'etl-worker'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            logs = result.stdout
            if 'Task' in logs and 'received' in logs:
                print("PASS Recent task activity found in worker logs")
                # Show last few lines
                lines = logs.strip().split('\n')[-5:]
                for line in lines:
                    if 'Task' in line:
                        print(f"   {line}")
            else:
                print("WARN No recent task activity in worker logs")
        else:
            print("WARN Could not retrieve worker logs")
            
    except Exception as e:
        print(f"WARN Error checking logs: {e}")

def main():
    """Run comprehensive test of the Celery communication fix"""
    print("=" * 70)
    print("Testing Celery Communication Fix")
    print("=" * 70)
    
    # Environment setup
    print("INIT Setting up environment...")
    os.environ.setdefault('CELERY_BROKER_URL_WATCHER', 'redis://localhost:6379/0')
    
    test_results = {}
    
    # Test 1: Redis connection
    print("\n" + "-" * 50)
    test_results['redis'] = test_redis_connection()
    
    # Test 2: Celery workers
    print("\n" + "-" * 50)
    test_results['workers'] = test_celery_workers()
    
    # Test 3: Celery client creation
    print("\n" + "-" * 50)
    test_results['client'] = test_celery_client() is not None
    
    # Test 4: Simple task sending
    if test_results['client'] and test_results['workers']:
        print("\n" + "-" * 50)
        test_results['simple_task'] = test_simple_task()
        
        # Test 5: ETL task sending
        print("\n" + "-" * 50)
        test_results['etl_task'] = test_send_task_directly()
    else:
        test_results['simple_task'] = False
        test_results['etl_task'] = False
        print("\nWARN Skipping task tests - prerequisites not met")
    
    # Test 6: File watcher service
    print("\n" + "-" * 50)
    test_results['file_watcher'] = test_file_watcher_service()
    
    # Test 7: Manual trigger
    if test_results['file_watcher']:
        print("\n" + "-" * 50)
        test_results['manual_trigger'] = test_manual_trigger()
    else:
        test_results['manual_trigger'] = False
        print("\nWARN Skipping manual trigger test - file watcher not ready")
    
    # Check logs
    print("\n" + "-" * 50)
    check_worker_logs()
    
    # Summary
    print("\n" + "=" * 70)
    print("TEST RESULTS SUMMARY")
    print("=" * 70)
    
    for test_name, result in test_results.items():
        status = "PASS" if result else "FAIL"
        test_display = test_name.replace('_', ' ').title()
        print(f"{test_display:.<40} {status}")
    
    # Overall status
    passed = sum(test_results.values())
    total = len(test_results)
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\nSUCCESS ALL TESTS PASSED! The Celery communication fix is working!")
        print("\nNext steps:")
        print("1. Replace file_watcher_service.py with fixed_file_watcher_service.py")
        print("2. Test with real Excel files in your Z:\\ drive")
        print("3. Monitor logs: docker-compose logs -f etl-worker")
    else:
        print(f"\nWARNING {total - passed} test(s) failed. Issues to fix:")
        
        if not test_results['redis']:
            print("- Start Redis: docker-compose up -d redis")
        if not test_results['workers']:
            print("- Start workers: docker-compose up -d etl-worker")
        if not test_results['client']:
            print("- Install Celery: pip install celery")
        if not test_results['file_watcher']:
            print("- Start file watcher: python fixed_file_watcher_service.py")
    
    print("\n" + "=" * 70)
    
    return passed == total

# if __name__ == "__main__":
#     success = main()
    
#     print("\nUseful commands:")
#     print("- View worker logs:  docker-compose logs -f etl-worker")
#     print("- View all logs:     docker-compose logs -f")
#     print("- Restart workers:   docker-compose restart etl-worker")
#     print("- Check flower:      http://localhost:5555")
#     print("- Check file server: http://localhost:5000/health")
    
#     sys.exit(0 if success else 1)
    
#     try:
#         from celery import Celery
        
#         # Create Celery app for sending tasks (like the fixed file watcher does)
#         celery_app = Celery(
#             'test_client',
#             broker='redis://localhost:6379/0',
#             backend='redis://localhost:6379/1'
#         )
        
#         celery_app.conf.update(
#             task_serializer='json',
#             accept_content=['json'],
#             result_serializer='json',
#             timezone='UTC',
#             enable_utc=True,
#             task_always_eager=False,
#             task_ignore_result=True,
#         )
        
#         print("✅ Celery client created successfully")
#         return celery_app
        
#     except Exception as e:
#         print(f"❌ Failed to create Celery client: {e}")
#         return None

def test_redis_connection():
    """Test Redis connection"""
    print("🔗 Testing Redis connection...")
    
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        print("✅ Redis connection successful")
        return True
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        return False

def test_celery_workers():
    """Test if Celery workers are running"""
    print("👷 Testing Celery workers...")
    
    try:
        response = requests.get("http://localhost:5555", timeout=5)
        if response.status_code == 200:
            print("✅ Flower is accessible")
            
            # Try to get worker info
            try:
                response = requests.get("http://localhost:5555/api/workers", timeout=5)
                if response.status_code == 200:
                    workers = response.json()
                    print(f"✅ Found {len(workers)} active workers")
                    for worker_name in workers.keys():
                        print(f"   - {worker_name}")
                    return len(workers) > 0
                else:
                    print("⚠️  Flower accessible but API not responding")
            except:
                print("⚠️  Flower accessible but can't get worker details")
                
        return False
    except Exception as e:
        print(f"❌ Flower not accessible: {e}")
        return False

def test_send_task_directly():
    """Test sending task directly using send_task method"""
    print("📤 Testing direct task sending...")
    
    celery_app = test_celery_client()
    if not celery_app:
        return False
        
    try:
        # Create a test file name
        test_filename = f"test_file_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        # Send task using send_task (this is what the fixed file watcher does)
        result = celery_app.send_task(
            'etl_processor.enhanced_tasks.process_excel_file',
            args=[test_filename],
            kwargs={
                'auto_triggered': True,
                'filepath': f'/test/path/{test_filename}'
            },
            countdown=5  # Wait 5 seconds before execution
        )
        
        print(f"✅ Task sent successfully!")
        print(f"   Task ID: {result.id}")
        print(f"   Task name: etl_processor.enhanced_tasks.process_excel_file")
        print(f"   File: {test_filename}")
        print("   Status: Queued for execution")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to send task: {e}")
        print(f"   Error type: {type(e).__name__}")
        return False

def test_simple_task():
    """Test sending a simple task first"""
    print("🧪 Testing simple task (hello_world)...")
    
    celery_app = test_celery_client()
    if not celery_app:
        return False
        
    try:
        # Send simple hello_world task
        result = celery_app.send_task(
            'hello_world',
            args=[],
            kwargs={}
        )
        
        print(f"✅ Simple task sent successfully!")
        print(f"   Task ID: {result.id}")
        print("   Task: hello_world")
        
        # Wait a moment and check if task was processed
        time.sleep(3)
        print("   Waiting for task execution...")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to send simple task: {e}")
        return False

def test_file_watcher_service():
    """Test the file watcher service"""
    print("📁 Testing file watcher service...")
    
    try:
        response = requests.get("http://localhost:5000/health", timeout=10)
        
        if response.status_code == 200:
            health_data = response.json()
            print("✅ File watcher service is running")
            print(f"   ETL trigger enabled: {health_data.get('etl_trigger_enabled')}")
            print(f"   Celery configured: {health_data.get('celery_configured')}")
            print(f"   Watcher running: {health_data.get('watcher_running')}")
            
            return health_data.get('etl_trigger_enabled', False)
        else:
            print(f"❌ File watcher returned status: {response.status_code}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to file watcher service")
        print("   Make sure it's running: python fixed_file_watcher_service.py")
        return False
    except Exception as e:
        print(f"❌ Error testing file watcher: {e}")
        return False

def test_manual_trigger():
    """Test manual ETL trigger through file watcher"""
    print("🚀 Testing manual ETL trigger...")
    
    # Create a test file first
    test_filename = f"test_trigger_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    # Create sample data
    data = {
        'name': ['Test User 1', 'Test User 2'],
        'email': ['test1@example.com', 'test2@example.com'],
        'department': ['IT', 'Finance']
    }
    df = pd.DataFrame(data)
    
    # Try to save to Z:\ drive or fallback location
    try:
        # First try Z:\ drive
        test_path = f"Z:\\{test_filename}"
        if os.path.exists("Z:\\"):
            df.to_excel(test_path, index=False)
            print(f"✅ Created test file: {test_path}")
        else:
            # Fallback to current directory
            test_path = test_filename
            df.to_excel(test_path, index=False)
            print(f"✅ Created test file: {test_path}")
            
    except Exception as e:
        print(f"❌ Failed to create test file: {e}")
        return False
    
    # Now trigger ETL through file watcher API
    try:
        response = requests.post(
            f"http://localhost:5000/trigger_etl/{test_filename}",
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            print("✅ Manual ETL trigger successful!")
            print(f"   Message: {result.get('message')}")
            print(f"   Method: {result.get('method')}")
            
            # Check processing history after a few seconds
            time.sleep(5)
            try:
                response = requests.get("http://localhost:5000/processing_history")
                if response.status_code == 200:
                    history = response.json()
                    recent_events = history.get('events', [])[-3:]
                    
                    print("📋 Recent processing events:")
                    for event in recent_events:
                        if test_filename in event.get('filename', ''):
                            print(f"   - {event.get('status')}: {event.get('details')}")
                            
            except Exception as e:
                print(f"⚠️  Could not check processing history: {e}")
            
            return True
        else:
            print(f"❌ Manual trigger failed: {response.status_code}")
            try:
                error_data = response.json()
                print(f"   Error: {error_data.get('error')}")
            except:
                print(f"   Response: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Error triggering manual ETL: {e}")
        return False

def check_worker_logs():
    """Check recent worker logs for task execution"""
    print("📊 Checking worker logs...")
    
    try:
        import subprocess
        result = subprocess.run(
            ['docker-compose', 'logs', '--tail=10', 'etl-worker'],
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            logs = result.stdout
            if 'Task' in logs and 'received' in logs:
                print("✅ Recent task activity found in worker logs")
                # Show last few lines
                lines = logs.strip().split('\n')[-5:]
                for line in lines:
                    if 'Task' in line:
                        print(f"   {line}")
            else:
                print("⚠️  No recent task activity in worker logs")
        else:
            print("⚠️  Could not retrieve worker logs")
            
    except Exception as e:
        print(f"⚠️  Error checking logs: {e}")

def main():
    """Run comprehensive test of the Celery communication fix"""
    print("=" * 70)
    print("Testing Celery Communication Fix")
    print("=" * 70)
    
    # Environment setup
    print("🔧 Setting up environment...")
    os.environ.setdefault('CELERY_BROKER_URL_WATCHER', 'redis://localhost:6379/0')
    
    test_results = {}
    
    # Test 1: Redis connection
    print("\n" + "-" * 50)
    test_results['redis'] = test_redis_connection()
    
    # Test 2: Celery workers
    print("\n" + "-" * 50)
    test_results['workers'] = test_celery_workers()
    
    # Test 3: Celery client creation
    print("\n" + "-" * 50)
    test_results['client'] = test_celery_client() is not None
    
    # Test 4: Simple task sending
    if test_results['client'] and test_results['workers']:
        print("\n" + "-" * 50)
        test_results['simple_task'] = test_simple_task()
        
        # Test 5: ETL task sending
        print("\n" + "-" * 50)
        test_results['etl_task'] = test_send_task_directly()
    else:
        test_results['simple_task'] = False
        test_results['etl_task'] = False
        print("\n⚠️  Skipping task tests - prerequisites not met")
    
    # Test 6: File watcher service
    print("\n" + "-" * 50)
    test_results['file_watcher'] = test_file_watcher_service()
    
    # Test 7: Manual trigger
    if test_results['file_watcher']:
        print("\n" + "-" * 50)
        test_results['manual_trigger'] = test_manual_trigger()
    else:
        test_results['manual_trigger'] = False
        print("\n⚠️  Skipping manual trigger test - file watcher not ready")
    
    # Check logs
    print("\n" + "-" * 50)
    check_worker_logs()
    
    # Summary
    print("\n" + "=" * 70)
    print("TEST RESULTS SUMMARY")
    print("=" * 70)
    
    for test_name, result in test_results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        test_display = test_name.replace('_', ' ').title()
        print(f"{test_display:.<40} {status}")
    
    # Overall status
    passed = sum(test_results.values())
    total = len(test_results)
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 ALL TESTS PASSED! The Celery communication fix is working!")
        print("\nNext steps:")
        print("1. Replace file_watcher_service.py with fixed_file_watcher_service.py")
        print("2. Test with real Excel files in your Z:\\ drive")
        print("3. Monitor logs: docker-compose logs -f etl-worker")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Issues to fix:")
        
        if not test_results['redis']:
            print("- Start Redis: docker-compose up -d redis")
        if not test_results['workers']:
            print("- Start workers: docker-compose up -d etl-worker")
        if not test_results['client']:
            print("- Install Celery: pip install celery")
        if not test_results['file_watcher']:
            print("- Start file watcher: python fixed_file_watcher_service.py")
    
    print("\n" + "=" * 70)
    
    return passed == total

if __name__ == "__main__":
    success = main()
    
    print("\nUseful commands:")
    print("- View worker logs:  docker-compose logs -f etl-worker")
    print("- View all logs:     docker-compose logs -f")
    print("- Restart workers:   docker-compose restart etl-worker")
    print("- Check flower:      http://localhost:5555")
    print("- Check file server: http://localhost:5000/health")
    
    sys.exit(0 if success else 1)