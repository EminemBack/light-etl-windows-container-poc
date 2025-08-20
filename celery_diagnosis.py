#!/usr/bin/env python3
"""
Comprehensive Celery System Diagnosis
This script will help identify exactly what's wrong with your Celery setup
"""

import subprocess
import requests
import redis
import time
import json
import sys
from datetime import datetime

def run_command(cmd, timeout=30):
    """Run a command and return result"""
    try:
        result = subprocess.run(
            cmd, 
            shell=True, 
            capture_output=True, 
            text=True, 
            timeout=timeout
        )
        return {
            'success': result.returncode == 0,
            'stdout': result.stdout.strip(),
            'stderr': result.stderr.strip(),
            'returncode': result.returncode
        }
    except subprocess.TimeoutExpired:
        return {
            'success': False,
            'stdout': '',
            'stderr': 'Command timed out',
            'returncode': -1
        }
    except Exception as e:
        return {
            'success': False,
            'stdout': '',
            'stderr': str(e),
            'returncode': -1
        }

def check_docker_services():
    """Check Docker Compose services status"""
    print("\n" + "="*60)
    print("DOCKER SERVICES STATUS")
    print("="*60)
    
    # Check if Docker is running
    result = run_command("docker version")
    if not result['success']:
        print("FAIL Docker is not running or not installed")
        print(f"Error: {result['stderr']}")
        return False
    else:
        print("PASS Docker is running")
    
    # Check Docker Compose services
    result = run_command("docker-compose ps")
    if result['success']:
        print("\nDocker Compose Services:")
        print(result['stdout'])
        
        # Parse the output to check individual services
        lines = result['stdout'].split('\n')[1:]  # Skip header
        services = {}
        
        for line in lines:
            if line.strip():
                parts = line.split()
                if len(parts) >= 2:
                    service_name = parts[0]
                    status = ' '.join(parts[1:])
                    services[service_name] = status
        
        # Check specific services
        critical_services = ['redis', 'etl-worker', 'flower', 'postgres']
        all_running = True
        
        for service in critical_services:
            service_found = False
            for container_name, status in services.items():
                if service in container_name:
                    service_found = True
                    if 'Up' in status:
                        print(f"PASS {service}: Running ({status})")
                    else:
                        print(f"FAIL {service}: Not running ({status})")
                        all_running = False
                    break
            
            if not service_found:
                print(f"WARN {service}: Container not found")
                all_running = False
        
        return all_running
    else:
        print("FAIL Could not get Docker Compose status")
        print(f"Error: {result['stderr']}")
        return False

def check_redis_detailed():
    """Detailed Redis connection and queue check"""
    print("\n" + "="*60)
    print("REDIS DETAILED CHECK")
    print("="*60)
    
    try:
        # Test Redis connection
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        print("PASS Redis connection successful")
        
        # Check Redis info
        info = r.info()
        print(f"PASS Redis version: {info.get('redis_version')}")
        print(f"PASS Connected clients: {info.get('connected_clients')}")
        
        # Check Celery queues
        queue_length = r.llen('celery')
        print(f"INFO Celery queue length: {queue_length}")
        
        # Check for any keys
        all_keys = r.keys('*')
        print(f"INFO Total Redis keys: {len(all_keys)}")
        
        # Show some keys for debugging
        celery_keys = [key.decode('utf-8') for key in all_keys if b'celery' in key]
        if celery_keys:
            print(f"INFO Celery-related keys: {celery_keys[:5]}")
        
        return True
        
    except redis.ConnectionError as e:
        print(f"FAIL Redis connection failed: {e}")
        return False
    except Exception as e:
        print(f"FAIL Redis error: {e}")
        return False

def check_flower_detailed():
    """Detailed Flower service check"""
    print("\n" + "="*60)
    print("FLOWER SERVICE CHECK")
    print("="*60)
    
    # Check if Flower port is open
    import socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = sock.connect_ex(('localhost', 5555))
    sock.close()
    
    if result == 0:
        print("PASS Port 5555 is open")
    else:
        print("FAIL Port 5555 is not accessible")
        print("This suggests Flower container is not running or not properly exposed")
        
        # Check if container exists
        result = run_command("docker-compose ps flower")
        if result['success']:
            print("Container status:")
            print(result['stdout'])
        else:
            print("Could not check Flower container status")
        
        return False
    
    # Test HTTP connection with authentication
    from requests.auth import HTTPBasicAuth
    auth = HTTPBasicAuth('admin', 'password123')
    
    try:
        response = requests.get("http://localhost:5555", auth=auth, timeout=5)
        print(f"PASS Flower HTTP response: {response.status_code}")
        
        if response.status_code == 200:
            print("PASS Flower web interface is accessible")
        else:
            print(f"WARN Flower returned unexpected status: {response.status_code}")
            
    except requests.exceptions.ConnectionError:
        print("FAIL Cannot connect to Flower HTTP interface")
        return False
    except Exception as e:
        print(f"FAIL Flower HTTP error: {e}")
        return False
    
    # Test Flower API endpoints with authentication
    try:
        # Try different API endpoints
        endpoints = [
            ("/api/workers", "Workers API"),
            ("/api/tasks", "Tasks API"), 
            ("/dashboard", "Dashboard")
        ]
        
        workers_found = 0
        
        for endpoint, description in endpoints:
            try:
                response = requests.get(f"http://localhost:5555{endpoint}", auth=auth, timeout=5)
                if response.status_code == 200:
                    print(f"PASS {description}: Accessible")
                    if endpoint == "/api/workers":
                        workers = response.json()
                        workers_found = len(workers)
                        print(f"INFO Found {workers_found} workers: {list(workers.keys())}")
                        
                        # Show worker details
                        for worker_name, worker_info in workers.items():
                            status = worker_info.get('status', 'unknown')
                            active = worker_info.get('active', 0)
                            print(f"     - {worker_name}: {status} ({active} active tasks)")
                        
                elif response.status_code == 401:
                    print(f"FAIL {description}: Authentication failed")
                    print("Check if username/password are correct")
                else:
                    print(f"WARN {description}: Status {response.status_code}")
            except Exception as e:
                print(f"WARN {description}: {e}")
        
        return workers_found > 0
        
    except Exception as e:
        print(f"WARN Could not test Flower APIs: {e}")
        return False

def check_celery_workers_direct():
    """Check Celery workers directly via Docker"""
    print("\n" + "="*60)
    print("CELERY WORKERS DIRECT CHECK")
    print("="*60)
    
    # Check worker container logs
    result = run_command("docker-compose logs --tail=20 etl-worker")
    if result['success']:
        print("Recent ETL worker logs:")
        logs = result['stdout']
        
        # Check for key indicators
        if 'ready' in logs.lower():
            print("PASS Worker shows 'ready' status")
        else:
            print("WARN Worker may not be ready")
            
        if 'connected to redis' in logs.lower():
            print("PASS Worker connected to Redis")
        else:
            print("WARN Worker may not be connected to Redis")
            
        if 'error' in logs.lower() or 'exception' in logs.lower():
            print("WARN Errors found in worker logs:")
            error_lines = [line for line in logs.split('\n') if 'error' in line.lower() or 'exception' in line.lower()]
            for line in error_lines[-3:]:  # Show last 3 errors
                print(f"  {line}")
        
        # Show last few lines of logs
        print("\nLast 5 log lines:")
        lines = logs.split('\n')[-5:]
        for line in lines:
            if line.strip():
                print(f"  {line}")
                
    else:
        print("FAIL Could not get worker logs")
        print(f"Error: {result['stderr']}")
        return False
    
    # Try to execute a command in the worker container
    result = run_command("docker-compose exec -T etl-worker celery -A etl_processor.celery_app status")
    if result['success']:
        print(f"\nCelery status from worker:")
        print(result['stdout'])
        return 'OK' in result['stdout']
    else:
        print(f"\nWARN Could not get Celery status from worker")
        print(f"Error: {result['stderr']}")
        
        # Try alternative command
        result = run_command("docker-compose exec -T etl-worker python -c \"from etl_processor.celery_app import app; print('Celery app loaded')\"")
        if result['success']:
            print("PASS Celery app can be imported in worker")
            return True
        else:
            print("FAIL Cannot import Celery app in worker")
            print(f"Error: {result['stderr']}")
            return False

def check_network_connectivity():
    """Check network connectivity between services"""
    print("\n" + "="*60)
    print("NETWORK CONNECTIVITY CHECK")
    print("="*60)
    
    # Test if worker can reach Redis
    result = run_command("docker-compose exec -T etl-worker python -c \"import redis; r=redis.Redis(host='redis', port=6379); r.ping(); print('Worker can reach Redis')\"")
    if result['success']:
        print("PASS Worker can connect to Redis")
    else:
        print("FAIL Worker cannot connect to Redis")
        print(f"Error: {result['stderr']}")
    
    # Test if worker can reach PostgreSQL
    result = run_command("docker-compose exec -T etl-worker python -c \"from etl_processor.database_postgres import test_connection; print(test_connection())\"")
    if result['success']:
        print("PASS Worker can connect to PostgreSQL")
        print(f"Result: {result['stdout']}")
    else:
        print("WARN Worker may have issues connecting to PostgreSQL")
        print(f"Error: {result['stderr']}")

def suggest_fixes():
    """Suggest specific fixes based on diagnosis"""
    print("\n" + "="*60)
    print("SUGGESTED FIXES")
    print("="*60)
    
    print("Based on your test results, try these steps:")
    print()
    print("1. RESTART ALL SERVICES:")
    print("   docker-compose down")
    print("   docker-compose up -d")
    print()
    print("2. CHECK FLOWER SPECIFICALLY:")
    print("   docker-compose up -d flower")
    print("   docker-compose logs flower")
    print()
    print("3. VERIFY PORTS:")
    print("   netstat -an | findstr :5555")
    print("   netstat -an | findstr :6379")
    print()
    print("4. REBUILD IF NEEDED:")
    print("   docker-compose down")
    print("   docker-compose build --no-cache")
    print("   docker-compose up -d")
    print()
    print("5. CHECK SPECIFIC LOGS:")
    print("   docker-compose logs -f flower")
    print("   docker-compose logs -f etl-worker")
    print("   docker-compose logs -f redis")

def main():
    """Run complete diagnosis"""
    print("CELERY SYSTEM DIAGNOSIS")
    print("Starting comprehensive check...")
    print(f"Time: {datetime.now()}")
    
    # Run all checks
    docker_ok = check_docker_services()
    redis_ok = check_redis_detailed()
    flower_ok = check_flower_detailed()
    workers_ok = check_celery_workers_direct()
    
    check_network_connectivity()
    
    # Summary
    print("\n" + "="*60)
    print("DIAGNOSIS SUMMARY")
    print("="*60)
    
    checks = {
        'Docker Services': docker_ok,
        'Redis': redis_ok,
        'Flower': flower_ok,
        'Celery Workers': workers_ok
    }
    
    for check_name, result in checks.items():
        status = "PASS" if result else "FAIL"
        print(f"{check_name:.<30} {status}")
    
    # Determine main issue
    if not docker_ok:
        print("\nMAIN ISSUE: Docker services are not running properly")
        print("ACTION: Run 'docker-compose up -d' to start all services")
    elif not flower_ok:
        print("\nMAIN ISSUE: Flower (Celery monitoring) is not accessible")
        print("ACTION: Check Flower container: 'docker-compose logs flower'")
    elif not workers_ok:
        print("\nMAIN ISSUE: Celery workers are not functioning properly")
        print("ACTION: Check worker logs: 'docker-compose logs etl-worker'")
    elif redis_ok and workers_ok and not flower_ok:
        print("\nGOOD NEWS: Redis and workers are fine, only Flower has issues")
        print("ACTION: Flower is optional for basic ETL operation")
        print("Your file watcher should still work even without Flower")
    else:
        print("\nSYSTEM STATUS: Mixed results - see individual checks above")
    
    suggest_fixes()
    
    return all([docker_ok, redis_ok, workers_ok])

if __name__ == "__main__":
    success = main()
    
    print(f"\nDiagnosis complete. Overall system health: {'GOOD' if success else 'NEEDS ATTENTION'}")
    print("\nNext steps:")
    print("1. Address any FAIL items above")
    print("2. Re-run this diagnosis: python celery_diagnosis.py")
    print("3. Test the fixed file watcher: python fixed_file_watcher_service.py")
    
    sys.exit(0 if success else 1)