import sys
import redis
from etl_processor.celery_app import app
from etl_processor.simple_tasks import hello_world as simple_test

def check_health():
    """Comprehensive health check"""
    print("Health Check Starting...\n")
    
    checks_passed = 0
    checks_total = 0
    
    # Check 1: Redis Connection
    checks_total += 1
    try:
        r = redis.Redis(host='redis', port=6379, db=0)
        r.ping()
        print("✓ Redis connection: OK")
        checks_passed += 1
    except Exception as e:
        print(f"✗ Redis connection: FAILED - {e}")
    
    # Check 2: Celery Worker
    checks_total += 1
    try:
        i = app.control.inspect()
        stats = i.stats()
        if stats:
            print(f"✓ Celery workers: {len(stats)} online")
            checks_passed += 1
        else:
            print("✗ Celery workers: No workers found")
    except Exception as e:
        print(f"✗ Celery workers: FAILED - {e}")
    
    # Check 3: Task Execution
    checks_total += 1
    try:
        result = simple_test.delay()
        output = result.get(timeout=5)
        print(f"✓ Task execution: OK - {output}")
        checks_passed += 1
    except Exception as e:
        print(f"✗ Task execution: FAILED - {e}")
    
    # Check 4: Queue Status
    checks_total += 1
    try:
        i = app.control.inspect()
        active = i.active()
        reserved = i.reserved()
        print(f"✓ Queue status: Active={len(active or {})}, Reserved={len(reserved or {})}")
        checks_passed += 1
    except Exception as e:
        print(f"✗ Queue status: FAILED - {e}")
    
    # Summary
    print(f"\n{'='*40}")
    print(f"Health Check Summary: {checks_passed}/{checks_total} passed")
    print(f"{'='*40}")
    
    return checks_passed == checks_total

if __name__ == "__main__":
    success = check_health()
    sys.exit(0 if success else 1)