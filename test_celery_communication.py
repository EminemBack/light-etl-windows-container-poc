#!/usr/bin/env python3
"""Test script to verify all services are communicating"""

import time
import requests
from datetime import datetime

def test_services():
    print("="*50)
    print("Testing Light ETL POC Services")
    print("="*50)
    
    # Test 1: Redis
    print("\n1. Testing Redis...")
    try:
        import redis
        r = redis.Redis(host='localhost', port=6379, db=0)
        r.ping()
        print("✓ Redis is accessible")
    except Exception as e:
        print(f"✗ Redis error: {e}")
    
    # Test 2: Flower
    print("\n2. Testing Flower...")
    try:
        response = requests.get("http://localhost:5555/workers")
        if response.status_code == 200:
            workers = response.json()
            print(f"✓ Flower is running")
            print(f"  Workers: {list(workers.keys())}")
        else:
            print(f"✗ Flower returned status: {response.status_code}")
    except Exception as e:
        print(f"✗ Flower error: {e}")
    
    # Test 3: Check worker queues
    print("\n3. Testing Celery Queues...")
    try:
        response = requests.get("http://localhost:5555/api/queues")
        if response.status_code == 200:
            queues = response.json()
            print(f"✓ Queues active")
            for queue_name, queue_info in queues.items():
                print(f"  Queue '{queue_name}': {queue_info.get('messages', 0)} messages")
    except Exception as e:
        print(f"✗ Queue error: {e}")
    
    print("\n" + "="*50)
    print("Test completed!")

if __name__ == "__main__":
    test_services()