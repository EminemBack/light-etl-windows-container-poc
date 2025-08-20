#!/usr/bin/env python3
"""Test connection between containers"""

import sys
import requests
from time import sleep

def test_fileserver(url="http://localhost:5000"):
    """Test the Windows file server"""
    print(f"Testing file server at {url}")
    
    max_retries = 5
    for i in range(max_retries):
        try:
            # Test health endpoint
            response = requests.get(f"{url}/health")
            if response.status_code == 200:
                print("✓ File server is healthy")
                print(f"  Response: {response.json()}")
                
                # Test list endpoint
                response = requests.get(f"{url}/list")
                if response.status_code == 200:
                    data = response.json()
                    print(f"✓ Found {data.get('count', 0)} files")
                    for file in data.get('files', [])[:3]:
                        print(f"  - {file['name']} ({file['size']} bytes)")
                    return True
            else:
                print(f"✗ Server returned status {response.status_code}")
                
        except requests.exceptions.ConnectionError:
            print(f"  Attempt {i+1}/{max_retries}: Connection failed, retrying...")
            sleep(5)
        except Exception as e:
            print(f"✗ Error: {e}")
            
    return False

if __name__ == "__main__":
    success = test_fileserver()
    sys.exit(0 if success else 1)