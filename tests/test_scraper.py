#!/usr/bin/env python3
"""
Test script for wayback snapshot scraper

This script can be used to test the blob storage functionality locally
before deploying to Azure Functions.
"""

import json
import os
import sys
from azure.storage.blob import BlobServiceClient

def test_blob_connectivity():
    """Test Azure Blob Storage connectivity"""
    connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    if not connection_string:
        print("ERROR: AZURE_STORAGE_CONNECTION_STRING environment variable not set")
        return False
    
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Try to list containers
        containers = list(blob_service_client.list_containers())
        print(f"Successfully connected to blob storage. Found {len(containers)} containers.")
        
        for container in containers:
            print(f"  - {container.name}")
        
        return True
        
    except Exception as e:
        print(f"ERROR: Failed to connect to blob storage: {e}")
        return False

def upload_test_urls(container_name="test", blob_name="static_urls.json"):
    """Upload a test URLs file to blob storage"""
    test_urls = {
        "test_company": [
            "https://example.com",
            "https://httpbin.org/html"
        ]
    }
    
    connection_string = os.environ.get('AZURE_STORAGE_CONNECTION_STRING')
    if not connection_string:
        print("ERROR: AZURE_STORAGE_CONNECTION_STRING environment variable not set")
        return False
    
    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # Create container if it doesn't exist
        container_client = blob_service_client.get_container_client(container_name)
        try:
            container_client.create_container()
            print(f"Created container: {container_name}")
        except Exception:
            print(f"Container {container_name} already exists")
        
        # Upload test URLs
        blob_client = blob_service_client.get_blob_client(
            container=container_name, 
            blob=blob_name
        )
        
        json_data = json.dumps(test_urls, indent=2)
        blob_client.upload_blob(json_data, overwrite=True)
        
        print(f"Successfully uploaded test URLs to {container_name}/{blob_name}")
        return True
        
    except Exception as e:
        print(f"ERROR: Failed to upload test URLs: {e}")
        return False

def main():
    """Run all tests"""
    print("=== Azure Blob Storage Wayback Scraper Test ===\n")
    
    tests = [
        ("Blob Storage Connectivity", test_blob_connectivity),
        ("Upload Test URLs", upload_test_urls),
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"Running: {test_name}")
        try:
            result = test_func()
            results.append((test_name, result))
            print(f"Result: {'PASS' if result else 'FAIL'}\n")
        except Exception as e:
            print(f"Result: ERROR - {e}\n")
            results.append((test_name, False))
    
    print("=== Test Summary ===")
    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"{test_name}: {status}")
    
    all_passed = all(result for _, result in results)
    print(f"\nOverall: {'ALL TESTS PASSED' if all_passed else 'SOME TESTS FAILED'}")
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())