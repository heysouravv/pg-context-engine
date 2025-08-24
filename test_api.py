#!/usr/bin/env python3
"""
Test script for the pg-context-engine API endpoints
"""

import requests
import json
import time
from typing import Dict, Any

BASE_URL = "http://localhost:8000"

def make_request(method: str, endpoint: str, data: Dict[str, Any] = None, params: Dict[str, Any] = None) -> Dict[str, Any]:
    """Make an HTTP request and return the JSON response"""
    url = f"{BASE_URL}{endpoint}"
    
    try:
        if method.upper() == "GET":
            response = requests.get(url, params=params)
        elif method.upper() == "POST":
            response = requests.post(url, json=data, headers={'Content-Type': 'application/json'})
        else:
            raise ValueError(f"Unsupported method: {method}")
        
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error making {method} request to {endpoint}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_detail = e.response.json()
                print(f"   Error details: {json.dumps(error_detail, indent=2)}")
            except:
                print(f"   Response text: {e.response.text}")
        return None

def test_global_mirror():
    """Test 1: Global mirror upload"""
    print("\n🔍 Testing Global Mirror Upload...")
    
    data = {
        "version": "v1",
        "rows": [
            {"id": 1, "status": "new", "country": "IN", "amount": 1200},
            {"id": 2, "status": "shipped", "country": "US", "amount": 800},
            {"id": 3, "status": "new", "country": "IN", "amount": 1500}
        ]
    }
    
    result = make_request("POST", "/mirror/global/orders", data)
    if result:
        print(f"✅ Global mirror upload successful: {result}")
        return result.get("workflow_id")
    return None

def test_user_context(workflow_id: str):
    """Test 2: Set user context for alice"""
    print("\n🔍 Testing User Context Setup...")
    
    data = {
        "filters": {"status": ["new"], "country": "IN"},
        "sort": {"by": "amount", "desc": True}
    }
    
    result = make_request("POST", "/context/alice/orders", data)
    if result:
        print(f"✅ User context setup successful: {result}")
        return result.get("workflow_id")
    return None

def test_sql_urls():
    """Test 3: Get SQL URLs"""
    print("\n🔍 Testing SQL URLs endpoint...")
    
    result = make_request("GET", "/sql/urls")
    if result:
        print(f"✅ SQL URLs retrieved: {json.dumps(result, indent=2)}")
    return result

def test_userdb_schema():
    """Test 4: Register UserDB table schema"""
    print("\n🔍 Testing UserDB Schema Registration...")
    
    data = {
        "pk_path": "$.id",
        "ts_path": "$.updated_at",
        "indexes": [
            {"name": "idx_status", "path": "$.status", "type": "string"},
            {"name": "idx_country", "path": "$.country", "type": "string"},
            {"name": "idx_amount", "path": "$.amount", "type": "integer"}
        ]
    }
    
    result = make_request("POST", "/userdb/alice/orders/schema", data)
    if result:
        print(f"✅ UserDB schema registration successful: {result}")
        return result.get("phy_table")
    return None

def test_userdb_info():
    """Test 5: Get UserDB table info"""
    print("\n🔍 Testing UserDB Info...")
    
    result = make_request("GET", "/userdb/alice/orders/info")
    if result:
        print(f"✅ UserDB info retrieved: {json.dumps(result, indent=2)}")
        return result.get("table", {}).get("phy_table")
    return None

def test_userdb_upsert_lww():
    """Test 6a: Upsert with LWW behavior"""
    print("\n🔍 Testing UserDB Upsert (LWW)...")
    
    data = {
        "mode": "lww",
        "rows": [
            {"item": {"id": "o1", "status": "new", "country": "IN", "amount": 1500, "updated_at": 1724460000}},
            {"item": {"id": "o2", "status": "shipped", "country": "US", "amount": 800, "updated_at": 1724461000}}
        ]
    }
    
    result = make_request("POST", "/userdb/alice/orders/upsert", data)
    if result:
        print(f"✅ Initial upsert successful: {result}")
    return result

def test_userdb_upsert_older_timestamp():
    """Test 6b: Test LWW with older timestamp (should not overwrite)"""
    print("\n🔍 Testing LWW with older timestamp...")
    
    data = {
        "mode": "lww",
        "rows": [
            {"item": {"id": "o1", "status": "new", "country": "IN", "amount": 999}, "client_ts": 1724450000}
        ]
    }
    
    result = make_request("POST", "/userdb/alice/orders/upsert", data)
    if result:
        print(f"✅ Older timestamp upsert successful (should not overwrite): {result}")
    return result

def test_userdb_upsert_newer_timestamp():
    """Test 6c: Test LWW with newer timestamp (should overwrite)"""
    print("\n🔍 Testing LWW with newer timestamp...")
    
    data = {
        "mode": "lww",
        "rows": [
            {"item": {"id": "o1", "status": "new", "country": "IN", "amount": 1600}, "client_ts": 1724470000}
        ]
    }
    
    result = make_request("POST", "/userdb/alice/orders/upsert", data)
    if result:
        print(f"✅ Newer timestamp upsert successful (should overwrite): {result}")
    return result

def test_userdb_upsert_force():
    """Test 6d: Test force overwrite (ignores timestamp)"""
    print("\n🔍 Testing Force Upsert...")
    
    data = {
        "mode": "force",
        "rows": [
            {"item": {"id": "o2", "status": "shipped", "country": "US", "amount": 999}}
        ]
    }
    
    result = make_request("POST", "/userdb/alice/orders/upsert", data)
    if result:
        print(f"✅ Force upsert successful: {result}")
    return result

def test_userdb_query_filtered():
    """Test 6e: Test filtered queries"""
    print("\n🔍 Testing Filtered Queries...")
    
    # Query with since timestamp and status filter
    params = {"since": 1724459000, "idx_status": "new", "limit": 100}
    result = make_request("GET", "/userdb/alice/orders/query", params=params)
    if result:
        print(f"✅ Filtered query (since + status) successful: {result}")
    
    # Query with multiple filters
    params = {"idx_status": "new", "idx_country": "IN", "order": "desc"}
    result = make_request("GET", "/userdb/alice/orders/query", params=params)
    if result:
        print(f"✅ Multi-filter query successful: {result}")
    
    return result

def test_userdb_delete():
    """Test 6f: Test delete operation"""
    print("\n🔍 Testing Delete Operation...")
    
    data = {"pks": ["o2"]}
    result = make_request("POST", "/userdb/alice/orders/delete", data)
    if result:
        print(f"✅ Delete operation successful: {result}")
    return result

def main():
    """Run all tests in sequence"""
    print("🚀 Starting pg-context-engine API Tests")
    print("=" * 50)
    
    # Wait for services to be ready
    print("⏳ Waiting for services to be ready...")
    time.sleep(5)
    
    try:
        # Test 1: Global mirror upload
        workflow_id_1 = test_global_mirror()
        
        # Test 2: User context
        workflow_id_2 = test_user_context(workflow_id_1)
        
        # Test 3: SQL URLs
        test_sql_urls()
        
        # Test 4: UserDB schema
        phy_table = test_userdb_schema()
        
        # Test 5: UserDB info
        test_userdb_info()
        
        # Test 6a: Initial upsert
        test_userdb_upsert_lww()
        
        # Test 6b: LWW with older timestamp
        test_userdb_upsert_older_timestamp()
        
        # Test 6c: LWW with newer timestamp
        test_userdb_upsert_newer_timestamp()
        
        # Test 6d: Force upsert
        test_userdb_upsert_force()
        
        # Test 6e: Filtered queries
        test_userdb_query_filtered()
        
        # Test 6f: Delete
        test_userdb_delete()
        
        print("\n🎉 All tests completed!")
        print(f"📊 Check Temporal UI at: http://localhost:8233")
        print(f"   - Look for workflow IDs: {workflow_id_1}, {workflow_id_2}")
        
    except Exception as e:
        print(f"\n❌ Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
