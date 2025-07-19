#!/usr/bin/env python3
"""
Simple test runner for TenantService integration tests.
Run with: poetry run python run_tests.py
"""

import os
import sys
import subprocess

def main():
    print("🧪 Running TenantService Integration Tests")
    print("=" * 50)
    
    # Check if PostgreSQL is running
    print("1. Checking PostgreSQL connection...")
    try:
        import psycopg2
        conn = psycopg2.connect(
            host="localhost",
            port="5432",
            database="legal_document_analyzer",
            user="postgres",
            password="postgres"
        )
        conn.close()
        print("✓ PostgreSQL connection successful")
    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        print("   Make sure PostgreSQL is running: docker-compose up -d")
        return 1
    
    # Check if .env file exists
    print("2. Checking configuration...")
    env_file = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_file):
        print("✓ .env file found")
    else:
        print("⚠ .env file not found (using defaults)")
    
    # Run the tests
    print("3. Running integration tests...")
    print()
    
    try:
        # Run pytest on the test file
        result = subprocess.run([
            sys.executable, "-m", "pytest", 
            "tests/test_tenant_integration.py", 
            "-v", "--tb=short"
        ], cwd=os.path.dirname(__file__))
        
        if result.returncode == 0:
            print("\n" + "=" * 50)
            print("🎉 ALL TESTS PASSED! 🎉")
            print("=" * 50)
        else:
            print("\n" + "=" * 50)
            print("❌ SOME TESTS FAILED")
            print("=" * 50)
        
        return result.returncode
        
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 