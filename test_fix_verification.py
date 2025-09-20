#!/usr/bin/env python3
"""
Test script to verify that the same-day period fix is working correctly.
"""

import sys
import os
from datetime import datetime

# Add the project directory to the path
sys.path.append('.')

from app.reports import generate_consolidated_vehicle_report

def test_same_day_fix():
    """Test that same-day periods work correctly"""
    print("🔍 Testing Same-Day Period Fix...")
    print("=" * 50)
    
    # Use a date that should have data
    test_date = datetime(2025, 9, 1)
    
    print(f"📅 Test Date: {test_date.strftime('%d/%m/%Y')}")
    print("-" * 30)
    
    # Test 1: Individual vehicle with same-day period
    print("\n🚗 Testing Individual Vehicle (Same Day)")
    try:
        result1 = generate_consolidated_vehicle_report(
            test_date, test_date,
            output_dir="reports",
            vehicle_filter="TFP-8H93"
        )
        
        if result1.get('success'):
            mode = result1.get('mode', 'unknown')
            print(f"✅ SUCCESS! Mode: {mode}")
            print(f"📄 File: {os.path.basename(result1.get('file_path', ''))}")
        else:
            print(f"❌ FAILED: {result1.get('error')}")
    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
    
    # Test 2: All vehicles with same-day period
    print("\n📋 Testing All Vehicles (Same Day)")
    try:
        result2 = generate_consolidated_vehicle_report(
            test_date, test_date,
            output_dir="reports",
            vehicle_filter=None
        )
        
        if result2.get('success'):
            mode = result2.get('mode', 'unknown')
            print(f"✅ SUCCESS! Mode: {mode}")
            print(f"📄 File: {os.path.basename(result2.get('file_path', ''))}")
        else:
            print(f"❌ FAILED: {result2.get('error')}")
    except Exception as e:
        print(f"❌ EXCEPTION: {e}")
    
    print("\n" + "=" * 50)
    print("✅ Same-Day Fix Verification Complete")

if __name__ == "__main__":
    test_same_day_fix()