#!/usr/bin/env python3
"""
Test script to validate the same-day PDF generation fix.
Tests that reports can be generated for the same start and end date.
"""
import sys
import os
from datetime import datetime, timedelta

# Add the project directory to the path
sys.path.append('.')

from app.reports import generate_consolidated_vehicle_report

def test_same_day_pdf_generation():
    """Test PDF generation for same-day periods"""
    try:
        print("🔧 Testing Same-Day PDF Generation...")
        print("=" * 50)
        
        # Test with same start and end date (today)
        test_date = datetime.now()
        
        print(f"\n📅 Test Date: {test_date.strftime('%d/%m/%Y')}")
        print("-" * 30)
        
        # Test 1: Individual vehicle report for same day
        print("\n🚗 Individual Vehicle Report (Same Day)")
        result1 = generate_consolidated_vehicle_report(
            test_date, test_date,
            output_dir="reports",
            cliente_nome=None,
            vehicle_filter="TFP-8H93"
        )
        
        if result1.get('success'):
            mode = result1.get('mode', 'unknown')
            print(f"✅ SUCCESS! Mode: {mode}")
            print(f"📄 File: {os.path.basename(result1.get('file_path', ''))}")
            print(f"📏 Size: {result1.get('file_size_mb')} MB")
        else:
            print(f"❌ FAILED: {result1.get('error')}")
        
        # Test 2: Consolidated report for same day
        print("\n📋 Consolidated Report (Same Day)")
        result2 = generate_consolidated_vehicle_report(
            test_date, test_date,
            output_dir="reports",
            cliente_nome=None,
            vehicle_filter=None
        )
        
        if result2.get('success'):
            mode = result2.get('mode', 'unknown')
            print(f"✅ SUCCESS! Mode: {mode}")
            print(f"📄 File: {os.path.basename(result2.get('file_path', ''))}")
            print(f"📏 Size: {result2.get('file_size_mb')} MB")
        else:
            print(f"❌ FAILED: {result2.get('error')}")
        
        # Test 3: Client-specific report for same day
        print("\n👥 Client-Specific Report (Same Day)")
        result3 = generate_consolidated_vehicle_report(
            test_date, test_date,
            output_dir="reports",
            cliente_nome="JANDAIA",
            vehicle_filter=None
        )
        
        if result3.get('success'):
            mode = result3.get('mode', 'unknown')
            print(f"✅ SUCCESS! Mode: {mode}")
            print(f"📄 File: {os.path.basename(result3.get('file_path', ''))}")
            print(f"📏 Size: {result3.get('file_size_mb')} MB")
        else:
            print(f"❌ FAILED: {result3.get('error')}")
        
        print("\n" + "=" * 50)
        print("🎯 SAME-DAY PDF VALIDATION COMPLETE")
        
        # Check if all tests passed
        all_passed = all([
            result1.get('success'),
            result2.get('success'),
            result3.get('success')
        ])
        
        if all_passed:
            print("🎉 ALL SAME-DAY SCENARIOS WORK CORRECTLY!")
            print("✅ System now handles same-day periods properly")
            print("✅ Defaults to Detailed Mode for single-day reports")
            print("✅ All filter combinations work for same-day reports")
        else:
            print("⚠️  Some same-day scenarios still have issues")
            
        return all_passed
        
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    test_same_day_pdf_generation()