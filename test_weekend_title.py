#!/usr/bin/env python3
"""
Test script to validate weekend title formatting with actual data
"""
import sys
sys.path.append('.')
from app.reports import format_weekend_title
from datetime import datetime, timedelta

def test_weekend_title_formatting():
    """Test the weekend title function with various date ranges"""
    print("🔍 Testing weekend title formatting...")
    
    # Test 1: Week with Saturday and Sunday
    start_date = datetime(2025, 9, 13)  # Friday
    end_date = datetime(2025, 9, 16)    # Monday
    print(f"\nTest 1 dates: {start_date.strftime('%A %d/%m/%Y')} to {end_date.strftime('%A %d/%m/%Y')}")
    
    # Check each day in the range
    current = start_date
    while current <= end_date:
        print(f"  {current.strftime('%A %d/%m/%Y')} - weekday(): {current.weekday()}")
        current += timedelta(days=1)
    
    title1 = format_weekend_title(start_date, end_date)
    print(f"Result: {title1}")
    
    # Test 2: Period starting on Saturday
    start_date = datetime(2025, 9, 14)  # Saturday
    end_date = datetime(2025, 9, 15)    # Sunday
    print(f"\nTest 2 dates: {start_date.strftime('%A %d/%m/%Y')} to {end_date.strftime('%A %d/%m/%Y')}")
    title2 = format_weekend_title(start_date, end_date)
    print(f"Result: {title2}")
    
    # Let's check September 2025 calendar
    print("\n📅 September 2025 calendar check:")
    for day in range(1, 30):
        date = datetime(2025, 9, day)
        if date.weekday() >= 5:
            print(f"  {date.strftime('%A %d/%m/%Y')} - weekday(): {date.weekday()}")
    
    print("\n✅ Weekend title tests completed!")

if __name__ == "__main__":
    test_weekend_title_formatting()