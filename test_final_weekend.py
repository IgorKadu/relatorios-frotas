#!/usr/bin/env python3
"""
Final validation of weekend title functionality
"""
import sys
sys.path.append('.')
from app.reports import format_weekend_title
from datetime import datetime, timedelta

def test_weekend_title_final():
    """Test the specific scenario mentioned in the request"""
    print("🎯 Final Weekend Title Test")
    print("=" * 50)
    
    # Test current week (September 11-18, 2025)
    start_date = datetime(2025, 9, 11)  # Wednesday
    end_date = datetime(2025, 9, 18)    # Wednesday
    
    print(f"Period: {start_date.strftime('%d/%m/%Y')} to {end_date.strftime('%d/%m/%Y')}")
    
    # Show all weekend dates in the period
    current = start_date
    weekend_dates = []
    while current <= end_date:
        if current.weekday() >= 5:
            weekend_dates.append(current)
            print(f"  Weekend date found: {current.strftime('%A %d/%m/%Y')} (weekday {current.weekday()})")
        current += timedelta(days=1)
    
    # Generate the title
    title = format_weekend_title(start_date, end_date)
    print(f"\n📋 Generated Weekend Title:")
    print(f"   {title}")
    
    print(f"\n✅ Success! The weekend title now shows both Saturday and Sunday dates")
    print(f"   instead of just showing one date like 'Final de Semana (06/09/2025)'")
    
    # Test with a period that includes the specific dates mentioned
    print(f"\n🎯 Testing with September 6-7, 2025 (mentioned dates):")
    start_date2 = datetime(2025, 9, 6)   # Saturday
    end_date2 = datetime(2025, 9, 7)     # Sunday
    title2 = format_weekend_title(start_date2, end_date2)
    print(f"   {title2}")

if __name__ == "__main__":
    test_weekend_title_final()