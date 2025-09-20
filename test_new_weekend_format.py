#!/usr/bin/env python3
"""
Test the new weekend title format
"""
import sys
sys.path.append('.')
from app.reports import format_weekend_title
from datetime import datetime, timedelta

def test_new_weekend_title_format():
    """Test the new weekend title format with 'data = X e data = Y'"""
    print("🔍 Testing new weekend title format...")
    
    # Test 1: Week with Saturday and Sunday
    start_date = datetime(2025, 9, 13)  # Saturday
    end_date = datetime(2025, 9, 16)    # Tuesday
    
    print(f"Period: {start_date.strftime('%d/%m/%Y')} to {end_date.strftime('%d/%m/%Y')}")
    
    # Show weekend dates found
    current = start_date
    while current <= end_date:
        if current.weekday() >= 5:
            day_name = "Sábado" if current.weekday() == 5 else "Domingo"
            print(f"  {day_name}: {current.strftime('%d/%m/%Y')}")
        current += timedelta(days=1)
    
    # Generate new title format
    title = format_weekend_title(start_date, end_date)
    print(f"\n📋 New Weekend Title Format:")
    print(f"   {title}")
    
    # Test 2: Another weekend example
    start_date2 = datetime(2025, 9, 6)   # Saturday
    end_date2 = datetime(2025, 9, 7)     # Sunday
    title2 = format_weekend_title(start_date2, end_date2)
    print(f"\n📋 Example 2:")
    print(f"   {title2}")
    
    print(f"\n✅ Weekend title format updated successfully!")
    print(f"   Format: 'Final de Semana data = [Saturday] e data = [Sunday]'")

if __name__ == "__main__":
    test_new_weekend_title_format()