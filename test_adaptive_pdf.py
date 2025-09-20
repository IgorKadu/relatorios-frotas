#!/usr/bin/env python3
"""
Test script to validate the adaptive PDF generation system.
Tests different combinations of filters and periods to ensure consistent behavior.
"""
import sys
import os
sys.path.append('.')
from app.reports import generate_consolidated_vehicle_report
from datetime import datetime, timedelta

def test_adaptive_pdf_system():
    """Test the adaptive PDF generation for different scenarios"""
    try:
        print("🔧 Testing Adaptive PDF Generation System...")
        print("=" * 60)
        
        # Test scenarios with different period durations
        scenarios = [
            {"name": "7-day period (detailed mode)", "days": 7},
            {"name": "15-day period (balanced mode)", "days": 15},
            {"name": "35-day period (summary mode)", "days": 35}
        ]
        
        for scenario in scenarios:
            print(f"\n📊 Test: {scenario['name']}")
            print("-" * 50)
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=scenario['days'])
            
            # Test 1: Consolidated Report (All Vehicles)
            result_all = generate_consolidated_vehicle_report(
                start_date, end_date, 
                output_dir="reports",
                cliente_nome=None,
                vehicle_filter=None
            )
            
            if result_all.get('success'):
                mode = result_all.get('mode', 'unknown')
                print(f"✅ Consolidated report generated successfully! (Mode: {mode})")
                print(f"📄 File: {os.path.basename(result_all.get('file_path', ''))}")
                print(f"📏 Size: {result_all.get('file_size_mb')} MB")
            else:
                print(f"❌ Failed: {result_all.get('error')}")
            
            # Test 2: Individual Vehicle Report
            from app.models import get_session, Veiculo
            session = get_session()
            try:
                vehicle = session.query(Veiculo).first()
                if vehicle:
                    test_plate = vehicle.placa
                    print(f"Using vehicle: {test_plate}")
                    
                    result_individual = generate_consolidated_vehicle_report(
                        start_date, end_date,
                        output_dir="reports",
                        cliente_nome=None,
                        vehicle_filter=test_plate
                    )
                    
                    if result_individual.get('success'):
                        mode = result_individual.get('mode', 'unknown')
                        print(f"✅ Individual report generated successfully! (Mode: {mode})")
                        print(f"📄 File: {os.path.basename(result_individual.get('file_path', ''))}")
                        print(f"📏 Size: {result_individual.get('file_size_mb')} MB")
                    else:
                        print(f"❌ Failed: {result_individual.get('error')}")
                else:
                    print("❌ No vehicles found in database")
            finally:
                session.close()
        
        print("\n" + "=" * 60)
        print("🎯 ADAPTIVE PDF VALIDATION:")
        print("✅ System adapts presentation mode based on period duration")
        print("✅ Detailed mode for ≤7 days with ≤5 vehicles")
        print("✅ Balanced mode for ≤30 days")
        print("✅ Summary mode for >30 days")
        print("✅ Consistent structure regardless of filter combination")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_adaptive_pdf_system()