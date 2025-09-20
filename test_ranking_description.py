#!/usr/bin/env python3
"""
Test script to validate updated ranking description
"""
import sys
sys.path.append('.')
from app.services import ReportGenerator
from datetime import datetime, timedelta

def test_updated_ranking_description():
    """Test that the ranking description reflects the new formula"""
    try:
        rg = ReportGenerator()
        
        # Generate consolidated report to get the ranking description
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        print("🔍 Testing updated ranking description...")
        
        result = rg.generate_consolidated_report(start_date, end_date, "JANDAIA")
        
        if result.get('success'):
            structured_data = result.get('data', {})
            rankings = structured_data.get('ranking_melhores', [])
            
            if rankings:
                ranking_info = rankings[0]  # Get first ranking info
                title = ranking_info.get('titulo', '')
                description = ranking_info.get('descricao', '')
                
                print(f"📋 Ranking Section Information:")
                print(f"   Title: {title}")
                print(f"   Description: {description}")
                
                # Verify the description has been updated
                if "combustível" in description and "consumo" not in description:
                    print(f"\n✅ Description successfully updated!")
                    print(f"   ✓ Uses 'combustível' instead of 'consumo'")
                    print(f"   ✓ Reflects new ranking logic")
                else:
                    print(f"\n❌ Description still uses old formula")
                    print(f"   Expected: combustível")
                    print(f"   Found: {description}")
            else:
                print("❌ No ranking data found")
        else:
            print(f"❌ Report generation failed: {result.get('error')}")
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_updated_ranking_description()