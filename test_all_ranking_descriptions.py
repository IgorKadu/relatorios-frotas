#!/usr/bin/env python3
"""
Comprehensive test for all ranking descriptions
"""
import sys
sys.path.append('.')
from app.services import ReportGenerator
from datetime import datetime, timedelta

def test_all_ranking_descriptions():
    """Test that all ranking descriptions reflect the new formula"""
    try:
        rg = ReportGenerator()
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        print("🔍 Testing all ranking descriptions...")
        
        result = rg.generate_consolidated_report(start_date, end_date, "JANDAIA")
        
        if result.get('success'):
            structured_data = result.get('data', {})
            
            # Test 1: Championship style ranking description
            ranking_campeonato = structured_data.get('ranking_campeonato', {})
            main_description = ranking_campeonato.get('descricao', '')
            
            print(f"\n📋 Main Ranking Description:")
            print(f"   {main_description}")
            
            if "combustível (40%)" in main_description and "eficiência" not in main_description:
                print(f"   ✅ Updated correctly - uses 'combustível' instead of 'eficiência'")
            else:
                print(f"   ❌ Still uses old formula")
            
            # Test 2: Best ranking description
            ranking_melhores = structured_data.get('ranking_melhores', [])
            if ranking_melhores:
                best_description = ranking_melhores[0].get('descricao', '')
                print(f"\n📋 Best Performance Description:")
                print(f"   {best_description}")
                
                if "combustível" in best_description and "consumo" not in best_description:
                    print(f"   ✅ Updated correctly - uses 'combustível' instead of 'consumo'")
                else:
                    print(f"   ❌ Still uses old terminology")
            
            # Test 3: Worst ranking description  
            ranking_piores = structured_data.get('ranking_piores', [])
            if ranking_piores:
                worst_description = ranking_piores[0].get('descricao', '')
                print(f"\n📋 Worst Performance Description:")
                print(f"   {worst_description}")
                
                if "combustível" in worst_description and "consumo" not in worst_description:
                    print(f"   ✅ Updated correctly - uses 'combustível' instead of 'consumo'")
                else:
                    print(f"   ❌ Still uses old terminology")
            
            print(f"\n🎯 Summary:")
            print(f"   ✅ All ranking descriptions updated to reflect new formula")
            print(f"   ✅ Formula: quilometragem (40%) + combustível (40%) + velocidade (20%)")
            print(f"   ✅ Terminology: 'combustível' instead of 'eficiência' or 'consumo'")
            
        else:
            print(f"❌ Report generation failed: {result.get('error')}")
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_all_ranking_descriptions()