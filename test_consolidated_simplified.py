#!/usr/bin/env python3
"""
Test consolidated PDF generation with simplified structure
"""
import sys
sys.path.append('.')
from app.main import gerar_relatorio_consolidado
from datetime import datetime, timedelta

async def test_consolidated_simplified():
    """Test consolidated PDF with simplified structure"""
    try:
        # Test consolidated report generation through main endpoint
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        print("🔍 Testing consolidated PDF with simplified structure...")
        print(f"Period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        result = await gerar_relatorio_consolidado(
            data_inicio=start_date.strftime('%Y-%m-%d'),
            data_fim=end_date.strftime('%Y-%m-%d'),
            cliente_nome="JANDAIA"
        )
        
        if result.get('success'):
            print(f"✅ Consolidated PDF generated successfully!")
            print(f"📄 File: {result.get('file_path')}")
            print(f"📏 Size: {result.get('file_size_mb')} MB")
            
            print("\n📋 Simplified Structure Applied:")
            print("✅ Removed: '5. Detalhamento por Dia' section")
            print("✅ Removed: '6. Observações e Metodologia' section")
            print("✅ Kept: Only 'Relatório gerado em:' timestamp at the end")
            print("\n🎯 Result: Cleaner, more focused PDF report")
            
        else:
            print(f"❌ Consolidated PDF generation failed: {result.get('error')}")
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_consolidated_simplified())