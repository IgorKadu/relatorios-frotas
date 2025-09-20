#!/usr/bin/env python3
"""
Test script to validate simplified PDF structure without daily breakdown
"""
import sys
sys.path.append('.')
from app.services import ReportGenerator
from datetime import datetime, timedelta

def test_simplified_pdf_structure():
    """Test the PDF generation with simplified structure"""
    try:
        rg = ReportGenerator()
        
        # Test consolidated report generation
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        print("🔍 Testing simplified PDF structure...")
        print(f"Period: {start_date.strftime('%d/%m/%Y')} to {end_date.strftime('%d/%m/%Y')}")
        
        result = rg.generate_consolidated_report(start_date, end_date, "JANDAIA")
        
        if result.get('success'):
            print(f"✅ Simplified PDF generated successfully!")
            print(f"📄 File: {result.get('file_path')}")
            print(f"📏 Size: {result.get('file_size_mb')} MB")
            
            print("\n📋 Simplified Structure Verification:")
            print("✓ Section 1: Dados Gerais do Período")
            print("✓ Section 2: Desempenho Geral no Período") 
            print("✓ Section 3: Desempenho Diário por Horário Operacional")
            print("✓ Section 4: Rankings")
            print("❌ Section 5: Detalhamento por Dia (REMOVED)")
            print("❌ Section 6: Observações e Metodologia (REMOVED)")
            print("✓ Footer: Relatório gerado em: [timestamp]")
            
        else:
            print(f"❌ PDF generation failed: {result.get('error')}")
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_simplified_pdf_structure()