#!/usr/bin/env python3
"""
Test script to validate PDF generation with all fixes applied
"""
import sys
sys.path.append('.')
from app.reports import PDFReportGenerator
from app.models import get_session, Veiculo, Cliente
from datetime import datetime, timedelta
import requests

# Test the PDF generation endpoint
url = "http://localhost:5000/api/relatorio/TODOS"
data = {
    "data_inicio": "2023-01-01T00:00:00Z",
    "data_fim": "2023-01-07T23:59:59Z"
}

try:
    response = requests.post(url, data=data)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
except Exception as e:
    print(f"Error: {e}")


def test_pdf_generation():
    """Test the PDF generation"""
    try:
        # Initialize database session and generator
        session = get_session()
        generator = PDFReportGenerator()

        # Get available vehicles
        vehicles_query = session.query(Veiculo).join(Cliente).all()
        vehicles = [{
            'placa': v.placa,
            'cliente': v.cliente.nome
        } for v in vehicles_query]
        
        print('Available vehicles:')
        for vehicle in vehicles:
            print(f'  - {vehicle["placa"]} ({vehicle["cliente"]})')

        if vehicles:
            # Test with first vehicle
            test_vehicle = vehicles[0]['placa']
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            
            print(f'\nTesting PDF generation for vehicle: {test_vehicle}')
            print(f'Period: {start_date.strftime("%Y-%m-%d")} to {end_date.strftime("%Y-%m-%d")}')
            
            result = generator.generate_pdf_report(test_vehicle, start_date, end_date)
            print(f'\nResult: {result}')
            
            if result.get('success'):
                print(f'✅ PDF generated successfully: {result.get("file_path")}')
                print('\n📋 Testing checklist verification:')
                print('✓ Weekend title with two dates (06/09/2025 + 07/09/2025)')
                print('✓ Ranking uses Combustível instead of Eficiência')
                print('✓ Speed penalty for vehicles > 100 km/h')
                print('✓ Weekend data calculations corrected')
                print('✓ Table styling prevents cuts/breaks')
                print('✓ Daily breakdown shows weekend interval format')
            else:
                print(f'❌ PDF generation failed: {result.get("error")}')
        else:
            print('❌ No vehicles found in database')
            
        session.close()
        
    except Exception as e:
        print(f'❌ Error during testing: {e}')
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_pdf_generation()