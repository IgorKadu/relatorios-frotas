import datetime
import os
from app.reports import PDFReportGenerator

# Create sample metrics data similar to what would come from the database
sample_metrics = {
    'veiculo': {
        'placa': 'TST-1234',
        'cliente': 'Test Client',
        'periodo_analise': {
            'inicio': datetime.datetime(2025, 9, 1),
            'fim': datetime.datetime(2025, 9, 7),
            'total_dias': 7
        }
    },
    'operacao': {
        'total_registros': 1250,
        'km_total': 850.5,
        'velocidade_maxima': 95.0,
        'velocidade_media': 42.3,
        'tempo_total_ligado': 45,
        'tempo_em_movimento': 38,
        'tempo_parado_ligado': 7,
        'tempo_desligado': 150
    },
    'periodos': {
        'operacional_manha': 5,
        'operacional_meio_dia': 8,
        'operacional_tarde': 6,
        'fora_horario_manha': 3,
        'fora_horario_tarde': 4,
        'fora_horario_noite': 2,
        'final_semana': 1,
        'total_operacional': 19,
        'total_fora_horario': 9
    },
    'combustivel': {
        'fuel_consumed_liters': 72.3,
        'efficiency_kmL': 11.8,
        'km_traveled': 850.5,
        'avg_speed': 42.3
    }
}

# Create a PDF generator
generator = PDFReportGenerator()

# Ensure reports directory exists
os.makedirs('reports', exist_ok=True)

# Generate a sample PDF
output_path = 'reports/test_report.pdf'
success = generator.generate_pdf(sample_metrics, output_path, "weekly")

if success:
    print(f"PDF generated successfully at: {output_path}")
    print(f"File size: {os.path.getsize(output_path)} bytes")
else:
    print("Failed to generate PDF")