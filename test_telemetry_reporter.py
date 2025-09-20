#!/usr/bin/env python3
"""
Script para testar o sistema de geração de relatórios de telemetria veicular
"""

import sys
import os
from datetime import datetime

# Adicionar o diretório app ao path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from app.telemetry_reporter import TelemetryReporter


def create_test_csv():
    """Cria um arquivo CSV de teste"""
    test_data = """timestamp;lat;lon;odometer;speed;vehicle_id;client_id
2025-09-01 08:00:00;-15.7801;-47.9292;1000.0;60.0;TEST001;ClientA
2025-09-01 08:30:00;-15.7810;-47.9300;1030.0;65.0;TEST001;ClientA
2025-09-01 09:00:00;-15.7820;-47.9310;1060.0;70.0;TEST001;ClientA
2025-09-01 09:30:00;-15.7830;-47.9320;1090.0;68.0;TEST001;ClientA
2025-09-01 10:00:00;-15.7840;-47.9330;1120.0;0.0;TEST001;ClientA
2025-09-01 10:30:00;-15.7850;-47.9340;1150.0;72.0;TEST001;ClientA
2025-09-01 11:00:00;-15.7860;-47.9350;1180.0;75.0;TEST001;ClientA
2025-09-01 11:30:00;-15.7870;-47.9360;1210.0;78.0;TEST001;ClientA
2025-09-01 12:00:00;-15.7880;-47.9370;1240.0;80.0;TEST001;ClientA
2025-09-01 12:30:00;-15.7890;-47.9380;1270.0;82.0;TEST001;ClientA
2025-09-01 13:00:00;-15.7900;-47.9390;1300.0;85.0;TEST001;ClientA
2025-09-01 14:00:00;-15.7910;-47.9400;1350.0;90.0;TEST001;ClientA
2025-09-01 15:00:00;-15.7920;-47.9410;1400.0;95.0;TEST001;ClientA
2025-09-01 16:00:00;-15.7930;-47.9420;1450.0;100.0;TEST001;ClientA
2025-09-01 17:00:00;-15.7940;-47.9430;1500.0;105.0;TEST001;ClientA
2025-09-01 18:00:00;-15.7950;-47.9440;1550.0;110.0;TEST001;ClientA
2025-09-01 19:00:00;-15.7960;-47.9450;1600.0;115.0;TEST001;ClientA
2025-09-01 20:00:00;-15.7970;-47.9460;1650.0;120.0;TEST001;ClientA
2025-09-01 21:00:00;-15.7980;-47.9470;1700.0;125.0;TEST001;ClientA
2025-09-01 22:00:00;-15.7990;-47.9480;1750.0;130.0;TEST001;ClientA
2025-09-02 08:00:00;-15.8000;-47.9490;1800.0;60.0;TEST001;ClientA
2025-09-02 09:00:00;-15.8010;-47.9500;1850.0;65.0;TEST001;ClientA
2025-09-02 10:00:00;-15.8020;-47.9510;1900.0;70.0;TEST001;ClientA
2025-09-02 11:00:00;-15.8030;-47.9520;1950.0;75.0;TEST001;ClientA
2025-09-02 12:00:00;-15.8040;-47.9530;2000.0;80.0;TEST001;ClientA
2025-09-02 13:00:00;-15.8050;-47.9540;2050.0;85.0;TEST001;ClientA
2025-09-02 14:00:00;-15.8060;-47.9550;2100.0;90.0;TEST001;ClientA
2025-09-02 15:00:00;-15.8070;-47.9560;2150.0;95.0;TEST001;ClientA
2025-09-02 16:00:00;-15.8080;-47.9570;2200.0;100.0;TEST001;ClientA
2025-09-02 17:00:00;-15.8090;-47.9580;2250.0;105.0;TEST001;ClientA
"""
    
    csv_path = os.path.join(os.path.dirname(__file__), 'data', 'test_reporter.csv')
    with open(csv_path, 'w', encoding='utf-8') as f:
        f.write(test_data)
    
    return csv_path


def main():
    print("🚀 Testando Sistema de Relatórios de Telemetria Veicular")
    print("=" * 60)
    
    # Criar arquivo CSV de teste
    csv_file_path = create_test_csv()
    print(f"📄 Arquivo de teste criado: {csv_file_path}")
    
    # Configurar parâmetros de teste
    start_date = datetime(2025, 9, 1)
    end_date = datetime(2025, 9, 2)
    output_dir = os.path.join(os.path.dirname(__file__), 'reports')
    client_name = "Cliente de Teste"
    
    print(f"📅 Período de teste: {start_date.strftime('%d/%m/%Y')} a {end_date.strftime('%d/%m/%Y')}")
    print(f"📂 Diretório de saída: {output_dir}")
    print()
    
    # Inicializar o sistema de relatórios
    reporter = TelemetryReporter()
    
    # Gerar relatório
    result = reporter.generate_report_from_csv(
        csv_file_path, output_dir, start_date, end_date, "Todos", client_name
    )
    
    if result['success']:
        print("✅ Relatório gerado com sucesso!")
        print()
        print("📤 Arquivos gerados:")
        for output_type, path in result['outputs'].items():
            print(f"   • {output_type}: {path}")
        
        # Exibir informações do relatório
        print()
        print("📊 Informações do relatório:")
        print(f"   • Estrutura: {result['report_structure']}")
        print(f"   • Período: {result['period_info']['days_count']} dias")
        
        processing_result = result['processing_result']
        distance_metrics = processing_result.get('distance_speed_metrics', {})
        print(f"   • Quilometragem total: {distance_metrics.get('total_km', 0):.2f} km")
        print(f"   • Velocidade máxima: {distance_metrics.get('max_speed', 0):.2f} km/h")
        
        # Exibir problemas de coerência se houver
        validation_results = result['validation_results']
        if validation_results.get('coherence_issues'):
            print()
            print("⚠️  Problemas de coerência identificados:")
            for issue in validation_results['coherence_issues']:
                print(f"   • {issue}")
        else:
            print()
            print("✅ Nenhum problema de coerência identificado")
            
    else:
        print(f"❌ Erro ao gerar relatório: {result['error']}")


if __name__ == "__main__":
    main()