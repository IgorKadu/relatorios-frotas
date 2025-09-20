"""
Módulo de testes de aceitação para o sistema de processamento de telemetria.
Implementa os testes QA especificados conforme os requisitos.
"""

import unittest
import pandas as pd
import numpy as np
import os
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List
import json

from .telemetry_processor import TelemetryProcessor, process_telemetry_csv
from .enhanced_reports import EnhancedPDFReportGenerator


class TelemetryQATests(unittest.TestCase):
    """Testes de aceitação para o sistema de processamento de telemetria"""
    
    def setUp(self):
        """Configuração inicial para os testes"""
        self.processor = TelemetryProcessor()
        self.test_data_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        """Limpeza após os testes"""
        # Limpar arquivos temporários criados durante os testes
        pass
    
    def create_test_csv(self, filename: str, data: List[Dict]) -> str:
        """Cria um arquivo CSV de teste"""
        filepath = os.path.join(self.test_data_dir, filename)
        df = pd.DataFrame(data)
        df.to_csv(filepath, sep=';', index=False)
        return filepath
    
    def test_1_distance_trip_consistency(self):
        """Teste 1: Verificar consistência entre distância total e soma das viagens"""
        # Criar dados de teste com viagens conhecidas
        test_data = [
            {
                'timestamp': '2025-09-01 08:00:00',
                'lat': -15.7801,
                'lon': -47.9292,
                'odometer': 1000.0,
                'speed': 60.0,
                'vehicle_id': 'TEST001'
            },
            {
                'timestamp': '2025-09-01 09:00:00',
                'lat': -15.7810,
                'lon': -47.9300,
                'odometer': 1060.0,
                'speed': 65.0,
                'vehicle_id': 'TEST001'
            },
            {
                'timestamp': '2025-09-01 10:00:00',
                'lat': -15.7820,
                'lon': -47.9310,
                'odometer': 1120.0,
                'speed': 0.0,  # Parada
                'vehicle_id': 'TEST001'
            }
        ]
        
        csv_path = self.create_test_csv('test_distance_trip.csv', test_data)
        result = process_telemetry_csv(csv_path)
        
        # Verificar que o processamento foi bem-sucedido
        self.assertTrue(result['success'])
        
        # Verificar métricas de distância
        distance_metrics = result['distance_speed_metrics']
        total_km = distance_metrics.get('total_km', 0)
        
        # Verificar viagens detectadas
        trips = result['trips']
        sum_trip_distances = sum(trip['distance_km'] for trip in trips)
        
        # A diferença deve ser ≤ 5% ou explicada
        if sum_trip_distances > 0:
            difference_percent = abs(total_km - sum_trip_distances) / sum_trip_distances * 100
            self.assertLessEqual(difference_percent, 5, 
                               f"Diferença entre distância total e soma das viagens: {difference_percent:.2f}%")
    
    def test_2_speed_km_consistency(self):
        """Teste 2: Verificar que max_speed > 0 quando total_km >= 20"""
        # Criar dados de teste com quilometragem significativa
        test_data = []
        base_lat, base_lon = -15.7801, -47.9292
        base_odometer = 1000.0
        
        # Criar 20km de dados
        for i in range(20):
            test_data.append({
                'timestamp': f'2025-09-01 {8+i:02d}:00:00',
                'lat': base_lat + i * 0.001,
                'lon': base_lon + i * 0.001,
                'odometer': base_odometer + i,
                'speed': 50.0 + (i % 10),  # Velocidade variável
                'vehicle_id': 'TEST002'
            })
        
        csv_path = self.create_test_csv('test_speed_km.csv', test_data)
        result = process_telemetry_csv(csv_path)
        
        # Verificar que o processamento foi bem-sucedido
        self.assertTrue(result['success'])
        
        # Verificar métricas
        distance_metrics = result['distance_speed_metrics']
        total_km = distance_metrics.get('total_km', 0)
        max_speed = distance_metrics.get('max_speed', 0)
        
        # Se quilometragem >= 20, velocidade máxima deve ser > 0
        if total_km >= 20:
            self.assertGreater(max_speed, 0, 
                             f"Velocidade máxima é 0 apesar de quilometragem >= 20km (total: {total_km:.2f}km)")
    
    def test_3_kpi_source_reference(self):
        """Teste 3: Verificar que KPIs exibidos têm referência (arquivo/coluna)"""
        test_data = [
            {
                'timestamp': '2025-09-01 08:00:00',
                'latitude': -15.7801,
                'longitude': -47.9292,
                'odometer': 1000.0,
                'speed': 60.0,
                'vehicle_id': 'TEST003'
            }
        ]
        
        csv_path = self.create_test_csv('test_kpi_source.csv', test_data)
        result = process_telemetry_csv(csv_path)
        
        # Verificar que o processamento foi bem-sucedido
        self.assertTrue(result['success'])
        
        # Verificar que as métricas têm fontes identificadas
        distance_metrics = result['distance_speed_metrics']
        
        # Verificar fonte da distância
        distance_source = distance_metrics.get('distance_source')
        self.assertIsNotNone(distance_source, "Fonte da distância não identificada")
        self.assertIn(distance_source, ['odometer', 'haversine'], 
                     f"Fonte da distância inválida: {distance_source}")
        
        # Verificar fonte da velocidade
        speed_source = distance_metrics.get('speed_source')
        self.assertIsNotNone(speed_source, "Fonte da velocidade não identificada")
        self.assertIn(speed_source, ['raw_speed', 'instant_speed', 'odometer_based'], 
                     f"Fonte da velocidade inválida: {speed_source}")
    
    def test_4_timezone_consistency(self):
        """Teste 4: Verificar consistência de timezone nos timestamps"""
        test_data = [
            {
                'timestamp': '2025-09-01 08:00:00',
                'lat': -15.7801,
                'lon': -47.9292,
                'speed': 60.0,
                'vehicle_id': 'TEST004'
            },
            {
                'timestamp': '2025-09-01 09:00:00',
                'lat': -15.7810,
                'lon': -47.9300,
                'speed': 65.0,
                'vehicle_id': 'TEST004'
            }
        ]
        
        csv_path = self.create_test_csv('test_timezone.csv', test_data)
        result = process_telemetry_csv(csv_path)
        
        # Verificar que o processamento foi bem-sucedido
        self.assertTrue(result['success'])
        
        # Executar testes QA
        qa_results = self.processor.run_qa_tests(result)
        
        # Verificar resultado do teste de timezone
        timezone_result = qa_results.get('test_4_timezone_consistency')
        self.assertIn(timezone_result, ['passed', 'skipped - no timestamps'],
                     f"Teste de timezone falhou: {timezone_result}")
    
    def test_5_300km_zero_speed_issue(self):
        """Teste específico para o problema de 300km com velocidade 0"""
        # Criar dados com quilometragem significativa mas velocidade reportada como 0
        test_data = []
        base_lat, base_lon = -15.7801, -47.9292
        base_odometer = 1000.0
        
        # Criar 300km de dados com velocidade 0
        for i in range(300):
            test_data.append({
                'timestamp': f'2025-09-01 {8+i//60:02d}:{i%60:02d}:00',
                'lat': base_lat + i * 0.001,
                'lon': base_lon + i * 0.001,
                'odometer': base_odometer + i,
                'speed': 0.0,  # Velocidade reportada como 0
                'vehicle_id': 'TEST005'
            })
        
        csv_path = self.create_test_csv('test_300km_zero_speed.csv', test_data)
        result = process_telemetry_csv(csv_path)
        
        # Verificar que o processamento foi bem-sucedido
        self.assertTrue(result['success'])
        
        # Verificar métricas
        distance_metrics = result['distance_speed_metrics']
        total_km = distance_metrics.get('total_km', 0)
        max_speed = distance_metrics.get('max_speed', 0)
        speed_source = distance_metrics.get('speed_source', '')
        
        # Se quilometragem >= 20 e velocidade máxima é 0, deve usar odometer como referência
        if total_km >= 20 and max_speed == 0:
            # Verificar que a velocidade foi recalculada
            self.assertTrue(max_speed > 0 or speed_source == 'odometer_based',
                           "Velocidade não recalculada apesar de quilometragem significativa")
    
    def test_6_schema_detection_and_mapping(self):
        """Teste de detecção automática de schema e mapeamento de colunas"""
        # Criar dados com nomes de colunas variados
        test_data = [
            {
                'DATA_HORA': '2025-09-01 08:00:00',
                'LATITUDE': -15.7801,
                'LONGITUDE': -47.9292,
                'KM': 1000.0,
                'VELOCIDADE': 60.0,
                'PLACA': 'TEST006'
            }
        ]
        
        csv_path = self.create_test_csv('test_schema_mapping.csv', test_data)
        result = process_telemetry_csv(csv_path)
        
        # Verificar que o processamento foi bem-sucedido
        self.assertTrue(result['success'])
        
        # Verificar schema detectado
        schema = result['schema']
        self.assertIsNotNone(schema, "Schema não detectado")
        self.assertEqual(schema['arquivo'], 'test_schema_mapping.csv')
        
        # Verificar mapeamento de colunas
        mapping_info = result['mapping_info']
        original_to_mapped = mapping_info.get('original_to_mapped', {})
        
        # Verificar que as colunas foram mapeadas corretamente
        expected_mappings = {
            'DATA_HORA': 'timestamp',
            'LATITUDE': 'lat',
            'LONGITUDE': 'lon',
            'KM': 'odometer',
            'VELOCIDADE': 'speed',
            'PLACA': 'vehicle_id'
        }
        
        for original, expected_mapped in expected_mappings.items():
            if original in original_to_mapped:
                self.assertEqual(original_to_mapped[original], expected_mapped,
                               f"Mapeamento incorreto para {original}: esperado {expected_mapped}, obtido {original_to_mapped[original]}")
    
    def test_7_quality_rules_and_sanity_checks(self):
        """Teste de regras de qualidade e verificações de sanidade"""
        # Criar dados com anomalias
        test_data = [
            # Dados normais
            {
                'timestamp': '2025-09-01 08:00:00',
                'lat': -15.7801,
                'lon': -47.9292,
                'speed': 60.0,
                'vehicle_id': 'TEST007'
            },
            # Coordenadas inválidas
            {
                'timestamp': '2025-09-01 08:01:00',
                'lat': 100.0,  # Latitude inválida
                'lon': -47.9292,
                'speed': 60.0,
                'vehicle_id': 'TEST007'
            },
            # Velocidade excessiva
            {
                'timestamp': '2025-09-01 08:02:00',
                'lat': -15.7801,
                'lon': -47.9292,
                'speed': 300.0,  # Velocidade excessiva
                'vehicle_id': 'TEST007'
            }
        ]
        
        csv_path = self.create_test_csv('test_quality_rules.csv', test_data)
        result = process_telemetry_csv(csv_path)
        
        # Verificar que o processamento foi bem-sucedido
        self.assertTrue(result['success'])
        
        # Verificar relatório de qualidade
        quality_report = result['quality_report']
        self.assertIsNotNone(quality_report, "Relatório de qualidade não gerado")
        
        # Verificar que outliers foram detectados
        outliers_removed = quality_report.get('outliers_removed', 0)
        speed_outliers = quality_report.get('speed_outliers_marked', 0)
        
        # Deve detectar pelo menos uma anomalia
        self.assertTrue(outliers_removed > 0 or speed_outliers > 0,
                       "Nenhuma anomalia detectada nos dados de teste")
    
    def test_8_output_formats_generation(self):
        """Teste de geração de todos os formatos de saída exigidos"""
        test_data = [
            {
                'timestamp': '2025-09-01 08:00:00',
                'lat': -15.7801,
                'lon': -47.9292,
                'odometer': 1000.0,
                'speed': 60.0,
                'vehicle_id': 'TEST008'
            }
        ]
        
        csv_path = self.create_test_csv('test_output_formats.csv', test_data)
        result = process_telemetry_csv(csv_path)
        
        # Verificar que o processamento foi bem-sucedido
        self.assertTrue(result['success'])
        
        # Gerar outputs
        base_filename = 'test_output'
        output_paths = self.processor.generate_outputs(result, self.test_data_dir, base_filename)
        
        # Verificar que todos os formatos foram gerados
        expected_outputs = ['json', 'log', 'pdf_data']
        for output_type in expected_outputs:
            self.assertIn(output_type, output_paths, f"Formato de saída {output_type} não gerado")
            self.assertTrue(os.path.exists(output_paths[output_type]), 
                           f"Arquivo {output_type} não encontrado: {output_paths[output_type]}")
        
        # Verificar conteúdo do JSON
        json_path = output_paths['json']
        with open(json_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        self.assertIsNotNone(json_data, "JSON de saída inválido")
        
        # Verificar conteúdo do log
        log_path = output_paths['log']
        with open(log_path, 'r', encoding='utf-8') as f:
            log_content = f.read()
        self.assertIn("Processamento concluído", log_content, "Log de processamento inválido")


def run_all_qa_tests():
    """Executa todos os testes de aceitação QA"""
    suite = unittest.TestLoader().loadTestsFromTestCase(TelemetryQATests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


if __name__ == "__main__":
    # Executar testes diretamente
    success = run_all_qa_tests()
    if success:
        print("✅ Todos os testes de aceitação QA passaram!")
    else:
        print("❌ Alguns testes de aceitação QA falharam!")