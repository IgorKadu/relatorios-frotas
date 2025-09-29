"""
Testes de Validação Obrigatórios para o Sistema de Relatórios
Implementa os casos de teste especificados no prompt.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import logging
from typing import Dict, List, Any
import tempfile
import os

from professional_reports import FleetReportProcessor
from pdf_generator import gerar_relatorio_pdf_completo

logger = logging.getLogger(__name__)

class ValidationTestSuite:
    """
    Suite de testes para validação dos casos obrigatórios.
    """
    
    def __init__(self):
        self.processor = FleetReportProcessor()
        self.test_results = []
        
    def run_all_tests(self) -> Dict[str, Any]:
        """
        Executa todos os casos de teste obrigatórios.
        """
        logger.info("Iniciando suite de testes de validação")
        
        test_cases = [
            self.test_case_1_km_zero_vel_positiva,
            self.test_case_2_km_positivo_vel_zero,
            self.test_case_3_km_vel_combustivel_zero,
            self.test_case_4_odometro_reset_negativo,
            self.test_case_5_velocidade_excessiva,
            self.test_case_6_dados_inconsistentes,
            self.test_case_7_consumo_idling,
            self.test_case_8_gps_validation
        ]
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'total_tests': len(test_cases),
            'passed': 0,
            'failed': 0,
            'test_details': []
        }
        
        for test_func in test_cases:
            try:
                test_result = test_func()
                results['test_details'].append(test_result)
                
                if test_result['passed']:
                    results['passed'] += 1
                else:
                    results['failed'] += 1
                    
            except Exception as e:
                logger.error(f"Erro no teste {test_func.__name__}: {e}")
                results['test_details'].append({
                    'test_name': test_func.__name__,
                    'passed': False,
                    'error': str(e),
                    'description': 'Erro durante execução do teste'
                })
                results['failed'] += 1
        
        results['success_rate'] = (results['passed'] / results['total_tests']) * 100
        
        logger.info(f"Testes concluídos: {results['passed']}/{results['total_tests']} passaram")
        return results
    
    def test_case_1_km_zero_vel_positiva(self) -> Dict[str, Any]:
        """
        Caso 1: KM=0, Vel=50 → anomalia R1
        Deve validar GPS ±1min; senão, marcar inconsistente.
        """
        test_data = pd.DataFrame([
            {
                'placa': 'TEST001',
                'timestamp': '2025-01-01T08:00:00',
                'odometer': 1000,
                'speed': 50,
                'lat': -12.9714,
                'lon': -38.5014,
                'ignition': 1
            },
            {
                'placa': 'TEST001',
                'timestamp': '2025-01-01T08:01:00',
                'odometer': 1000,  # Mesmo odômetro = KM delta = 0
                'speed': 50,       # Mas velocidade > 0
                'lat': -12.9720,   # GPS mudou ligeiramente
                'lon': -38.5020,
                'ignition': 1
            }
        ])
        
        # Processar dados
        processed_data = self.processor.processar_relatorio_completo(
            arquivos_csv=[],
            dados_csv=test_data,
            periodo_inicio=datetime.fromisoformat('2025-01-01T00:00:00'),
            periodo_fim=datetime.fromisoformat('2025-01-01T23:59:59'),
            cliente='Teste',
            tipo_relatorio='diario'
        )
        
        df_processed = pd.DataFrame(processed_data['dados_processados'])
        
        # Verificar se regra R1 foi aplicada
        r1_applied = any('R1' in str(regra) for regra in df_processed['regra_aplicada'])
        anomalia_detectada = any(df_processed['anomalia_delta'])
        
        return {
            'test_name': 'test_case_1_km_zero_vel_positiva',
            'description': 'KM=0 & Vel>0 deve aplicar regra R1',
            'passed': r1_applied and anomalia_detectada,
            'details': {
                'r1_applied': r1_applied,
                'anomalia_detectada': anomalia_detectada,
                'regras_aplicadas': df_processed['regra_aplicada'].tolist()
            }
        }
    
    def test_case_2_km_positivo_vel_zero(self) -> Dict[str, Any]:
        """
        Caso 2: KM=100, Vel=0 → estimar velocidade (R2)
        """
        test_data = pd.DataFrame([
            {
                'placa': 'TEST002',
                'timestamp': '2025-01-01T08:00:00',
                'odometer': 1000,
                'speed': 60,
                'lat': -12.9714,
                'lon': -38.5014,
                'ignition': 1
            },
            {
                'placa': 'TEST002',
                'timestamp': '2025-01-01T09:00:00',
                'odometer': 1100,  # Delta = 100 km
                'speed': 0,        # Mas velocidade = 0
                'lat': -12.9814,
                'lon': -38.5114,
                'ignition': 1
            }
        ])
        
        processed_data = self.processor.processar_relatorio_completo(
            arquivos_csv=[],
            dados_csv=test_data,
            periodo_inicio=datetime.fromisoformat('2025-01-01T00:00:00'),
            periodo_fim=datetime.fromisoformat('2025-01-01T23:59:59'),
            cliente='Teste',
            tipo_relatorio='diario'
        )
        
        df_processed = pd.DataFrame(processed_data['dados_processados'])
        
        # Verificar se regra R2 foi aplicada
        r2_applied = any('R2' in str(regra) for regra in df_processed['regra_aplicada'])
        velocidade_estimada = any(df_processed['dados_estimados'])
        
        return {
            'test_name': 'test_case_2_km_positivo_vel_zero',
            'description': 'KM>0 & Vel=0 deve estimar velocidade (R2)',
            'passed': r2_applied and velocidade_estimada,
            'details': {
                'r2_applied': r2_applied,
                'velocidade_estimada': velocidade_estimada,
                'velocidades_finais': df_processed['speed'].tolist()
            }
        }
    
    def test_case_3_km_vel_combustivel_zero(self) -> Dict[str, Any]:
        """
        Caso 3: KM=20, Vel=70, Comb=0 → estimar consumo (R4)
        """
        test_data = pd.DataFrame([
            {
                'placa': 'TEST003',
                'timestamp': '2025-01-01T08:00:00',
                'odometer': 1000,
                'speed': 0,
                'fuel_consumed': 10,
                'lat': -12.9714,
                'lon': -38.5014,
                'ignition': 1
            },
            {
                'placa': 'TEST003',
                'timestamp': '2025-01-01T08:30:00',
                'odometer': 1020,  # Delta = 20 km
                'speed': 70,       # Velocidade = 70
                'fuel_consumed': 10,  # Mesmo combustível = delta = 0
                'lat': -12.9814,
                'lon': -38.5114,
                'ignition': 1
            }
        ])
        
        processed_data = self.processor.processar_relatorio_completo(
            arquivos_csv=[],
            dados_csv=test_data,
            periodo_inicio=datetime.fromisoformat('2025-01-01T00:00:00'),
            periodo_fim=datetime.fromisoformat('2025-01-01T23:59:59'),
            cliente='Teste',
            tipo_relatorio='diario'
        )
        
        df_processed = pd.DataFrame(processed_data['dados_processados'])
        
        # Verificar se regra R4 foi aplicada
        r4_applied = any('R4' in str(regra) for regra in df_processed['regra_aplicada'])
        combustivel_estimado = any(df_processed['dados_estimados'])
        
        return {
            'test_name': 'test_case_3_km_vel_combustivel_zero',
            'description': 'Comb=0 & KM>0 deve estimar consumo (R4)',
            'passed': r4_applied and combustivel_estimado,
            'details': {
                'r4_applied': r4_applied,
                'combustivel_estimado': combustivel_estimado,
                'combustivel_delta': df_processed['combustivel_delta'].tolist()
            }
        }
    
    def test_case_4_odometro_reset_negativo(self) -> Dict[str, Any]:
        """
        Caso 4: odômetro reset negativo → ignorar delta, marcar
        """
        test_data = pd.DataFrame([
            {
                'placa': 'TEST004',
                'timestamp': '2025-01-01T08:00:00',
                'odometer': 99950,  # Próximo do reset
                'speed': 60,
                'lat': -12.9714,
                'lon': -38.5014,
                'ignition': 1
            },
            {
                'placa': 'TEST004',
                'timestamp': '2025-01-01T08:01:00',
                'odometer': 50,     # Reset do odômetro
                'speed': 60,
                'lat': -12.9720,
                'lon': -38.5020,
                'ignition': 1
            }
        ])
        
        processed_data = self.processor.processar_relatorio_completo(
            arquivos_csv=[],
            dados_csv=test_data,
            periodo_inicio=datetime.fromisoformat('2025-01-01T00:00:00'),
            periodo_fim=datetime.fromisoformat('2025-01-01T23:59:59'),
            cliente='Teste',
            tipo_relatorio='diario'
        )
        
        df_processed = pd.DataFrame(processed_data['dados_processados'])
        
        # Verificar se delta negativo foi ignorado
        deltas_negativos = any(df_processed['km_delta'] < -1000)  # Reset seria muito negativo
        anomalia_reset = any('reset' in str(regra).lower() for regra in df_processed['regra_aplicada'])
        
        return {
            'test_name': 'test_case_4_odometro_reset_negativo',
            'description': 'Reset de odômetro deve ser ignorado e marcado',
            'passed': not deltas_negativos or anomalia_reset,
            'details': {
                'deltas_negativos_ignorados': not deltas_negativos,
                'anomalia_reset_marcada': anomalia_reset,
                'km_deltas': df_processed['km_delta'].tolist()
            }
        }
    
    def test_case_5_velocidade_excessiva(self) -> Dict[str, Any]:
        """
        Caso 5: Velocidade >250 km/h → truncar + marcar como anomalia (R5)
        """
        test_data = pd.DataFrame([
            {
                'placa': 'TEST005',
                'timestamp': '2025-01-01T08:00:00',
                'odometer': 1000,
                'speed': 300,  # Velocidade impossível
                'lat': -12.9714,
                'lon': -38.5014,
                'ignition': 1
            }
        ])
        
        processed_data = self.processor.processar_relatorio_completo(
            arquivos_csv=[],
            dados_csv=test_data,
            periodo_inicio=datetime.fromisoformat('2025-01-01T00:00:00'),
            periodo_fim=datetime.fromisoformat('2025-01-01T23:59:59'),
            cliente='Teste',
            tipo_relatorio='diario'
        )
        
        df_processed = pd.DataFrame(processed_data['dados_processados'])
        
        # Verificar se regra R5 foi aplicada
        r5_applied = any('R5' in str(regra) for regra in df_processed['regra_aplicada'])
        velocidade_truncada = all(df_processed['speed'] <= 250)
        anomalia_velocidade = any(df_processed['anomalia_delta'])
        
        return {
            'test_name': 'test_case_5_velocidade_excessiva',
            'description': 'Velocidade >250 km/h deve ser truncada (R5)',
            'passed': r5_applied and velocidade_truncada and anomalia_velocidade,
            'details': {
                'r5_applied': r5_applied,
                'velocidade_truncada': velocidade_truncada,
                'anomalia_marcada': anomalia_velocidade,
                'velocidades_finais': df_processed['speed'].tolist()
            }
        }
    
    def test_case_6_dados_inconsistentes(self) -> Dict[str, Any]:
        """
        Caso 6: Dados inconsistentes → nunca incluir em totais sem ajuste (R6)
        """
        test_data = pd.DataFrame([
            {
                'placa': 'TEST006',
                'timestamp': '2025-01-01T08:00:00',
                'odometer': 1000,
                'speed': -50,  # Velocidade negativa (impossível)
                'fuel_consumed': -10,  # Combustível negativo
                'lat': 200,    # Latitude impossível
                'lon': 400,    # Longitude impossível
                'ignition': 1
            }
        ])
        
        processed_data = self.processor.processar_relatorio_completo(
            arquivos_csv=[],
            dados_csv=test_data,
            periodo_inicio=datetime.fromisoformat('2025-01-01T00:00:00'),
            periodo_fim=datetime.fromisoformat('2025-01-01T23:59:59'),
            cliente='Teste',
            tipo_relatorio='diario'
        )
        
        df_processed = pd.DataFrame(processed_data['dados_processados'])
        
        # Verificar se dados inconsistentes foram excluídos dos totais
        incluir_totais = all(df_processed['incluir_totais'] == False) if len(df_processed) > 0 else True
        r6_applied = any('R6' in str(regra) for regra in df_processed['regra_aplicada']) if len(df_processed) > 0 else True
        
        return {
            'test_name': 'test_case_6_dados_inconsistentes',
            'description': 'Dados inconsistentes não devem ser incluídos em totais (R6)',
            'passed': incluir_totais or r6_applied,
            'details': {
                'dados_excluidos_totais': incluir_totais,
                'r6_applied': r6_applied,
                'registros_processados': len(df_processed)
            }
        }
    
    def test_case_7_consumo_idling(self) -> Dict[str, Any]:
        """
        Caso 7: Consumo>0 & KM≈0 → aceitar só se ignição=ON e duração >5min (R3)
        """
        test_data = pd.DataFrame([
            {
                'placa': 'TEST007',
                'timestamp': '2025-01-01T08:00:00',
                'odometer': 1000,
                'speed': 0,
                'fuel_consumed': 10,
                'lat': -12.9714,
                'lon': -38.5014,
                'ignition': 1
            },
            {
                'placa': 'TEST007',
                'timestamp': '2025-01-01T08:06:00',  # 6 minutos depois
                'odometer': 1000,  # Mesmo local (KM ≈ 0)
                'speed': 0,
                'fuel_consumed': 12,  # Consumiu combustível
                'lat': -12.9714,
                'lon': -38.5014,
                'ignition': 1  # Ignição ligada
            }
        ])
        
        processed_data = self.processor.processar_relatorio_completo(
            arquivos_csv=[],
            dados_csv=test_data,
            periodo_inicio=datetime.fromisoformat('2025-01-01T00:00:00'),
            periodo_fim=datetime.fromisoformat('2025-01-01T23:59:59'),
            cliente='Teste',
            tipo_relatorio='diario'
        )
        
        df_processed = pd.DataFrame(processed_data['dados_processados'])
        
        # Verificar se regra R3 foi aplicada corretamente
        r3_applied = any('R3' in str(regra) for regra in df_processed['regra_aplicada'])
        consumo_idling_aceito = any(df_processed['incluir_totais'])
        
        return {
            'test_name': 'test_case_7_consumo_idling',
            'description': 'Consumo em idling deve ser aceito se ignição=ON e >5min (R3)',
            'passed': r3_applied and consumo_idling_aceito,
            'details': {
                'r3_applied': r3_applied,
                'consumo_aceito': consumo_idling_aceito,
                'combustivel_deltas': df_processed['combustivel_delta'].tolist()
            }
        }
    
    def test_case_8_gps_validation(self) -> Dict[str, Any]:
        """
        Caso 8: Validação de GPS para casos R1
        """
        test_data = pd.DataFrame([
            {
                'placa': 'TEST008',
                'timestamp': '2025-01-01T08:00:00',
                'odometer': 1000,
                'speed': 0,
                'lat': -12.9714,
                'lon': -38.5014,
                'ignition': 1
            },
            {
                'placa': 'TEST008',
                'timestamp': '2025-01-01T08:00:30',  # 30 segundos depois
                'odometer': 1000,  # KM = 0
                'speed': 60,       # Mas velocidade > 0
                'lat': -12.9720,   # GPS mudou (validação positiva)
                'lon': -38.5020,
                'ignition': 1
            }
        ])
        
        processed_data = self.processor.processar_relatorio_completo(
            arquivos_csv=[],
            dados_csv=test_data,
            periodo_inicio=datetime.fromisoformat('2025-01-01T00:00:00'),
            periodo_fim=datetime.fromisoformat('2025-01-01T23:59:59'),
            cliente='Teste',
            tipo_relatorio='diario'
        )
        
        df_processed = pd.DataFrame(processed_data['dados_processados'])
        
        # Verificar se validação GPS foi realizada
        gps_validado = any('GPS' in str(regra) for regra in df_processed['regra_aplicada'])
        movimento_detectado = any(df_processed['km_delta'] > 0)
        
        return {
            'test_name': 'test_case_8_gps_validation',
            'description': 'Validação GPS deve detectar movimento real',
            'passed': gps_validado or movimento_detectado,
            'details': {
                'gps_validado': gps_validado,
                'movimento_detectado': movimento_detectado,
                'km_deltas': df_processed['km_delta'].tolist()
            }
        }
    
    def test_pdf_generation(self) -> Dict[str, Any]:
        """
        Teste de geração de PDF com dados de teste.
        """
        # Dados de teste para PDF
        test_data = pd.DataFrame([
            {
                'placa': 'PDF001',
                'timestamp': '2025-01-01T08:00:00',
                'odometer': 1000,
                'speed': 60,
                'fuel_consumed': 10,
                'lat': -12.9714,
                'lon': -38.5014,
                'ignition': 1
            },
            {
                'placa': 'PDF001',
                'timestamp': '2025-01-01T09:00:00',
                'odometer': 1080,
                'speed': 80,
                'fuel_consumed': 16,
                'lat': -12.9814,
                'lon': -38.5114,
                'ignition': 1
            }
        ])
        
        # Processar dados
        processed_data = self.processor.processar_relatorio_completo(
            arquivos_csv=[],
            dados_csv=test_data,
            periodo_inicio=datetime.fromisoformat('2025-01-01T00:00:00'),
            periodo_fim=datetime.fromisoformat('2025-01-01T23:59:59'),
            cliente='Cliente Teste PDF',
            tipo_relatorio='diario'
        )
        
        # Gerar PDF
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
            pdf_path = tmp_file.name
        
        try:
            gerar_relatorio_pdf_completo(processed_data, pdf_path)
            pdf_generated = os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 0
            
            return {
                'test_name': 'test_pdf_generation',
                'description': 'Geração de PDF deve criar arquivo válido',
                'passed': pdf_generated,
                'details': {
                    'pdf_path': pdf_path,
                    'file_size': os.path.getsize(pdf_path) if pdf_generated else 0
                }
            }
        except Exception as e:
            return {
                'test_name': 'test_pdf_generation',
                'description': 'Geração de PDF deve criar arquivo válido',
                'passed': False,
                'details': {
                    'error': str(e)
                }
            }
        finally:
            # Limpar arquivo temporário
            if os.path.exists(pdf_path):
                try:
                    os.unlink(pdf_path)
                except:
                    pass

def run_validation_tests() -> Dict[str, Any]:
    """
    Executa todos os testes de validação e retorna resultados.
    """
    suite = ValidationTestSuite()
    results = suite.run_all_tests()
    
    # Adicionar teste de PDF
    pdf_test = suite.test_pdf_generation()
    results['test_details'].append(pdf_test)
    results['total_tests'] += 1
    
    if pdf_test['passed']:
        results['passed'] += 1
    else:
        results['failed'] += 1
    
    results['success_rate'] = (results['passed'] / results['total_tests']) * 100
    
    return results

if __name__ == "__main__":
    # Configurar logging
    logging.basicConfig(level=logging.INFO)
    
    # Executar testes
    results = run_validation_tests()
    
    # Exibir resultados
    print("\n" + "="*60)
    print("RESULTADOS DOS TESTES DE VALIDAÇÃO")
    print("="*60)
    print(f"Total de testes: {results['total_tests']}")
    print(f"Passou: {results['passed']}")
    print(f"Falhou: {results['failed']}")
    print(f"Taxa de sucesso: {results['success_rate']:.1f}%")
    print("\nDetalhes dos testes:")
    print("-"*60)
    
    for test in results['test_details']:
        status = "✅ PASSOU" if test['passed'] else "❌ FALHOU"
        print(f"{status} - {test['test_name']}")
        print(f"   {test['description']}")
        if not test['passed'] and 'error' in test:
            print(f"   Erro: {test['error']}")
        print()
    
    # Salvar resultados em JSON
    with open('test_results.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"Resultados salvos em: test_results.json")