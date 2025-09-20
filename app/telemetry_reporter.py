"""
Módulo principal para geração de relatórios de telemetria veicular em PDF com validação de dados.
Implementa todas as regras especificadas para filtragem, cálculo, validação e apresentação coerente dos dados.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union
import pandas as pd
import numpy as np
from io import BytesIO
import matplotlib.pyplot as plt
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, 
    PageBreak, Image, KeepTogether
)
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT, TA_JUSTIFY

from .telemetry_processor import TelemetryProcessor, process_telemetry_csv, convert_numpy_types
from .enhanced_reports import EnhancedPDFReportGenerator


class TelemetryReporter:
    """Classe principal para geração de relatórios de telemetria veicular"""
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Inicializa o sistema de relatórios de telemetria
        
        Args:
            config: Dicionário com configurações do sistema
        """
        self.config = config or {}
        self.processor = TelemetryProcessor(self.config)
        self.report_generator = EnhancedPDFReportGenerator()
        
        # Configurações padrão para validação de dados
        self.speed_outlier_threshold = self.config.get('speed_outlier_threshold', 220)
        self.gps_jump_distance_km = self.config.get('gps_jump_distance_km', 500)
        
    def validate_data_coherence(self, processing_result: Dict) -> Dict:
        """
        Valida a coerência dos dados conforme as regras especificadas
        
        Args:
            processing_result: Resultado do processamento de telemetria
            
        Returns:
            Dicionário com informações de validação
        """
        validation_results = {
            'coherence_issues': [],
            'corrections_made': [],
            'data_quality': 'good'
        }
        
        distance_metrics = processing_result.get('distance_speed_metrics', {})
        total_km = distance_metrics.get('total_km', 0)
        max_speed = distance_metrics.get('max_speed', 0)
        
        # Regra 1: Se km_total > 0 então velocidade_max deve ser > 0
        if total_km > 0 and max_speed <= 0:
            validation_results['coherence_issues'].append(
                f"Contradição: km_total > 0 ({total_km:.2f} km) mas velocidade_max = 0"
            )
            validation_results['data_quality'] = 'poor'
            
        # Regra 2: Se velocidade_max > 0 então km_total deve ser > 0
        if max_speed > 0 and total_km <= 0:
            validation_results['coherence_issues'].append(
                f"Contradição: velocidade_max > 0 ({max_speed:.2f} km/h) mas km_total = 0"
            )
            validation_results['data_quality'] = 'poor'
            
        # Regra 3: Verificar se há sensores inconsistentes
        if distance_metrics.get('sensor_issue', False):
            validation_results['coherence_issues'].append(
                "Sensor inconsistente detectado"
            )
            validation_results['data_quality'] = 'poor'
            
        return validation_results
    
    def filter_data_by_period(self, df: pd.DataFrame, start_date: datetime, 
                            end_date: datetime) -> pd.DataFrame:
        """
        Filtra os dados pelo período especificado (inclusivo)
        
        Args:
            df: DataFrame com dados de telemetria
            start_date: Data inicial (inclusiva)
            end_date: Data final (inclusiva)
            
        Returns:
            DataFrame filtrado
        """
        if 'timestamp' not in df.columns:
            return df
            
        # Converter timestamps
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Calcular o número correto de dias (inclusivo)
        days_count = (end_date - start_date).days + 1
        
        # Filtrar dados dentro do período
        mask = (df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)
        filtered_df = df[mask].copy()
        
        return filtered_df
    
    def determine_report_structure(self, start_date: datetime, end_date: datetime, 
                                vehicle_count: int) -> str:
        """
        Determina a estrutura do relatório com base no período e número de veículos
        
        Args:
            start_date: Data inicial
            end_date: Data final
            vehicle_count: Número de veículos
            
        Returns:
            Tipo de estrutura ('detailed' ou 'summary')
        """
        # Calcular dias corretamente (inclusivo)
        days_count = (end_date - start_date).days + 1
        
        # Estrutura detalhada para ≤ 7 dias E ≤ 5 veículos
        if days_count <= 7 and vehicle_count <= 5:
            return 'detailed'
        # Estrutura resumida para períodos maiores
        else:
            return 'summary'
    
    def generate_report_from_csv(self, csv_file_path: str, output_dir: str,
                               start_date: datetime, end_date: datetime,
                               vehicles: Union[str, List[str]] = "Todos",
                               client_name: Optional[str] = None) -> Dict:
        """
        Gera relatório completo a partir de arquivo CSV com todas as validações
        
        Args:
            csv_file_path: Caminho para o arquivo CSV de telemetria
            output_dir: Diretório de saída para os arquivos gerados
            start_date: Data inicial do período (inclusiva)
            end_date: Data final do período (inclusiva)
            vehicles: Lista de veículos ou "Todos"
            client_name: Nome do cliente (opcional)
            
        Returns:
            Dicionário com informações sobre os arquivos gerados
        """
        try:
            # Criar diretório de saída se não existir
            os.makedirs(output_dir, exist_ok=True)
            
            # Extrair nome base do arquivo
            base_filename = os.path.splitext(os.path.basename(csv_file_path))[0]
            
            # 1. Processar o arquivo CSV
            print(f"📊 Processando arquivo: {csv_file_path}")
            processing_result = process_telemetry_csv(csv_file_path, self.config)
            
            if not processing_result.get('success', False):
                raise Exception(f"Falha no processamento: {processing_result.get('error', 'Erro desconhecido')}")
            
            print("✅ Processamento concluído com sucesso!")
            
            # 2. Filtrar dados pelo período
            processed_df = pd.DataFrame(processing_result.get('processed_data', []))
            if not processed_df.empty:
                filtered_df = self.filter_data_by_period(processed_df, start_date, end_date)
                processing_result['processed_data'] = filtered_df.to_dict('records')
                print(f"📅 Dados filtrados para período: {start_date.strftime('%d/%m/%Y')} a {end_date.strftime('%d/%m/%Y')} ({(end_date - start_date).days + 1} dias)")
            
            # 3. Validar coerência dos dados
            print("🔍 Validando coerência dos dados...")
            validation_results = self.validate_data_coherence(processing_result)
            
            if validation_results['coherence_issues']:
                print("⚠️  Problemas de coerência encontrados:")
                for issue in validation_results['coherence_issues']:
                    print(f"   • {issue}")
            
            # 4. Determinar estrutura do relatório
            vehicle_count = len(set(record.get('vehicle_id', '') for record in processing_result.get('processed_data', [])))
            report_structure = self.determine_report_structure(start_date, end_date, vehicle_count)
            print(f"📋 Estrutura do relatório: {report_structure} ({vehicle_count} veículos, {(end_date - start_date).days + 1} dias)")
            
            # 5. Executar testes de QA
            print("🧪 Executando testes de qualidade...")
            qa_results = self.processor.run_qa_tests(processing_result)
            print("✅ Testes de qualidade concluídos!")
            
            # 6. Gerar todos os outputs exigidos
            print("📁 Gerando outputs...")
            
            # Gerar outputs adicionais (JSON, CSV de anomalias, logs)
            additional_outputs = self.processor.generate_outputs(
                processing_result, output_dir, base_filename
            )
            
            # Gerar relatório PDF aprimorado
            pdf_path = os.path.join(output_dir, f"Relatorio_{base_filename}.pdf")
            pdf_success = self.report_generator.create_enhanced_pdf_report(
                processing_result, qa_results, pdf_path, client_name
            )
            
            if pdf_success:
                additional_outputs['pdf'] = pdf_path
                print(f"✅ Relatório PDF gerado: {pdf_path}")
            else:
                print("❌ Falha ao gerar relatório PDF")
            
            # 7. Retornar informações completas
            result = {
                'success': True,
                'processing_result': processing_result,
                'validation_results': validation_results,
                'qa_results': qa_results,
                'outputs': additional_outputs,
                'report_structure': report_structure,
                'period_info': {
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat(),
                    'days_count': (end_date - start_date).days + 1
                },
                'message': 'Processamento e geração de relatórios concluídos com sucesso'
            }
            
            return result
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': f'Falha no processamento: {str(e)}'
            }
    
    def generate_detailed_report_content(self, processing_result: Dict, 
                                       validation_results: Dict, 
                                       qa_results: Dict) -> Dict:
        """
        Gera conteúdo detalhado para relatórios de curto período
        
        Args:
            processing_result: Resultados do processamento
            validation_results: Resultados da validação
            qa_results: Resultados dos testes QA
            
        Returns:
            Dicionário com conteúdo detalhado do relatório
        """
        content = {
            'executive_summary': self._generate_executive_summary(processing_result, validation_results),
            'daily_breakdown': self._generate_daily_breakdown(processing_result),
            'performance_ranking': self._generate_performance_ranking(processing_result),
            'inconsistencies': self._generate_inconsistencies_report(validation_results, qa_results),
            'recommendations': self._generate_recommendations(processing_result, validation_results)
        }
        
        return content
    
    def generate_summary_report_content(self, processing_result: Dict,
                                      validation_results: Dict,
                                      qa_results: Dict) -> Dict:
        """
        Gera conteúdo resumido para relatórios de longo período
        
        Args:
            processing_result: Resultados do processamento
            validation_results: Resultados da validação
            qa_results: Resultados dos testes QA
            
        Returns:
            Dicionário com conteúdo resumido do relatório
        """
        content = {
            'executive_summary': self._generate_executive_summary(processing_result, validation_results),
            'period_summary': self._generate_period_summary(processing_result),
            'trends': self._generate_trends_analysis(processing_result),
            'inconsistencies': self._generate_inconsistencies_report(validation_results, qa_results),
            'recommendations': self._generate_recommendations(processing_result, validation_results)
        }
        
        return content
    
    def _generate_executive_summary(self, processing_result: Dict, validation_results: Dict) -> Dict:
        """Gera resumo executivo"""
        distance_metrics = processing_result.get('distance_speed_metrics', {})
        trips = processing_result.get('trips', [])
        
        return {
            'total_distance_km': distance_metrics.get('total_km', 0),
            'max_speed_kmh': distance_metrics.get('max_speed', 0),
            'total_trips': len(trips),
            'data_quality': validation_results.get('data_quality', 'unknown'),
            'coherence_issues_count': len(validation_results.get('coherence_issues', []))
        }
    
    def _generate_daily_breakdown(self, processing_result: Dict) -> List[Dict]:
        """Gera detalhamento diário"""
        processed_data = processing_result.get('processed_data', [])
        if not processed_data:
            return []
        
        # Converter para DataFrame para facilitar análise
        df = pd.DataFrame(processed_data)
        if 'timestamp' not in df.columns:
            return []
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['date'] = df['timestamp'].dt.date
        
        # Agrupar por data
        daily_data = []
        for date, group in df.groupby('date'):
            daily_metrics = {
                'date': date.isoformat(),
                'total_distance_km': group['odometer'].max() - group['odometer'].min() if 'odometer' in group.columns else 0,
                'max_speed_kmh': group['speed'].max() if 'speed' in group.columns else 0,
                'record_count': len(group)
            }
            daily_data.append(daily_metrics)
        
        return daily_data
    
    def _generate_performance_ranking(self, processing_result: Dict) -> List[Dict]:
        """Gera ranking de desempenho"""
        processed_data = processing_result.get('processed_data', [])
        if not processed_data:
            return []
        
        # Converter para DataFrame
        df = pd.DataFrame(processed_data)
        if 'vehicle_id' not in df.columns:
            return []
        
        # Agrupar por veículo
        ranking_data = []
        for vehicle_id, group in df.groupby('vehicle_id'):
            vehicle_metrics = {
                'vehicle_id': vehicle_id,
                'total_distance_km': group['odometer'].max() - group['odometer'].min() if 'odometer' in group.columns else 0,
                'max_speed_kmh': group['speed'].max() if 'speed' in group.columns else 0,
                'avg_speed_kmh': group['speed'].mean() if 'speed' in group.columns else 0,
                'record_count': len(group)
            }
            ranking_data.append(vehicle_metrics)
        
        # Ordenar por quilometragem total
        ranking_data.sort(key=lambda x: x['total_distance_km'], reverse=True)
        return ranking_data
    
    def _generate_inconsistencies_report(self, validation_results: Dict, qa_results: Dict) -> List[str]:
        """Gera relatório de inconsistências"""
        inconsistencies = []
        
        # Adicionar problemas de coerência
        inconsistencies.extend(validation_results.get('coherence_issues', []))
        
        # Adicionar limitações dos testes QA
        limitations = qa_results.get('limitations', [])
        inconsistencies.extend(limitations)
        
        return inconsistencies
    
    def _generate_recommendations(self, processing_result: Dict, validation_results: Dict) -> List[str]:
        """Gera recomendações baseadas nos dados"""
        recommendations = []
        
        # Recomendações baseadas na qualidade dos dados
        data_quality = validation_results.get('data_quality', 'unknown')
        if data_quality == 'poor':
            recommendations.append("Recomenda-se verificar os sensores de velocidade e GPS dos veículos")
            recommendations.append("Considerar recalibração dos dispositivos de telemetria")
        
        # Recomendações baseadas nas métricas
        distance_metrics = processing_result.get('distance_speed_metrics', {})
        max_speed = distance_metrics.get('max_speed', 0)
        
        if max_speed > 100:
            recommendations.append("Monitorar veículos com velocidade máxima acima de 100 km/h")
        
        return recommendations
    
    def _generate_period_summary(self, processing_result: Dict) -> Dict:
        """Gera resumo do período"""
        processed_data = processing_result.get('processed_data', [])
        if not processed_data:
            return {}
        
        # Converter para DataFrame
        df = pd.DataFrame(processed_data)
        if 'timestamp' not in df.columns:
            return {}
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        return {
            'start_date': df['timestamp'].min().isoformat(),
            'end_date': df['timestamp'].max().isoformat(),
            'total_days': (df['timestamp'].max().date() - df['timestamp'].min().date()).days + 1,
            'total_records': len(df),
            'unique_vehicles': df['vehicle_id'].nunique() if 'vehicle_id' in df.columns else 0
        }
    
    def _generate_trends_analysis(self, processing_result: Dict) -> Dict:
        """Gera análise de tendências"""
        processed_data = processing_result.get('processed_data', [])
        if not processed_data:
            return {}
        
        # Converter para DataFrame
        df = pd.DataFrame(processed_data)
        if 'timestamp' not in df.columns:
            return {}
        
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['week'] = df['timestamp'].dt.isocalendar().week
        
        # Agrupar por semana
        weekly_data = []
        for week, group in df.groupby('week'):
            weekly_metrics = {
                'week': int(week),
                'avg_distance_km': (group['odometer'].max() - group['odometer'].min()) / len(group['vehicle_id'].unique()) if 'odometer' in group.columns and 'vehicle_id' in group.columns else 0,
                'avg_speed_kmh': group['speed'].mean() if 'speed' in group.columns else 0
            }
            weekly_data.append(weekly_metrics)
        
        return {
            'weekly_trends': weekly_data,
            'total_weeks': len(weekly_data)
        }


def main():
    """Função principal para execução do sistema de relatórios"""
    print("📊 Sistema de Relatórios de Telemetria Veicular")
    print("=" * 50)
    
    # Verificar argumentos da linha de comando
    if len(sys.argv) < 4:
        print("Uso: python telemetry_reporter.py <caminho_arquivo_csv> <data_inicial> <data_final> [diretorio_saida] [nome_cliente]")
        print()
        print("Exemplo: python telemetry_reporter.py dados/telemetria.csv 2025-09-01 2025-09-07 relatorios/ \"Cliente Exemplo\"")
        return
    
    csv_file_path = sys.argv[1]
    start_date_str = sys.argv[2]
    end_date_str = sys.argv[3]
    output_dir = sys.argv[4] if len(sys.argv) > 4 else "relatorios"
    client_name = sys.argv[5] if len(sys.argv) > 5 else None
    
    # Converter datas
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    except ValueError as e:
        print(f"❌ Formato de data inválido: {e}")
        return
    
    # Verificar se o arquivo CSV existe
    if not os.path.exists(csv_file_path):
        print(f"❌ Arquivo não encontrado: {csv_file_path}")
        return
    
    # Inicializar sistema de relatórios
    reporter = TelemetryReporter()
    
    # Processar arquivo e gerar relatórios
    print(f"📄 Processando: {csv_file_path}")
    print(f"📅 Período: {start_date.strftime('%d/%m/%Y')} a {end_date.strftime('%d/%m/%Y')}")
    print(f"📂 Diretório de saída: {output_dir}")
    if client_name:
        print(f"👤 Cliente: {client_name}")
    print()
    
    result = reporter.generate_report_from_csv(
        csv_file_path, output_dir, start_date, end_date, "Todos", client_name
    )
    
    if result['success']:
        print("✅ Processamento concluído com sucesso!")
        print()
        print("📤 Arquivos gerados:")
        for output_type, path in result['outputs'].items():
            print(f"   • {output_type}: {path}")
        
        # Exibir resumo das métricas principais
        processing_result = result['processing_result']
        distance_metrics = processing_result.get('distance_speed_metrics', {})
        trips = processing_result.get('trips', [])
        
        print()
        print("📈 Resumo das métricas:")
        print(f"   • Quilometragem total: {distance_metrics.get('total_km', 0):.2f} km")
        print(f"   • Velocidade máxima: {distance_metrics.get('max_speed', 0):.2f} km/h")
        print(f"   • Número de viagens: {len(trips)}")
        print(f"   • Estrutura do relatório: {result['report_structure']}")
        print(f"   • Período: {result['period_info']['days_count']} dias")
        
        # Exibir resultados da validação
        validation_results = result['validation_results']
        if validation_results.get('coherence_issues'):
            print()
            print("⚠️  Problemas de coerência identificados:")
            for issue in validation_results['coherence_issues']:
                print(f"   • {issue}")
        
        # Exibir resultados dos testes QA
        qa_results = result['qa_results']
        print()
        print("🧪 Resultados dos testes QA:")
        passed_tests = 0
        total_tests = 0
        for test_name, test_result in qa_results.items():
            if test_name not in ['limitations', 'error']:
                total_tests += 1
                if test_result == 'passed':
                    passed_tests += 1
                    status = "✅"
                elif test_result == 'skipped':
                    status = "⏭️"
                else:
                    status = "❌"
                print(f"   {status} {test_name}: {test_result}")
        
        print(f"   Total: {passed_tests}/{total_tests} testes passaram")
        
        # Exibir limitações se houver
        limitations = qa_results.get('limitations', [])
        if limitations:
            print()
            print("⚠️  Limitações identificadas:")
            for limitation in limitations:
                print(f"   • {limitation}")
    else:
        print(f"❌ Erro no processamento: {result['error']}")
    
    print()
    print("🏁 Processo concluído!")


if __name__ == "__main__":
    main()