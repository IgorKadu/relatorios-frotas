"""
Módulo principal do sistema de processamento de telemetria veicular.
Integra todas as funcionalidades em uma solução completa conforme especificação.
"""

import os
import sys
from datetime import datetime
from typing import Dict, Optional
import pandas as pd

from .telemetry_processor import TelemetryProcessor, process_telemetry_csv
from .enhanced_reports import EnhancedPDFReportGenerator, generate_enhanced_report
from .test_telemetry_qa import run_all_qa_tests


class TelemetryProcessingSystem:
    """Sistema completo de processamento de telemetria veicular"""
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Inicializa o sistema de processamento de telemetria
        
        Args:
            config: Dicionário com configurações do sistema
        """
        self.config = config or {}
        self.processor = TelemetryProcessor(self.config)
        self.report_generator = EnhancedPDFReportGenerator()
    
    def process_csv_and_generate_report(self, csv_file_path: str, output_dir: str, 
                                      client_name: Optional[str] = None) -> Dict:
        """
        Processa um arquivo CSV e gera todos os outputs exigidos
        
        Args:
            csv_file_path: Caminho para o arquivo CSV de telemetria
            output_dir: Diretório de saída para os arquivos gerados
            client_name: Nome do cliente (opcional)
            
        Returns:
            Dicionário com informações sobre os arquivos gerados
        """
        try:
            # Criar diretório de saída se não existir
            os.makedirs(output_dir, exist_ok=True)
            
            # Extrair nome base do arquivo
            base_filename = os.path.splitext(os.path.basename(csv_file_path))[0]
            
            # 1. Processar o arquivo CSV com todas as etapas
            print(f"📊 Processando arquivo: {csv_file_path}")
            processing_result = process_telemetry_csv(csv_file_path, self.config)
            
            if not processing_result.get('success', False):
                raise Exception(f"Falha no processamento: {processing_result.get('error', 'Erro desconhecido')}")
            
            print("✅ Processamento concluído com sucesso!")
            
            # 2. Executar testes de QA
            print("🧪 Executando testes de qualidade...")
            qa_results = self.processor.run_qa_tests(processing_result)
            print("✅ Testes de qualidade concluídos!")
            
            # 3. Verificar se há limitações e incluir na seção apropriada
            limitations = qa_results.get('limitations', [])
            if limitations:
                print("⚠️  Limitações identificadas:")
                for limitation in limitations:
                    print(f"   • {limitation}")
            
            # 4. Gerar todos os outputs exigidos
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
            
            # 5. Retornar informações completas
            result = {
                'success': True,
                'processing_result': processing_result,
                'qa_results': qa_results,
                'outputs': additional_outputs,
                'message': 'Processamento e geração de relatórios concluídos com sucesso'
            }
            
            return result
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'message': f'Falha no processamento: {str(e)}'
            }
    
    def run_comprehensive_qa_validation(self) -> bool:
        """
        Executa validação QA abrangente do sistema completo
        
        Returns:
            Boolean indicando se todos os testes passaram
        """
        print("🔍 Executando validação QA abrangente do sistema...")
        return run_all_qa_tests()
    
    def get_system_info(self) -> Dict:
        """
        Retorna informações sobre o sistema e suas configurações
        
        Returns:
            Dicionário com informações do sistema
        """
        return {
            'system_name': 'Sistema de Processamento de Telemetria Veicular',
            'version': '1.0.0',
            'configuration': self.config,
            'features': [
                'Detecção automática de schema',
                'Mapeamento de colunas com fallback',
                'Regras de qualidade e saneamento',
                'Cálculo de distância via haversine',
                'Detecção de viagens',
                'Geração de relatórios PDF adaptativos',
                'Múltiplos formatos de saída',
                'Testes de aceitação QA'
            ],
            'timestamp': datetime.now().isoformat()
        }


def main():
    """Função principal para execução do sistema"""
    print("🚀 Sistema de Processamento de Telemetria Veicular")
    print("=" * 50)
    
    # Exibir informações do sistema
    system = TelemetryProcessingSystem()
    info = system.get_system_info()
    
    print(f"Sistema: {info['system_name']}")
    print(f"Versão: {info['version']}")
    print(f"Data/Hora: {info['timestamp']}")
    print()
    
    # Verificar argumentos da linha de comando
    if len(sys.argv) < 2:
        print("Uso: python telemetry_system.py <caminho_arquivo_csv> [diretorio_saida] [nome_cliente]")
        print()
        print("Exemplo: python telemetry_system.py dados/telemetria.csv relatorios/ \"Cliente Exemplo\"")
        return
    
    csv_file_path = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) > 2 else "relatorios"
    client_name = sys.argv[3] if len(sys.argv) > 3 else None
    
    # Verificar se o arquivo CSV existe
    if not os.path.exists(csv_file_path):
        print(f"❌ Arquivo não encontrado: {csv_file_path}")
        return
    
    # Processar arquivo e gerar relatórios
    print(f"📄 Processando: {csv_file_path}")
    print(f"📂 Diretório de saída: {output_dir}")
    if client_name:
        print(f"👤 Cliente: {client_name}")
    print()
    
    result = system.process_csv_and_generate_report(csv_file_path, output_dir, client_name)
    
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


# Exemplo de uso programático
def example_usage():
    """Exemplo de uso programático do sistema"""
    print("📝 Exemplo de uso programático:")
    
    # Configuração do sistema
    config = {
        'speed_outlier_threshold': 220,
        'trip_speed_threshold': 3,
        'trip_min_duration_s': 60,
        'gps_jump_distance_km': 500
    }
    
    # Inicializar sistema
    system = TelemetryProcessingSystem(config)
    
    # Processar arquivo de exemplo (substituir pelo caminho real)
    # result = system.process_csv_and_generate_report(
    #     'caminho/para/arquivo.csv',
    #     'diretorio/de/saida',
    #     'Nome do Cliente'
    # )
    # 
    # if result['success']:
    #     print("Processamento concluído com sucesso!")
    # else:
    #     print(f"Erro: {result['error']}")


if __name__ == "__main__":
    # Se chamado diretamente, executar função principal
    if len(sys.argv) > 1:
        main()
    else:
        # Exibir informações do sistema
        system = TelemetryProcessingSystem()
        info = system.get_system_info()
        print("🚀 Sistema de Processamento de Telemetria Veicular")
        print("=" * 50)
        print(f"Versão: {info['version']}")
        print()
        print(".Funcionalidades:")
        for feature in info['features']:
            print(f"   • {feature}")
        print()
        print("Para processar um arquivo CSV, use:")
        print("   python telemetry_system.py <caminho_arquivo_csv> [diretorio_saida] [nome_cliente]")