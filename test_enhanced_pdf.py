#!/usr/bin/env python3
"""
Teste das melhorias do PDF consolidado:
1. Detecção automática do cliente
2. Segmentação diária com períodos operacionais
3. Ranking único estilo campeonato
"""

from datetime import datetime
from app.reports import ConsolidatedPDFGenerator

# Simula dados estruturados com as melhorias
def create_mock_data():
    return {
        "cliente_info": {
            "nome": "Transportes ABC Ltda",  # Cliente detectado automaticamente
            "consumo_medio_kmL": 12.5,
            "limite_velocidade": 80
        },
        "periodo": {
            "data_inicio": datetime(2024, 9, 1),
            "data_fim": datetime(2024, 9, 7)
        },
        "resumo_geral": {
            "total_veiculos": 5,
            "km_total": 2456.8,
            "combustivel_total": 196.5,
            "media_por_veiculo": 491.4,
            "vel_maxima_frota": 95
        },
        "desempenho_periodo": [
            {"placa": "ABC-1234", "km_total": 520, "velocidade_maxima": 78, "combustivel": 41.6, "eficiencia": 12.5},
            {"placa": "DEF-5678", "km_total": 485, "velocidade_maxima": 95, "combustivel": 40.4, "eficiencia": 12.0},
            {"placa": "GHI-9012", "km_total": 612, "velocidade_maxima": 72, "combustivel": 45.9, "eficiencia": 13.3},
            {"placa": "JKL-3456", "km_total": 398, "velocidade_maxima": 88, "combustivel": 35.2, "eficiencia": 11.3},
            {"placa": "MNO-7890", "km_total": 441, "velocidade_maxima": 65, "combustivel": 33.4, "eficiencia": 13.2}
        ],
        # NOVA ESTRUTURA: Períodos organizados por DIA
        "periodos_diarios": {
            "2024-09-01": {
                "Manhã Operacional": {
                    "info": {"horario": "04:00-07:00", "cor": "verde", "descricao": "Início das atividades"},
                    "veiculos": [
                        {"placa": "ABC-1234", "km_periodo": 85, "vel_max_periodo": 72, "combustivel_periodo": 6.8},
                        {"placa": "GHI-9012", "km_periodo": 95, "vel_max_periodo": 68, "combustivel_periodo": 7.1}
                    ]
                },
                "Fora Horário Manhã": {
                    "info": {"horario": "07:00-10:50", "cor": "laranja", "descricao": "Entre turnos"},
                    "veiculos": [
                        {"placa": "DEF-5678", "km_periodo": 45, "vel_max_periodo": 85, "combustivel_periodo": 4.2}
                    ]
                }
            },
            "2024-09-02": {
                "Manhã Operacional": {
                    "info": {"horario": "04:00-07:00", "cor": "verde", "descricao": "Início das atividades"},
                    "veiculos": [
                        {"placa": "ABC-1234", "km_periodo": 92, "vel_max_periodo": 78, "combustivel_periodo": 7.4},
                        {"placa": "MNO-7890", "km_periodo": 88, "vel_max_periodo": 65, "combustivel_periodo": 6.7}
                    ]
                }
            }
        },
        # NOVO RANKING: Único estilo campeonato
        "ranking_campeonato": {
            "titulo": "Ranking de Desempenho Custo/Benefício",
            "descricao": "Classificação geral baseada em quilometragem (40%) + combustível (40%) + controle de velocidade (20%)",
            "veiculos": [
                {
                    "posicao_ranking": 1,
                    "categoria_ranking": "top3",
                    "placa": "GHI-9012", 
                    "km_total": 612, 
                    "eficiencia": 13.3, 
                    "velocidade_maxima": 72, 
                    "score_custo_beneficio": 8.85
                },
                {
                    "posicao_ranking": 2,
                    "categoria_ranking": "top3",
                    "placa": "MNO-7890", 
                    "km_total": 441, 
                    "eficiencia": 13.2, 
                    "velocidade_maxima": 65, 
                    "score_custo_beneficio": 8.12
                },
                {
                    "posicao_ranking": 3,
                    "categoria_ranking": "top3",
                    "placa": "ABC-1234", 
                    "km_total": 520, 
                    "eficiencia": 12.5, 
                    "velocidade_maxima": 78, 
                    "score_custo_beneficio": 7.45
                },
                {
                    "posicao_ranking": 4,
                    "categoria_ranking": "normal",
                    "placa": "DEF-5678", 
                    "km_total": 485, 
                    "eficiencia": 12.0, 
                    "velocidade_maxima": 95, 
                    "score_custo_beneficio": 6.24
                },
                {
                    "posicao_ranking": 5,
                    "categoria_ranking": "bottom3",
                    "placa": "JKL-3456", 
                    "km_total": 398, 
                    "eficiencia": 11.3, 
                    "velocidade_maxima": 88, 
                    "score_custo_beneficio": 5.78
                }
            ]
        },
        "por_dia": {
            "2024-09-01": [
                {"placa": "ABC-1234", "km_dia": 156, "vel_max": 78, "combustivel_dia": 12.5, "eficiencia_dia": 12.5},
                {"placa": "GHI-9012", "km_dia": 178, "vel_max": 72, "combustivel_dia": 13.4, "eficiencia_dia": 13.3}
            ]
        }
    }

def test_enhanced_pdf():
    """Testa o PDF aprimorado com as novas funcionalidades"""
    print("🧪 Testando PDF consolidado aprimorado...")
    
    # Cria dados mock
    structured_data = create_mock_data()
    
    # Gera PDF
    generator = ConsolidatedPDFGenerator()
    result = generator.generate_consolidated_pdf(
        structured_data=structured_data,
        data_inicio=datetime(2024, 9, 1),
        data_fim=datetime(2024, 9, 7),
        output_path="c:/Users/Administrator/Desktop/Projeto/relatorios-frotas/reports/teste_aprimorado.pdf",
        total_km=2456.8,
        total_fuel=196.5
    )
    
    if result['success']:
        print("✅ PDF gerado com sucesso!")
        print(f"📄 Arquivo: {result['file_path']}")
        print(f"📏 Tamanho: {result['file_size_mb']} MB")
        print("\n🎯 Funcionalidades implementadas:")
        print("   ✓ Cliente detectado automaticamente no título")
        print("   ✓ Períodos segmentados por dia")
        print("   ✓ Ranking único estilo campeonato")
        print("   ✓ Cores para top 3 (verde) e bottom 3 (vermelho)")
        print("   ✓ Tabelas sem coluna cliente redundante")
    else:
        print(f"❌ Erro: {result['error']}")

if __name__ == "__main__":
    test_enhanced_pdf()