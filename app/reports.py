"""
Módulo para geração de relatórios PDF com insights de telemetria veicular.
"""

import os
import base64
from datetime import datetime
from typing import Dict, List, Optional
from io import BytesIO
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch, cm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, 
    PageBreak, Image, KeepTogether
)
from reportlab.platypus.frames import Frame
from reportlab.platypus.doctemplate import PageTemplate
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT, TA_JUSTIFY
from reportlab.graphics.shapes import Drawing, String
from reportlab.graphics.charts.linecharts import HorizontalLineChart
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.charts.piecharts import Pie
from reportlab.graphics.widgets.markers import makeMarker
import pandas as pd
import numpy as np

from .services import ReportGenerator
from .models import get_session, Veiculo, Cliente

class PDFReportGenerator:
    """Classe para gerar relatórios PDF profissionais"""
    
    def __init__(self):
        self.report_generator = ReportGenerator()
        self.styles = getSampleStyleSheet()
        self.setup_custom_styles()
    
    def setup_custom_styles(self):
        """Configura estilos customizados para o PDF"""
        # Estilo do título principal
        self.styles.add(ParagraphStyle(
            name='TitleStyle',
            parent=self.styles['Title'],
            fontSize=24,
            textColor=colors.HexColor('#2C3E50'),
            alignment=TA_CENTER,
            spaceAfter=20
        ))
        
        # Estilo de subtítulo
        self.styles.add(ParagraphStyle(
            name='SubtitleStyle',
            parent=self.styles['Heading1'],
            fontSize=16,
            textColor=colors.HexColor('#3498DB'),
            alignment=TA_LEFT,
            spaceBefore=15,
            spaceAfter=10
        ))
        
        # Estilo para métricas
        self.styles.add(ParagraphStyle(
            name='MetricStyle',
            parent=self.styles['Normal'],
            fontSize=12,
            alignment=TA_LEFT,
            spaceBefore=5,
            spaceAfter=5
        ))
        
        # Estilo para insights
        self.styles.add(ParagraphStyle(
            name='InsightStyle',
            parent=self.styles['Normal'],
            fontSize=11,
            alignment=TA_JUSTIFY,
            spaceBefore=8,
            spaceAfter=8,
            leftIndent=20,
            rightIndent=20
        ))
    
    def create_cover_page(self, metrics: Dict) -> List:
        """Cria a página de capa do relatório"""
        story = []
        
        # Título principal
        title = f"Relatório de Telemetria Veicular"
        story.append(Paragraph(title, self.styles['TitleStyle']))
        
        # Informações do veículo
        veiculo_info = metrics.get('veiculo', {})
        cliente = veiculo_info.get('cliente', 'N/A')
        placa = veiculo_info.get('placa', 'N/A')
        
        story.append(Spacer(1, 30))
        
        # Dados do cliente e veículo
        info_text = f"""
        <b>Cliente:</b> {cliente}<br/>
        <b>Placa do Veículo:</b> {placa}<br/>
        """
        story.append(Paragraph(info_text, self.styles['Normal']))
        
        story.append(Spacer(1, 30))
        
        # Período de análise
        periodo = veiculo_info.get('periodo_analise', {})
        if periodo:
            inicio = periodo.get('inicio', datetime.now()).strftime('%d/%m/%Y')
            fim = periodo.get('fim', datetime.now()).strftime('%d/%m/%Y')
            total_dias = periodo.get('total_dias', 0)
            
            periodo_text = f"""
            <b>Período de Análise:</b><br/>
            De {inicio} a {fim}<br/>
            Total: {total_dias} dias
            """
            story.append(Paragraph(periodo_text, self.styles['Normal']))
        
        story.append(Spacer(1, 50))
        
        # Data de geração
        data_geracao = datetime.now().strftime('%d/%m/%Y às %H:%M')
        story.append(Paragraph(f"Relatório gerado em: {data_geracao}", 
                              self.styles['Normal']))
        
        story.append(PageBreak())
        return story
    
    def create_executive_summary(self, metrics: Dict, insights: List[str]) -> List:
        """Cria o sumário executivo"""
        story = []
        
        story.append(Paragraph("Sumário Executivo", self.styles['SubtitleStyle']))
        
        operacao = metrics.get('operacao', {})
        
        # Métricas principais em tabela
        summary_data = [
            ['Métrica', 'Valor'],
            ['Total de Registros', f"{operacao.get('total_registros', 0):,}"],
            ['Quilometragem Total', f"{operacao.get('km_total', 0):.2f} km"],
            ['Velocidade Máxima', f"{operacao.get('velocidade_maxima', 0)} km/h"],
            ['Velocidade Média', f"{operacao.get('velocidade_media', 0):.1f} km/h"],
            ['Tempo Ligado', f"{operacao.get('tempo_total_ligado', 0)} registros"],
            ['Tempo em Movimento', f"{operacao.get('tempo_em_movimento', 0)} registros"]
        ]
        
        # Adiciona dados de combustível se disponível
        if 'combustivel' in metrics:
            fuel_data = metrics['combustivel']
            summary_data.extend([
                ['Combustível Estimado', f"{fuel_data['fuel_consumed_liters']:.2f} L"],
                ['Eficiência', f"{fuel_data['efficiency_kmL']:.2f} km/L"]
            ])
        
        summary_table = Table(summary_data, colWidths=[3*inch, 2*inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3498DB')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        story.append(summary_table)
        story.append(Spacer(1, 20))
        
        # Principais insights
        story.append(Paragraph("Principais Insights:", self.styles['SubtitleStyle']))
        
        for insight in insights[:5]:  # Limita a 5 insights principais
            story.append(Paragraph(f"• {insight}", self.styles['InsightStyle']))
        
        story.append(PageBreak())
        return story
    
    def create_operational_analysis(self, metrics: Dict) -> List:
        """Cria análise operacional detalhada similar ao exemplo fornecido"""
        story = []
        
        periodos = metrics.get('periodos', {})
        veiculo_info = metrics.get('veiculo', {})
        operacao = metrics.get('operacao', {})
        
        # DENTRO DO HORÁRIO OPERACIONAL
        story.append(Paragraph("DENTRO DO HORÁRIO OPERACIONAL", self.styles['SubtitleStyle']))
        
        # Períodos operacionais com tabelas detalhadas
        periods = [
            ('04:00 as 07:00', 'operacional_manha', colors.lightgreen),
            ('10:50 as 13:00', 'operacional_meio_dia', colors.lightblue),
            ('16:50 as 19:00', 'operacional_tarde', colors.lightyellow)
        ]
        
        for period_title, period_key, bg_color in periods:
            story.append(Paragraph(period_title, self.styles['Normal']))
            
            data = [
                ['Cliente', 'Placa', 'Velocidade máxima atingida(Km/h)', 'Odômetro (Km)', 'Tempo total ligado', 'Tempo em movimento', 'Tempo ocioso', 'Tempo desligado', 'Período', 'Setor'],
                [veiculo_info.get('cliente', 'N/A')[:8], veiculo_info.get('placa', 'N/A'), 
                 str(operacao.get('velocidade_maxima', 0)), f"{operacao.get('km_total', 0):.2f}",
                 f"{periodos.get(period_key, 0):02d}:00", 
                 f"{operacao.get('tempo_em_movimento', 0):02d}:00",
                 f"{operacao.get('tempo_parado_ligado', 0):02d}:00",
                 f"{operacao.get('tempo_desligado', 0):02d}:00",
                 f"{str(veiculo_info.get('periodo_analise', {}).get('inicio', 'N/A'))[:10]} - {str(veiculo_info.get('periodo_analise', {}).get('fim', 'N/A'))[:10]}",
                 'ESCOLAR']
            ]
            
            table = Table(data, colWidths=[0.6*inch, 0.6*inch, 0.9*inch, 0.7*inch, 0.8*inch, 0.8*inch, 0.7*inch, 0.8*inch, 1.2*inch, 0.6*inch])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4CAF50')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 8),
                ('FONTSIZE', (0, 1), (-1, -1), 7),
                ('BACKGROUND', (0, 1), (-1, -1), bg_color),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
            ]))
            
            story.append(table)
            story.append(Spacer(1, 8))
        
        # TOTAL OPERACIONAL
        total_op = periodos.get('operacional_manha', 0) + periodos.get('operacional_meio_dia', 0) + periodos.get('operacional_tarde', 0)
        story.append(Paragraph("TOTAL - DENTRO DO HORÁRIO OPERACIONAL", self.styles['Normal']))
        
        total_data = [
            ['Cliente', 'Placa', 'Velocidade máxima atingida(Km/h)', 'Odômetro (Km)', 'Tempo total ligado', 'Tempo em movimento', 'Tempo ocioso', 'Tempo desligado', 'Período', 'Setor'],
            [veiculo_info.get('cliente', 'N/A')[:8], veiculo_info.get('placa', 'N/A'), 
             str(operacao.get('velocidade_maxima', 0)), f"{operacao.get('km_total', 0):.2f}",
             f"{total_op:02d}:00", 
             f"{operacao.get('tempo_em_movimento', 0):02d}:00",
             f"{operacao.get('tempo_parado_ligado', 0):02d}:00",
             f"{operacao.get('tempo_desligado', 0):02d}:00",
             f"{str(veiculo_info.get('periodo_analise', {}).get('inicio', 'N/A'))[:10]} - {str(veiculo_info.get('periodo_analise', {}).get('fim', 'N/A'))[:10]}",
             'ESCOLAR']
        ]
        
        total_table = Table(total_data, colWidths=[0.6*inch, 0.6*inch, 0.9*inch, 0.7*inch, 0.8*inch, 0.8*inch, 0.7*inch, 0.8*inch, 1.2*inch, 0.6*inch])
        total_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#4CAF50')),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
        ]))
        
        story.append(total_table)
        story.append(PageBreak())
        
        # FINAL DE SEMANA
        story.append(Paragraph("FINAL DE SEMANA - SÁBADO + DOMINGO", self.styles['SubtitleStyle']))
        
        weekend_data = [
            ['Cliente', 'Placa', 'Velocidade máxima atingida(Km/h)', 'Odômetro (Km)', 'Tempo total ligado', 'Tempo em movimento', 'Tempo ocioso', 'Tempo desligado', 'Período', 'Setor'],
            [veiculo_info.get('cliente', 'N/A')[:8], veiculo_info.get('placa', 'N/A'), 
             str(operacao.get('velocidade_maxima', 0)), f"{operacao.get('km_total', 0):.2f}",
             f"{periodos.get('final_semana', 0):02d}:00", 
             f"{operacao.get('tempo_em_movimento', 0):02d}:00",
             f"{operacao.get('tempo_parado_ligado', 0):02d}:00",
             f"{operacao.get('tempo_desligado', 0):02d}:00",
             f"{str(veiculo_info.get('periodo_analise', {}).get('inicio', 'N/A'))[:10]} - {str(veiculo_info.get('periodo_analise', {}).get('fim', 'N/A'))[:10]}",
             'ESCOLAR']
        ]
        
        weekend_table = Table(weekend_data, colWidths=[0.6*inch, 0.6*inch, 0.9*inch, 0.7*inch, 0.8*inch, 0.8*inch, 0.7*inch, 0.8*inch, 1.2*inch, 0.6*inch])
        weekend_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E91E63')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            ('FONTSIZE', (0, 1), (-1, -1), 7),
            ('BACKGROUND', (0, 1), (-1, -1), colors.lightpink),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
        ]))
        
        story.append(weekend_table)
        story.append(Spacer(1, 20))
        
        # FORA DO HORÁRIO
        story.append(Paragraph("FORA DO HORÁRIO", self.styles['SubtitleStyle']))
        
        out_periods = [
            ('07:00 as 10:50', 'fora_horario_manha'),
            ('13:00 as 16:50', 'fora_horario_tarde')
        ]
        
        for period_title, period_key in out_periods:
            story.append(Paragraph(period_title, self.styles['Normal']))
            
            data = [
                ['Cliente', 'Placa', 'Velocidade máxima atingida(Km/h)', 'Odômetro (Km)', 'Tempo total ligado', 'Tempo em movimento', 'Tempo ocioso', 'Tempo desligado', 'Período', 'Setor'],
                [veiculo_info.get('cliente', 'N/A')[:8], veiculo_info.get('placa', 'N/A'), 
                 str(operacao.get('velocidade_maxima', 0)), f"{operacao.get('km_total', 0):.2f}",
                 f"{periodos.get(period_key, 0):02d}:00", 
                 f"{operacao.get('tempo_em_movimento', 0):02d}:00",
                 f"{operacao.get('tempo_parado_ligado', 0):02d}:00",
                 f"{operacao.get('tempo_desligado', 0):02d}:00",
                 f"{str(veiculo_info.get('periodo_analise', {}).get('inicio', 'N/A'))[:10]} - {str(veiculo_info.get('periodo_analise', {}).get('fim', 'N/A'))[:10]}",
                 'ESCOLAR']
            ]
            
            table = Table(data, colWidths=[0.6*inch, 0.6*inch, 0.9*inch, 0.7*inch, 0.8*inch, 0.8*inch, 0.7*inch, 0.8*inch, 1.2*inch, 0.6*inch])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#FF5722')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 8),
                ('FONTSIZE', (0, 1), (-1, -1), 7),
                ('BACKGROUND', (0, 1), (-1, -1), colors.mistyrose),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
            ]))
            
            story.append(table)
            story.append(Spacer(1, 8))
        
        # TOTAL FORA DO HORÁRIO
        total_fora = periodos.get('fora_horario_manha', 0) + periodos.get('fora_horario_tarde', 0) + periodos.get('fora_horario_noite', 0)
        story.append(Paragraph("TOTAL - FORA DO HORÁRIO OPERACIONAL", self.styles['Normal']))
        
        total_fora_data = [
            ['Cliente', 'Placa', 'Velocidade máxima atingida(Km/h)', 'Odômetro (Km)', 'Tempo total ligado', 'Tempo em movimento', 'Tempo ocioso', 'Tempo desligado', 'Período', 'Setor'],
            [veiculo_info.get('cliente', 'N/A')[:8], veiculo_info.get('placa', 'N/A'), 
             str(operacao.get('velocidade_maxima', 0)), f"{operacao.get('km_total', 0):.2f}",
             f"{total_fora:02d}:00", 
             f"{operacao.get('tempo_em_movimento', 0):02d}:00",
             f"{operacao.get('tempo_parado_ligado', 0):02d}:00",
             f"{operacao.get('tempo_desligado', 0):02d}:00",
             f"{str(veiculo_info.get('periodo_analise', {}).get('inicio', 'N/A'))[:10]} - {str(veiculo_info.get('periodo_analise', {}).get('fim', 'N/A'))[:10]}",
             'ESCOLAR']
        ]
        
        total_fora_table = Table(total_fora_data, colWidths=[0.6*inch, 0.6*inch, 0.9*inch, 0.7*inch, 0.8*inch, 0.8*inch, 0.7*inch, 0.8*inch, 1.2*inch, 0.6*inch])
        total_fora_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#FF5722')),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE')
        ]))
        
        story.append(total_fora_table)
        story.append(PageBreak())
        return story
        
        # Análise de conectividade
        conectividade = metrics.get('conectividade', {})
        if conectividade:
            story.append(Paragraph("Status de Conectividade:", self.styles['Normal']))
            
            conn_data = [
                ['Indicador', 'Status', 'Observações'],
                ['GPS', f"{conectividade.get('gps_ok', 0)} OK", 'Funcionamento normal'],
                ['GPRS', f"{conectividade.get('gprs_ok', 0)} OK", 'Comunicação estável'],
                ['Problemas', f"{conectividade.get('problemas_conexao', 0)}", 'Verificar se necessário']
            ]
            
            conn_table = Table(conn_data, colWidths=[1.5*inch, 1.5*inch, 2*inch])
            conn_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E74C3C')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 11),
                ('BACKGROUND', (0, 1), (-1, -1), colors.lightblue),
                ('GRID', (0, 0), (-1, -1), 1, colors.black)
            ]))
            
            story.append(conn_table)
        
        story.append(PageBreak())
        return story
    
    def create_fuel_analysis(self, metrics: Dict) -> List:
        """Cria análise de combustível"""
        story = []
        
        if 'combustivel' not in metrics:
            return story
        
        story.append(Paragraph("Análise de Consumo de Combustível", 
                              self.styles['SubtitleStyle']))
        
        fuel_data = metrics['combustivel']
        
        # Dados de combustível
        fuel_info = [
            ['Métrica', 'Valor', 'Unidade'],
            ['Distância Percorrida', f"{fuel_data['km_traveled']:.2f}", 'km'],
            ['Combustível Estimado', f"{fuel_data['fuel_consumed_liters']:.2f}", 'litros'],
            ['Eficiência Real', f"{fuel_data['efficiency_kmL']:.2f}", 'km/L'],
            ['Velocidade Média', f"{fuel_data['avg_speed']:.2f}", 'km/h']
        ]
        
        fuel_table = Table(fuel_info, colWidths=[2*inch, 1.5*inch, 1*inch])
        fuel_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#F39C12')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 11),
            ('BACKGROUND', (0, 1), (-1, -1), colors.lightyellow),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))
        
        story.append(fuel_table)
        story.append(Spacer(1, 20))
        
        # Recomendações de economia
        story.append(Paragraph("Recomendações para Economia:", self.styles['Normal']))
        
        recommendations = []
        if fuel_data['efficiency_kmL'] < 10:
            recommendations.append("• Revisar estilo de condução - acelerações e frenagens bruscas consomem mais combustível")
            recommendations.append("• Verificar manutenção do veículo - filtros e óleo em dia melhoram a eficiência")
        
        if fuel_data['avg_speed'] > 80:
            recommendations.append("• Reduzir velocidade média - velocidades acima de 80 km/h aumentam significativamente o consumo")
        
        if not recommendations:
            recommendations.append("• Eficiência dentro do esperado - manter práticas atuais de condução")
        
        for rec in recommendations:
            story.append(Paragraph(rec, self.styles['InsightStyle']))
        
        story.append(PageBreak())
        return story
    
    def create_recommendations(self, insights: List[str]) -> List:
        """Cria seção de recomendações"""
        story = []
        
        story.append(Paragraph("Recomendações e Próximos Passos", 
                              self.styles['SubtitleStyle']))
        
        # Categoriza insights
        security_insights = [i for i in insights if '🚨' in i or 'velocidade' in i.lower()]
        efficiency_insights = [i for i in insights if '⛽' in i or 'combustível' in i.lower()]
        operation_insights = [i for i in insights if '📊' in i or 'operação' in i.lower()]
        connectivity_insights = [i for i in insights if '📡' in i or 'conectividade' in i.lower()]
        
        if security_insights:
            story.append(Paragraph("Segurança e Conformidade:", self.styles['Normal']))
            for insight in security_insights:
                story.append(Paragraph(f"• {insight}", self.styles['InsightStyle']))
            story.append(Spacer(1, 10))
        
        if efficiency_insights:
            story.append(Paragraph("Eficiência Operacional:", self.styles['Normal']))
            for insight in efficiency_insights:
                story.append(Paragraph(f"• {insight}", self.styles['InsightStyle']))
            story.append(Spacer(1, 10))
        
        if operation_insights:
            story.append(Paragraph("Otimização Operacional:", self.styles['Normal']))
            for insight in operation_insights:
                story.append(Paragraph(f"• {insight}", self.styles['InsightStyle']))
            story.append(Spacer(1, 10))
        
        if connectivity_insights:
            story.append(Paragraph("Conectividade e Monitoramento:", self.styles['Normal']))
            for insight in connectivity_insights:
                story.append(Paragraph(f"• {insight}", self.styles['InsightStyle']))
        
        # Plano de ação geral
        story.append(Spacer(1, 20))
        story.append(Paragraph("Plano de Ação Sugerido:", self.styles['Normal']))
        
        action_plan = [
            "1. Revisar pontos de excesso de velocidade identificados",
            "2. Implementar treinamento de condução econômica se necessário", 
            "3. Verificar equipamentos de telemetria em caso de problemas de conectividade",
            "4. Acompanhar métricas mensalmente para identificar tendências",
            "5. Considerar rotas alternativas para otimizar operação fora do horário comercial"
        ]
        
        for action in action_plan:
            story.append(Paragraph(action, self.styles['InsightStyle']))
        
        return story
    
    def generate_pdf_report(self, placa: str, data_inicio: datetime, data_fim: datetime, output_path: Optional[str] = None) -> Dict:
        """
        Gera relatório PDF completo
        """
        try:
            # Gera análise completa
            analysis = self.report_generator.generate_complete_analysis(placa, data_inicio, data_fim)
            
            if not analysis['success']:
                return analysis
            
            # Define caminho de saída
            if not output_path:
                filename = f"relatorio_{placa}_{data_inicio.strftime('%Y%m%d')}_{data_fim.strftime('%Y%m%d')}.pdf"
                output_path = os.path.join(os.path.dirname(__file__), '..', 'reports', filename)
            
            # Cria diretório se não existir
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Cria documento PDF
            doc = SimpleDocTemplate(output_path, pagesize=A4, 
                                  rightMargin=72, leftMargin=72, 
                                  topMargin=72, bottomMargin=18)
            
            # Constrói o conteúdo
            story = []
            
            # Capa
            story.extend(self.create_cover_page(analysis['metrics']))
            
            # Sumário executivo
            story.extend(self.create_executive_summary(analysis['metrics'], analysis['insights']))
            
            # Análise operacional detalhada com nova estrutura
            story.extend(self.create_operational_analysis(analysis['metrics']))
            
            # Análise de combustível
            story.extend(self.create_fuel_analysis(analysis['metrics']))
            
            # Recomendações
            story.extend(self.create_recommendations(analysis['insights']))
            
            # Gera o PDF
            doc.build(story)
            
            # Calcula tamanho do arquivo
            file_size = os.path.getsize(output_path) / (1024 * 1024)  # MB
            
            return {
                'success': True,
                'file_path': output_path,
                'file_size_mb': round(file_size, 2),
                'metrics': analysis['metrics'],
                'data_count': analysis['data_count']
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

def generate_vehicle_report(placa: str, data_inicio: datetime, data_fim: datetime, output_dir: Optional[str] = None) -> Dict:
    """
    Função de conveniência para gerar relatório de veículo
    """
    generator = PDFReportGenerator()
    
    if output_dir:
        filename = f"relatorio_{placa}_{data_inicio.strftime('%Y%m%d')}_{data_fim.strftime('%Y%m%d')}.pdf"
        output_path = os.path.join(output_dir, filename)
    else:
        output_path = None
    
    return generator.generate_pdf_report(placa, data_inicio, data_fim, output_path)

def generate_consolidated_vehicle_report(data_inicio: datetime, data_fim: datetime, output_dir: Optional[str] = None, cliente_nome: Optional[str] = None) -> Dict:
    """
    Gera relatório consolidado em PDF com foco no cliente usando nova estrutura de dados
    """
    try:
        # Usa o novo método do ReportGenerator para obter dados estruturados
        report_gen = ReportGenerator()
        consolidated_result = report_gen.generate_consolidated_report(data_inicio, data_fim, cliente_nome, output_dir or '')
        
        if not consolidated_result.get('success'):
            return consolidated_result
        
        structured_data = consolidated_result['data']
        total_km = consolidated_result['total_km']
        total_fuel = consolidated_result['total_fuel']
        
        # Gera PDF consolidado com nova estrutura
        generator = ConsolidatedPDFGenerator()
        
        if output_dir:
            cliente_nome_clean = structured_data['cliente_info']['nome'].replace(' ', '_').replace('/', '_')
            filename = f"relatorio_consolidado_{cliente_nome_clean}_{data_inicio.strftime('%Y%m%d')}_{data_fim.strftime('%Y%m%d')}.pdf"
            output_path = os.path.join(output_dir, filename)
        else:
            output_path = None
        
        return generator.generate_consolidated_pdf(
            structured_data, data_inicio, data_fim, output_path, total_km, total_fuel
        )
        
    except Exception as e:
        return {
            'success': False,
            'error': f'Erro ao gerar relatório consolidado: {str(e)}'
        }

class ConsolidatedPDFGenerator:
    """Gerador de PDF para relatórios consolidados com formatação profissional"""
    
    def __init__(self):
        self.styles = getSampleStyleSheet()
        self.setup_custom_styles()
    
    def setup_custom_styles(self):
        """Configura estilos customizados para PDF profissional"""
        # Título principal
        self.styles.add(ParagraphStyle(
            name='TitleStyle',
            parent=self.styles['Title'],
            fontSize=26,
            textColor=colors.HexColor('#1A4B8C'),
            alignment=TA_CENTER,
            spaceAfter=25,
            fontName='Helvetica-Bold'
        ))
        
        # Seção título
        self.styles.add(ParagraphStyle(
            name='SectionTitle',
            parent=self.styles['Heading1'],
            fontSize=18,
            textColor=colors.HexColor('#2E86AB'),
            alignment=TA_LEFT,
            spaceBefore=20,
            spaceAfter=12,
            fontName='Helvetica-Bold'
        ))
        
        # Subseção título
        self.styles.add(ParagraphStyle(
            name='SubsectionTitle',
            parent=self.styles['Heading2'],
            fontSize=14,
            textColor=colors.HexColor('#34495E'),
            alignment=TA_LEFT,
            spaceBefore=12,
            spaceAfter=8,
            fontName='Helvetica-Bold'
        ))
        
        # Texto de observação
        self.styles.add(ParagraphStyle(
            name='ObservationStyle',
            parent=self.styles['Normal'],
            fontSize=10,
            textColor=colors.HexColor('#7F8C8D'),
            alignment=TA_LEFT,
            spaceBefore=5,
            spaceAfter=5,
            leftIndent=15
        ))
        
        # Texto de alerta
        self.styles.add(ParagraphStyle(
            name='AlertStyle',
            parent=self.styles['Normal'],
            fontSize=11,
            textColor=colors.HexColor('#E74C3C'),
            alignment=TA_LEFT,
            spaceBefore=5,
            spaceAfter=5,
            fontName='Helvetica-Bold'
        ))
        
        # Texto de sucesso
        self.styles.add(ParagraphStyle(
            name='SuccessStyle',
            parent=self.styles['Normal'],
            fontSize=11,
            textColor=colors.HexColor('#27AE60'),
            alignment=TA_LEFT,
            spaceBefore=5,
            spaceAfter=5,
            fontName='Helvetica-Bold'
        ))
    
    def generate_consolidated_pdf(self, structured_data: Dict, data_inicio: datetime, 
                                data_fim: datetime, output_path: Optional[str], total_km: float, total_fuel: float) -> Dict:
        """Gera o PDF consolidado com nova estrutura: períodos + veículos juntos, rankings e dados diários"""
        try:
            if not output_path:
                filename = f"relatorio_consolidado_{data_inicio.strftime('%Y%m%d')}_{data_fim.strftime('%Y%m%d')}.pdf"
                output_path = os.path.join(os.path.dirname(__file__), '..', 'reports', filename)
            
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            doc = SimpleDocTemplate(output_path, pagesize=A4, rightMargin=50, leftMargin=50, topMargin=60, bottomMargin=50)
            story = []
            
            # CABEÇALHO
            cliente_nome = structured_data['cliente_info']['nome']
            title = f"Relatório Consolidado de Frota – {cliente_nome}"
            story.append(Paragraph(title, self.styles['TitleStyle']))
            story.append(Spacer(1, 10))
            
            periodo_text = f"<b>Período:</b> {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}"
            story.append(Paragraph(periodo_text, self.styles['Normal']))
            story.append(Spacer(1, 25))
            
            # 1. RESUMO GERAL
            self._add_general_summary(story, structured_data, total_km, total_fuel)
            
            # 2. DESEMPENHO GERAL DO PERÍODO
            self._add_period_performance_table(story, structured_data)
            
            # 3. PERÍODOS + VEÍCULOS
            self._add_periods_with_vehicles(story, structured_data)
            
            # 4. RANKINGS
            self._add_performance_rankings(story, structured_data)
            
            # 5. DIÁRIO
            self._add_daily_performance(story, structured_data)
            
            # 6. OBSERVAÇÕES
            self._add_footer_observations(story)
            
            doc.build(story)
            
            file_size = os.path.getsize(output_path) if output_path else 0
            file_size_mb = round(file_size / (1024 * 1024), 2)
            
            return {
                'success': True,
                'file_path': output_path,
                'file_size_mb': file_size_mb,
                'message': f'Relatório consolidado gerado com sucesso'
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Erro ao gerar PDF: {str(e)}'
            }
    
    def _add_general_summary(self, story, structured_data, total_km, total_fuel):
        """Adiciona resumo geral com métricas principais focado no cliente"""
        story.append(Paragraph("1. Dados Gerais do Período", self.styles['SectionTitle']))
        
        resumo = structured_data['resumo_geral']
        cliente_info = structured_data['cliente_info']
        
        summary_data = [
            ['Métrica', 'Valor'],
            ['Total de Veículos', f"{resumo['total_veiculos']}"],
            ['Quilometragem Total', f"{total_km:,.1f} km"],
            ['Combustível Total Estimado', f"{total_fuel:,.1f} L"],
            ['Média por Veículo', f"{resumo['media_por_veiculo']:,.1f} km"],
            ['Velocidade Máxima da Frota', f"{resumo['vel_maxima_frota']:.0f} km/h"]
        ]
        
        # Adiciona informações específicas do cliente se disponível
        if cliente_info.get('consumo_medio_kmL'):
            summary_data.append(['Consumo Médio Esperado', f"{cliente_info['consumo_medio_kmL']:.1f} km/L"])
        if cliente_info.get('limite_velocidade'):
            summary_data.append(['Limite de Velocidade', f"{cliente_info['limite_velocidade']} km/h"])
        
        summary_table = Table(summary_data, colWidths=[3*inch, 2*inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1A4B8C')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F8F9FA')),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
            ('ALIGN', (1, 1), (1, -1), 'RIGHT'),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#BDC3C7')),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))
        
        story.append(summary_table)
        story.append(Spacer(1, 30))
    
    def _add_period_performance_table(self, story, structured_data):
        """Adiciona tabela geral consolidada do período com métricas da frota"""
        story.append(Paragraph("2. Desempenho Geral no Período", self.styles['SectionTitle']))
        
        desempenho_periodo = structured_data.get('desempenho_periodo', [])
        
        if not desempenho_periodo:
            story.append(Paragraph("Nenhum dado de desempenho disponível.", self.styles['Normal']))
            return
        
        story.append(Paragraph(
            "Tabela consolidada com dados gerais de todos os veículos no período:",
            self.styles['Normal']
        ))
        story.append(Spacer(1, 10))
        
        # Tabela consolidada sem coluna cliente - só as colunas essenciais
        table_data = [['Placa', 'Km', 'Vel. Máx.', 'Combustível', 'Eficiência']]
        
        for vehicle in desempenho_periodo:
            table_data.append([
                vehicle['placa'],
                f"{vehicle['km_total']:,.0f}",
                f"{vehicle['velocidade_maxima']:.0f}",
                f"{vehicle['combustivel']:.1f}",
                f"{vehicle['eficiencia']:.1f}"
            ])
        
        period_table = Table(table_data, colWidths=[1.2*inch, 1.2*inch, 1.2*inch, 1.2*inch, 1.2*inch])
        period_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2E86AB')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F4F6F7')),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('ALIGN', (0, 1), (-1, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#BDC3C7')),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))
        
        story.append(period_table)
        story.append(Spacer(1, 30))
    
    def _add_periods_with_vehicles(self, story, structured_data):
        """Adiciona períodos operacionais organizados POR DIA (nova estrutura)"""
        story.append(Paragraph("3. Desempenho Diário por Horário Operacional", self.styles['SectionTitle']))
        
        story.append(Paragraph(
            "Dados organizados dia a dia com detalhamento por período operacional:",
            self.styles['Normal']
        ))
        story.append(Spacer(1, 15))
        
        # Use nova estrutura diária
        periodos_diarios = structured_data.get('periodos_diarios', {})
        
        if not periodos_diarios:
            story.append(Paragraph("Nenhum dado diário disponível.", self.styles['Normal']))
            return
        
        # Define cores por tipo de período
        color_map = {
            'verde': colors.HexColor('#27AE60'),
            'laranja': colors.HexColor('#F39C12'),
            'cinza': colors.HexColor('#95A5A6')
        }
        
        # Limita aos 7 dias mais recentes para não sobrecarregar o PDF
        dias_ordenados = sorted(periodos_diarios.keys())[-7:]
        
        # Agrupa dias de final de semana consecutivos para exibir título conjunto
        weekend_groups = []
        current_group = []
        
        for dia_str in dias_ordenados:
            periodos_do_dia = periodos_diarios[dia_str]
            
            if not periodos_do_dia:
                continue
                
            # Verifica se é final de semana (Saturday = 5, Sunday = 6)
            try:
                data_obj = datetime.strptime(dia_str, '%Y-%m-%d')
                is_weekend = data_obj.weekday() >= 5
            except:
                data_obj = None
                is_weekend = False
            
            if is_weekend and data_obj:
                current_group.append((dia_str, periodos_do_dia, data_obj))
            else:
                # Se temos um grupo de weekend, adicionamos à lista
                if current_group:
                    weekend_groups.append(current_group)
                    current_group = []
                # Adiciona dia da semana individual
                if data_obj:
                    weekend_groups.append([(dia_str, periodos_do_dia, data_obj)])
        
        # Adiciona último grupo se for weekend
        if current_group:
            weekend_groups.append(current_group)
        
        for group in weekend_groups:
            if len(group) == 2 and all(data_obj.weekday() >= 5 for _, _, data_obj in group):
                # É um final de semana completo (Sábado + Domingo)
                sabado_data = group[0][2]
                domingo_data = group[1][2]
                
                weekend_title = f"Final de Semana ({sabado_data.strftime('%d/%m/%Y')} + {domingo_data.strftime('%d/%m/%Y')})"
                story.append(Paragraph(f"<b>{weekend_title}</b>", self.styles['SubsectionTitle']))
                story.append(Spacer(1, 8))
                
                # Processa ambos os dias do final de semana sem cabeçalho de data
                for dia_str, periodos_do_dia, data_obj in group:
                    for nome_periodo, periodo_data in periodos_do_dia.items():
                        period_info = periodo_data['info']
                        vehicles_list = periodo_data['veiculos']
                        
                        if not vehicles_list:
                            continue
                        
                        periodo_title = f"{nome_periodo} ({period_info['horario']})"
                        story.append(Paragraph(periodo_title, self.styles['Normal']))
                        story.append(Spacer(1, 5))
                        
                        period_color = color_map.get(period_info['cor'], colors.HexColor('#95A5A6'))
                        
                        # Tabela SEM coluna cliente - colunas essenciais
                        vehicle_data = [['Placa', 'Km', 'Vel. Máx.', 'Combustível']]
                        
                        for vehicle in vehicles_list:
                            vehicle_data.append([
                                vehicle['placa'],
                                f"{vehicle['km_periodo']:.0f}",
                                f"{vehicle['vel_max_periodo']:.0f}",
                                f"{vehicle['combustivel_periodo']:.1f}"
                            ])
                        
                        vehicles_table = Table(vehicle_data, colWidths=[1.5*inch, 1.5*inch, 1.5*inch, 1.5*inch])
                        vehicles_table.setStyle(TableStyle([
                            ('BACKGROUND', (0, 0), (-1, 0), period_color),
                            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                            ('FONTSIZE', (0, 0), (-1, 0), 9),
                            ('BACKGROUND', (0, 1), (-1, -1), period_color.clone(alpha=0.1)),
                            ('FONTSIZE', (0, 1), (-1, -1), 8),
                            ('ALIGN', (0, 1), (-1, -1), 'CENTER'),
                            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#BDC3C7')),
                            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                        ]))
                        
                        story.append(vehicles_table)
                        story.append(Spacer(1, 10))
            else:
                # Dias individuais (ou final de semana incompleto)
                for dia_str, periodos_do_dia, data_obj in group:
                    if data_obj.weekday() >= 5:  # É weekend mas só um dia
                        weekend_title = f"Final de Semana ({data_obj.strftime('%d/%m/%Y')})"
                        story.append(Paragraph(f"<b>{weekend_title}</b>", self.styles['SubsectionTitle']))
                    else:
                        # Título do dia normal
                        data_formatted = data_obj.strftime('%d/%m/%Y')
                        story.append(Paragraph(f"<b>Data: {data_formatted}</b>", self.styles['SubsectionTitle']))
                    
                    story.append(Spacer(1, 8))
                    
                    # Para cada período do dia
                    for nome_periodo, periodo_data in periodos_do_dia.items():
                        period_info = periodo_data['info']
                        vehicles_list = periodo_data['veiculos']
                        
                        if not vehicles_list:
                            continue
                        
                        periodo_title = f"{nome_periodo} ({period_info['horario']})"
                        story.append(Paragraph(periodo_title, self.styles['Normal']))
                        story.append(Spacer(1, 5))
                        
                        period_color = color_map.get(period_info['cor'], colors.HexColor('#95A5A6'))
                        
                        # Tabela SEM coluna cliente - colunas essenciais
                        vehicle_data = [['Placa', 'Km', 'Vel. Máx.', 'Combustível']]
                        
                        for vehicle in vehicles_list:
                            vehicle_data.append([
                                vehicle['placa'],
                                f"{vehicle['km_periodo']:.0f}",
                                f"{vehicle['vel_max_periodo']:.0f}",
                                f"{vehicle['combustivel_periodo']:.1f}"
                            ])
                        
                        vehicles_table = Table(vehicle_data, colWidths=[1.5*inch, 1.5*inch, 1.5*inch, 1.5*inch])
                        vehicles_table.setStyle(TableStyle([
                            ('BACKGROUND', (0, 0), (-1, 0), period_color),
                            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                            ('FONTSIZE', (0, 0), (-1, 0), 9),
                            ('BACKGROUND', (0, 1), (-1, -1), period_color.clone(alpha=0.1)),
                            ('FONTSIZE', (0, 1), (-1, -1), 8),
                            ('ALIGN', (0, 1), (-1, -1), 'CENTER'),
                            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#BDC3C7')),
                            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                        ]))
                        
                        story.append(vehicles_table)
                        story.append(Spacer(1, 10))
            
            story.append(Spacer(1, 15))  # Espaço entre grupos
        
        if len(periodos_diarios) > 7:
            story.append(Paragraph(f"<i>Nota: Exibindo os 7 dias mais recentes. Total de {len(periodos_diarios)} dias disponíveis.</i>", self.styles['ObservationStyle']))
        
        story.append(Spacer(1, 20))
    
    def _add_performance_rankings(self, story, structured_data):
        """Adiciona ranking único estilo campeonato (classificação)"""
        story.append(Paragraph("4. Ranking de Desempenho Custo/Benefício", self.styles['SectionTitle']))
        
        # Usa o novo ranking campeonato
        ranking_campeonato = structured_data.get('ranking_campeonato', {})
        
        if not ranking_campeonato or not ranking_campeonato.get('veiculos'):
            story.append(Paragraph("Nenhum dado de ranking disponível.", self.styles['Normal']))
            return
        
        story.append(Paragraph(ranking_campeonato.get('titulo', 'Rankings'), self.styles['SubsectionTitle']))
        story.append(Paragraph(f"<i>{ranking_campeonato.get('descricao', '')}</i>", self.styles['Normal']))
        story.append(Spacer(1, 10))
        
        # Tabela única estilo campeonato
        ranking_data = [['Posição', 'Placa', 'Km', 'Combustível', 'Vel. Máx.', 'Score C/B']]
        
        veiculos = ranking_campeonato['veiculos']
        for vehicle in veiculos:
            posicao = vehicle['posicao_ranking']
            ranking_data.append([
                f"{posicao}º",
                vehicle['placa'],
                f"{vehicle['km_total']:,.0f}",
                f"{vehicle['combustivel']:.1f}L",  # Mostra combustível em litros
                f"{vehicle['velocidade_maxima']:.0f}",
                f"{vehicle['score_custo_beneficio']:.2f}"
            ])
        
        ranking_table = Table(ranking_data, colWidths=[0.8*inch, 1*inch, 1*inch, 1*inch, 1*inch, 1*inch])
        
        # Estilo da tabela com cores para top 3 e bottom 3
        table_style = [
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2C3E50')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('ALIGN', (0, 1), (-1, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#BDC3C7')),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]
        
        # Aplica cores: verde para top 3, vermelho para bottom 3
        for i, vehicle in enumerate(veiculos, 1):
            row_idx = i  # +1 porque primeira linha é header
            categoria = vehicle.get('categoria_ranking', 'normal')
            
            if categoria == 'top3':
                # Verde para top 3
                table_style.append(('BACKGROUND', (0, row_idx), (-1, row_idx), colors.HexColor('#D5EDDA')))
                table_style.append(('TEXTCOLOR', (0, row_idx), (-1, row_idx), colors.HexColor('#155724')))
            elif categoria == 'bottom3':
                # Vermelho para bottom 3
                table_style.append(('BACKGROUND', (0, row_idx), (-1, row_idx), colors.HexColor('#F8D7DA')))
                table_style.append(('TEXTCOLOR', (0, row_idx), (-1, row_idx), colors.HexColor('#721C24')))
            else:
                # Cinza claro para o meio
                table_style.append(('BACKGROUND', (0, row_idx), (-1, row_idx), colors.HexColor('#F8F9FA')))
        
        ranking_table.setStyle(TableStyle(table_style))
        
        story.append(ranking_table)
        story.append(Spacer(1, 15))
        
        # Legenda das cores
        legend_text = [
            "<b>Legenda:</b>",
            "• 🟢 <b>Verde:</b> Top 3 (melhores desempenhos)",
            "• 🔴 <b>Vermelho:</b> Bottom 3 (desempenhos críticos)",
            "• ⚪ <b>Cinza:</b> Desempenho intermediário"
        ]
        
        for legend in legend_text:
            if legend.startswith('<b>Legenda:</b>'):
                story.append(Paragraph(legend, self.styles['Normal']))
            else:
                story.append(Paragraph(legend, self.styles['ObservationStyle']))
        
        story.append(Spacer(1, 20))
    
    def _create_cost_benefit_ranking_table(self, story, ranking, header_color, bg_color):
        """Cria tabela de ranking custo/benefício sem coluna cliente"""
        categoria = ranking['categoria']
        veiculos = ranking['veiculos']
        criterio = ranking['criterio']
        descricao = ranking.get('descricao', '')
        
        story.append(Paragraph(f"<b>{categoria}:</b>", self.styles['Normal']))
        if descricao:
            story.append(Paragraph(f"<i>{descricao}</i>", self.styles['ObservationStyle']))
        
        ranking_data = [['Posição', 'Placa', 'Km', 'Combustível', 'Vel. Máx.', 'Score C/B']]
        
        for i, vehicle in enumerate(veiculos, 1):
            if criterio == 'score_custo_beneficio':
                score_value = f"{vehicle['score_custo_beneficio']:.2f}"
            else:
                score_value = "N/A"
            
            ranking_data.append([
                f"{i}º",
                vehicle['placa'],
                f"{vehicle['km_total']:,.0f}",
                f"{vehicle['combustivel']:.1f}L",  # Mostra combustível em litros
                f"{vehicle['velocidade_maxima']:.0f}",
                score_value
            ])
        
        ranking_table = Table(ranking_data, colWidths=[0.8*inch, 1*inch, 1*inch, 1*inch, 1*inch, 1*inch])
        ranking_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), header_color),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('BACKGROUND', (0, 1), (-1, -1), bg_color),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('ALIGN', (0, 1), (-1, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#BDC3C7')),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))
        
        story.append(ranking_table)
        story.append(Spacer(1, 10))
    
    def _create_ranking_table(self, story, ranking, header_color, bg_color):
        categoria = ranking['categoria']
        veiculos = ranking['veiculos'][:3]
        criterio = ranking['criterio']
        
        story.append(Paragraph(f"<b>{categoria}:</b>", self.styles['Normal']))
        
        ranking_data = [['Posição', 'Placa', 'Cliente', 'Valor']]
        for i, vehicle in enumerate(veiculos, 1):
            if criterio == 'km_total':
                valor = f"{vehicle['km_total']:,.1f} km"
            elif criterio == 'eficiencia':
                valor = f"{vehicle['eficiencia']:.1f} km/L"
            else:
                valor = "N/A"
            
            ranking_data.append([
                f"{i}º",
                vehicle['placa'],
                vehicle['cliente'][:20] + '...' if len(vehicle['cliente']) > 20 else vehicle['cliente'],
                valor
            ])
        
        ranking_table = Table(ranking_data, colWidths=[0.8*inch, 1*inch, 2*inch, 1.2*inch])
        ranking_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), header_color),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('BACKGROUND', (0, 1), (-1, -1), bg_color),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('ALIGN', (0, 1), (-1, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#BDC3C7')),
        ]))
        
        story.append(ranking_table)
        story.append(Spacer(1, 10))
    
    def _add_daily_performance(self, story, structured_data):
        """Adiciona desempenho diário da frota sem coluna cliente"""
        story.append(Paragraph("5. Detalhamento por Dia", self.styles['SectionTitle']))
        
        por_dia = structured_data['por_dia']
        if not por_dia:
            story.append(Paragraph("Nenhum dado diário disponível.", self.styles['Normal']))
            return
        
        story.append(Paragraph(
            "Desempenho diário com dados resumidos de todos os veículos:",
            self.styles['Normal']
        ))
        story.append(Spacer(1, 10))
        
        # Tabela consolidada por dia
        daily_data = [['Data', 'Veículos Ativos', 'Km Total', 'Combustível Total']]
        
        # Organiza datas para identificar finais de semana consecutivos
        sorted_dates = sorted(por_dia.items())
        processed_dates = set()
        
        for i, (date_str, vehicles_day) in enumerate(sorted_dates):
            if date_str in processed_dates:
                continue
                
            date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            
            # Verifica se é sábado e se o próximo dia é domingo
            if (date_obj.weekday() == 5 and  # Sábado
                i + 1 < len(sorted_dates) and
                datetime.strptime(sorted_dates[i + 1][0], '%Y-%m-%d').weekday() == 6):  # Domingo
                
                # Consolida sábado + domingo
                sunday_date_str, sunday_vehicles = sorted_dates[i + 1]
                
                # Soma os dados dos dois dias
                total_km_weekend = (sum(v['km_dia'] for v in vehicles_day) + 
                                  sum(v['km_dia'] for v in sunday_vehicles))
                total_fuel_weekend = (sum(v['combustivel_dia'] for v in vehicles_day) + 
                                    sum(v['combustivel_dia'] for v in sunday_vehicles))
                
                # Conta veículos únicos nos dois dias
                all_weekend_vehicles = set(v['placa'] for v in vehicles_day)
                all_weekend_vehicles.update(v['placa'] for v in sunday_vehicles)
                num_vehicles_weekend = len(all_weekend_vehicles)
                
                # Formata as datas
                saturday_formatted = date_obj.strftime('%d/%m/%Y')
                sunday_formatted = datetime.strptime(sunday_date_str, '%Y-%m-%d').strftime('%d/%m/%Y')
                
                daily_data.append([
                    f"{saturday_formatted} + {sunday_formatted}",  # Final de semana
                    str(num_vehicles_weekend),
                    f"{total_km_weekend:,.0f}",
                    f"{total_fuel_weekend:.1f}"
                ])
                
                # Marca ambas as datas como processadas
                processed_dates.add(date_str)
                processed_dates.add(sunday_date_str)
                
            else:
                # Dia individual (ou domingo solto)
                total_km_day = sum(v['km_dia'] for v in vehicles_day)
                total_fuel_day = sum(v['combustivel_dia'] for v in vehicles_day)
                num_vehicles = len(vehicles_day)
                
                daily_data.append([
                    date_obj.strftime('%d/%m/%Y'),
                    str(num_vehicles),
                    f"{total_km_day:,.0f}",
                    f"{total_fuel_day:.1f}"
                ])
                
                processed_dates.add(date_str)
        
        # Limita a 10 dias mais recentes para não sobrecarregar o PDF
        if len(daily_data) > 11:  # 1 header + 10 days
            daily_data = [daily_data[0]] + daily_data[-10:]
            story.append(Paragraph("<i>Mostrando os 10 dias mais recentes</i>", self.styles['ObservationStyle']))
        
        daily_table = Table(daily_data, colWidths=[1.5*inch, 1.5*inch, 1.5*inch, 1.5*inch])
        daily_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#34495E')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#ECF0F1')),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('ALIGN', (0, 1), (-1, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#BDC3C7')),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))
        
        story.append(daily_table)
        story.append(Spacer(1, 30))
    
    def _add_footer_observations(self, story):
        """Adiciona observações e metodologia no rodapé"""
        story.append(Paragraph("6. Observações e Metodologia", self.styles['SectionTitle']))
        
        observations = [
            "<b>Períodos Operacionais:</b>",
            "• Operacional: 04:00-07:00, 10:50-13:00, 16:50-19:00 (seg-sex)",
            "• Fora do Horário: 07:00-10:50, 13:00-16:50, 19:00-04:00 (seg-sex)",
            "• Final de Semana: sábados e domingos (período completo)",
            "",
            "<b>Cálculo de Score Custo/Benefício:</b>",
            "• Quilometragem (40%): maior valor = melhor desempenho",
            "• Combustível (40%): menor consumo = melhor desempenho",
            "• Controle velocidade (20%): menores picos = melhor desempenho",
            "• Penalidade proporcional: -0.02 pontos por cada km/h acima de 100",
            "",
            "<b>Estimativas:</b>",
            "• Combustível estimado com base no consumo médio do cliente",
            "• Dados sujeitos à precisão dos equipamentos de telemetria",
            "",
            "<b>Cores das Tabelas:</b>",
            "• Verde: períodos operacionais",
            "• Laranja: fora do horário operacional",
            "• Cinza: final de semana"
        ]
        
        for obs in observations:
            if obs == "":
                story.append(Spacer(1, 5))
            else:
                story.append(Paragraph(obs, self.styles['ObservationStyle']))
        
        story.append(Spacer(1, 20))
        
        # Data de geração
        data_geracao = datetime.now().strftime('%d/%m/%Y às %H:%M')
        story.append(Paragraph(
            f"<i>Relatório gerado em: {data_geracao}</i>",
            self.styles['ObservationStyle']
        ))

if __name__ == "__main__":
    # Teste do gerador
    print("Gerador de relatórios PDF carregado com sucesso!")
