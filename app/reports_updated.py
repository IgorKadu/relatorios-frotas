"""
Módulo para geração de relatórios PDF com insights de telemetria veicular.
"""

import os
import base64
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
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
from html import escape
import pandas as pd
import numpy as np
import logging

# Logger padronizado do módulo (evita NameError e facilita auditoria)
logger = logging.getLogger("relatorios_frotas.reports")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter(
        fmt='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
    ))
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)

from .services import ReportGenerator, DataQualityRules, PeriodAggregator, HighlightGenerator, TelemetryAnalyzer
from .models import get_session, Veiculo, Cliente


def format_speed(speed: Optional[float], distance_km: Optional[float] = None, include_unit: bool = True, decimals: int = 0) -> str:
    """
    Formata velocidade máxima com regras de negócio e locale BR.
    Regras:
    - Ocultar (retornar '—') quando velocidade == 0 e km_total > 0.
    - Quando km_total == 0 e velocidade == 0, exibir "0 km/h" (ou "0" se include_unit=False).
    - Tratar None/negativos como 0.
    - Aplicar separadores brasileiros e casas decimais configuráveis (padrão 0).
    """
    # Sanitização de entradas
    try:
        v = float(speed or 0)
    except (ValueError, TypeError):
        v = 0.0
    if v < 0:
        v = 0.0

    dist = None
    if distance_km is not None:
        try:
            dist = float(distance_km or 0)
        except (ValueError, TypeError):
            dist = 0.0
        if dist < 0:
            dist = 0.0

    # Regra de ocultação
    if v == 0.0 and (dist is not None and dist > 0):
        return '—'

    # Formatação padrão BR
    text = f"{v:,.{decimals}f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{text} km/h" if include_unit else text


class PDFReportGenerator:
    """Classe para gerar relatórios PDF profissionais"""
    
    def __init__(self):
        self.report_generator = ReportGenerator()
        self.analyzer = None  # Será inicializado quando necessário
        self.styles = getSampleStyleSheet()
        self.setup_custom_styles()
    
    def _get_analyzer(self):
        """Inicializa o analisador se necessário"""
        if self.analyzer is None:
            from .services import TelemetryAnalyzer
            self.analyzer = TelemetryAnalyzer()
        return self.analyzer
    
    def _format_distance(self, km_value: float, decimals: int = 1) -> str:
        """Formata distância de modo inteligente: usa metros quando < 1 km, caso contrário km."""
        try:
            if km_value is None:
                return '0 m'
            if km_value < 0:
                km_value = 0
            if km_value < 1:
                metros = round(km_value * 1000)
                return f"{metros:,} m".replace(',', '.')
            fmt = f"{{:,.{decimals}f}} km"
            return fmt.format(km_value).replace(',', 'X').replace('.', ',').replace('X', '.')
        except Exception:
            try:
                return f"{float(km_value):.{decimals}f} km"
            except Exception:
                return '0 km'
    
    def setup_custom_styles(self):
        """Configura estilos customizados para o PDF"""
        # Estilo do título principal
        self.styles.add(ParagraphStyle(
            name='TitleStyle',
            parent=self.styles['Title'],
            fontSize=26,
            textColor=colors.HexColor('#1A4B8C'),
            alignment=TA_CENTER,
            spaceAfter=25,
            fontName='Helvetica-Bold'
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
        
        # Estilos padronizados com o consolidado
        try:
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
        except KeyError:
            pass
        try:
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
        except KeyError:
            pass
        
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
    
    def create_cover_page(self, metrics: Dict, report_type: str = "default") -> List:
        """Cria a página de capa do relatório"""
        story = []
        
        # Título principal com tipo de relatório
        if report_type == "daily":
            title = f"Relatório Diário de Telemetria Veicular"
        elif report_type == "weekly":
            title = f"Relatório Semanal de Telemetria Veicular"
        elif report_type == "biweekly":
            title = f"Relatório Quinzenal de Telemetria Veicular"
        elif report_type == "monthly":
            title = f"Relatório Mensal de Telemetria Veicular"
        else:
            title = f"Relatório de Telemetria Veicular"
            
        story.append(Paragraph(escape(title), self.styles['TitleStyle']))
        
        # Informações do veículo
        veiculo_info = metrics.get('veiculo', {})
        cliente = escape(str(veiculo_info.get('cliente', 'N/A')))
        placa = escape(str(veiculo_info.get('placa', 'N/A')))
        
        story.append(Spacer(1, 30))
        
        # Dados do cliente e veículo
        info_text = f"""
        <b>Cliente:</b> {cliente}<br/>
        <b>Placa do Veículo:</b> {placa}<br/>
        <b>Tipo de Relatório:</b> {title.replace('Relatório ', '')}<br/>
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
        story.append(Paragraph(f"Relatório gerado em: {escape(data_geracao)}", 
                              self.styles['Normal']))
        
        story.append(PageBreak())
        return story
    
    def create_executive_summary(self, metrics: Dict, insights: List[str]) -> List:
        """Cria o sumário executivo"""
        story = []
        
        story.append(Paragraph("1. Sumário Executivo", self.styles.get('SectionTitle', self.styles['SubtitleStyle'])))
        
        operacao = metrics.get('operacao', {})
        
        # Métricas principais em tabela
        summary_data = [
            ['Métrica', 'Valor'],
            ['Total de Registros', f"{operacao.get('total_registros', 0):,}"],
            ['Quilometragem Total', self._format_distance(operacao.get('km_total', 0), decimals=2)],
            ['Velocidade Máxima', format_speed(operacao.get('velocidade_maxima', 0), operacao.get('km_total', 0), include_unit=True, decimals=0)],
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
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2E86AB')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F4F6F7')),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#BDC3C7')),
            # Prevenção de quebras
            ('NOSPLIT', (0, 0), (-1, -1)),
            ('WORDWRAP', (0, 0), (-1, -1)),
            ('SPLITLONGWORDS', (0, 0), (-1, -1), True)
        ]))
        
        story.append(summary_table)
        story.append(Spacer(1, 20))
        
        # Principais insights
        story.append(Paragraph("Principais Insights:", self.styles['SubtitleStyle']))
        
        for insight in insights[:5]:  # Limita a 5 insights principais
            story.append(Paragraph(f"• {escape(str(insight))}", self.styles['InsightStyle']))
        
        return story
    
    def create_period_performance(self, metrics: Dict) -> List:
        """Adiciona a seção 'Desempenho Geral no Período' padronizada (igual ao consolidado)
        para um único veículo (uma linha).
        """
        story = []
        veiculo_info = metrics.get('veiculo', {})
        operacao = metrics.get('operacao', {})
        fuel = metrics.get('combustivel', {})

        # Título padronizado da seção
        story.append(Paragraph("2. Desempenho Geral no Período", self.styles.get('SectionTitle', self.styles['SubtitleStyle'])))
        story.append(Paragraph(
            "Tabela consolidada com dados gerais do veículo no período:",
            self.styles['Normal']
        ))
        story.append(Spacer(1, 10))

        # Cabeçalho e linha única (veículo atual)
        headers = ['Placa', 'Km', 'Vel. Máx.', 'Combustível', 'Eficiência']
        row = [
            veiculo_info.get('placa', 'N/A'),
            self._format_distance(operacao.get('km_total', 0), decimals=2),
            format_speed(operacao.get('velocidade_maxima', 0), operacao.get('km_total', 0), include_unit=False, decimals=0),
            f"{fuel.get('fuel_consumed_liters', 0.0):.1f}",
            f"{fuel.get('efficiency_kmL', 0.0):.1f}"
        ]

        table = Table([headers, row], colWidths=[1.2*inch, 1.2*inch, 1.2*inch, 1.2*inch, 1.2*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2E86AB')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F4F6F7')),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('ALIGN', (0, 1), (-1, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#BDC3C7')),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            # Prevenções de quebras
            ('NOSPLIT', (0, 0), (-1, -1)),
            ('WORDWRAP', (0, 0), (-1, -1)),
            ('SPLITLONGWORDS', (0, 0), (-1, -1), True),
        ]))

        story.append(table)
        story.append(Spacer(1, 20))
        return story
    
    def _create_performance_chart(self, metrics: Dict) -> Optional[Drawing]:
        """Cria um gráfico de desempenho com base nos dados do veículo"""
        try:
            # Verifica se temos dados suficientes
            operacao = metrics.get('operacao', {})
            periodos = metrics.get('periodos', {})
            
            if not operacao or not periodos:
                return None
            
            # Cria um gráfico de barras simples
            drawing = Drawing(400, 200)
            
            # Dados para o gráfico
            period