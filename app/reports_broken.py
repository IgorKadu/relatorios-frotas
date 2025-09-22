"""
Módulo para geração de relatórios PDF com insights de telemetria veicular.
"""

import os
import base64
from datetime import datetime, timedelta
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


def format_weekend_title(start_date: datetime, end_date: datetime) -> str:
    """
    Formata o título do final de semana de forma padronizada e profissional,
    exibindo o intervalo Sábado + Domingo neste formato: "Final de Semana (21/09/2025 + 22/09/2025)".
    """
    interval = format_weekend_interval(start_date, end_date)
    return f"Final de Semana ({interval})" if interval else "Final de Semana"


def format_weekend_interval(start_date: datetime, end_date: datetime) -> str:
    """
    Retorna apenas o intervalo de datas do final de semana (Sábado - Domingo) no
    formato "dd/mm/yyyy - dd/mm/yyyy". Se não encontrar o par completo, retorna vazio.
    """
    saturday = None
    sunday = None
    current_date = start_date

    # Primeiro, tenta encontrar um par consecutivo Sábado->Domingo
    while current_date <= end_date:
        if current_date.weekday() == 5:  # Sábado
            nxt = current_date + timedelta(days=1)
            if nxt <= end_date and nxt.weekday() == 6:  # Domingo
                saturday = current_date
                sunday = nxt
                break
        current_date += timedelta(days=1)

    # Se não encontrou par consecutivo, tenta localizar separadamente
    if not (saturday and sunday):
        current_date = start_date
        while current_date <= end_date and (not saturday or not sunday):
            if current_date.weekday() == 5 and not saturday:
                saturday = current_date
            if current_date.weekday() == 6 and not sunday:
                sunday = current_date
            current_date += timedelta(days=1)

    if saturday and sunday:
        return f"{saturday.strftime('%d/%m/%Y')} + {sunday.strftime('%d/%m/%Y')}"
    return ""


def safe_numeric_sum(data_list: List, field: str) -> float:
    """
    Soma valores numéricos de uma lista de forma segura
    """
    total = 0.0
    for item in data_list:
        value = item.get(field, 0)
        try:
            total += float(value or 0)
        except (ValueError, TypeError):
            continue
    return total


def safe_numeric_max(data_list: List, field: str) -> float:
    """
    Encontra o valor máximo de uma lista de forma segura
    """
    max_val = 0.0
    for item in data_list:
        value = item.get(field, 0)
        try:
            max_val = max(max_val, float(value or 0))
        except (ValueError, TypeError):
            continue
    return max_val

# =====================
# Helper de formatação de velocidade (nível de módulo)
# =====================
from typing import Optional

def _format_br_number(value: float, decimals: int = 0) -> str:
    """Formata número no padrão brasileiro: milhar com ponto e decimais com vírgula."""
    try:
        v = float(value or 0)
    except (ValueError, TypeError):
        v = 0.0
    formatted = f"{v:,.{decimals}f}"
    # Converte padrão en_US -> pt_BR
    return formatted.replace(",", "X").replace(".", ",").replace("X", ".")

# ==============================
# ESTRATÉGIAS PARA DIFERENTES PERÍODOS
# ==============================

class ReportStrategy:
    """Interface base para estratégias de relatório"""
    
    def __init__(self, styles):
        self.styles = styles
    
    def build_content(self, story: List, structured_data: Dict, data_inicio: datetime, 
                     data_fim: datetime, total_km: float, total_fuel: float) -> None:
        """Constrói o conteúdo específico da estratégia"""
        raise NotImplementedError


class DailyWeeklyStrategy(ReportStrategy):
    """
    Estratégia para relatórios diários e semanais (≤7 dias)
    Foco em dados específicos e detalhados para cada dia/semana
    """
    
    def build_content(self, story: List, structured_data: Dict, data_inicio: datetime, 
                     data_fim: datetime, total_km: float, total_fuel: float) -> None:
        """Constrói conteúdo para períodos curtos com máximo detalhamento usando DADOS REAIS"""
        
        # Inicializa analisador para buscar dados reais do banco
        analyzer = TelemetryAnalyzer()
        
        try:
            # Coleta dados reais de cada veículo do banco de dados
            vehicles_data = {}
            all_vehicles_daily_data = {}
            
            for vehicle_info in structured_data.get('desempenho_periodo', []):
                placa = vehicle_info.get('placa', 'N/A')
                
                # BUSCA DADOS REAIS DO BANCO DE DADOS
                df_real = analyzer.get_vehicle_data(placa, data_inicio, data_fim)
                
                if not df_real.empty:
                    # Aplica validação de qualidade aos dados reais
                    df_validated = DataQualityRules.validate_telemetry_consistency(df_real)
                    
                    # Gera métricas reais usando o analisador
                    real_metrics = analyzer.generate_summary_metrics(df_validated, placa)
                    
                    # Agrega dados diários reais
                    daily_data_real = PeriodAggregator.aggregate_daily(df_validated)
                    all_vehicles_daily_data[placa] = daily_data_real
                    
                    # Calcula métricas validadas reais
                    km_total_real = real_metrics.get('quilometragem_total', 0)
                    velocidade_max_real = real_metrics.get('velocidade_maxima', 0)
                    velocidade_media_real = real_metrics.get('velocidade_media', 0)
                    tempo_movimento_real = real_metrics.get('tempo_movimento_horas', 0)
                    alertas_real = len(df_validated[df_validated['velocidade_kmh'] > 80]) if not df_validated.empty else 0
                    
                    # Calcula combustível com dados reais
                    fuel_real = DataQualityRules.calculate_fuel_consistency(
                        km_total_real, velocidade_media_real, tempo_movimento_real
                    )
                    
                    vehicles_data[placa] = {
                        'km_total': km_total_real,
                        'velocidade_max': velocidade_max_real,
                        'velocidade_media': velocidade_media_real,
                        'combustivel_estimado': fuel_real,
                        'tempo_movimento_horas': tempo_movimento_real,
                        'alertas_velocidade': alertas_real,
                        'total_registros': len(df_validated),
                        'registros_originais': len(df_real)
                    }
                else:
                    # Se não há dados, registra como zero (dados reais)
                    vehicles_data[placa] = {
                        'km_total': 0,
                        'velocidade_max': 0,
                        'velocidade_media': 0,
                        'combustivel_estimado': None,
                        'tempo_movimento_horas': 0,
                        'alertas_velocidade': 0,
                        'total_registros': 0,
                        'registros_originais': 0
                    }
                    all_vehicles_daily_data[placa] = {}
            
            # 1. Resumo geral com dados reais validados
            self._add_validated_summary(story, structured_data, vehicles_data, total_km, total_fuel)
            
            # 2. Detalhamento diário com dados reais
            self._add_daily_detailed_breakdown_real(story, all_vehicles_daily_data, data_inicio, data_fim)
            
            # 3. Análise por período operacional com dados reais
            self._add_operational_periods_analysis_real(story, vehicles_data, all_vehicles_daily_data)
            
            # 4. Tabelas de performance por veículo com dados reais
            self._add_vehicle_performance_tables(story, vehicles_data)
            
            # 5. Insights baseados em dados reais
            self._add_real_data_insights(story, vehicles_data, all_vehicles_daily_data, data_inicio, data_fim)
            
        finally:
            # Sempre fecha a sessão do banco
            if hasattr(analyzer, 'session'):
                analyzer.session.close()
    
    def _add_validated_summary(self, story: List, structured_data: Dict, vehicles_data: Dict, total_km: float, total_fuel: float) -> None:
        """Adiciona resumo com dados validados"""
        story.append(Paragraph("<b>RESUMO EXECUTIVO - DADOS VALIDADOS</b>", self.styles['Heading2Style']))
        story.append(Spacer(1, 10))
        
        # Calcula métricas validadas
        valid_vehicles = len([v for v in vehicles_data.values() if v['km_total'] > 0])
        total_valid_km = sum([v['km_total'] for v in vehicles_data.values()])
        total_alerts = sum([v['alertas_velocidade'] for v in vehicles_data.values()])
        
        summary_data = [
            ['Métrica', 'Valor', 'Observações'],
            ['Veículos com Operação Válida', str(valid_vehicles), 'Dados consistentes KM/Velocidade'],
            ['Quilometragem Validada', f"{total_valid_km:,.1f} km".replace(',', '.'), 'Apenas dados consistentes'],
            ['Alertas de Velocidade', str(total_alerts), 'Excesso de velocidade registrado'],
            ['Taxa de Dados Válidos', f"{(total_valid_km/max(total_km, 1)):.1%}", 'Proporção de dados utilizáveis']
        ]
        
        table = Table(summary_data, colWidths=[3*inch, 2*inch, 3*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2E7D32')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black)
        ]))
        
        story.append(table)
        story.append(Spacer(1, 20))
    
    def _add_daily_detailed_breakdown_real(self, story: List, all_vehicles_daily_data: Dict, data_inicio: datetime, data_fim: datetime) -> None:
        """Adiciona detalhamento dia por dia usando DADOS REAIS do banco"""
        story.append(Paragraph("<b>DETALHAMENTO DIÁRIO - DADOS REAIS</b>", self.styles['Heading2Style']))
        story.append(Spacer(1, 10))
        
        current_date = data_inicio
        while current_date <= data_fim:
            day_name = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo'][current_date.weekday()]
            
            story.append(Paragraph(
                f"<b>{day_name}, {current_date.strftime('%d/%m/%Y')}</b>", 
                self.styles['Heading3Style']
            ))
            
            # Calcula dados reais do dia para todos os veículos
            day_totals = {
                'km_total': 0,
                'alertas_total': 0,
                'veiculos_operando': 0,
                'registros_total': 0
            }
            
            for placa, daily_data in all_vehicles_daily_data.items():
                if daily_data and current_date.date() in daily_data:
                    day_info = daily_data[current_date.date()]
                    day_totals['km_total'] += day_info.get('km_total', 0)
                    day_totals['alertas_total'] += day_info.get('alertas_velocidade', 0)
                    day_totals['registros_total'] += day_info.get('total_registros', 0)
                    if day_info.get('km_total', 0) > 0:
                        day_totals['veiculos_operando'] += 1
            
            # Status baseado em dados reais
            if day_totals['veiculos_operando'] == 0:
                status = "🔴 Sem operação registrada"
            elif day_totals['alertas_total'] > 10:
                status = f"⚠️ {day_totals['alertas_total']} alertas de velocidade - Atenção necessária"
            elif day_totals['alertas_total'] > 0:
                status = f"🟡 {day_totals['alertas_total']} alertas - Operação com cuidados"
            else:
                status = "🟢 Operação normal - Sem alertas críticos"
            
            # Detalhes do dia com dados reais
            details_text = f"""
            <i>Status:</i> {status}<br/>
            <i>Quilometragem Total:</i> {day_totals['km_total']:,.1f} km<br/>
            <i>Veículos Operando:</i> {day_totals['veiculos_operando']}<br/>
            <i>Registros de Telemetria:</i> {day_totals['registros_total']:,}
            """.replace(',', '.')
            
            story.append(Paragraph(details_text, self.styles['Normal']))
            story.append(Spacer(1, 10))
            
            current_date += timedelta(days=1)
    
    def _add_operational_periods_analysis_real(self, story: List, vehicles_data: Dict, all_vehicles_daily_data: Dict) -> None:
        """Análise detalhada dos períodos operacionais usando DADOS REAIS"""
        story.append(Paragraph("<b>ANÁLISE POR PERÍODO OPERACIONAL - DADOS REAIS</b>", self.styles['Heading2Style']))
        story.append(Spacer(1, 10))
        
        # Agrega dados de períodos operacionais de todos os veículos e dias
        period_totals = {
            'operacional_manha': {'km': 0, 'registros': 0},
            'operacional_meio_dia': {'km': 0, 'registros': 0},
            'operacional_tarde': {'km': 0, 'registros': 0},
            'final_semana': {'km': 0, 'registros': 0},
            'fora_horario': {'km': 0, 'registros': 0}
        }
        
        # Calcula totais reais por período
        for placa, daily_data in all_vehicles_daily_data.items():
            for date, day_info in daily_data.items():
                periodos = day_info.get('periodos_operacionais', {})
                km_day = day_info.get('km_total', 0)
                registros_day = day_info.get('total_registros', 0)
                
                # Distribui proporcionalmente por período baseado nos registros reais
                total_period_records = sum(periodos.values()) if periodos else 1
                
                for period, count in periodos.items():
                    if period in period_totals:
                        proportion = count / total_period_records if total_period_records > 0 else 0
                        period_totals[period]['km'] += km_day * proportion
                        period_totals[period]['registros'] += count
                    elif period.startswith('fora_horario'):
                        proportion = count / total_period_records if total_period_records > 0 else 0
                        period_totals['fora_horario']['km'] += km_day * proportion
                        period_totals['fora_horario']['registros'] += count
        
        # Cria tabela com dados reais de períodos
        periods_data = [['Período', 'KM Total', 'Registros', 'Percentual KM']]
        
        total_km_all_periods = sum([p['km'] for p in period_totals.values()])
        
        periods_info = [
            ('Manhã (04:00-07:00)', 'operacional_manha'),
            ('Meio-dia (10:50-13:00)', 'operacional_meio_dia'), 
            ('Tarde (16:50-19:00)', 'operacional_tarde'),
            ('Final de Semana', 'final_semana'),
            ('Fora de Horário', 'fora_horario')
        ]
        
        for period_name, period_key in periods_info:
            km = period_totals[period_key]['km']
            registros = period_totals[period_key]['registros']
            percentual = (km / total_km_all_periods * 100) if total_km_all_periods > 0 else 0
            
            periods_data.append([
                period_name,
                f"{km:,.1f} km".replace(',', '.'),
                f"{registros:,}".replace(',', '.'),
                f"{percentual:.1f}%"
            ])
        
        table = Table(periods_data, colWidths=[3*inch, 2*inch, 1.5*inch, 1.5*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#795548')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black)
        ]))
        
        story.append(table)
        story.append(Spacer(1, 15))
    
    def _add_real_data_insights(self, story: List, vehicles_data: Dict, all_vehicles_daily_data: Dict, data_inicio: datetime, data_fim: datetime) -> None:
        """Adiciona insights baseados em dados reais do banco"""
        story.append(Paragraph("<b>INSIGHTS BASEADOS EM DADOS REAIS</b>", self.styles['Heading2Style']))
        story.append(Spacer(1, 10))
        
        # Gera highlights reais usando o sistema de highlights
        highlights = HighlightGenerator.compute_highlights(
            all_vehicles_daily_data,
            {},  # Weekly data não aplicável para período curto
            vehicles_data
        )
        
        # Mostra insights reais
        if highlights.get('insights_gerais'):
            story.append(Paragraph("<b>📊 Insights Automáticos:</b>", self.styles['Heading3Style']))
            for insight in highlights['insights_gerais']:
                story.append(Paragraph(f"• {insight}", self.styles['Normal']))
            story.append(Spacer(1, 10))
        
        # Mostra alertas reais 
        if highlights.get('alertas_importantes'):
            story.append(Paragraph("<b>⚠️ Alertas Identificados:</b>", self.styles['Heading3Style']))
            for alerta in highlights['alertas_importantes']:
                story.append(Paragraph(f"• {alerta}", self.styles['Normal']))
            story.append(Spacer(1, 10))
        
        # Estatísticas gerais dos dados reais
        total_vehicles = len(vehicles_data)
        active_vehicles = len([v for v in vehicles_data.values() if v['km_total'] > 0])
        total_km_real = sum([v['km_total'] for v in vehicles_data.values()])
        total_alerts_real = sum([v['alertas_velocidade'] for v in vehicles_data.values()])
        total_records = sum([v['total_registros'] for v in vehicles_data.values()])
        
        stats_text = f"""
        <b>📈 Estatísticas do Período (Dados Reais):</b><br/>
        • Total de veículos analisados: {total_vehicles}<br/>
        • Veículos com operação: {active_vehicles}<br/>
        • Quilometragem total validada: {total_km_real:,.1f} km<br/>
        • Total de alertas de velocidade: {total_alerts_real}<br/>
        • Registros de telemetria processados: {total_records:,}<br/>
        • Período analisado: {(data_fim - data_inicio).days + 1} dias
        """.replace(',', '.')
        
        story.append(Paragraph(stats_text, self.styles['Normal']))
        story.append(Spacer(1, 20))
    
    def _add_vehicle_performance_tables(self, story: List, vehicles_data: Dict) -> None:
        """Tabelas de performance individual por veículo"""
        story.append(Paragraph("<b>PERFORMANCE POR VEÍCULO</b>", self.styles['Heading2Style']))
        story.append(Spacer(1, 10))
        
        if vehicles_data:
            performance_data = [['Placa', 'KM Total', 'Combustível Est.', 'Alertas', 'Status']]
            
            for placa, data in vehicles_data.items():
                km = data['km_total']
                fuel = data['combustivel_estimado']
                alerts = data['alertas_velocidade']
                
                status = "✅ Dados Válidos" if km > 0 and fuel else "❌ Dados Inconsistentes"
                fuel_text = f"{fuel:.1f}L" if fuel else "—"
                
                performance_data.append([
                    placa,
                    f"{km:,.1f} km".replace(',', '.'),
                    fuel_text,
                    str(alerts),
                    status
                ])
            
            table = Table(performance_data, colWidths=[1.5*inch, 1.5*inch, 1.5*inch, 1*inch, 2*inch])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1976D2')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.black)
            ]))
            
            story.append(table)
            story.append(Spacer(1, 20))
    
    def _add_daily_charts(self, story: List, structured_data: Dict, data_inicio: datetime, data_fim: datetime) -> None:
        """Gráficos específicos para análise diária"""
        story.append(Paragraph("<b>GRÁFICOS DE ANÁLISE DIÁRIA</b>", self.styles['Heading2Style']))
        story.append(Spacer(1, 10))
        
        # Placeholder para gráficos específicos
        story.append(Paragraph("• Gráfico de velocidade por hora do dia", self.styles['Normal']))
        story.append(Paragraph("• Distribuição de operação por período", self.styles['Normal']))
        story.append(Paragraph("• Mapa de calor de atividade diária", self.styles['Normal']))
        story.append(Spacer(1, 20))


class MediumTermStrategy(ReportStrategy):
    """
    Estratégia para relatórios de médio prazo (8-30 dias)
    Dados gerais + análise gráfica das semanas + highlights de piores dias e veículos
    """
    
    def build_content(self, story: List, structured_data: Dict, data_inicio: datetime, 
                     data_fim: datetime, total_km: float, total_fuel: float) -> None:
        """Constrói conteúdo para períodos médios com foco em análise semanal usando DADOS REAIS"""
        
        # Inicializa analisador para buscar dados reais do banco
        analyzer = TelemetryAnalyzer()
        
        try:
            # Coleta dados reais de cada veículo do banco de dados
            all_vehicles_data = {}
            all_vehicles_weekly_data = {}
            all_vehicles_daily_data = {}
            
            for vehicle_info in structured_data.get('desempenho_periodo', []):
                placa = vehicle_info.get('placa', 'N/A')
                
                # BUSCA DADOS REAIS DO BANCO DE DADOS
                df_real = analyzer.get_vehicle_data(placa, data_inicio, data_fim)
                
                if not df_real.empty:
                    # Aplica validação de qualidade aos dados reais
                    df_validated = DataQualityRules.validate_telemetry_consistency(df_real)
                    
                    # Gera métricas reais usando o analisador
                    real_metrics = analyzer.generate_summary_metrics(df_validated, placa)
                    
                    # Agrega dados semanais e diários reais
                    weekly_data_real = PeriodAggregator.aggregate_weekly(df_validated)
                    daily_data_real = PeriodAggregator.aggregate_daily(df_validated)
                    
                    all_vehicles_data[placa] = real_metrics
                    all_vehicles_weekly_data[placa] = weekly_data_real
                    all_vehicles_daily_data[placa] = daily_data_real
                else:
                    all_vehicles_data[placa] = {}
                    all_vehicles_weekly_data[placa] = {}
                    all_vehicles_daily_data[placa] = {}
            
            # Agrega dados por semana e identifica highlights REAIS
            weekly_data = self._aggregate_weekly_data_real(all_vehicles_weekly_data)
            highlights = self._compute_period_highlights_real(all_vehicles_data, all_vehicles_weekly_data, all_vehicles_daily_data)
            
            # 1. Resumo geral do período com dados reais
            self._add_general_period_summary(story, structured_data, data_inicio, data_fim, total_km, total_fuel)
            
            # 2. Análise semanal com gráficos baseados em dados reais
            self._add_weekly_analysis_charts_real(story, weekly_data, data_inicio, data_fim)
            
            # 3. Highlights reais: piores e melhores dias
            self._add_daily_highlights_real(story, highlights)
            
            # 4. Rankings de veículos baseados em dados reais
            self._add_vehicle_rankings_real(story, highlights)
            
            # 5. Insights baseados em dados reais do período
            self._add_period_insights_real(story, highlights, weekly_data)
            
        finally:
            # Sempre fecha a sessão do banco
            if hasattr(analyzer, 'session'):
                analyzer.session.close()
    
    def _aggregate_weekly_data_real(self, all_vehicles_weekly_data: Dict) -> Dict:
        """Agrega dados semanais REAIS de todos os veículos"""
        consolidated_weekly = {}
        
        # Agrega dados semanais de todos os veículos
        for placa, weekly_data in all_vehicles_weekly_data.items():
            for week_period, week_info in weekly_data.items():
                if week_period not in consolidated_weekly:
                    consolidated_weekly[week_period] = {
                        'periodo': week_info.get('periodo', week_period),
                        'km_total': 0,
                        'velocidade_max': 0,
                        'tempo_ligado_horas': 0,
                        'tempo_movimento_horas': 0,
                        'alertas_velocidade': 0,
                        'veiculos_operando': 0,
                        'registros_total': 0
                    }
                
                # Soma dados reais de cada veículo
                consolidated_weekly[week_period]['km_total'] += week_info.get('km_total', 0)
                consolidated_weekly[week_period]['velocidade_max'] = max(
                    consolidated_weekly[week_period]['velocidade_max'],
                    week_info.get('velocidade_max', 0)
                )
                consolidated_weekly[week_period]['tempo_ligado_horas'] += week_info.get('tempo_ligado_horas', 0)
                consolidated_weekly[week_period]['tempo_movimento_horas'] += week_info.get('tempo_movimento_horas', 0)
                consolidated_weekly[week_period]['alertas_velocidade'] += week_info.get('alertas_velocidade', 0)
                consolidated_weekly[week_period]['registros_total'] += week_info.get('total_registros', 0)
                
                if week_info.get('km_total', 0) > 0:
                    consolidated_weekly[week_period]['veiculos_operando'] += 1
        
        # Calcula produtividade e eficiência reais
        for week_period, week_data in consolidated_weekly.items():
            if week_data['tempo_movimento_horas'] > 0:
                week_data['produtividade'] = week_data['km_total'] / week_data['tempo_movimento_horas']
            else:
                week_data['produtividade'] = 0
                
        return consolidated_weekly
    
    def _compute_period_highlights_real(self, all_vehicles_data: Dict, all_vehicles_weekly_data: Dict, all_vehicles_daily_data: Dict) -> Dict:
        """Computa highlights REAIS para o período médio usando dados do banco"""
        
        # Usa o sistema de highlights real implementado
        highlights = HighlightGenerator.compute_highlights(
            all_vehicles_daily_data,
            all_vehicles_weekly_data,
            all_vehicles_data
        )
        
        return highlights
    
    def _add_general_period_summary(self, story: List, structured_data: Dict, data_inicio: datetime, 
                                   data_fim: datetime, total_km: float, total_fuel: float) -> None:
        """Resumo geral para períodos médios"""
        period_days = (data_fim - data_inicio).days + 1
        
        story.append(Paragraph("<b>RESUMO GERAL DO PERÍODO</b>", self.styles['Heading2Style']))
        story.append(Spacer(1, 10))
        
        summary_text = f"""
        <b>Período Analisado:</b> {period_days} dias ({data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')})<br/>
        <b>Quilometragem Total:</b> {total_km:,.1f} km<br/>
        <b>Combustível Estimado:</b> {total_fuel:,.1f} litros<br/>
        <b>Produtividade Média:</b> {(total_km/period_days):,.1f} km/dia<br/>
        """
        
        story.append(Paragraph(summary_text.replace(',', '.'), self.styles['Normal']))
        story.append(Spacer(1, 15))
    
    def _add_weekly_analysis_charts_real(self, story: List, weekly_data: Dict, data_inicio: datetime, data_fim: datetime) -> None:
        """Análise semanal com dados reais para períodos médios"""
        story.append(Paragraph("<b>ANÁLISE SEMANAL - DADOS REAIS</b>", self.styles['Heading2Style']))
        story.append(Spacer(1, 10))
        
        # Cria tabela com dados semanais reais
        weekly_table_data = [['Semana', 'KM Total', 'Veículos', 'Produtividade', 'Alertas']]
        
        for week_period, week_info in weekly_data.items():
            km_total = week_info.get('km_total', 0)
            veiculos = week_info.get('veiculos_operando', 0)
            produtividade = week_info.get('produtividade', 0)
            alertas = week_info.get('alertas_velocidade', 0)
            
            weekly_table_data.append([
                week_info.get('periodo', week_period),
                f"{km_total:,.1f} km".replace(',', '.'),
                str(veiculos),
                f"{produtividade:,.1f} km/h".replace(',', '.'),
                str(alertas)
            ])
        
        table = Table(weekly_table_data, colWidths=[2*inch, 1.5*inch, 1*inch, 1.5*inch, 1*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2E7D32')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black)
        ]))
        
        story.append(table)
        story.append(Spacer(1, 20))
    
    def _add_daily_highlights_real(self, story: List, highlights: Dict) -> None:
        """Highlights de dias com dados reais"""
        story.append(Paragraph("<b>📈 MELHORES E PIORES DIAS - DADOS REAIS</b>", self.styles['Heading2Style']))
        story.append(Spacer(1, 10))
        
        # Melhores dias
        if highlights.get('melhores_dias'):
            story.append(Paragraph("<b>🏆 Melhores Dias:</b>", self.styles['Heading3Style']))
            for dia in highlights['melhores_dias'][:3]:  # Top 3
                story.append(Paragraph(f"• {dia.get('data', 'N/A')} - {dia.get('motivo', 'Alta performance')}", self.styles['Normal']))
            story.append(Spacer(1, 10))
        
        # Piores dias
        if highlights.get('piores_dias'):
            story.append(Paragraph("<b>⚠️ Dias com Atenção Necessária:</b>", self.styles['Heading3Style']))
            for dia in highlights['piores_dias'][:3]:  # Top 3
                story.append(Paragraph(f"• {dia.get('data', 'N/A')} - {dia.get('motivo', 'Performance baixa')}", self.styles['Normal']))
            story.append(Spacer(1, 10))
        
        story.append(Spacer(1, 15))
    
    def _add_vehicle_rankings_real(self, story: List, highlights: Dict) -> None:
        """Rankings de veículos com dados reais"""
        story.append(Paragraph("<b>🚛 RANKING DE VEÍCULOS - DADOS REAIS</b>", self.styles['Heading2Style']))
        story.append(Spacer(1, 10))
        
        # Melhor veículo
        if highlights.get('melhor_veiculo'):
            melhor = highlights['melhor_veiculo']
            story.append(Paragraph("<b>🥇 Melhor Performance:</b>", self.styles['Heading3Style']))
            story.append(Paragraph(
                f"Veículo: {melhor.get('placa', 'N/A')} - {melhor.get('km_total', 0):,.1f} km".replace(',', '.'),
                self.styles['Normal']
            ))
            story.append(Spacer(1, 10))
        
        # Pior veículo
        if highlights.get('pior_veiculo'):
            pior = highlights['pior_veiculo']
            story.append(Paragraph("<b>⚠️ Necessita Atenção:</b>", self.styles['Heading3Style']))
            story.append(Paragraph(
                f"Veículo: {pior.get('placa', 'N/A')} - {pior.get('alertas', 0)} alertas",
                self.styles['Normal']
            ))
            story.append(Spacer(1, 10))
        
        story.append(Spacer(1, 15))
    
    def _add_period_insights_real(self, story: List, highlights: Dict, weekly_data: Dict) -> None:
        """Insights do período baseados em dados reais"""
        story.append(Paragraph("<b>💡 INSIGHTS DO PERÍODO - DADOS REAIS</b>", self.styles['Heading2Style']))
        story.append(Spacer(1, 10))
        
        # Insights automáticos baseados em dados reais
        if highlights.get('insights_gerais'):
            story.append(Paragraph("<b>📊 Análises Automáticas:</b>", self.styles['Heading3Style']))
            for insight in highlights['insights_gerais']:
                story.append(Paragraph(f"• {insight}", self.styles['Normal']))
            story.append(Spacer(1, 10))
        
        # Tendências semanais
        if weekly_data:
            weeks_list = list(weekly_data.items())
            if len(weeks_list) >= 2:
                first_week = weeks_list[0][1]
                last_week = weeks_list[-1][1]
                
                km_trend = last_week.get('km_total', 0) - first_week.get('km_total', 0)
                alert_trend = last_week.get('alertas_velocidade', 0) - first_week.get('alertas_velocidade', 0)
                
                story.append(Paragraph("<b>📈 Tendências:</b>", self.styles['Heading3Style']))
                
                if km_trend > 0:
                    story.append(Paragraph(f"• Quilometragem crescente: +{km_trend:,.1f} km na última semana".replace(',', '.'), self.styles['Normal']))
                elif km_trend < 0:
                    story.append(Paragraph(f"• Quilometragem decrescente: {km_trend:,.1f} km na última semana".replace(',', '.'), self.styles['Normal']))
                
                if alert_trend > 0:
                    story.append(Paragraph(f"• ⚠️ Aumento de alertas: +{alert_trend} na última semana", self.styles['Normal']))
                elif alert_trend < 0:
                    story.append(Paragraph(f"• ✅ Redução de alertas: {alert_trend} na última semana", self.styles['Normal']))
                
                story.append(Spacer(1, 10))
        
        story.append(Spacer(1, 20))
    
    def _add_weekly_analysis_charts(self, story: List, weekly_data: Dict, data_inicio: datetime, data_fim: datetime) -> None:
        """Análise gráfica das semanas"""
        story.append(Paragraph("<b>ANÁLISE SEMANAL - GRÁFICOS COMPARATIVOS</b>", self.styles['Heading2Style']))
        story.append(Spacer(1, 10))
        
        # Tabela de dados semanais
        if weekly_data:
            week_table_data = [['Semana', 'Período', 'KM Total', 'Dias Operação', 'Produtividade']]
            
            for week_name, week_info in weekly_data.items():
                week_table_data.append([
                    week_name,
                    week_info['periodo'],
                    f"{week_info['km_total']:,.1f} km".replace(',', '.'),
                    f"{week_info['dias_operacao']} dias",
                    f"{week_info['produtividade']:,.1f} km/dia".replace(',', '.')
                ])
            
            table = Table(week_table_data, colWidths=[1.5*inch, 2*inch, 1.5*inch, 1.3*inch, 1.5*inch])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#FF9800')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.black)
            ]))
            
            story.append(table)
        
        story.append(Spacer(1, 10))
        story.append(Paragraph("📊 <i>Gráficos: Evolução semanal de KM • Comparativo de produtividade • Tendências operacionais</i>", self.styles['Normal']))
        story.append(Spacer(1, 20))
    
    def _add_daily_highlights(self, story: List, highlights: Dict) -> None:
        """Destaca os melhores e piores dias do período"""
        story.append(Paragraph("<b>HIGHLIGHTS DO PERÍODO - MELHORES E PIORES DIAS</b>", self.styles['Heading2Style']))
        story.append(Spacer(1, 10))
        
        # Piores dias
        story.append(Paragraph("<b>🔴 Piores Dias:</b>", self.styles['Heading3Style']))
        for day in highlights.get('piores_dias', []):
            story.append(Paragraph(f"• {day['data']}: {day['motivo']}", self.styles['Normal']))
        
        story.append(Spacer(1, 10))
        
        # Melhores dias
        story.append(Paragraph("<b>🟢 Melhores Dias:</b>", self.styles['Heading3Style']))
        for day in highlights.get('melhores_dias', []):
            story.append(Paragraph(f"• {day['data']}: {day['motivo']}", self.styles['Normal']))
        
        story.append(Spacer(1, 20))
    
    def _add_vehicle_rankings(self, story: List, highlights: Dict) -> None:
        """Rankings de veículos para o período"""
        story.append(Paragraph("<b>RANKING DE VEÍCULOS - MELHOR E PIOR PERFORMANCE</b>", self.styles['Heading2Style']))
        story.append(Spacer(1, 10))
        
        melhor = highlights.get('melhor_veiculo', {})
        pior = highlights.get('pior_veiculo', {})
        
        ranking_data = [
            ['Posição', 'Placa', 'KM Total', 'Performance', 'Observações'],
            ['🥇 Melhor', melhor.get('placa', 'N/A'), f"{melhor.get('km_total', 0):,.1f} km".replace(',', '.'), 
             f"{melhor.get('eficiencia', 0)}%", 'Excelente produtividade'],
            ['🔻 Pior', pior.get('placa', 'N/A'), f"{pior.get('km_total', 0):,.1f} km".replace(',', '.'), 
             f"{pior.get('alertas', 0)} alertas", 'Necessita atenção']
        ]
        
        table = Table(ranking_data, colWidths=[1.2*inch, 1.5*inch, 1.5*inch, 1.5*inch, 2*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4CAF50')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black)
        ]))
        
        story.append(table)
        story.append(Spacer(1, 20))
    
    def _add_period_insights(self, story: List, highlights: Dict, weekly_data: Dict) -> None:
        """Insights e recomendações para o período"""
        story.append(Paragraph("<b>INSIGHTS E RECOMENDAÇÕES</b>", self.styles['Heading2Style']))
        story.append(Spacer(1, 10))
        
        insights_text = """
        <b>📈 Tendências Identificadas:</b><br/>
        • Produtividade crescente ao longo das semanas<br/>
        • Concentração de alertas em dias específicos<br/>
        • Padrão semanal consistente de operação<br/><br/>
        
        <b>🎯 Recomendações:</b><br/>
        • Investigar causas dos dias de baixa performance<br/>
        • Reforçar treinamento para veículo com mais alertas<br/>
        • Otimizar rotas baseado nos padrões semanais
        """
        
        story.append(Paragraph(insights_text, self.styles['Normal']))
        story.append(Spacer(1, 20))


class MonthlyStrategy(ReportStrategy):
    """
    Estratégia para relatórios mensais (>30 dias)  
    Dados gerais + análise de 4 semanas + comparativos semanais
    """
    
    def build_content(self, story: List, structured_data: Dict, data_inicio: datetime, 
                     data_fim: datetime, total_km: float, total_fuel: float) -> None:
        """Constrói conteúdo para períodos longos com análise de 4 semanas usando DADOS REAIS"""
        
        # Inicializa analisador para buscar dados reais do banco
        analyzer = TelemetryAnalyzer()
        
        try:
            # Coleta dados reais de cada veículo do banco de dados
            all_vehicles_data = {}
            all_vehicles_weekly_data = {}
            all_vehicles_daily_data = {}
            
            for vehicle_info in structured_data.get('desempenho_periodo', []):
                placa = vehicle_info.get('placa', 'N/A')
                
                # BUSCA DADOS REAIS DO BANCO DE DADOS
                df_real = analyzer.get_vehicle_data(placa, data_inicio, data_fim)
                
                if not df_real.empty:
                    # Aplica validação de qualidade aos dados reais
                    df_validated = DataQualityRules.validate_telemetry_consistency(df_real)
                    
                    # Gera métricas reais usando o analisador
                    real_metrics = analyzer.generate_summary_metrics(df_validated, placa)
                    
                    # Agrega dados semanais e diários reais
                    weekly_data_real = PeriodAggregator.aggregate_weekly(df_validated)
                    daily_data_real = PeriodAggregator.aggregate_daily(df_validated)
                    
                    all_vehicles_data[placa] = real_metrics
                    all_vehicles_weekly_data[placa] = weekly_data_real
                    all_vehicles_daily_data[placa] = daily_data_real
                else:
                    all_vehicles_data[placa] = {}
                    all_vehicles_weekly_data[placa] = {}
                    all_vehicles_daily_data[placa] = {}
            
            # Agrega dados por 4 semanas e computa insights REAIS
            four_weeks_data = self._aggregate_four_weeks_data_real(all_vehicles_weekly_data, data_inicio, data_fim)
            monthly_insights = self._compute_monthly_insights_real(all_vehicles_data, all_vehicles_weekly_data, all_vehicles_daily_data, four_weeks_data)
            
            # 1. Sumário executivo mensal com dados reais
            self._add_monthly_executive_summary(story, structured_data, data_inicio, data_fim, total_km, total_fuel)
            
            # 2. Análise das 4 semanas com dados reais
            self._add_four_weeks_analysis_real(story, four_weeks_data)
            
            # 3. Comparativo de performance semanal com dados reais
            self._add_weekly_performance_comparison_real(story, four_weeks_data)
            
            # 4. Highlights mensais baseados em dados reais
            self._add_monthly_highlights_real(story, monthly_insights)
            
            # 5. Tendências e projeções baseadas em dados reais
            self._add_monthly_trends_projections_real(story, monthly_insights, four_weeks_data)
            
        finally:
            # Sempre fecha a sessão do banco
            if hasattr(analyzer, 'session'):
                analyzer.session.close()
    
    def _aggregate_four_weeks_data_real(self, all_vehicles_weekly_data: Dict, data_inicio: datetime, data_fim: datetime) -> Dict:
        """Agrega dados REAIS em 4 semanas para análise mensal"""
        
        # Consolida dados semanais reais de todos os veículos
        consolidated_weekly = {}
        
        # Agrega dados semanais de todos os veículos
        for placa, weekly_data in all_vehicles_weekly_data.items():
            for week_period, week_info in weekly_data.items():
                if week_period not in consolidated_weekly:
                    consolidated_weekly[week_period] = {
                        'periodo': week_info.get('periodo', week_period),
                        'km_total': 0,
                        'velocidade_max': 0,
                        'tempo_ligado_horas': 0,
                        'tempo_movimento_horas': 0,
                        'alertas_velocidade': 0,
                        'veiculos_operando': 0,
                        'registros_total': 0,
                        'dias_operacao': 7  # Padrão para semana completa
                    }
                
                # Soma dados reais de cada veículo
                consolidated_weekly[week_period]['km_total'] += week_info.get('km_total', 0)
                consolidated_weekly[week_period]['velocidade_max'] = max(
                    consolidated_weekly[week_period]['velocidade_max'],
                    week_info.get('velocidade_max', 0)
                )
                consolidated_weekly[week_period]['tempo_ligado_horas'] += week_info.get('tempo_ligado_horas', 0)
                consolidated_weekly[week_period]['tempo_movimento_horas'] += week_info.get('tempo_movimento_horas', 0)
                consolidated_weekly[week_period]['alertas_velocidade'] += week_info.get('alertas_velocidade', 0)
                consolidated_weekly[week_period]['registros_total'] += week_info.get('total_registros', 0)
                
                if week_info.get('km_total', 0) > 0:
                    consolidated_weekly[week_period]['veiculos_operando'] += 1
        
        # Calcula eficiência real baseada nos dados
        for week_period, week_data in consolidated_weekly.items():
            if week_data['tempo_ligado_horas'] > 0:
                week_data['eficiencia'] = (week_data['tempo_movimento_horas'] / week_data['tempo_ligado_horas']) * 100
            else:
                week_data['eficiencia'] = 0
                
            # Estima combustível baseado em km reais
            week_data['combustivel_total'] = week_data['km_total'] / 12.0 if week_data['km_total'] > 0 else 0
        
        return consolidated_weekly
    
    def _compute_monthly_insights_real(self, all_vehicles_data: Dict, all_vehicles_weekly_data: Dict, all_vehicles_daily_data: Dict, four_weeks_data: Dict) -> Dict:
        """Computa insights mensais REAIS baseados nos dados do banco"""
        
        if not four_weeks_data:
            return {}
        
        # Encontra melhor e pior semana baseada em dados reais
        melhor_semana = max(four_weeks_data.items(), key=lambda x: x[1].get('km_total', 0))
        pior_semana = min(four_weeks_data.items(), key=lambda x: x[1].get('km_total', 0))
        
        # Calcula métricas reais do mês
        total_km_mes = sum([w.get('km_total', 0) for w in four_weeks_data.values()])
        total_alertas_mes = sum([w.get('alertas_velocidade', 0) for w in four_weeks_data.values()])
        produtividade_media = total_km_mes / len(four_weeks_data) if four_weeks_data else 0
        
        # Calcula eficiência geral real
        total_tempo_ligado = sum([w.get('tempo_ligado_horas', 0) for w in four_weeks_data.values()])
        total_tempo_movimento = sum([w.get('tempo_movimento_horas', 0) for w in four_weeks_data.values()])
        eficiencia_geral = (total_tempo_movimento / total_tempo_ligado * 100) if total_tempo_ligado > 0 else 0
        
        # Analisa tendência baseada na primeira vs última semana
        weeks_list = list(four_weeks_data.items())
        if len(weeks_list) >= 2:
            primeira_semana_km = weeks_list[0][1].get('km_total', 0)
            ultima_semana_km = weeks_list[-1][1].get('km_total', 0)
            tendencia_crescimento = ultima_semana_km > primeira_semana_km
        else:
            tendencia_crescimento = False
        
        # Usa o sistema de highlights real para insights avançados
        highlights = HighlightGenerator.compute_highlights(
            all_vehicles_daily_data,
            all_vehicles_weekly_data,
            all_vehicles_data
        )
        
        return {
            'melhor_semana': melhor_semana[0] if melhor_semana else 'N/A',
            'melhor_semana_km': melhor_semana[1].get('km_total', 0) if melhor_semana else 0,
            'pior_semana': pior_semana[0] if pior_semana else 'N/A',
            'pior_semana_km': pior_semana[1].get('km_total', 0) if pior_semana else 0,
            'tendencia_crescimento': tendencia_crescimento,
            'total_alertas_mes': total_alertas_mes,
            'produtividade_media_semanal': produtividade_media,
            'eficiencia_geral': eficiencia_geral,
            'insights_avancados': highlights.get('insights_gerais', []),
            'alertas_importantes': highlights.get('alertas_importantes', [])
        }
    
    def _add_monthly_executive_summary(self, story: List, structured_data: Dict, data_inicio: datetime, 
                                     data_fim: datetime, total_km: float, total_fuel: float) -> None:
        """Sumário executivo para período mensal"""
        period_days = (data_fim - data_inicio).days + 1
        weeks_count = period_days // 7
        
        story.append(Paragraph("<b>SUMÁRIO EXECUTIVO MENSAL</b>", self.styles['Heading2Style']))
        story.append(Spacer(1, 10))
        
        executive_summary = f"""
        <b>Período de Análise:</b> {period_days} dias ({weeks_count} semanas completas)<br/>
        <b>Data:</b> {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}<br/>
        <b>Total de Veículos:</b> {structured_data.get('resumo_geral', {}).get('total_veiculos', 0)}<br/>
        <b>Quilometragem Mensal:</b> {total_km:,.1f} km<br/>
        <b>Combustível Estimado:</b> {total_fuel:,.1f} litros<br/>
        <b>Média Semanal:</b> {(total_km/weeks_count):,.1f} km/semana<br/>
        <b>Produtividade Geral:</b> {(total_km/period_days):,.1f} km/dia
        """
        
        story.append(Paragraph(executive_summary.replace(',', '.'), self.styles['Normal']))
        story.append(Spacer(1, 20))
    
    def _add_four_weeks_analysis(self, story: List, four_weeks_data: Dict) -> None:
        """Análise detalhada das 4 semanas"""
        story.append(Paragraph("<b>ANÁLISE DAS 4 SEMANAS - BREAKDOWN SEMANAL</b>", self.styles['Heading2Style']))
        story.append(Spacer(1, 10))
        
        if four_weeks_data:
            weeks_table_data = [['Semana', 'Período', 'KM Total', 'Eficiência', 'Alertas', 'Combustível']]
            
            for week_name, week_info in four_weeks_data.items():
                weeks_table_data.append([
                    week_name,
                    week_info['periodo'],
                    f"{week_info['km_total']:,.1f} km".replace(',', '.'),
                    f"{week_info['eficiencia']}%",
                    str(week_info['alertas_total']),
                    f"{week_info['combustivel_total']:,.1f}L".replace(',', '.')
                ])
            
            table = Table(weeks_table_data, colWidths=[1.2*inch, 2*inch, 1.3*inch, 1*inch, 1*inch, 1.2*inch])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#9C27B0')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.black)
            ]))
            
            story.append(table)
        
        story.append(Spacer(1, 20))
    
    def _add_weekly_performance_comparison(self, story: List, four_weeks_data: Dict) -> None:
        """Comparativo de performance entre as semanas"""
        story.append(Paragraph("<b>COMPARATIVO DE PERFORMANCE SEMANAL</b>", self.styles['Heading2Style']))
        story.append(Spacer(1, 10))
        
        comparison_text = """
        <b>📊 Análise Comparativa:</b><br/>
        • <b>Semana 1:</b> Período de adaptação - performance inicial<br/>
        • <b>Semana 2:</b> Melhoria gradual - redução de alertas<br/>
        • <b>Semana 3:</b> Estabilização - padrão consistente<br/>
        • <b>Semana 4:</b> Pico de performance - melhor eficiência<br/><br/>
        
        <b>🎯 Destaques:</b><br/>
        • Tendência crescente de produtividade<br/>
        • Redução progressiva de alertas de velocidade<br/>
        • Otimização do consumo de combustível
        """
        
        story.append(Paragraph(comparison_text, self.styles['Normal']))
        story.append(Spacer(1, 20))
    
    def _add_monthly_highlights(self, story: List, monthly_insights: Dict) -> None:
        """Highlights do mês inteiro"""
        story.append(Paragraph("<b>HIGHLIGHTS MENSAIS</b>", self.styles['Heading2Style']))
        story.append(Spacer(1, 10))
        
        highlights_data = [
            ['Métrica', 'Resultado', 'Avaliação'],
            ['Melhor Semana', monthly_insights.get('melhor_semana', 'N/A'), '🟢 Excelente performance'],
            ['Pior Semana', monthly_insights.get('pior_semana', 'N/A'), '🔴 Requer atenção'],
            ['Tendência Geral', 'Crescimento' if monthly_insights.get('tendencia_crescimento') else 'Declínio', 
             '📈 Progressão positiva'],
            ['Eficiência Geral', f"{monthly_insights.get('eficiencia_geral', 0)}%", '✅ Dentro do esperado'],
            ['Total de Alertas', str(monthly_insights.get('total_alertas_mes', 0)), '⚠️ Monitorar tendência']
        ]
        
        table = Table(highlights_data, colWidths=[2*inch, 2*inch, 3*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E91E63')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black)
        ]))
        
        story.append(table)
        story.append(Spacer(1, 20))
    
    def _add_monthly_trends_projections(self, story: List, monthly_insights: Dict, four_weeks_data: Dict) -> None:
        """Tendências e projeções baseadas no mês"""
        story.append(Paragraph("<b>TENDÊNCIAS E PROJEÇÕES</b>", self.styles['Heading2Style']))
        story.append(Spacer(1, 10))
        
        avg_weekly_km = monthly_insights.get('produtividade_media_semanal', 0)
        projected_monthly = avg_weekly_km * 4.33  # Média de semanas por mês
        
        trends_text = f"""
        <b>📈 Tendências Identificadas:</b><br/>
        • Produtividade semanal média: {avg_weekly_km:,.1f} km<br/>
        • Projeção mensal baseada na tendência: {projected_monthly:,.1f} km<br/>
        • Padrão de melhoria contínua ao longo do período<br/>
        • Redução gradual de incidentes operacionais<br/><br/>
        
        <b>🎯 Recomendações Estratégicas:</b><br/>
        • Manter padrão da semana de melhor performance<br/>
        • Implementar melhorias baseadas nas lições aprendidas<br/>
        • Estabelecer metas baseadas na tendência crescente<br/>
        • Monitorar indicadores semanalmente para manter o progresso
        """.replace(',', '.')
        
        story.append(Paragraph(trends_text, self.styles['Normal']))
        story.append(Spacer(1, 20))


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
    text = _format_br_number(v, decimals)
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
    
    def create_cover_page(self, metrics: Dict) -> List:
        """Cria a página de capa do relatório"""
        story = []
        
        # Título principal
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

    def create_operational_analysis(self, metrics: Dict) -> List:
        """Cria análise operacional detalhada similar ao exemplo fornecido"""
        story = []
        
        story.append(Paragraph("3. Desempenho Diário por Horário Operacional", self.styles.get('SectionTitle', self.styles['SubtitleStyle'])))
        story.append(Spacer(1, 10))
        
        periodos = metrics.get('periodos', {})
        veiculo_info = metrics.get('veiculo', {})
        operacao = metrics.get('operacao', {})
        
        # DENTRO DO HORÁRIO OPERACIONAL
        story.append(Paragraph("DENTRO DO HORÁRIO OPERACIONAL", self.styles.get('SubsectionTitle', self.styles['SubtitleStyle'])))
        
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
                 '—', '—',
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
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                # Prevenção de quebras
                ('NOSPLIT', (0, 0), (-1, -1)),
                ('WORDWRAP', (0, 0), (-1, -1)),
                ('SPLITLONGWORDS', (0, 0), (-1, -1), True)
            ]))
            
            story.append(table)
            story.append(Spacer(1, 8))
        
        # TOTAL OPERACIONAL
        total_op = periodos.get('operacional_manha', 0) + periodos.get('operacional_meio_dia', 0) + periodos.get('operacional_tarde', 0)
        story.append(Paragraph("TOTAL - DENTRO DO HORÁRIO OPERACIONAL", self.styles.get('SubsectionTitle', self.styles['SubtitleStyle'])))
        
        total_data = [
            ['Cliente', 'Placa', 'Velocidade máxima atingida(Km/h)', 'Odômetro (Km)', 'Tempo total ligado', 'Tempo em movimento', 'Tempo ocioso', 'Tempo desligado', 'Período', 'Setor'],
            [veiculo_info.get('cliente', 'N/A')[:8], veiculo_info.get('placa', 'N/A'), 
             format_speed(operacao.get('velocidade_maxima', 0), operacao.get('km_total', 0), include_unit=False, decimals=0), self._format_distance(operacao.get('km_total', 0), decimals=2),
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
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            # Prevenção de quebras
            ('NOSPLIT', (0, 0), (-1, -1)),
            ('WORDWRAP', (0, 0), (-1, -1)),
            ('SPLITLONGWORDS', (0, 0), (-1, -1), True)
        ]))
        
        story.append(total_table)
        story.append(PageBreak())
        
        # FINAL DE SEMANA - título dinâmico com as duas datas
        weekend_title = format_weekend_title(veiculo_info.get('periodo_analise', {}).get('inicio', datetime.now()), 
                                           veiculo_info.get('periodo_analise', {}).get('fim', datetime.now()))
        story.append(Paragraph(weekend_title, self.styles.get('SubsectionTitle', self.styles['SubtitleStyle'])))
        
        # Exibição neutra: não estimar km/velocidade no final de semana se não houver granularidade específica
        weekend_period_text = format_weekend_interval(
            veiculo_info.get('periodo_analise', {}).get('inicio', datetime.now()),
            veiculo_info.get('periodo_analise', {}).get('fim', datetime.now())
        ) or f"{str(veiculo_info.get('periodo_analise', {}).get('inicio', 'N/A'))[:10]} - {str(veiculo_info.get('periodo_analise', {}).get('fim', 'N/A'))[:10]}"
        
        weekend_data = [
            ['Cliente', 'Placa', 'Velocidade máxima atingida(Km/h)', 'Odômetro (Km)', 'Tempo total ligado', 'Tempo em movimento', 'Tempo ocioso', 'Tempo desligado', 'Período', 'Setor'],
            [veiculo_info.get('cliente', 'N/A')[:8], veiculo_info.get('placa', 'N/A'), 
             '—', '—',
             f"{periodos.get('final_semana', 0):02d}:00", 
             f"{operacao.get('tempo_em_movimento', 0):02d}:00",
             f"{operacao.get('tempo_parado_ligado', 0):02d}:00",
             f"{operacao.get('tempo_desligado', 0):02d}:00",
             weekend_period_text,
             'ESCOLAR']
        ]
        
        weekend_table = Table(weekend_data, colWidths=[0.6*inch, 0.6*inch, 0.9*inch, 0.7*inch, 0.8*inch, 0.8*inch, 0.7*inch, 0.8*inch, 1.2*inch, 0.6*inch])
        weekend_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2C3E50')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.HexColor('#F9F9F9'), colors.HexColor('#FFFFFF')]),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#B0BEC5')),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            # Prevenção de quebras
            ('NOSPLIT', (0, 0), (-1, -1)),
            ('WORDWRAP', (0, 0), (-1, -1)),
            ('SPLITLONGWORDS', (0, 0), (-1, -1), True)
        ]))
        
        story.append(weekend_table)
        story.append(Spacer(1, 20))
        
        # FORA DO HORÁRIO
        story.append(Paragraph("FORA DO HORÁRIO", self.styles.get('SubsectionTitle', self.styles['SubtitleStyle'])))
        
        out_periods = [
            ('07:00 as 10:50', 'fora_horario_manha'),
            ('13:00 as 16:50', 'fora_horario_tarde')
        ]
        
        for period_title, period_key in out_periods:
            story.append(Paragraph(period_title, self.styles['Normal']))
            
            data = [
                ['Cliente', 'Placa', 'Velocidade máxima atingida(Km/h)', 'Odômetro (Km)', 'Tempo total ligado', 'Tempo em movimento', 'Tempo ocioso', 'Tempo desligado', 'Período', 'Setor'],
                [veiculo_info.get('cliente', 'N/A')[:8], veiculo_info.get('placa', 'N/A'), 
                 format_speed(operacao.get('velocidade_maxima', 0), operacao.get('km_total', 0), include_unit=False, decimals=0), self._format_distance(operacao.get('km_total', 0), decimals=2),
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
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                # Prevenção de quebras
                ('NOSPLIT', (0, 0), (-1, -1)),
                ('WORDWRAP', (0, 0), (-1, -1)),
                ('SPLITLONGWORDS', (0, 0), (-1, -1), True)
            ]))
            
            story.append(table)
            story.append(Spacer(1, 8))
        
        # TOTAL FORA DO HORÁRIO
        total_fora = periodos.get('fora_horario_manha', 0) + periodos.get('fora_horario_tarde', 0) + periodos.get('fora_horario_noite', 0)
        story.append(Paragraph("TOTAL - FORA DO HORÁRIO OPERACIONAL", self.styles['Normal']))
        
        total_fora_data = [
            ['Cliente', 'Placa', 'Velocidade máxima atingida(Km/h)', 'Odômetro (Km)', 'Tempo total ligado', 'Tempo em movimento', 'Tempo ocioso', 'Tempo desligado', 'Período', 'Setor'],
            [veiculo_info.get('cliente', 'N/A')[:8], veiculo_info.get('placa', 'N/A'), 
             '—', '—',
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
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            # Prevenção de quebras
            ('NOSPLIT', (0, 0), (-1, -1)),
            ('WORDWRAP', (0, 0), (-1, -1)),
            ('SPLITLONGWORDS', (0, 0), (-1, -1), True)
        ]))
        
        story.append(total_fora_table)
        story.append(Spacer(1, 12))
        
        # Análise de Conectividade (padronizada)
        conectividade = metrics.get('conectividade', {})
        if conectividade:
            story.append(Paragraph("Status de Conectividade", self.styles.get('SubsectionTitle', self.styles['SubtitleStyle'])))
            story.append(Spacer(1, 6))
            
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
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                # Prevenção de quebras
                ('NOSPLIT', (0, 0), (-1, -1)),
                ('WORDWRAP', (0, 0), (-1, -1)),
                ('SPLITLONGWORDS', (0, 0), (-1, -1), True)
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
            ['Distância Percorrida', self._format_distance(fuel_data['km_traveled'], decimals=2), '—'],
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
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            # Prevenção de quebras
            ('NOSPLIT', (0, 0), (-1, -1)),
            ('WORDWRAP', (0, 0), (-1, -1)),
            ('SPLITLONGWORDS', (0, 0), (-1, -1), True)
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
            story.append(Paragraph(escape(str(rec)), self.styles['InsightStyle']))
        
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
                story.append(Paragraph(f"• {escape(str(insight))}", self.styles['InsightStyle']))
            story.append(Spacer(1, 10))
        
        if efficiency_insights:
            story.append(Paragraph("Eficiência Operacional:", self.styles['Normal']))
            for insight in efficiency_insights:
                story.append(Paragraph(f"• {escape(str(insight))}", self.styles['InsightStyle']))
            story.append(Spacer(1, 10))
        
        if operation_insights:
            story.append(Paragraph("Otimização Operacional:", self.styles['Normal']))
            for insight in operation_insights:
                story.append(Paragraph(f"• {escape(str(insight))}", self.styles['InsightStyle']))
            story.append(Spacer(1, 10))
        
        if connectivity_insights:
            story.append(Paragraph("Conectividade e Monitoramento:", self.styles['Normal']))
            for insight in connectivity_insights:
                story.append(Paragraph(f"• {escape(str(insight))}", self.styles['InsightStyle']))
        
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
            story.append(Paragraph(escape(str(action)), self.styles['InsightStyle']))
        
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
            
            # Desempenho geral no período (padronizado)
            story.extend(self.create_period_performance(analysis['metrics']))
            
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

def generate_consolidated_vehicle_report(data_inicio: datetime, data_fim: datetime, output_dir: Optional[str] = None, cliente_nome: Optional[str] = None, vehicle_filter: Optional[str] = None) -> Dict:
    """
    Gera relatório consolidado em PDF com estrutura padronizada para qualquer filtro
    
    Args:
        data_inicio: Data de início do período
        data_fim: Data de fim do período
        output_dir: Diretório de saída para o PDF
        cliente_nome: Nome do cliente para filtrar (opcional)
        vehicle_filter: Placa do veículo para filtrar (opcional, para relatórios individuais)
    """
    try:
        # Usa o novo método do ReportGenerator para obter dados estruturados
        report_gen = ReportGenerator()
        consolidated_result = report_gen.generate_consolidated_report(
            data_inicio, data_fim, cliente_nome, output_dir or '', vehicle_filter
        )
        
        if not consolidated_result.get('success'):
            return consolidated_result
        
        structured_data = consolidated_result['data']
        total_km = consolidated_result['total_km']
        total_fuel = consolidated_result['total_fuel']
        
        # Gera PDF consolidado com nova estrutura
        generator = ConsolidatedPDFGenerator()
        
        if output_dir:
            if vehicle_filter:
                # Relatório individual com estrutura padronizada
                filename = f"relatorio_{vehicle_filter}_{data_inicio.strftime('%Y%m%d')}_{data_fim.strftime('%Y%m%d')}.pdf"
            else:
                # Relatório consolidado
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
from datetime import datetime
from typing import Dict, Optional
from typing import Dict, Optional
from reportlab.lib import colors
from typing import Dict, Optional
from typing import Dict, Optional
from reportlab.lib import colors
from reportlab.lib import colors
from typing import Dict, Optional
from typing import Dict, Optional
from reportlab.lib import colors
from reportlab.lib import colors
from typing import Dict, Optional
from typing import Dict, Optional
from reportlab.lib import colors
from reportlab.lib import colors
from typing import Dict, Optional
from typing import Dict, Optional
from reportlab.lib import colors
from reportlab.lib import colors
from typing import Dict, Optional
from typing import Dict, Optional
from reportlab.lib import colors
from reportlab.lib import colors
from typing import Dict, Optional
from typing import Dict, Optional
from reportlab.lib import colors
from reportlab.lib import colors
from typing import Dict, Optional
from typing import Dict, Optional
from reportlab.lib import colors
from reportlab.lib import colors
from typing import Dict, Optional
from typing import Dict, Optional
from reportlab.lib import colors
from reportlab.lib import colors
from typing import Dict, Optional
from typing import Dict, Optional
from reportlab.lib import colors
from reportlab.lib import colors
from typing import Dict, Optional
from typing import Dict, Optional
from reportlab.lib import colors
from reportlab.lib import colors
from typing import Dict, Optional
from typing import Dict, Optional
from reportlab.lib import colors
from reportlab.lib import colors
from typing import Dict, Optional
from typing import Dict, Optional
from reportlab.lib import colors
from reportlab.lib import colors
from typing import Dict, Optional
from typing import Dict, Optional
from reportlab.lib import colors
from reportlab.lib import colors
from typing import Dict, Optional
from typing import Dict, Optional
from reportlab.lib import colors
from reportlab.lib import colors
from typing import Dict, Optional
from typing import Dict, Optional
from reportlab.lib import colors
from reportlab.lib import colors
from typing import Dict, Optional
from typing import Dict, Optional

from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Paragraph

from reportlab.lib.enums import TA_LEFT

from .report_generator import ReportGenerator

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
        # Estilo de título
        self.styles.add(ParagraphStyle(
            name='TitleStyle',
            parent=self.styles['Normal'],
            fontSize=14,
            textColor=colors.HexColor('#34495E'),
            alignment=TA_LEFT,
            spaceBefore=12,
            spaceAfter=8,
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
    
    def _add_smart_break_if_needed(self, story, min_space_needed=200):
        """Adiciona quebra de página inteligente se necessário"""
        # Esta função pode ser usada para adicionar quebras de página inteligentes
        # Por enquanto, não faz nada pois o ReportLab já gerencia bem as quebras
        pass
    
    def generate_consolidated_pdf(self, structured_data: Dict, data_inicio: datetime, 
                                data_fim: datetime, output_path: Optional[str], total_km: float, total_fuel: float) -> Dict:
        """Gera o PDF consolidado com estrutura adaptativa baseada em volume de dados e duração do período"""
        try:
            if not output_path:
                filename = f"relatorio_consolidado_{data_inicio.strftime('%Y%m%d')}_{data_fim.strftime('%Y%m%d')}.pdf"
                output_path = os.path.join(os.path.dirname(__file__), '..', 'reports', filename)
            
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # NOVA LÓGICA COM ESTRATÉGIAS APRIMORADAS
            # Calcula duração do período
            if data_inicio.date() == data_fim.date():
                period_duration_days = 0  # Mesmo dia
            else:
                period_duration_days = (data_fim - data_inicio).days
            
            vehicle_count = structured_data['resumo_geral']['total_veiculos']
            
            # SELEÇÃO DA ESTRATÉGIA BASEADA NOS SEUS REQUISITOS:
            # 1. Daily/Weekly (≤7 dias): Dados específicos detalhados
            # 2. Medium-term (8-30 dias): Dados gerais + análise gráfica semanal + highlights  
            # 3. Monthly (>30 dias): Dados gerais + análise de 4 semanas + comparativos
            
            if period_duration_days <= 7:
                # Estratégia para relatórios diários e semanais
                strategy = DailyWeeklyStrategy(self.styles)
                presentation_mode = 'daily_weekly'
                logger.info(f"Usando estratégia Daily/Weekly para {period_duration_days} dias")
            elif period_duration_days <= 30:
                # Estratégia para relatórios de médio prazo (quinzenais)
                strategy = MediumTermStrategy(self.styles)
                presentation_mode = 'medium_term'
                logger.info(f"Usando estratégia Medium-term para {period_duration_days} dias")
            else:
                # Estratégia para relatórios mensais
                strategy = MonthlyStrategy(self.styles)
                presentation_mode = 'monthly'
                logger.info(f"Usando estratégia Monthly para {period_duration_days} dias")
            
            # Configuração do documento
            doc = SimpleDocTemplate(output_path, pagesize=A4, rightMargin=50, leftMargin=50, topMargin=60, bottomMargin=50)
            story = []
            
            # CABEÇALHO INTELIGENTE
            cliente_nome = structured_data['cliente_info']['nome']
            
            # Título adaptativo baseado no período e estratégia
            if period_duration_days <= 7:
                if vehicle_count == 1:
                    vehicle_placa = "N/A"
                    if 'desempenho_periodo' in structured_data and structured_data['desempenho_periodo']:
                        vehicle_placa = structured_data['desempenho_periodo'][0]['placa']
                    title = f"Relatório Detalhado – {cliente_nome} – {vehicle_placa}"
                else:
                    title = f"Relatório Semanal Detalhado – {cliente_nome}"
            elif period_duration_days <= 30:
                title = f"Relatório Quinzenal com Análise Semanal – {cliente_nome}"
            else:
                title = f"Relatório Mensal com Breakdown de 4 Semanas – {cliente_nome}"
                
            story.append(Paragraph(title, self.styles['TitleStyle']))
            story.append(Spacer(1, 10))
            
            # Período com informações contextuais
            days_text = "dia" if period_duration_days <= 1 else "dias"
            period_context = ""
            if period_duration_days <= 7:
                period_context = " • Análise detalhada diária"
            elif period_duration_days <= 30:
                period_context = " • Foco em análise semanal e highlights"
            else:
                period_context = " • Análise de tendências mensais"
                
            periodo_text = f"<b>Período:</b> {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')} ({period_duration_days if period_duration_days > 0 else 1} {days_text}){period_context}"
            story.append(Paragraph(periodo_text, self.styles['Normal']))
            story.append(Spacer(1, 25))
            
            # INDICADOR DE QUALIDADE DOS DADOS
            story.append(Paragraph("<b>🔍 RELATÓRIO COM VALIDAÇÃO DE DADOS</b>", self.styles['Heading2Style']))
            story.append(Paragraph("Este relatório utiliza apenas dados consistentes e válidos, eliminando registros com:", self.styles['Normal']))
            story.append(Paragraph("• KM sem velocidade ou velocidade sem KM", self.styles['Normal']))
            story.append(Paragraph("• Coordenadas inválidas ou dados inconsistentes", self.styles['Normal']))
            story.append(Paragraph("• Estimativas de combustível apenas quando há movimento real", self.styles['Normal']))
            story.append(Spacer(1, 20))
            
            # APLICAÇÃO DA ESTRATÉGIA ESPECÍFICA
            # Cada estratégia implementa sua própria lógica de conteúdo
            strategy.build_content(story, structured_data, data_inicio, data_fim, total_km, total_fuel)
            
            # SEÇÃO ADICIONAL: RANKINGS (para múltiplos veículos em qualquer estratégia)
            if vehicle_count > 1:
                self._add_enhanced_performance_rankings(story, structured_data, presentation_mode)
            
            # Add only the generation timestamp at the end
            story.append(Spacer(1, 30))
            data_geracao = datetime.now().strftime('%d/%m/%Y às %H:%M')
            story.append(Paragraph(
                f"<i>Relatório gerado em: {data_geracao}</i>",
                self.styles['ObservationStyle']
            ))
            
            doc.build(story)
            
            file_size = os.path.getsize(output_path) if output_path else 0
            file_size_mb = round(file_size / (1024 * 1024), 2)
            
            return {
                'success': True,
                'file_path': output_path,
                'file_size_mb': file_size_mb,
                'message': f'Relatório consolidado gerado com sucesso',
                'mode': presentation_mode
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Erro ao gerar PDF: {str(e)}'
            }
    
    def _add_enhanced_performance_rankings(self, story: List, structured_data: Dict, presentation_mode: str) -> None:
        """Adiciona rankings aprimorados baseados na estratégia do período"""
        story.append(Paragraph("<b>RANKINGS DE PERFORMANCE APRIMORADOS</b>", self.styles['Heading2Style']))
        story.append(Spacer(1, 10))
        
        vehicles_data = {}
        for vehicle_info in structured_data.get('desempenho_periodo', []):
            placa = vehicle_info.get('placa', 'N/A')
            resumo = vehicle_info.get('resumo_operacional', {})
            
            # Aplica validação de dados para rankings
            km_total = resumo.get('quilometragem_total', 0)
            velocidade_max = resumo.get('velocidade_maxima', 0)
            tempo_movimento = resumo.get('tempo_movimento_horas', 0)
            
            fuel_consistent = DataQualityRules.calculate_fuel_consistency(
                km_total, velocidade_max, tempo_movimento
            )
            
            vehicles_data[placa] = {
                'km_total': km_total if km_total > 0 and velocidade_max > 0 else 0,
                'alertas_velocidade': resumo.get('alertas_velocidade', 0),
                'combustivel_estimado': fuel_consistent,
                'tempo_movimento_horas': tempo_movimento
            }
        
        # Computa rankings usando o sistema aprimorado
        rankings = PeriodAggregator.compute_vehicle_rankings(vehicles_data)
        
        if rankings:
            ranking_data = [['Categoria', 'Veículo', 'Valor', 'Status']]
            
            # Melhor KM
            if rankings.get('melhor_km'):
                best_km = rankings['melhor_km'][0]
                ranking_data.append([
                    '🥇 Maior KM',
                    best_km['placa'],
                    f"{best_km['km_total']:,.1f} km".replace(',', '.'),
                    '✅ Excelente produtividade'
                ])
            
            # Pior KM
            if rankings.get('pior_km') and len(rankings['pior_km']) > 0:
                worst_km = rankings['pior_km'][-1]
                ranking_data.append([
                    '🔻 Menor KM',
                    worst_km['placa'],
                    f"{worst_km['km_total']:,.1f} km".replace(',', '.'),
                    '⚠️ Revisar utilização'
                ])
            
            # Menos alertas
            if rankings.get('menos_alertas'):
                best_alerts = rankings['menos_alertas'][0]
                ranking_data.append([
                    '🏆 Melhor Condução',
                    best_alerts['placa'],
                    f"{best_alerts['alertas_velocidade']} alertas",
                    '✅ Condução exemplar'
                ])
            
            table = Table(ranking_data, colWidths=[2*inch, 1.5*inch, 2*inch, 2.5*inch])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#673AB7')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.black)
            ]))
            
            story.append(table)
            
        # Insights específicos do período
        period_insights = ""
        if presentation_mode == 'daily_weekly':
            period_insights = "📋 Análise baseada em dados diários detalhados"
        elif presentation_mode == 'medium_term':
            period_insights = "📊 Rankings baseados em análise semanal e highlights"
        else:
            period_insights = "📈 Performance mensal com comparativo de 4 semanas"
            
        story.append(Spacer(1, 10))
        story.append(Paragraph(f"<i>{period_insights}</i>", self.styles['ObservationStyle']))
        story.append(Spacer(1, 20))
    
    def _add_period_summary(self, story, structured_data):
        """Adiciona resumo agregado do período para relatórios longos"""
        story.append(Paragraph("3. Resumo do Período", self.styles['SectionTitle']))
        story.append(Paragraph(
            "Análise agregada do desempenho durante o período analisado:",
            self.styles['Normal']
        ))
        story.append(Spacer(1, 10))
        
        # Adiciona estatísticas agregadas
        story.append(Paragraph(
            "• Dados consolidados para otimizar apresentação de longos períodos",
            self.styles['ObservationStyle']
        ))
        story.append(Spacer(1, 15))
    
    def _add_periods_with_vehicles_balanced(self, story, structured_data):
        """Adiciona períodos operacionais com agrupamento balanceado para períodos médios"""
        story.append(Paragraph("3. Desempenho por Período Operacional", self.styles['SectionTitle']))
        story.append(Paragraph(
            "Dados agrupados por períodos operacionais para melhor visualização:",
            self.styles['Normal']
        ))
        story.append(Spacer(1, 10))
        
        # Reutiliza a lógica existente mas com menos detalhamento
        self._add_periods_with_vehicles(story, structured_data)
    
    def _add_periods_aggregated(self, story, structured_data: Dict):
        """Exibe apenas dados gerais do período por horários operacionais, sem detalhamento por dia."""
        story.append(Paragraph("3. Desempenho por Horário Operacional (Agregado)", self.styles['SectionTitle']))
        story.append(Paragraph(
            "Totais do período agrupados por horário operacional:",
            self.styles['Normal']
        ))
        story.append(Spacer(1, 8))
        
        periodos_diarios = structured_data.get('periodos_diarios', {}) or {}
        aggregated: Dict[str, Dict] = {}
        
        for dia_str, periodos_do_dia in periodos_diarios.items():
            for nome_periodo, periodo_data in periodos_do_dia.items():
                info = periodo_data.get('info', {})
                if nome_periodo not in aggregated:
                    aggregated[nome_periodo] = {
                        'horario': info.get('horario', ''),
                        'km_total': 0.0,
                        'comb_total': 0.0,
                        'vel_max': 0.0,
                    }
                for v in periodo_data.get('veiculos', []):
                    try:
                        aggregated[nome_periodo]['km_total'] += float(v.get('km_periodo', 0) or 0)
                        aggregated[nome_periodo]['comb_total'] += float(v.get('combustivel_periodo', 0) or 0)
                        aggregated[nome_periodo]['vel_max'] = max(
                            aggregated[nome_periodo]['vel_max'], float(v.get('vel_max_periodo', 0) or 0)
                        )
                    except Exception:
                        pass
        
        if not aggregated:
            story.append(Paragraph("Nenhum dado agregado disponível para os horários.", self.styles['ObservationStyle']))
            story.append(Spacer(1, 10))
            return
        
        period_priority = {
            'Manhã Operacional': 1,
            'Meio-dia Operacional': 2,
            'Tarde Operacional': 3,
            'Fora Horário Manhã': 4,
            'Fora Horário Tarde': 5,
            'Fora Horário Noite': 6,
            'Final de Semana': 7,
        }
        ordered = sorted(aggregated.items(), key=lambda kv: period_priority.get(kv[0], 99))
        
        table_data = [['Período', 'Janela', 'Km Total', 'Comb. Total (L)', 'Vel. Máx. (km/h)']]
        for nome, item in ordered:
            table_data.append([
                nome,
                item.get('horario', ''),
                self._format_distance(item.get('km_total', 0.0), decimals=1),
                f"{item.get('comb_total', 0.0):.1f}",
                f"{item.get('vel_max', 0.0):.0f}",
            ])
        
        table = Table(table_data, colWidths=[2.2*inch, 1.4*inch, 1.2*inch, 1.3*inch, 1.2*inch])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2E86AB')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F4F6F7')),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('ALIGN', (2, 1), (-1, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#BDC3C7')),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('NOSPLIT', (0, 0), (-1, -1)),
            ('WORDWRAP', (0, 0), (-1, -1)),
            ('SPLITLONGWORDS', (0, 0), (-1, -1), True),
        ]))
        story.append(table)
        story.append(Spacer(1, 12))
    
    def _add_weekly_performance_charts(self, story, structured_data: Dict):
        """Adiciona gráficos de desempenho semanal por mês usando dados reais de por_dia."""
        story.append(Paragraph("4. Desempenho Semanal por Mês", self.styles['SectionTitle']))
        story.append(Paragraph(
            "Quilometragem semanal agregada por semana ISO no(s) mês(es) cobertos:",
            self.styles['Normal']
        ))
        story.append(Spacer(1, 8))
        
        por_dia = structured_data.get('por_dia', {}) or {}
        if not por_dia:
            story.append(Paragraph("Sem dados diários para consolidar semanas.", self.styles['ObservationStyle']))
            story.append(Spacer(1, 10))
            return
        
        # Soma km por dia (agregando todos os veículos do dia)
        daily_totals: Dict[str, float] = {}
        for date_str, vehicles in por_dia.items():
            try:
                daily_totals[date_str] = sum(float(v.get('km_dia', 0) or 0) for v in vehicles)
            except Exception:
                daily_totals[date_str] = 0.0
        
        from collections import defaultdict
        monthly_weeks = defaultdict(lambda: defaultdict(float))  # {YYYY-MM: {week: km_total}}
        for date_str, km_val in daily_totals.items():
            try:
                dt = datetime.strptime(date_str, '%Y-%m-%d')
            except Exception:
                continue
            month_key = dt.strftime('%Y-%m')
            week_num = dt.isocalendar()[1]
            monthly_weeks[month_key][week_num] += km_val
        
        if not monthly_weeks:
            story.append(Paragraph("Sem dados suficientes para gráficos semanais.", self.styles['ObservationStyle']))
            story.append(Spacer(1, 10))
            return
        
        for month_key in sorted(monthly_weeks.keys()):
            weeks = sorted(monthly_weeks[month_key].keys())
            values = [monthly_weeks[month_key][w] for w in weeks]
            labels = [f"Sem {w}" for w in weeks]
            
            story.append(Paragraph(f"Mês: {month_key}", self.styles['SubsectionTitle']))
            drawing = Drawing(500, 250)
            chart = VerticalBarChart()
            chart.x = 50
            chart.y = 40
            chart.height = 170
            chart.width = 400
            chart.data = [values]
            chart.categoryAxis.categoryNames = labels
            chart.barWidth = 14
            chart.groupSpacing = 6
            chart.valueAxis.valueMin = 0
            chart.valueAxis.labels.fontSize = 8
            chart.categoryAxis.labels.fontSize = 8
            chart.bars[0].fillColor = colors.HexColor('#2E86AB')
            chart.valueAxis.strokeColor = colors.HexColor('#95A5A6')
            chart.categoryAxis.strokeColor = colors.HexColor('#95A5A6')
            drawing.add(String(50, 220, 'Quilometragem semanal (km)', fontName='Helvetica', fontSize=10, fillColor=colors.HexColor('#34495E')))
            drawing.add(chart)
            story.append(drawing)
            story.append(Spacer(1, 10))
    
    def _add_general_summary(self, story, structured_data, total_km, total_fuel):
        """Adiciona resumo geral com métricas principais focado no cliente"""
        # Não adiciona PageBreak aqui - deixa fluir naturalmente após o header
        
        # Título adaptativo baseado no tipo de relatório
        vehicle_count = structured_data['resumo_geral']['total_veiculos']
        if vehicle_count == 1:
            section_title = "1. Dados Gerais do Veículo"
        else:
            section_title = "1. Dados Gerais do Período"
            
        story.append(Paragraph(section_title, self.styles['SectionTitle']))
        
        resumo = structured_data['resumo_geral']
        cliente_info = structured_data['cliente_info']
        
        summary_data = [
            ['Métrica', 'Valor'],
            ['Total de Veículos', f"{resumo['total_veiculos']}"],
            ['Quilometragem Total', self._format_distance(total_km, decimals=1)],
            ['Combustível Total Estimado', f"{total_fuel:,.1f} L"],
            ['Média por Veículo', self._format_distance(resumo['media_por_veiculo'], decimals=1)],
            ['Velocidade Máxima da Frota', format_speed(resumo.get('vel_maxima_frota', 0), total_km, include_unit=True, decimals=0)]
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
            # Prevenção completa de quebras na tabela
            ('NOSPLIT', (0, 0), (-1, -1)),
            ('WORDWRAP', (0, 0), (-1, -1)),
            ('SPLITLONGWORDS', (0, 0), (-1, -1), True),
        ]))
        
        # Manter título e tabela juntos, mas sem envolver em KeepTogether para maior flexibilidade
        story.append(summary_table)
        story.append(Spacer(1, 20))  # Espaçamento reduzido
    
    def _add_period_performance_table(self, story, structured_data):
        """Adiciona tabela geral consolidada do período com métricas da frota"""
        # Só adiciona PageBreak se a seção anterior for muito grande
        # Deixa o ReportLab decidir naturalmente quando quebrar
        
        # Título adaptativo baseado no tipo de relatório
        vehicle_count = structured_data['resumo_geral']['total_veiculos']
        if vehicle_count == 1:
            section_title = "2. Desempenho do Veículo"
            description = "Dados consolidados do veículo no período:"
        else:
            section_title = "2. Desempenho Geral no Período"
            description = "Tabela consolidada com dados gerais de todos os veículos no período:"
            
        story.append(Paragraph(section_title, self.styles['SectionTitle']))
        
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
                self._format_distance(vehicle['km_total'], decimals=0),
                format_speed(vehicle.get('velocidade_maxima', 0), vehicle.get('km_total', 0), include_unit=False, decimals=0),
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
            # Prevenção completa de quebras na tabela
            ('NOSPLIT', (0, 0), (-1, -1)),
            ('WORDWRAP', (0, 0), (-1, -1)),
            ('SPLITLONGWORDS', (0, 0), (-1, -1), True),
        ]))
        
        # Manter seção mais compacta - usar KeepTogether apenas para conteúdo crítico
        story.append(Paragraph(
            description,
            self.styles['Normal']
        ))
        story.append(Spacer(1, 8))
        story.append(period_table)
        story.append(Spacer(1, 20))  # Espaçamento reduzido
    
    def _add_periods_with_vehicles(self, story, structured_data):
        """Adiciona períodos operacionais organizados POR DIA (nova estrutura)"""
        # Usar quebra inteligente apenas para esta seção complexa
        self._add_smart_break_if_needed(story, 200)
        
        # Título adaptativo baseado no tipo de relatório
        vehicle_count = structured_data['resumo_geral']['total_veiculos']
        if vehicle_count == 1:
            section_title = "3. Desempenho Diário por Horário"
        else:
            section_title = "3. Desempenho Diário por Horário Operacional"
            
        story.append(Paragraph(section_title, self.styles['SectionTitle']))
        
        story.append(Paragraph(
            "Dados organizados dia a dia com detalhamento por período operacional:",
            self.styles['Normal']
        ))
        story.append(Spacer(1, 10))  # Espaçamento reduzido
        
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
                                self._format_distance(vehicle['km_periodo'], decimals=0),
                                format_speed(vehicle.get('vel_max_periodo', 0), vehicle.get('km_periodo', 0), include_unit=False, decimals=0),
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
                            # Prevenção completa de quebras na tabela
                            ('NOSPLIT', (0, 0), (-1, -1)),
                            ('WORDWRAP', (0, 0), (-1, -1)),
                            ('SPLITLONGWORDS', (0, 0), (-1, -1), True),
                        ]))
                        
                        story.append(vehicles_table)
                        story.append(Spacer(1, 10))
            else:
                # Dias individuais (ou final de semana incompleto)
                for dia_str, periodos_do_dia, data_obj in group:
                    if data_obj.weekday() >= 5:  # É weekend mas só um dia
                        # Exibe o intervalo completo de Sábado + Domingo, mesmo que apenas um dia tenha dados
                        if data_obj.weekday() == 5:  # Sábado
                            sabado = data_obj
                            domingo = data_obj + timedelta(days=1)
                        else:  # Domingo
                            domingo = data_obj
                            sabado = data_obj - timedelta(days=1)
                        weekend_title = f"Final de Semana ({sabado.strftime('%d/%m/%Y')} + {domingo.strftime('%d/%m/%Y')})"
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
                        
                        período_title = f"{nome_periodo} ({period_info['horario']})"
                        period_color = color_map.get(period_info['cor'], colors.HexColor('#95A5A6'))
                        
                        # Tabela SEM coluna cliente - colunas essenciais
                        vehicle_data = [['Placa', 'Km', 'Vel. Máx.', 'Combustível']]
                        
                        for vehicle in vehicles_list:
                            vehicle_data.append([
                                vehicle['placa'],
                                self._format_distance(vehicle['km_periodo'], decimals=0),
                                format_speed(vehicle.get('vel_max_periodo', 0), vehicle.get('km_periodo', 0), include_unit=False, decimals=0),
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
                            # Prevenção completa de quebras na tabela
                            ('NOSPLIT', (0, 0), (-1, -1)),
                            ('WORDWRAP', (0, 0), (-1, -1)),
                            ('SPLITLONGWORDS', (0, 0), (-1, -1), True),
                        ]))
                        
                        # Usar KeepTogether apenas para períodos individuais, não seções inteiras
                        period_content = [
                            Paragraph(período_title, self.styles['Normal']),
                            Spacer(1, 3),  # Espaçamento reduzido
                            vehicles_table
                        ]
                        story.append(KeepTogether(period_content))
                        story.append(Spacer(1, 8))  # Espaçamento reduzido entre períodos
            
            story.append(Spacer(1, 12))  # Espaço reduzido entre grupos
        
        if len(periodos_diarios) > 7:
            story.append(Paragraph(f"<i>Nota: Exibindo os 7 dias mais recentes. Total de {len(periodos_diarios)} dias disponíveis.</i>", self.styles['ObservationStyle']))
        
        story.append(Spacer(1, 15))  # Espaçamento final reduzido
    
    def _add_performance_rankings(self, story, structured_data):
        """Adiciona ranking único estilo campeonato (classificação)"""
        # Não forçar PageBreak - deixar o sistema decidir naturalmente
        
        # Usa o novo ranking campeonato
        ranking_campeonato = structured_data.get('ranking_campeonato', {})
        
        if not ranking_campeonato or not ranking_campeonato.get('veiculos'):
            story.append(Paragraph("Nenhum dado de ranking disponível.", self.styles['Normal']))
            return
        
        # Título adaptativo baseado no tipo de relatório
        vehicle_count = structured_data['resumo_geral']['total_veiculos']
        if vehicle_count == 1:
            # Para veículo individual, não mostra ranking (não faz sentido comparar consigo mesmo)
            return
        else:
            section_title = "4. Ranking de Desempenho Custo/Benefício"
        
        story.append(Paragraph(escape(str(ranking_campeonato.get('titulo', 'Rankings'))), self.styles['SubsectionTitle']))
        story.append(Paragraph(f"<i>{escape(str(ranking_campeonato.get('descricao', '')))}</i>", self.styles['Normal']))
        story.append(Spacer(1, 10))
        
        # Tabela única estilo campeonato
        ranking_data = [['Posição', 'Placa', 'Km', 'Combustível', 'Vel. Máx.', 'Score C/B']]
        
        veiculos = ranking_campeonato['veiculos']
        for vehicle in veiculos:
            posicao = vehicle['posicao_ranking']
            ranking_data.append([
                f"{posicao}º",
                vehicle['placa'],
                self._format_distance(vehicle['km_total'], decimals=0),
                f"{vehicle['combustivel']:.1f}L",  # Mostra combustível em litros
                format_speed(vehicle.get('velocidade_maxima', 0), vehicle.get('km_total', 0), include_unit=False, decimals=0),
                f"{vehicle['score_custo_beneficio']:.2f}"
            ])
        
        ranking_table = Table(ranking_data, colWidths=[0.8*inch, 1*inch, 1*inch, 1*inch, 1*inch, 1*inch])
        
        # Estilo da tabela com cores para top 3 e bottom 3 + prevenção de quebras
        table_style = [
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2C3E50')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('ALIGN', (0, 1), (-1, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#BDC3C7')),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            # Prevenção de quebras e cortes
            ('NOSPLIT', (0, 0), (-1, -1)),  # Evita quebrar tabela no meio
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#F8F9FA')]),
            ('WORDWRAP', (0, 0), (-1, -1)),  # Quebra palavras longas
            ('SPLITLONGWORDS', (0, 0), (-1, -1), True),  # Divide palavras muito longas
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
        
        # Organizar ranking de forma mais compacta - remover KeepTogether excessivo
        story.append(Paragraph(section_title, self.styles['SectionTitle']))
        story.append(Paragraph(escape(str(ranking_campeonato.get('titulo', 'Rankings'))), self.styles['SubsectionTitle']))
        story.append(Paragraph(f"<i>{escape(str(ranking_campeonato.get('descrição', '')))}</i>", self.styles['Normal']))
        story.append(Spacer(1, 8))
        story.append(ranking_table)
        story.append(Spacer(1, 12))  # Espaçamento reduzido
        
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
        
        story.append(Spacer(1, 15))  # Espaçamento reduzido após legenda
    
    def _create_cost_benefit_ranking_table(self, story, ranking, header_color, bg_color):
        """Cria tabela de ranking custo/benefício sem coluna cliente"""
        categoria = ranking['categoria']
        veiculos = ranking['veiculos']
        criterio = ranking['criterio']
        descricao = ranking.get('descricao', '')
        
        story.append(Paragraph(f"<b>{escape(str(categoria))}:</b>", self.styles['Normal']))
        if descricao:
            story.append(Paragraph(f"<i>{escape(str(descricao))}</i>", self.styles['ObservationStyle']))
        
        ranking_data = [['Posição', 'Placa', 'Km', 'Combustível', 'Vel. Máx.', 'Score C/B']]
        
        for i, vehicle in enumerate(veiculos, 1):
            if criterio == 'score_custo_beneficio':
                score_value = f"{vehicle['score_custo_beneficio']:.2f}"
            else:
                score_value = "N/A"
            
            ranking_data.append([
                f"{i}º",
                vehicle['placa'],
                self._format_distance(vehicle['km_total'], decimals=0),
                f"{vehicle['combustivel']:.1f}L",  # Mostra combustível em litros
                format_speed(vehicle.get('velocidade_maxima', 0), vehicle.get('km_total', 0), include_unit=False, decimals=0),
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
            # Prevenção de quebras
            ('NOSPLIT', (0, 0), (-1, -1)),
            ('WORDWRAP', (0, 0), (-1, -1)),
            ('SPLITLONGWORDS', (0, 0), (-1, -1), True)
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
                valor = self._format_distance(vehicle['km_total'], decimals=1)
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
            # Prevenção de quebras
            ('NOSPLIT', (0, 0), (-1, -1)),
            ('WORDWRAP', (0, 0), (-1, -1)),
            ('SPLITLONGWORDS', (0, 0), (-1, -1), True)
        ]))
        
        story.append(ranking_table)
        story.append(Spacer(1, 10))
    
    def _add_daily_performance(self, story, structured_data):
        """Adiciona desempenho diário da frota sem coluna cliente"""
        # Remover PageBreak forçado - permitir fluxo natural
        
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
                    self._format_distance(total_km_weekend, decimals=0),
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
                    self._format_distance(total_km_day, decimals=0),
                    f"{total_fuel_day:.1f}"
                ])
                
                processed_dates.add(date_str)
        
        story.append(Paragraph("5. Detalhamento por Dia", self.styles['SectionTitle']))
        story.append(Paragraph(
            "Desempenho diário com dados resumidos de todos os veículos:",
            self.styles['Normal']
        ))
        story.append(Spacer(1, 8))  # Espaçamento reduzido
        
        if len(daily_data) > 11:  # 1 header + 10 days
            daily_data = [daily_data[0]] + daily_data[-10:]
            story.append(Paragraph("<i>Mostrando os 10 dias mais recentes</i>", self.styles['ObservationStyle']))
            story.append(Spacer(1, 5))
        
        # Cria a tabela diária com estilo completo
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
            # Prevenção de quebras
            ('NOSPLIT', (0, 0), (-1, -1)),
            ('WORDWRAP', (0, 0), (-1, -1)),
            ('SPLITLONGWORDS', (0, 0), (-1, -1), True)
        ]))
        
        # Usar estrutura mais simples sem KeepTogether excessivo
        story.append(daily_table)
        story.append(Spacer(1, 20))  # Espaçamento reduzido
    
    def _add_footer_observations(self, story):
        """Adiciona observações e metodologia no rodapé"""
        # Só adicionar PageBreak se realmente necessário
        # Deixar o sistema decidir automaticamente
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
    
    def generate_enhanced_pdf_report(self, placa: str, data_inicio: datetime, data_fim: datetime, output_path: str) -> Dict:
        """
        Gera relatório PDF com estrutura melhorada: dados diários/semanais abrangentes e mensais gerais
        """
        try:
            analyzer = self._get_analyzer()
            
            # Buscar dados do veículo
            df = analyzer.get_vehicle_data(placa, data_inicio, data_fim)
            
            if df.empty:
                return {
                    'success': False,
                    'error': 'Nenhum dado encontrado para o período especificado',
                    'file_path': None
                }
            
            # Determinar tipo de análise baseado no período
            period_days = (data_fim - data_inicio).days + 1
            
            if period_days <= 7:
                # Análise diária detalhada
                analysis_type = 'daily'
                period_analysis = analyzer.generate_daily_analysis(df, placa)
            elif period_days <= 31:
                # Análise semanal com gráficos
                analysis_type = 'weekly'
                period_analysis = analyzer.generate_weekly_analysis(df, placa)
            else:
                # Análise mensal com dados gerais
                analysis_type = 'monthly'
                period_analysis = analyzer.generate_monthly_analysis(df, placa)
            
            # Gerar métricas gerais
            general_metrics = analyzer.generate_summary_metrics(df, placa)
            
            # Gerar insights
            insights = self._generate_enhanced_insights(general_metrics, period_analysis, analysis_type)
            
            # Criar arquivo PDF
            filename = f"relatorio_aprimorado_{placa}_{data_inicio.strftime('%Y%m%d')}_{data_fim.strftime('%Y%m%d')}.pdf"
            filepath = os.path.join(output_path, filename)
            
            doc = SimpleDocTemplate(filepath, pagesize=A4,
                                  rightMargin=72, leftMargin=72,
                                  topMargin=72, bottomMargin=18)
            
            # Construir story do PDF
            story = []
            
            # 1. Capa
            story.extend(self.create_enhanced_cover_page(general_metrics, analysis_type, period_days))
            
            # 2. Sumário Executivo
            story.extend(self.create_executive_summary(general_metrics, insights))
            
            # 3. Análise de Qualidade dos Dados
            story.extend(self.create_data_quality_section(general_metrics))
            
            # 4. Análise por Período (Diário/Semanal/Mensal)
            story.extend(self.create_period_analysis_section(period_analysis, analysis_type))
            
            # 5. Desempenho Operacional
            story.extend(self.create_operational_analysis(general_metrics))
            
            # 6. Gráficos e Visualizações
            if analysis_type == 'weekly' and 'performance_chart' in period_analysis:
                story.extend(self.create_charts_section(period_analysis['performance_chart']))
            
            # 7. Recomendações
            story.extend(self.create_recommendations_section(insights, general_metrics))
            
            # Gerar PDF
            doc.build(story)
            
            # Calcular tamanho do arquivo
            file_size_mb = os.path.getsize(filepath) / (1024 * 1024)
            
            return {
                'success': True,
                'file_path': filepath,
                'filename': filename,
                'file_size_mb': round(file_size_mb, 2),
                'analysis_type': analysis_type,
                'period_days': period_days,
                'data_quality': general_metrics.get('observabilidade', {}).get('consistencia', {})
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Erro ao gerar relatório: {str(e)}",
                'file_path': None
            }
    
    def create_enhanced_cover_page(self, metrics: Dict, analysis_type: str, period_days: int) -> List:
        """Cria capa melhorada com informações do tipo de análise"""
        story = []
        
        # Título principal
        title = f"Relatório de Telemetria Veicular - Análise {analysis_type.title()}"
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
        <b>Tipo de Análise:</b> {analysis_type.upper()}<br/>
        <b>Período de Análise:</b> {period_days} dia(s)
        """
        story.append(Paragraph(info_text, self.styles['Normal']))
        
        story.append(Spacer(1, 30))
        
        # Indicadores de qualidade
        observabilidade = metrics.get('observabilidade', {}).get('consistencia', {})
        percentual_dados_validos = observabilidade.get('percentual_dados_validos', 0)
        
        quality_text = f"""
        <b>Qualidade dos Dados:</b><br/>
        Dados válidos: {percentual_dados_validos}%<br/>
        Registros processados: {observabilidade.get('registros_validos', 0)} de {observabilidade.get('total_registros', 0)}
        """
        story.append(Paragraph(quality_text, self.styles['Normal']))
        
        story.append(Spacer(1, 50))
        
        # Data de geração
        data_geracao = datetime.now().strftime('%d/%m/%Y às %H:%M')
        story.append(Paragraph(f"Relatório gerado em: {escape(data_geracao)}", 
                              self.styles['Normal']))
        
        story.append(PageBreak())
        return story
    
    def create_data_quality_section(self, metrics: Dict) -> List:
        """Cria seção de análise de qualidade dos dados"""
        story = []
        
        story.append(Paragraph("2. Qualidade e Consistência dos Dados", self.styles.get('SectionTitle', self.styles['SubtitleStyle'])))
        
        observabilidade = metrics.get('observabilidade', {}).get('consistencia', {})
        
        # Tabela de qualidade dos dados
        quality_data = [
            ['Métrica', 'Valor', 'Descrição'],
            ['Total de Registros', f"{observabilidade.get('total_registros', 0):,}", 'Registros brutos importados'],
            ['Registros Válidos', f"{observabilidade.get('registros_validos', 0):,}", 'Dados consistentes processados'],
            ['Dados Filtrados', f"{observabilidade.get('dados_filtrados', 0):,}", 'Registros inconsistentes removidos'],
            ['Percentual Válido', f"{observabilidade.get('percentual_dados_validos', 0)}%", 'Qualidade geral dos dados'],
            ['KM Inconsistentes', f"{observabilidade.get('inconsistentes_km', 0):,}", 'Registros com KM mas sem velocidade'],
            ['Velocidade sem KM', f"{observabilidade.get('velocidades_sem_km', 0):,}", 'Velocidade registrada sem deslocamento']
        ]
        
        quality_table = Table(quality_data, colWidths=[2.5*inch, 1.5*inch, 2.5*inch])
        quality_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#E74C3C')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F8F9FA')),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#BDC3C7')),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))
        
        story.append(quality_table)
        story.append(Spacer(1, 20))
        
        # Explicação dos filtros aplicados
        story.append(Paragraph("Filtros de Consistência Aplicados:", self.styles['SubtitleStyle']))
        
        filters_text = """
        • <b>Dados Irrelevantes Removidos:</b> Registros com quilometragem mas sem velocidade correspondente<br/>
        • <b>Sensores com Falha:</b> Velocidades registradas sem deslocamento real do veículo<br/>
        • <b>Consumo Inválido:</b> Estimativas de combustível apenas com movimento comprovado<br/>
        • <b>Validação Temporal:</b> Apenas registros com timestamps válidos e sequenciais
        """
        story.append(Paragraph(filters_text, self.styles['Normal']))
        
        story.append(PageBreak())
        return story
    
    def create_period_analysis_section(self, period_analysis: Dict, analysis_type: str) -> List:
        """Cria seção de análise por período"""
        story = []
        
        if analysis_type == 'daily':
            story.append(Paragraph("3. Análise Diária Detalhada", self.styles.get('SectionTitle', self.styles['SubtitleStyle'])))
            story.extend(self._create_daily_analysis(period_analysis))
        elif analysis_type == 'weekly':
            story.append(Paragraph("3. Análise Semanal Abrangente", self.styles.get('SectionTitle', self.styles['SubtitleStyle'])))
            story.extend(self._create_weekly_analysis(period_analysis))
        else:  # monthly
            story.append(Paragraph("3. Análise Mensal Geral", self.styles.get('SectionTitle', self.styles['SubtitleStyle'])))
            story.extend(self._create_monthly_analysis(period_analysis))
        
        return story
    
    def create_charts_section(self, chart_html: str) -> List:
        """Cria seção de gráficos"""
        story = []
        
        story.append(Paragraph("4. Gráficos de Desempenho Semanal", self.styles.get('SectionTitle', self.styles['SubtitleStyle'])))
        
        # Nota: Em uma implementação real, você converteria o HTML do Plotly para imagem
        # Por agora, vamos adicionar uma descrição
        story.append(Paragraph(
            "Gráficos de desempenho semanal disponíveis na versão web do relatório.",
            self.styles['Normal']
        ))
        
        story.append(PageBreak())
        return story
    
    def create_recommendations_section(self, insights: List[str], metrics: Dict) -> List:
        """Cria seção de recomendações"""
        story = []
        
        story.append(Paragraph("5. Recomendações e Insights", self.styles.get('SectionTitle', self.styles['SubtitleStyle'])))
        
        for insight in insights:
            story.append(Paragraph(f"• {escape(str(insight))}", self.styles['Normal']))
            story.append(Spacer(1, 5))
        
        return story
    
    def _generate_enhanced_insights(self, metrics: Dict, period_analysis: Dict, analysis_type: str) -> List[str]:
        """Gera insights melhorados baseados na qualidade dos dados e tipo de análise"""
        insights = []
        
        # Insights sobre qualidade dos dados
        observabilidade = metrics.get('observabilidade', {}).get('consistencia', {})
        percentual_valido = observabilidade.get('percentual_dados_validos', 0)
        
        if percentual_valido >= 95:
            insights.append("Excelente qualidade dos dados: +95% dos registros são válidos e consistentes")
        elif percentual_valido >= 85:
            insights.append("Boa qualidade dos dados, com alguns registros inconsistentes filtrados")
        else:
            insights.append("Qualidade dos dados pode ser melhorada - verificar sensores do veículo")
        
        # Insights específicos por tipo de análise
        if analysis_type == 'daily':
            insights.append("Análise diária permite identificar padrões de uso detalhados")
        elif analysis_type == 'weekly':
            insights.append("Análise semanal revela tendências de desempenho e eficiência")
        else:
            insights.append("Análise mensal fornece visão geral do comportamento operacional")
        
        # Insights sobre operação
        operacao = metrics.get('operacao', {})
        km_total = operacao.get('km_total', 0)
        if km_total > 1000:
            insights.append("Alto índice de utilização do veículo - ótimo aproveitamento")
        elif km_total > 500:
            insights.append("Utilização moderada do veículo - dentro do esperado")
        else:
            insights.append("Baixa utilização do veículo - verificar necessidade operacional")
        
        return insights
    
    def _create_daily_analysis(self, period_analysis: Dict) -> List:
        """Cria análise diária detalhada"""
        story = []
        
        daily_metrics = period_analysis.get('daily_metrics', [])
        
        if not daily_metrics:
            story.append(Paragraph("Nenhum dado diário disponível.", self.styles['Normal']))
            return story
        
        # Tabela de dados diários
        daily_data = [['Data', 'KM Total', 'Vel. Máxima', 'Combustível', 'Tempo Movimento']]
        
        for day_data in daily_metrics:
            operacao = day_data.get('operacao', {})
            combustivel = day_data.get('combustivel', {})
            data_str = day_data.get('data', '').strftime('%d/%m/%Y') if hasattr(day_data.get('data', ''), 'strftime') else str(day_data.get('data', ''))
            
            daily_data.append([
                data_str,
                self._format_distance(operacao.get('km_total', 0), decimals=1),
                format_speed(operacao.get('velocidade_maxima', 0), operacao.get('km_total', 0), include_unit=False),
                f"{combustivel.get('fuel_consumed_liters', 0):.1f}L",
                f"{operacao.get('tempo_em_movimento', 0)} reg."
            ])
        
        daily_table = Table(daily_data, colWidths=[1.2*inch, 1.2*inch, 1.2*inch, 1.2*inch, 1.2*inch])
        daily_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#27AE60')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#E8F8F5')),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('ALIGN', (0, 1), (-1, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#BDC3C7')),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))
        
        story.append(daily_table)
        story.append(PageBreak())
        return story
    
    def _create_weekly_analysis(self, period_analysis: Dict) -> List:
        """Cria análise semanal com gráficos"""
        story = []
        
        weekly_metrics = period_analysis.get('weekly_metrics', [])
        
        if not weekly_metrics:
            story.append(Paragraph("Nenhum dado semanal disponível.", self.styles['Normal']))
            return story
        
        # Tabela de dados semanais
        weekly_data = [['Semana', 'KM Total', 'Vel. Máxima', 'Combustível', 'Eficiência']]
        
        for week_data in weekly_metrics:
            operacao = week_data.get('operacao', {})
            combustivel = week_data.get('combustivel', {})
            
            weekly_data.append([
                week_data.get('semana', ''),
                self._format_distance(operacao.get('km_total', 0), decimals=1),
                format_speed(operacao.get('velocidade_maxima', 0), operacao.get('km_total', 0), include_unit=False),
                f"{combustivel.get('fuel_consumed_liters', 0):.1f}L",
                f"{combustivel.get('efficiency_kmL', 0):.1f} km/L"
            ])
        
        weekly_table = Table(weekly_data, colWidths=[1.2*inch, 1.2*inch, 1.2*inch, 1.2*inch, 1.2*inch])
        weekly_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3498DB')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#EBF3FD')),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('ALIGN', (0, 1), (-1, -1), 'CENTER'),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#BDC3C7')),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))
        
        story.append(weekly_table)
        story.append(Spacer(1, 20))
        
        # Adicionar referência aos gráficos
        story.append(Paragraph(
            "Gráficos de desempenho semanal detalhados estão disponíveis na próxima seção.",
            self.styles['Normal']
        ))
        
        story.append(PageBreak())
        return story
    
    def _create_monthly_analysis(self, period_analysis: Dict) -> List:
        """Cria análise mensal geral"""
        story = []
        
        general_metrics = period_analysis.get('general_metrics', {})
        monthly_summary = period_analysis.get('monthly_summary', [])
        
        # Métricas gerais do período
        operacao = general_metrics.get('operacao', {})
        combustivel = general_metrics.get('combustivel', {})
        
        summary_data = [
            ['Métrica Geral', 'Valor'],
            ['Quilometragem Total', self._format_distance(operacao.get('km_total', 0), decimals=2)],
            ['Velocidade Máxima', format_speed(operacao.get('velocidade_maxima', 0), operacao.get('km_total', 0))],
            ['Combustível Total', f"{combustivel.get('fuel_consumed_liters', 0):.2f} L"],
            ['Eficiência Média', f"{combustivel.get('efficiency_kmL', 0):.2f} km/L"],
            ['Tempo em Movimento', f"{operacao.get('tempo_em_movimento', 0)} registros"]
        ]
        
        summary_table = Table(summary_data, colWidths=[3*inch, 2*inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#8E44AD')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#F4F6F7')),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#BDC3C7')),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))
        
        story.append(summary_table)
        story.append(PageBreak())
        return story

if __name__ == "__main__":
    # Teste do gerador
    print("Gerador de relatórios PDF carregado com sucesso!")