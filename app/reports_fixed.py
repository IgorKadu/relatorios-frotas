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
        
        <b>🎯