"""
Serviços de análise e geração de insights para telemetria veicular.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
from typing import Dict, List, Tuple, Optional
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, or_
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import folium
from folium import plugins
import base64
from io import BytesIO

# ==============================
# LOGGING E FEATURE FLAGS GLOBAIS
# ==============================
import logging

# Logger padronizado do módulo (evita NameError e facilita auditoria)
logger = logging.getLogger("relatorios_frotas.services")
if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter(
        fmt='%(asctime)s [%(levelname)s] %(name)s - %(message)s'
    ))
    logger.addHandler(_handler)
logger.setLevel(logging.INFO)

# Flag de feature para cálculo de KM consistente
# True  -> soma KM apenas quando há incremento de odômetro e velocidade > 0
# False -> comportamento legado (soma todo incremento de odômetro)
CONSISTENT_SPEED_KM_ONLY = True

from .models import Cliente, Veiculo, PosicaoHistorica, get_session
from .utils import get_fuel_consumption_estimate


# ==============================
# REGRAS DE VALIDAÇÃO DE DADOS
# ==============================

class DataQualityRules:
    """
    Regras para validação e filtragem de dados inconsistentes
    """
    
    @staticmethod
    def validate_telemetry_consistency(df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove registros com dados inconsistentes que não fazem sentido para relatórios profissionais:
        - KM > 0 mas velocidade = 0
        - Velocidade > 0 mas KM = 0  
        - KM e velocidade > 0 mas consumo de combustível = 0
        """
        if df.empty:
            return df
        
        logger.info(f"Validando consistência de {len(df)} registros")
        original_count = len(df)
        
        # Cria cópia para não modificar o original
        df_clean = df.copy()
        
        # Regra 1: Remove KM > 0 com velocidade = 0
        if 'odometro_periodo_km' in df_clean.columns and 'velocidade_kmh' in df_clean.columns:
            mask_invalid_km_speed = (
                (df_clean['odometro_periodo_km'] > 0) & 
                (df_clean['velocidade_kmh'] == 0)
            )
            df_clean = df_clean[~mask_invalid_km_speed]
            
        # Regra 2: Remove velocidade > 0 com KM = 0 (quando em movimento)
        if 'velocidade_kmh' in df_clean.columns and 'odometro_periodo_km' in df_clean.columns and 'em_movimento' in df_clean.columns:
            mask_invalid_speed_km = (
                (df_clean['velocidade_kmh'] > 0) & 
                (df_clean['odometro_periodo_km'] == 0) &
                (df_clean['em_movimento'] == True)
            )
            df_clean = df_clean[~mask_invalid_speed_km]
        
        # Regra 3: Valida dados de GPS/GPRS quando disponíveis
        if 'latitude' in df_clean.columns and 'longitude' in df_clean.columns:
            mask_invalid_coords = (
                (df_clean['latitude'] == 0) & 
                (df_clean['longitude'] == 0)
            )
            df_clean = df_clean[~mask_invalid_coords]
        
        removed_count = original_count - len(df_clean)
        if removed_count > 0:
            logger.info(f"Removidos {removed_count} registros inconsistentes ({removed_count/original_count:.1%})")
        
        return df_clean
    
    @staticmethod
    def calculate_fuel_consistency(km_total: float, speed_avg: float, movement_time_hours: float) -> Optional[float]:
        """
        Calcula consumo de combustível apenas quando os dados fazem sentido
        """
        # Só calcula se há movimento real
        if km_total <= 0 or speed_avg <= 0 or movement_time_hours <= 0:
            return None
        
        # Estima baseado em velocidade média e tempo
        return get_fuel_consumption_estimate(km_total, speed_avg)


# ==============================
# AGREGADORES PARA DIFERENTES PERÍODOS
# ==============================

class PeriodAggregator:
    """
    Agregador de dados para diferentes períodos de análise
    """
    
    @staticmethod
    def aggregate_daily(df: pd.DataFrame) -> Dict:
        """
        Agrega dados por dia
        """
        if df.empty:
            return {}
        
        # Aplica validação de qualidade
        df_clean = DataQualityRules.validate_telemetry_consistency(df)
        
        daily_data = {}
        df_clean['date'] = pd.to_datetime(df_clean['data_evento']).dt.date
        
        for date, day_df in df_clean.groupby('date'):
            daily_data[date] = {
                'total_registros': len(day_df),
                'km_total': day_df['odometro_periodo_km'].sum() if CONSISTENT_SPEED_KM_ONLY else day_df['odometro_periodo_km'].sum(),
                'velocidade_max': day_df['velocidade_kmh'].max(),
                'velocidade_media': day_df[day_df['velocidade_kmh'] > 0]['velocidade_kmh'].mean() if len(day_df[day_df['velocidade_kmh'] > 0]) > 0 else 0,
                'tempo_ligado_horas': len(day_df[day_df['ligado'] == True]) * 5 / 60,  # 5min intervals
                'tempo_movimento_horas': len(day_df[day_df['em_movimento'] == True]) * 5 / 60,
                'alertas_velocidade': len(day_df[day_df['velocidade_kmh'] > 80]),
                'periodos_operacionais': day_df['periodo_operacional'].value_counts().to_dict()
            }
            
            # Adiciona consumo de combustível validado
            km = daily_data[date]['km_total']
            speed_avg = daily_data[date]['velocidade_media']
            movement_hours = daily_data[date]['tempo_movimento_horas']
            daily_data[date]['combustivel_estimado'] = DataQualityRules.calculate_fuel_consistency(
                km, speed_avg, movement_hours
            )
        
        return daily_data
    
    @staticmethod  
    def aggregate_weekly(df: pd.DataFrame) -> Dict:
        """
        Agrega dados por semana
        """
        if df.empty:
            return {}
        
        df_clean = DataQualityRules.validate_telemetry_consistency(df)
        
        weekly_data = {}
        df_clean['week'] = pd.to_datetime(df_clean['data_evento']).dt.to_period('W')
        
        for week, week_df in df_clean.groupby('week'):
            week_start = week.start_time.date()
            week_end = week.end_time.date()
            
            weekly_data[f"{week_start} a {week_end}"] = {
                'periodo': f"Semana de {week_start.strftime('%d/%m')} a {week_end.strftime('%d/%m')}",
                'total_registros': len(week_df),
                'km_total': week_df['odometro_periodo_km'].sum(),
                'velocidade_max': week_df['velocidade_kmh'].max(),
                'velocidade_media': week_df[week_df['velocidade_kmh'] > 0]['velocidade_kmh'].mean() if len(week_df[week_df['velocidade_kmh'] > 0]) > 0 else 0,
                'tempo_ligado_horas': len(week_df[week_df['ligado'] == True]) * 5 / 60,
                'tempo_movimento_horas': len(week_df[week_df['em_movimento'] == True]) * 5 / 60,
                'alertas_velocidade': len(week_df[week_df['velocidade_kmh'] > 80]),
                'dias_operacao': pd.to_datetime(week_df['data_evento']).dt.date.nunique(),
                'periodos_operacionais': week_df['periodo_operacional'].value_counts().to_dict()
            }
            
            # Adiciona análise de produtividade semanal
            km = weekly_data[f"{week_start} a {week_end}"]['km_total']
            days = weekly_data[f"{week_start} a {week_end}"]['dias_operacao']
            weekly_data[f"{week_start} a {week_end}"]['produtividade_km_dia'] = km / days if days > 0 else 0
        
        return weekly_data
    
    @staticmethod
    def compute_vehicle_rankings(vehicles_data: Dict) -> Dict:
        """
        Computa rankings de veículos para destacar melhores e piores performances
        """
        if not vehicles_data:
            return {}
        
        rankings = {
            'melhor_km': [],
            'pior_km': [],
            'melhor_eficiencia': [],
            'pior_eficiencia': [],
            'mais_alertas': [],
            'menos_alertas': []
        }
        
        # Prepara dados para ranking
        vehicles_metrics = []
        for placa, data in vehicles_data.items():
            if data.get('km_total', 0) > 0:  # Só considera veículos com movimento real
                vehicles_metrics.append({
                    'placa': placa,
                    'km_total': data.get('km_total', 0),
                    'eficiencia': data.get('km_total', 0) / max(data.get('tempo_movimento_horas', 1), 1),
                    'alertas_velocidade': data.get('alertas_velocidade', 0),
                    'combustivel_estimado': data.get('combustivel_estimado', 0)
                })
        
        if vehicles_metrics:
            # Ranking por KM
            sorted_by_km = sorted(vehicles_metrics, key=lambda x: x['km_total'], reverse=True)
            rankings['melhor_km'] = sorted_by_km[:3]
            rankings['pior_km'] = sorted_by_km[-3:]
            
            # Ranking por eficiência
            sorted_by_efficiency = sorted(vehicles_metrics, key=lambda x: x['eficiencia'], reverse=True)
            rankings['melhor_eficiencia'] = sorted_by_efficiency[:3]
            rankings['pior_eficiencia'] = sorted_by_efficiency[-3:]
            
            # Ranking por alertas
            sorted_by_alerts = sorted(vehicles_metrics, key=lambda x: x['alertas_velocidade'])
            rankings['menos_alertas'] = sorted_by_alerts[:3]
            rankings['mais_alertas'] = sorted_by_alerts[-3:]
        
        return rankings


# ==============================
# SISTEMA DE HIGHLIGHTS E INSIGHTS
# ==============================

class HighlightGenerator:
    """
    Gerador de insights e highlights para relatórios
    """
    
    @staticmethod
    def compute_highlights(daily_data: Dict, weekly_data: Dict, vehicles_data: Dict) -> Dict:
        """
        Computa highlights principais baseado nos dados agregados
        """
        highlights = {
            'piores_dias': [],
            'melhores_dias': [],
            'melhor_veiculo': None,
            'pior_veiculo': None,
            'insights_gerais': [],
            'alertas_importantes': []
        }
        
        # Análise dos piores e melhores dias
        if daily_data:
            day_metrics = []
            for date, metrics in daily_data.items():
                if metrics.get('km_total', 0) > 0:  # Só considera dias com movimento
                    efficiency = metrics.get('km_total', 0) / max(metrics.get('tempo_movimento_horas', 1), 1)
                    day_metrics.append({
                        'data': date,
                        'km_total': metrics.get('km_total', 0),
                        'eficiencia': efficiency,
                        'alertas': metrics.get('alertas_velocidade', 0),
                        'tempo_movimento': metrics.get('tempo_movimento_horas', 0)
                    })
            
            if day_metrics:
                # Piores dias (menos KM ou mais alertas)
                worst_by_km = sorted(day_metrics, key=lambda x: x['km_total'])[:3]
                worst_by_alerts = sorted(day_metrics, key=lambda x: x['alertas'], reverse=True)[:3]
                
                highlights['piores_dias'] = {
                    'menor_km': worst_by_km,
                    'mais_alertas': worst_by_alerts
                }
                
                # Melhores dias (mais KM, maior eficiência)
                best_by_km = sorted(day_metrics, key=lambda x: x['km_total'], reverse=True)[:3]
                best_by_efficiency = sorted(day_metrics, key=lambda x: x['eficiencia'], reverse=True)[:3]
                
                highlights['melhores_dias'] = {
                    'maior_km': best_by_km,
                    'maior_eficiencia': best_by_efficiency
                }
        
        # Rankings de veículos
        vehicle_rankings = PeriodAggregator.compute_vehicle_rankings(vehicles_data)
        if vehicle_rankings:
            if vehicle_rankings.get('melhor_km'):
                highlights['melhor_veiculo'] = vehicle_rankings['melhor_km'][0]
            if vehicle_rankings.get('pior_km'):
                highlights['pior_veiculo'] = vehicle_rankings['pior_km'][-1]
        
        # Insights gerais baseados nos dados
        highlights['insights_gerais'] = HighlightGenerator._generate_insights(
            daily_data, weekly_data, vehicles_data
        )
        
        # Alertas importantes
        highlights['alertas_importantes'] = HighlightGenerator._generate_alerts(
            daily_data, weekly_data, vehicles_data
        )
        
        return highlights
    
    @staticmethod
    def _generate_insights(daily_data: Dict, weekly_data: Dict, vehicles_data: Dict) -> List[str]:
        """
        Gera insights automáticos baseados nos padrões dos dados
        """
        insights = []
        
        if daily_data:
            total_days = len(daily_data)
            days_with_movement = len([d for d in daily_data.values() if d.get('km_total', 0) > 0])
            
            if days_with_movement > 0:
                utilization_rate = days_with_movement / total_days
                if utilization_rate < 0.5:
                    insights.append(f"Taxa de utilização baixa: apenas {utilization_rate:.1%} dos dias tiveram movimento")
                elif utilization_rate > 0.9:
                    insights.append(f"Excelente taxa de utilização: {utilization_rate:.1%} dos dias com operação")
                
                # Análise de velocidade
                avg_speeds = [d.get('velocidade_media', 0) for d in daily_data.values() if d.get('km_total', 0) > 0]
                if avg_speeds:
                    overall_avg_speed = sum(avg_speeds) / len(avg_speeds)
                    if overall_avg_speed > 60:
                        insights.append(f"Velocidade média elevada: {overall_avg_speed:.1f} km/h - revisar padrões de condução")
                    elif overall_avg_speed < 20:
                        insights.append(f"Velocidade média baixa: {overall_avg_speed:.1f} km/h - possível operação urbana intensiva")
                
                # Análise de alertas
                total_alerts = sum([d.get('alertas_velocidade', 0) for d in daily_data.values()])
                if total_alerts > 0:
                    alert_rate = total_alerts / days_with_movement
                    if alert_rate > 5:
                        insights.append(f"Alto índice de alertas de velocidade: {alert_rate:.1f} por dia operacional")
        
        if weekly_data:
            # Análise de produtividade semanal
            weekly_productivities = [w.get('produtividade_km_dia', 0) for w in weekly_data.values()]
            if weekly_productivities:
                avg_productivity = sum(weekly_productivities) / len(weekly_productivities)
                insights.append(f"Produtividade média: {avg_productivity:.1f} km por dia operacional")
        
        if vehicles_data and len(vehicles_data) > 1:
            # Análise de disparidade entre veículos
            km_totals = [v.get('km_total', 0) for v in vehicles_data.values() if v.get('km_total', 0) > 0]
            if km_totals and len(km_totals) > 1:
                max_km = max(km_totals)
                min_km = min(km_totals)
                if max_km > 0 and min_km > 0:
                    disparity = max_km / min_km
                    if disparity > 3:
                        insights.append(f"Grande disparidade na utilização: veículo mais usado fez {disparity:.1f}x mais quilometragem")
        
        return insights
    
    @staticmethod
    def _generate_alerts(daily_data: Dict, weekly_data: Dict, vehicles_data: Dict) -> List[str]:
        """
        Gera alertas importantes baseados em padrões problemáticos
        """
        alerts = []
        
        # Alertas baseados em dados diários
        if daily_data:
            for date, data in daily_data.items():
                alerts_count = data.get('alertas_velocidade', 0)
                if alerts_count > 10:
                    alerts.append(f"⚠️ {date.strftime('%d/%m/%Y')}: {alerts_count} alertas de velocidade em um dia")
                
                km_total = data.get('km_total', 0)
                movement_time = data.get('tempo_movimento_horas', 0)
                if km_total > 0 and movement_time > 12:
                    alerts.append(f"⚠️ {date.strftime('%d/%m/%Y')}: Operação prolongada - {movement_time:.1f} horas de movimento")
        
        # Alertas baseados em veículos
        if vehicles_data:
            for placa, data in vehicles_data.items():
                alerts_count = data.get('alertas_velocidade', 0)
                km_total = data.get('km_total', 0)
                
                if km_total > 0 and alerts_count > 0:
                    alert_rate = alerts_count / (km_total / 100)  # alertas por 100km
                    if alert_rate > 5:
                        alerts.append(f"🚨 {placa}: Alto índice de alertas - {alert_rate:.1f} por 100km")
                
                fuel_estimated = data.get('combustivel_estimado')
                if fuel_estimated and fuel_estimated > 100:  # Consumo muito alto
                    alerts.append(f"⛽ {placa}: Consumo elevado estimado - {fuel_estimated:.1f}L")
        
        return alerts


class TelemetryAnalyzer:
    """Classe principal para análise de dados de telemetria"""
    
    def __init__(self):
        self.session = get_session()
        
        # Configurações de estilo para gráficos
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
    
    def __del__(self):
        """Fecha a sessão do banco ao destruir o objeto"""
        if hasattr(self, 'session'):
            self.session.close()
    
    def get_vehicle_data(self, placa: str, data_inicio: datetime, data_fim: datetime) -> pd.DataFrame:
        """
        Busca dados de um veículo em um período específico
        """
        try:
            # Handle same day periods - when start and end date are the same, 
            # adjust end date to include the entire day
            if data_inicio.date() == data_fim.date():
                # For same day, set end time to end of day (23:59:59)
                adjusted_data_fim = data_fim.replace(hour=23, minute=59, second=59, microsecond=999999)
            else:
                adjusted_data_fim = data_fim
            
            # Query para buscar dados
            query = self.session.query(PosicaoHistorica).join(Veiculo).filter(
                and_(
                    Veiculo.placa == placa,
                    PosicaoHistorica.data_evento >= data_inicio,
                    PosicaoHistorica.data_evento <= adjusted_data_fim
                )
            ).order_by(PosicaoHistorica.data_evento)
            
            # Converte para DataFrame
            dados = []
            for registro in query.all():
                dados.append({
                    'data_evento': registro.data_evento,
                    'velocidade_kmh': registro.velocidade_kmh,
                    'ignicao': registro.ignicao,
                    'latitude': registro.latitude,
                    'longitude': registro.longitude,
                    'endereco': registro.endereco,
                    'odometro_periodo_km': registro.odometro_periodo_km,
                    'odometro_embarcado_km': registro.odometro_embarcado_km,
                    'bateria_pct': registro.bateria_pct,
                    'tensao_v': registro.tensao_v,
                    'tipo_evento': registro.tipo_evento,
                    'gps_status': registro.gps_status,
                    'gprs_status': registro.gprs_status
                })
            
            df = pd.DataFrame(dados)
            
            if not df.empty:
                # Adiciona colunas calculadas
                df['periodo_operacional'] = df['data_evento'].apply(self._classify_operational_period)
                df['em_movimento'] = df['ignicao'].isin(['LM'])
                df['ligado'] = df['ignicao'].isin(['L', 'LP', 'LM'])
                
            return df
            
        except Exception as e:
            print(f"Erro ao buscar dados do veículo: {str(e)}")
            return pd.DataFrame()
    
    def _classify_operational_period(self, timestamp: datetime) -> str:
        """Classifica período operacional conforme definição do cliente"""
        # Final de semana (Sábado e Domingo)
        if timestamp.weekday() >= 5:  # 5=Sábado, 6=Domingo
            return 'final_semana'
        
        current_time = timestamp.time()
        
        # Horários Operacionais
        if time(4, 0) <= current_time < time(7, 0):  # 04:00 às 07:00
            return 'operacional_manha'
        elif time(10, 50) <= current_time < time(13, 0):  # 10:50 às 13:00
            return 'operacional_meio_dia'
        elif time(16, 50) <= current_time < time(19, 0):  # 16:50 às 19:00
            return 'operacional_tarde'
        
        # Fora de Horário Operacional
        elif time(7, 0) <= current_time < time(10, 50):  # 07:00 às 10:50
            return 'fora_horario_manha'
        elif time(13, 0) <= current_time < time(16, 50):  # 13:00 às 16:50
            return 'fora_horario_tarde'
        else:  # 19:00 às 04:00 (próximo dia)
            return 'fora_horario_noite'
    
    def generate_summary_metrics(self, df: pd.DataFrame, placa: str) -> Dict:
        """
        Gera métricas resumidas dos dados
        """
        if df.empty:
            return {}
        
        # Busca dados do veículo e cliente
        veiculo = self.session.query(Veiculo).filter_by(placa=placa).first()
        cliente = veiculo.cliente if veiculo else None
        
        # Garantir tipos numéricos corretos
        df = df.copy()
        df['velocidade_kmh'] = pd.to_numeric(df['velocidade_kmh'], errors='coerce').fillna(0.0)
        df['odometro_periodo_km'] = pd.to_numeric(df['odometro_periodo_km'], errors='coerce').fillna(0.0)
        
        # Flags de estado
        df['em_movimento'] = df.get('em_movimento', df['velocidade_kmh'] > 0)
        df['ligado'] = df.get('ligado', df['ignicao'].isin(['L', 'LP', 'LM']))
        
        # Cálculo robusto de quilometragem: soma dos incrementos positivos do odômetro
        odom_diff = df['odometro_periodo_km'].diff().fillna(0).clip(lower=0)
        
        # Validação aprimorada de dados relevantes
        # 1. Consistência: considerar deslocamento apenas quando há incremento de odômetro E velocidade > 0
        valid_displacement_mask = (odom_diff > 0) & (df['velocidade_kmh'] > 0)
        
        # 2. Filtrar dados irrelevantes: remover registros com KM mas sem velocidade
        inconsistent_km_mask = (odom_diff > 0) & (df['velocidade_kmh'] <= 0)
        
        # 3. Filtrar velocidades sem deslocamento real (possíveis erros de sensor)
        speed_without_movement_mask = (df['velocidade_kmh'] > 5) & (odom_diff <= 0)
        
        # Seleciona estratégia pelo feature flag (sempre usar modo consistente)
        if CONSISTENT_SPEED_KM_ONLY:
            km_total_calc = float(odom_diff[valid_displacement_mask].sum())
            vel_validas = df.loc[valid_displacement_mask, 'velocidade_kmh']
            
            # Filtrar dados para análise temporal (apenas registros válidos)
            df_clean = df[valid_displacement_mask | ((df['velocidade_kmh'] == 0) & (odom_diff == 0))]
        else:
            # Modo legado: considera todos os incrementos de odômetro
            km_total_calc = float(odom_diff.sum())
            vel_validas = df['velocidade_kmh']
            df_clean = df
        
        velocidade_maxima_calc = float(vel_validas.max()) if not vel_validas.empty else 0.0
        velocidade_media_calc = float(vel_validas.mean()) if not vel_validas.empty else 0.0
        
        # Métricas de consistência para auditoria/observabilidade (aprimoradas)
        inconsistentes_km = int(inconsistent_km_mask.sum())
        velocidades_sem_km = int(speed_without_movement_mask.sum())
        total_registros = int(len(df))
        registros_validos = int(len(df_clean))
        deslocamentos_consistentes = int(valid_displacement_mask.sum())
        deslocamentos_totais = int((odom_diff > 0).sum())
        dados_filtrados = total_registros - registros_validos
        
        # Log estruturado
        try:
            logger.info({
                'event': 'summary_metrics_computed',
                'placa': placa,
                'periodo': {
                    'inicio': str(df['data_evento'].min()),
                    'fim': str(df['data_evento'].max())
                },
                'flags': {
                    'CONSISTENT_SPEED_KM_ONLY': CONSISTENT_SPEED_KM_ONLY
                },
                'counters': {
                    'total_registros': total_registros,
                    'registros_validos': registros_validos,
                    'dados_filtrados': dados_filtrados,
                    'deslocamentos_totais': deslocamentos_totais,
                    'deslocamentos_consistentes': deslocamentos_consistentes,
                    'inconsistentes_km': inconsistentes_km,
                    'velocidades_sem_km': velocidades_sem_km
                },
                'metrics_preview': {
                    'km_total': round(km_total_calc, 3),
                    'velocidade_maxima': round(velocidade_maxima_calc, 2),
                    'velocidade_media': round(velocidade_media_calc, 2)
                }
            })
        except Exception:
            pass
        
        metrics = {
            'veiculo': {
                'placa': placa,
                'cliente': cliente.nome if cliente else 'N/A',
                'periodo_analise': {
                    'inicio': df['data_evento'].min(),
                    'fim': df['data_evento'].max(),
                    'total_dias': (df['data_evento'].max() - df['data_evento'].min()).days + 1
                }
            },
            'operacao': {
                'total_registros': total_registros,
                'km_total': km_total_calc,
                'velocidade_maxima': velocidade_maxima_calc if km_total_calc > 0 else 0.0,
                'velocidade_media': velocidade_media_calc if km_total_calc > 0 else 0.0,
                'tempo_total_ligado': int(len(df[df['ligado']])),
                'tempo_em_movimento': int(len(df[df['em_movimento']])),
                # Tempo em movimento apenas em trechos consistentes
                'tempo_em_movimento_consistente': int(valid_displacement_mask.sum()),
                'tempo_parado_ligado': int(len(df[(df['ligado']) & (~df['em_movimento'])])),
                'tempo_desligado': int(len(df[~df['ligado']]))
            },
            'periodos': {
                # Horários Operacionais detalhados
                'operacional_manha': int(len(df[df['periodo_operacional'] == 'operacional_manha'])),
                'operacional_meio_dia': int(len(df[df['periodo_operacional'] == 'operacional_meio_dia'])),
                'operacional_tarde': int(len(df[df['periodo_operacional'] == 'operacional_tarde'])),
                
                # Fora de Horário Operacional detalhados
                'fora_horario_manha': int(len(df[df['periodo_operacional'] == 'fora_horario_manha'])),
                'fora_horario_tarde': int(len(df[df['periodo_operacional'] == 'fora_horario_tarde'])),
                'fora_horario_noite': int(len(df[df['periodo_operacional'] == 'fora_horario_noite'])),
                
                # Final de Semana
                'final_semana': int(len(df[df['periodo_operacional'] == 'final_semana'])),
                
                # Totais calculados
                'total_operacional': int(len(df[df['periodo_operacional'].isin(['operacional_manha', 'operacional_meio_dia', 'operacional_tarde'])])),
                'total_fora_horario': int(len(df[df['periodo_operacional'].isin(['fora_horario_manha', 'fora_horario_tarde', 'fora_horario_noite'])])),
            },
            'conectividade': {
                'gps_ok': int(df['gps_status'].sum()),
                'gprs_ok': int(df['gprs_status'].sum()),
                'problemas_conexao': int(len(df) - min(df['gps_status'].sum(), df['gprs_status'].sum()))
            },
            'observabilidade': {
                'consistencia': {
                    'CONSISTENT_SPEED_KM_ONLY': CONSISTENT_SPEED_KM_ONLY,
                    'total_registros': total_registros,
                    'registros_validos': registros_validos,
                    'dados_filtrados': dados_filtrados,
                    'deslocamentos_totais': deslocamentos_totais,
                    'deslocamentos_consistentes': deslocamentos_consistentes,
                    'inconsistentes_km': inconsistentes_km,
                    'velocidades_sem_km': velocidades_sem_km,
                    'percentual_dados_validos': round((registros_validos / total_registros * 100), 2) if total_registros > 0 else 0
                }
            }
        }
        
        # Estimativa de combustível (derivada) – manter apenas como estimativa e não usar para "corrigir" km
        if metrics['operacao']['km_total'] > 0:
            fuel_data = get_fuel_consumption_estimate(
                metrics['operacao']['km_total'],
                metrics['operacao']['velocidade_media'],
                cliente.consumo_medio_kmL if cliente else 12.0
            )
            metrics['combustivel'] = fuel_data
        
        # Eventos especiais
        eventos_especiais = df[df['tipo_evento'].str.contains('Excesso|Violado|Bloq', na=False, case=False)]
        tipos_eventos_dict = {}
        if not eventos_especiais.empty:
            tipos_series = pd.Series(eventos_especiais['tipo_evento'])
            tipos_eventos_dict = tipos_series.value_counts().to_dict()
        
        metrics['eventos'] = {
            'total_eventos_especiais': int(len(eventos_especiais)),
            'tipos_eventos': tipos_eventos_dict
        }
        
        return metrics

    def generate_daily_analysis(self, df: pd.DataFrame, placa: str) -> Dict:
        """
        Gera análise detalhada por dia para dados diários/semanais abrangentes
        """
        if df.empty:
            return {}
        
        # Agrupar dados por dia
        df_copy = df.copy()
        df_copy['data'] = pd.to_datetime(df_copy['data_evento']).dt.date
        
        daily_data = []
        for data, group in df_copy.groupby('data'):
            day_metrics = self.generate_summary_metrics(group, placa)
            day_metrics['data'] = data
            daily_data.append(day_metrics)
        
        return {
            'period_type': 'daily',
            'total_days': len(daily_data),
            'daily_metrics': daily_data
        }
    
    def generate_weekly_analysis(self, df: pd.DataFrame, placa: str) -> Dict:
        """
        Gera análise semanal com gráficos de desempenho
        """
        if df.empty:
            return {}
        
        # Agrupar dados por semana
        df_copy = df.copy()
        df_copy['week'] = pd.to_datetime(df_copy['data_evento']).dt.isocalendar().week
        df_copy['year'] = pd.to_datetime(df_copy['data_evento']).dt.year
        df_copy['year_week'] = df_copy['year'].astype(str) + '-W' + df_copy['week'].astype(str).str.zfill(2)
        
        weekly_data = []
        for week, group in df_copy.groupby('year_week'):
            week_metrics = self.generate_summary_metrics(group, placa)
            week_metrics['semana'] = week
            week_metrics['periodo_inicio'] = group['data_evento'].min()
            week_metrics['periodo_fim'] = group['data_evento'].max()
            weekly_data.append(week_metrics)
        
        # Criar gráfico de desempenho semanal
        weekly_chart = self.create_weekly_performance_chart(weekly_data)
        
        return {
            'period_type': 'weekly',
            'total_weeks': len(weekly_data),
            'weekly_metrics': weekly_data,
            'performance_chart': weekly_chart
        }
    
    def generate_monthly_analysis(self, df: pd.DataFrame, placa: str) -> Dict:
        """
        Gera análise mensal com dados gerais
        """
        if df.empty:
            return {}
        
        # Análise geral do período completo
        general_metrics = self.generate_summary_metrics(df, placa)
        
        # Agrupar dados por mês para resumo
        df_copy = df.copy()
        df_copy['month'] = pd.to_datetime(df_copy['data_evento']).dt.to_period('M')
        
        monthly_summary = []
        for month, group in df_copy.groupby('month'):
            month_metrics = self.generate_summary_metrics(group, placa)
            month_metrics['mes'] = str(month)
            monthly_summary.append(month_metrics)
        
        return {
            'period_type': 'monthly',
            'general_metrics': general_metrics,
            'monthly_summary': monthly_summary
        }
    
    def create_weekly_performance_chart(self, weekly_data: List[Dict]) -> str:
        """
        Cria gráfico de desempenho semanal com Plotly
        """
        if not weekly_data:
            return ""
        
        # Extrair dados para gráfico
        weeks = [w.get('semana', '') for w in weekly_data]
        km_totals = [w.get('operacao', {}).get('km_total', 0) for w in weekly_data]
        max_speeds = [w.get('operacao', {}).get('velocidade_maxima', 0) for w in weekly_data]
        fuel_consumption = [w.get('combustivel', {}).get('fuel_consumed_liters', 0) for w in weekly_data]
        
        # Criar subplots
        fig = make_subplots(
            rows=3, cols=1,
            subplot_titles=('Quilometragem Semanal', 'Velocidade Máxima Semanal', 'Consumo de Combustível Semanal'),
            vertical_spacing=0.08
        )
        
        # Gráfico de quilometragem
        fig.add_trace(
            go.Scatter(
                x=weeks, y=km_totals,
                mode='lines+markers',
                name='KM Total',
                line=dict(color='blue', width=3),
                marker=dict(size=8)
            ),
            row=1, col=1
        )
        
        # Gráfico de velocidade máxima
        fig.add_trace(
            go.Scatter(
                x=weeks, y=max_speeds,
                mode='lines+markers',
                name='Velocidade Máxima',
                line=dict(color='red', width=3),
                marker=dict(size=8)
            ),
            row=2, col=1
        )
        
        # Gráfico de consumo de combustível
        fig.add_trace(
            go.Scatter(
                x=weeks, y=fuel_consumption,
                mode='lines+markers',
                name='Consumo (L)',
                line=dict(color='green', width=3),
                marker=dict(size=8)
            ),
            row=3, col=1
        )
        
        # Configurar layout
        fig.update_layout(
            title='Desempenho Semanal do Veículo',
            height=800,
            showlegend=False
        )
        
        # Atualizar eixos
        fig.update_xaxes(title_text="Semana", row=3, col=1)
        fig.update_yaxes(title_text="KM", row=1, col=1)
        fig.update_yaxes(title_text="km/h", row=2, col=1)
        fig.update_yaxes(title_text="Litros", row=3, col=1)
        
        return fig.to_html(include_plotlyjs='inline', div_id="weekly_performance_chart")

    def create_speed_chart(self, df: pd.DataFrame) -> str:
        """
        Cria gráfico de velocidade ao longo do tempo
        """
        if df.empty:
            return ""
        
        fig = go.Figure()
        
        # Gráfico de velocidade
        fig.add_trace(go.Scatter(
            x=df['data_evento'],
            y=df['velocidade_kmh'],
            mode='lines',
            name='Velocidade (km/h)',
            line=dict(color='blue', width=1)
        ))
        
        # Linha de velocidade máxima permitida (80 km/h)
        fig.add_hline(y=80, line_dash="dash", line_color="red", 
                     annotation_text="Limite de Velocidade")
        
        fig.update_layout(
            title='Velocidade ao Longo do Tempo',
            xaxis_title='Data/Hora',
            yaxis_title='Velocidade (km/h)',
            hovermode='x unified',
            height=400
        )
        
        # Converte para HTML
        return fig.to_html(include_plotlyjs='inline', div_id="speed_chart")
    
    def create_operational_periods_chart(self, df: pd.DataFrame) -> str:
        """
        Cria gráfico de distribuição por períodos operacionais
        """
        if df.empty:
            return ""
        
        periodo_counts = df['periodo_operacional'].value_counts()
        
        fig = go.Figure(data=[
            go.Pie(
                labels=periodo_counts.index,
                values=periodo_counts.values,
                hole=0.3
            )
        ])
        
        fig.update_layout(
            title='Distribuição por Períodos Operacionais',
            height=400
        )
        
        return fig.to_html(include_plotlyjs='inline', div_id="periods_chart")
    
    def create_ignition_status_chart(self, df: pd.DataFrame) -> str:
        """
        Cria gráfico de status da ignição
        """
        if df.empty:
            return ""
        
        # Mapeamento de status
        status_map = {
            'D': 'Desligado',
            'L': 'Ligado',
            'LP': 'Ligado Parado',
            'LM': 'Ligado Movimento'
        }
        
        df_status = df.copy()
        df_status['status_ignicao'] = df_status['ignicao'].astype(str).replace(status_map)
        status_counts = df_status['status_ignicao'].value_counts()
        
        fig = go.Figure(data=[
            go.Bar(
                x=status_counts.index,
                y=status_counts.values,
                marker_color=['red', 'green', 'orange', 'blue']
            )
        ])
        
        fig.update_layout(
            title='Distribuição do Status da Ignição',
            xaxis_title='Status',
            yaxis_title='Quantidade de Registros',
            height=400
        )
        
        return fig.to_html(include_plotlyjs='inline', div_id="ignition_chart")
    
    def create_route_map(self, df: pd.DataFrame) -> str:
        """
        Cria mapa interativo da rota percorrida
        """
        if df.empty:
            return "<p>Dados de localização não disponíveis para gerar mapa.</p>"
        
        # Check if all latitude and longitude values are NaN
        lat_lon_data = df[['latitude', 'longitude']]
        if lat_lon_data.isna().all().all():
            return "<p>Dados de localização não disponíveis para gerar mapa.</p>"
        
        # Remove registros sem coordenadas válidas
        df_map = df.dropna(subset=['latitude', 'longitude'])
        
        if df_map.empty:
            return "<p>Dados de localização não disponíveis para gerar mapa.</p>"
        
        # Centro do mapa
        center_lat = float(df_map['latitude'].mean())
        center_lon = float(df_map['longitude'].mean())
        
        # Cria mapa
        m = folium.Map(
            location=[float(center_lat), float(center_lon)],
            zoom_start=12,
            tiles='OpenStreetMap'
        )
        
        # Adiciona rota
        coords = df_map[['latitude', 'longitude']].values.tolist()
        folium.PolyLine(
            coords,
            color='blue',
            weight=3,
            opacity=0.8
        ).add_to(m)
        
        # Marcadores de início e fim
        if len(df_map) > 0:
            # Ponto inicial
            folium.Marker(
                [float(df_map.iloc[0]['latitude']), float(df_map.iloc[0]['longitude'])],
                popup='Início',
                icon=folium.Icon(color='green', icon='play')
            ).add_to(m)
            
            # Ponto final
            if len(df_map) > 1:
                folium.Marker(
                    [float(df_map.iloc[-1]['latitude']), float(df_map.iloc[-1]['longitude'])],
                    popup='Fim',
                    icon=folium.Icon(color='red', icon='stop')
                ).add_to(m)
        
        # Adiciona pontos de velocidade alta (>80 km/h)
        high_speed = df_map[df_map['velocidade_kmh'] > 80]
        for _, point in high_speed.iterrows():
            folium.CircleMarker(
                [float(point['latitude']), float(point['longitude'])],
                radius=5,
                popup=f"Velocidade: {point['velocidade_kmh']} km/h",
                color='red',
                fill=True,
                fillColor='red'
            ).add_to(m)
        
        # Converte para HTML
        return m._repr_html_()
    
    def create_detailed_route_map(self, df: pd.DataFrame) -> str:
        """
        Cria mapa detalhado de rotas com dados operacionais
        """
        if df.empty or df[['latitude', 'longitude']].isna().all().all():
            return "<p>Dados de localização não disponíveis para gerar mapa.</p>"
        
        # Remove registros sem coordenadas válidas
        df_map = df.dropna(subset=['latitude', 'longitude'])
        
        if df_map.empty:
            return "<p>Dados de localização não disponíveis para gerar mapa.</p>"
        
        # Centro do mapa
        center_lat = float(df_map['latitude'].mean())
        center_lon = float(df_map['longitude'].mean())
        
        # Cria mapa
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=12,
            tiles='OpenStreetMap'
        )
        
        # Cores por período operacional
        period_colors = {
            'operacional_manha': '#28a745',     # Verde
            'operacional_meio_dia': '#17a2b8',  # Azul claro
            'operacional_tarde': '#007bff',     # Azul
            'fora_horario_manha': '#ffc107',    # Amarelo
            'fora_horario_tarde': '#fd7e14',    # Laranja
            'fora_horario_noite': '#6f42c1',    # Roxo
            'final_semana': '#dc3545'           # Vermelho
        }
        
        # Agrupa pontos por período para criar rotas coloridas
        for periodo, color in period_colors.items():
            periodo_data = df_map[df_map['periodo_operacional'] == periodo]
            if not periodo_data.empty:
                coords = [[float(row['latitude']), float(row['longitude'])] for _, row in periodo_data.iterrows()]
                if len(coords) > 1:
                    folium.PolyLine(
                        coords,
                        color=color,
                        weight=4,
                        opacity=0.8,
                        popup=f'Período: {periodo}'
                    ).add_to(m)
        
        # Adiciona pontos com informações detalhadas
        for idx, point in df_map.iterrows():
            periodo = point['periodo_operacional']
            color = period_colors.get(str(periodo), 'gray')
            
            # Popup com informações detalhadas
            popup_html = f"""
            <div style="width: 200px;">
                <b>Data/Hora:</b> {pd.to_datetime(point['data_evento']).strftime('%d/%m/%Y %H:%M')}<br>
                <b>Velocidade:</b> {point['velocidade_kmh']} km/h<br>
                <b>Período:</b> {periodo}<br>
                <b>Status:</b> {point['ignicao']}<br>
                <b>Endereço:</b> {str(point.get('endereco', 'N/A'))[:50]}...
            </div>
            """
            
            # Tamanho do marcador baseado na velocidade
            radius = min(max(point['velocidade_kmh'] / 10, 3), 15)
            
            folium.CircleMarker(
                [float(point['latitude']), float(point['longitude'])],
                radius=radius,
                popup=folium.Popup(popup_html, max_width=250),
                color=color,
                fill=True,
                fillColor=color,
                fillOpacity=0.7,
                weight=2
            ).add_to(m)
        
        # Adiciona legenda
        legend_html = '''
        <div style="position: fixed; 
                    bottom: 50px; left: 50px; width: 200px; height: 150px; 
                    background-color: white; border:2px solid grey; z-index:9999; 
                    font-size:14px; padding: 10px">
        <h4>Períodos Operacionais</h4>
        <p><span style="color:#28a745">●</span> Manhã (04:00-07:00)</p>
        <p><span style="color:#17a2b8">●</span> Meio-dia (10:50-13:00)</p>
        <p><span style="color:#007bff">●</span> Tarde (16:50-19:00)</p>
        <p><span style="color:#ffc107">●</span> Fora Horário Manhã</p>
        <p><span style="color:#fd7e14">●</span> Fora Horário Tarde</p>
        <p><span style="color:#6f42c1">●</span> Fora Horário Noite</p>
        <p><span style="color:#dc3545">●</span> Final de Semana</p>
        </div>
        '''
        m.get_root().add_child(folium.Element(legend_html))
        
        # Converte para HTML
        return m._repr_html_()
    
    def create_fuel_consumption_analysis(self, metrics: Dict) -> str:
        """
        Cria análise de consumo de combustível
        """
        if 'combustivel' not in metrics:
            return "<p>Dados insuficientes para análise de combustível.</p>"
        
        fuel_data = metrics['combustivel']
        
        html = f"""
        <div class="fuel-analysis">
            <h3>Análise de Consumo de Combustível</h3>
            <div class="fuel-metrics">
                <div class="metric">
                    <span class="label">Distância Percorrida:</span>
                    <span class="value">{fuel_data['km_traveled']:.2f} km</span>
                </div>
                <div class="metric">
                    <span class="label">Combustível Estimado:</span>
                    <span class="value">{fuel_data['fuel_consumed_liters']:.2f} litros</span>
                </div>
                <div class="metric">
                    <span class="label">Eficiência:</span>
                    <span class="value">{fuel_data['efficiency_kmL']:.2f} km/L</span>
                </div>
                <div class="metric">
                    <span class="label">Velocidade Média:</span>
                    <span class="value">{fuel_data['avg_speed']:.2f} km/h</span>
                </div>
            </div>
        </div>
        """
        
        return html
    
    def generate_insights_and_recommendations(self, metrics: Dict) -> List[str]:
        """
        Gera insights e recomendações baseados nas métricas
        """
        insights = []
        
        if not metrics:
            return ["Dados insuficientes para gerar insights."]
        
        # Análise de eficiência operacional
        operacao = metrics.get('operacao', {})
        periodos = metrics.get('periodos', {})
        
        # Insight sobre utilização
        total_registros = operacao.get('total_registros', 0)
        tempo_movimento = operacao.get('tempo_em_movimento', 0)
        
        if total_registros > 0:
            percentual_movimento = (tempo_movimento / total_registros) * 100
            if percentual_movimento < 30:
                insights.append(f"⚠️ Veículo em movimento apenas {percentual_movimento:.1f}% do tempo. Considere otimizar o uso.")
            elif percentual_movimento > 70:
                insights.append(f"✅ Boa utilização do veículo: {percentual_movimento:.1f}% do tempo em movimento.")
        
        # Insight sobre velocidade
        velocidade_maxima = operacao.get('velocidade_maxima', 0)
        if velocidade_maxima > 80:
            insights.append(f"🚨 Velocidade máxima registrada: {velocidade_maxima} km/h. Excesso de velocidade detectado!")
        
        # Insight sobre períodos operacionais
        fora_horario = periodos.get('fora_horario', 0)
        final_semana = periodos.get('final_semana', 0)
        total_fora_periodo = fora_horario + final_semana
        
        if total_fora_periodo > total_registros * 0.3:
            insights.append(f"📊 {((total_fora_periodo/total_registros)*100):.1f}% da operação fora do horário comercial.")
        
        # Insight sobre combustível
        if 'combustivel' in metrics:
            fuel_data = metrics['combustivel']
            if fuel_data['efficiency_kmL'] < 10:
                insights.append(f"⛽ Eficiência de combustível baixa: {fuel_data['efficiency_kmL']:.1f} km/L. Revisar estilo de condução.")
            elif fuel_data['efficiency_kmL'] > 15:
                insights.append(f"✅ Excelente eficiência de combustível: {fuel_data['efficiency_kmL']:.1f} km/L.")
        
        # Insight sobre conectividade
        conectividade = metrics.get('conectividade', {})
        problemas = conectividade.get('problemas_conexao', 0)
        if problemas > total_registros * 0.1:
            insights.append(f"📡 {problemas} problemas de conectividade detectados. Verificar equipamentos de telemetria.")
        
        # Recomendações gerais
        if not insights:
            insights.append("✅ Operação dentro dos parâmetros normais. Continue o bom trabalho!")
        
        return insights

class ReportGenerator:
    """Classe para gerar relatórios completos"""
    
    def __init__(self):
        self.analyzer = TelemetryAnalyzer()
    
    def generate_complete_analysis(self, placa: str, data_inicio: datetime, data_fim: datetime) -> Dict:
        """
        Gera análise completa de um veículo
        """
        # Busca dados
        df = self.analyzer.get_vehicle_data(placa, data_inicio, data_fim)
        
        if df.empty:
            return {
                'success': False,
                'message': 'Nenhum dado encontrado para o período especificado.'
            }
        
        # Gera métricas
        metrics = self.analyzer.generate_summary_metrics(df, placa)
        
        # Estatísticas diárias para gráficos/tabelas agregadas (consistentes)
        df_daily = df.copy()
        df_daily['date'] = df_daily['data_evento'].dt.date
        df_daily['velocidade_kmh'] = pd.to_numeric(df_daily['velocidade_kmh'], errors='coerce').fillna(0.0)
        df_daily['odometro_periodo_km'] = pd.to_numeric(df_daily['odometro_periodo_km'], errors='coerce').fillna(0.0)
        daily_stats = []
        for day, g in df_daily.groupby('date'):
            # Ordena e calcula deltas de odômetro
            g = g.sort_values('data_evento').copy()
            diffs = g['odometro_periodo_km'].diff().fillna(0).clip(lower=0)
            # Máscara de consistência: deslocou (delta odômetro > 0) e registrou velocidade > 0
            valid = (diffs > 0) & (g['velocidade_kmh'] > 0)
            # Apenas trechos consistentes entram na conta diária
            km_day = float(diffs[valid].sum())
            avg_speed_day = float(g.loc[valid, 'velocidade_kmh'].mean()) if valid.any() else 0.0
            max_speed_day = float(g.loc[valid, 'velocidade_kmh'].max()) if valid.any() else 0.0
            daily_stats.append({'date': day.isoformat(), 'km': km_day, 'avg_speed': avg_speed_day, 'max_speed': max_speed_day})

        # Gera gráficos (HTML) existentes
        charts = {
            'speed_chart': self.analyzer.create_speed_chart(df),
            'periods_chart': self.analyzer.create_operational_periods_chart(df),
            'ignition_chart': self.analyzer.create_ignition_status_chart(df),
            'route_map': self.analyzer.create_route_map(df)
        }

        # Gera análises especiais
        fuel_analysis = self.analyzer.create_fuel_consumption_analysis(metrics)
        insights = self.analyzer.generate_insights_and_recommendations(metrics)
        
        return {
            'success': True,
            'metrics': metrics,
            'charts': charts,
            'fuel_analysis': fuel_analysis,
            'insights': insights,
            'data_count': int(len(df)),
            'daily_stats': daily_stats
        }

    def generate_consolidated_report(self, data_inicio: datetime, data_fim: datetime, cliente_nome: Optional[str] = None, reports_dir: Optional[str] = None, vehicle_filter: Optional[str] = None) -> Dict:
        """
        Gera relatório consolidado com foco no cliente e rankings custo/benefício
        Suporta filtro por veículo individual para relatórios padronizados
        """
        try:
            # Handle same day periods - when start and end date are the same, 
            # adjust end date to include the entire day
            if data_inicio.date() == data_fim.date():
                # For same day, set end time to end of day (23:59:59)
                adjusted_data_fim = data_fim.replace(hour=23, minute=59, second=59, microsecond=999999)
            else:
                adjusted_data_fim = data_fim
            
            session = get_session()
            
            # Constrói consulta base
            query = session.query(Veiculo).join(Cliente)
            
            # Filtra por cliente se especificado
            if cliente_nome and cliente_nome != 'TODOS':
                query = query.filter(Cliente.nome.ilike(f"%{cliente_nome}%"))
                cliente_obj = session.query(Cliente).filter(
                    Cliente.nome.ilike(f"%{cliente_nome}%")
                ).first()
            
            # Filtra por veículo individual se especificado
            if vehicle_filter:
                query = query.filter(Veiculo.placa.ilike(f"%{vehicle_filter}%"))
                vehicles = query.all()
                if vehicles:
                    cliente_obj = vehicles[0].cliente
                else:
                    session.close()
                    return {
                        'success': False,
                        'error': f'Veículo {vehicle_filter} não encontrado no sistema'
                    }
            else:
                # Sem filtro de veículo - pega todos os veículos do cliente/sistema
                vehicles = query.all()
                if vehicles:
                    # Detecta cliente automaticamente do primeiro veículo com dados
                    cliente_obj = vehicles[0].cliente
                    # Se houver apenas um cliente, usa esse. Se vários, usa "Vários Clientes"
                    clientes_unicos = list(set([v.cliente.nome for v in vehicles if v.cliente]))
                    if len(clientes_unicos) == 1:
                        cliente_obj = vehicles[0].cliente
                    else:
                        cliente_obj = type('Cliente', (), {'nome': 'Vários Clientes', 'consumo_medio_kmL': 12.0, 'limite_velocidade': 80})()
                else:
                    cliente_obj = None
            
            if not vehicles:
                session.close()
                return {
                    'success': False,
                    'error': f'Nenhum veículo encontrado{" para o cliente " + cliente_nome if cliente_nome else ""} no sistema'
                }
            
            # Estrutura de dados consolidados
            consolidated_data = {
                "cliente_info": {
                    "nome": cliente_obj.nome if cliente_obj else "Todos os Clientes",
                    "consumo_medio_kmL": cliente_obj.consumo_medio_kmL if cliente_obj else None,
                    "limite_velocidade": cliente_obj.limite_velocidade if cliente_obj else None
                },
                "periodo": {
                    "data_inicio": data_inicio,
                    "data_fim": data_fim
                },
                "resumo_geral": {
                    "total_veiculos": 0,
                    "km_total": 0,
                    "combustivel_total": 0,
                    "media_por_veiculo": 0,
                    "vel_maxima_frota": 0
                },
                "desempenho_periodo": [],  # Tabela consolidada do período
                "periodos": {},
                "por_dia": {},
                "ranking_melhores": [],
                "ranking_piores": [],
                "detalhes_veiculos": []
            }
            
            # Processamento de cada veículo
            all_vehicles_data = []
            total_km = 0
            total_fuel = 0
            max_speed_fleet = 0
            
            for vehicle in vehicles:
                try:
                    # Gera análise individual using adjusted end date for same-day periods
                    df = self.analyzer.get_vehicle_data(str(vehicle.placa), data_inicio, adjusted_data_fim)
                    
                    if df.empty:
                        continue
                    
                    metrics = self.analyzer.generate_summary_metrics(df, str(vehicle.placa))
                    
                    if metrics:
                        operacao = metrics.get('operacao', {})
                        combustivel_data = metrics.get('combustivel', {})
                        
                        # Calcula score custo/benefício
                        km_total_veh = operacao.get('km_total', 0)
                        vel_max_veh = operacao.get('velocidade_maxima', 0)
                        vel_media_veh = operacao.get('velocidade_media', 0)
                        combustivel_veh = combustivel_data.get('fuel_consumed_liters', 0)
                        eficiencia_veh = combustivel_data.get('efficiency_kmL', 0)
                        
                        # Score custo/benefício (quanto maior, melhor)
                        # Nova fórmula: Quilometragem (40%) + Combustível (40%) + Controle de velocidade (20%)
                        # Penaliza proporcionalmente velocidades acima de 100 km/h
                        
                        # Normalizações para cálculos proporcionais
                        km_norm = (km_total_veh / 100) * 0.4  # Quilometragem (40%)
                        
                        # Combustível: inverte a lógica - menor consumo = melhor score
                        # Normaliza com base em 50L como referência
                        fuel_norm = (max(0, 50 - combustivel_veh) / 50) * 0.4  # Combustível (40%)
                        
                        # Controle de velocidade (20%)
                        speed_control_norm = (max(0, 100 - vel_max_veh) / 100) * 0.2
                        
                        # Penalidade proporcional para velocidades > 100 km/h
                        speed_penalty = 0
                        if vel_max_veh > 100:
                            # Penalidade proporcional: para cada km/h acima de 100, desconta 0.02 pontos
                            excess_speed = vel_max_veh - 100
                            speed_penalty = excess_speed * 0.02
                        
                        score_beneficio = km_norm + fuel_norm + speed_control_norm - speed_penalty
                        
                        vehicle_summary = {
                            'placa': str(vehicle.placa),
                            'km_total': km_total_veh,
                            'velocidade_maxima': vel_max_veh,
                            'velocidade_media': vel_media_veh,
                            'tempo_movimento': operacao.get('tempo_em_movimento', 0),
                            'combustivel': combustivel_veh,
                            'eficiencia': eficiencia_veh,
                            'score_custo_beneficio': score_beneficio,
                            'dataframe': df,
                            'periodos_detalhes': metrics.get('periodos', {})
                        }
                        
                        all_vehicles_data.append(vehicle_summary)
                        total_km += km_total_veh
                        total_fuel += combustivel_veh
                        max_speed_fleet = max(max_speed_fleet, vel_max_veh)
                        
                except Exception as e:
                    print(f"Erro ao processar veículo {vehicle.placa}: {e}")
                    continue
            
            session.close()
            
            if not all_vehicles_data:
                return {
                    'success': False,
                    'error': 'Nenhum dado encontrado para o período especificado'
                }
            
            # Resumo geral
            consolidated_data["resumo_geral"] = {
                "total_veiculos": len(all_vehicles_data),
                "km_total": total_km,
                "combustivel_total": total_fuel,
                "media_por_veiculo": total_km / len(all_vehicles_data) if all_vehicles_data else 0,
                "vel_maxima_frota": max_speed_fleet
            }
            
            # Desempenho consolidado do período
            consolidated_data["desempenho_periodo"] = [
                {
                    'placa': vehicle['placa'],
                    'km_total': vehicle['km_total'],
                    'velocidade_maxima': vehicle['velocidade_maxima'],
                    'combustivel': vehicle['combustivel'],
                    'eficiencia': vehicle['eficiencia']
                }
                for vehicle in sorted(all_vehicles_data, key=lambda x: x['km_total'], reverse=True)
            ]
            
            # Agrupamento por períodos operacionais (mantém estrutura existente)
            periods_definition = {
                'operacional_manha': {
                    'nome': 'Manhã Operacional',
                    'horario': '04:00 - 07:00',
                    'descricao': 'Início das atividades operacionais',
                    'cor': 'verde'
                },
                'operacional_meio_dia': {
                    'nome': 'Meio-dia Operacional', 
                    'horario': '10:50 - 13:00',
                    'descricao': 'Atividades do meio-dia',
                    'cor': 'verde'
                },
                'operacional_tarde': {
                    'nome': 'Tarde Operacional',
                    'horario': '16:50 - 19:00',
                    'descricao': 'Encerramento das atividades',
                    'cor': 'verde'
                },
                'fora_horario_manha': {
                    'nome': 'Fora Horário Manhã',
                    'horario': '07:00 - 10:50',
                    'descricao': 'Período entre turnos matutinos',
                    'cor': 'laranja'
                },
                'fora_horario_tarde': {
                    'nome': 'Fora Horário Tarde',
                    'horario': '13:00 - 16:50',
                    'descricao': 'Período entre turnos vespertinos',
                    'cor': 'laranja'
                },
                'fora_horario_noite': {
                    'nome': 'Fora Horário Noite',
                    'horario': '19:00 - 04:00',
                    'descricao': 'Período noturno e madrugada',
                    'cor': 'laranja'
                },
                'final_semana': {
                    'nome': 'Final de Semana',
                    'horario': 'Sábado + Domingo',
                    'descricao': 'Dados combinados do final de semana',
                    'cor': 'cinza'
                }
            }
            
            # Organizar dados por DIA e depois por PERÍODO (nova estrutura)
            all_dates = set()
            daily_period_data = {}
            
            for vehicle_data in all_vehicles_data:
                df = vehicle_data['dataframe']
                if not df.empty:
                    dates = df['data_evento'].dt.date.unique()
                    all_dates.update(dates)
            
            # Para cada dia, organiza por período
            for date in sorted(all_dates):
                date_str = date.strftime('%Y-%m-%d')
                daily_period_data[date_str] = {}
                
                for period_key, period_info in periods_definition.items():
                    period_vehicles = []
                    
                    for vehicle_data in all_vehicles_data:
                        df = vehicle_data['dataframe']
                        # Filtra por dia E por período
                        daily_df = df[df['data_evento'].dt.date == date]
                        period_df = daily_df[daily_df['periodo_operacional'] == period_key]
                        
                        if not period_df.empty:
                            # Calcula métricas consistentes para o período: considerar apenas trechos com
                            # incremento de odômetro (> 0) e velocidade > 0
                            period_df_sorted = period_df.sort_values('data_evento').copy()
                            period_df_sorted['velocidade_kmh'] = pd.to_numeric(period_df_sorted['velocidade_kmh'], errors='coerce').fillna(0.0)
                            period_df_sorted['odometro_periodo_km'] = pd.to_numeric(period_df_sorted['odometro_periodo_km'], errors='coerce').fillna(0.0)
                            diffs = period_df_sorted['odometro_periodo_km'].diff().fillna(0).clip(lower=0)
                            valid = (diffs > 0) & (period_df_sorted['velocidade_kmh'] > 0)
                            km_periodo_val = float(diffs[valid].sum())

                            # Proporção de combustível permanece proporcional ao número de registros no período
                            combustivel_periodo_calc = vehicle_data['combustivel'] * (len(period_df_sorted) / len(df)) if len(df) > 0 else 0

                            period_summary = {
                                'placa': vehicle_data['placa'],
                                'km_periodo': km_periodo_val,
                                'vel_max_periodo': float(period_df_sorted.loc[valid, 'velocidade_kmh'].max()) if valid.any() else 0.0,
                                'combustivel_periodo': combustivel_periodo_calc,
                                'eficiencia_periodo': vehicle_data['eficiencia']
                            }
                            period_vehicles.append(period_summary)
                    
                    if period_vehicles:
                        daily_period_data[date_str][period_info['nome']] = {
                            'info': period_info,
                            'veiculos': period_vehicles
                        }
            
            # Salva a estrutura diária no lugar dos períodos antigos
            consolidated_data["periodos_diarios"] = daily_period_data
            
            # Mantém estrutura de períodos consolidados para compatibilidade
            for period_key, period_info in periods_definition.items():
                period_vehicles = []
                
                for vehicle_data in all_vehicles_data:
                    df = vehicle_data['dataframe']
                    period_df = df[df['periodo_operacional'] == period_key]
                    
                    if not period_df.empty:
                        period_df_sorted = period_df.sort_values('data_evento').copy()
                        period_df_sorted['velocidade_kmh'] = pd.to_numeric(period_df_sorted['velocidade_kmh'], errors='coerce').fillna(0.0)
                        period_df_sorted['odometro_periodo_km'] = pd.to_numeric(period_df_sorted['odometro_periodo_km'], errors='coerce').fillna(0.0)
                        diffs = period_df_sorted['odometro_periodo_km'].diff().fillna(0).clip(lower=0)
                        if CONSISTENT_SPEED_KM_ONLY:
                            valid = (diffs > 0) & (period_df_sorted['velocidade_kmh'] > 0)
                        else:
                            valid = (diffs > 0)
                        km_periodo_val = float(diffs[valid].sum())
                        vel_max_val = float(period_df_sorted.loc[valid, 'velocidade_kmh'].max()) if valid.any() else 0.0
                        
                        period_summary = {
                            'placa': vehicle_data['placa'],
                            'km_periodo': km_periodo_val,
                            'vel_max_periodo': vel_max_val,
                            'combustivel_periodo': vehicle_data['combustivel'] * (len(period_df_sorted) / len(df)) if len(df) > 0 else 0,
                            'eficiencia_periodo': vehicle_data['eficiencia']
                        }
                        period_vehicles.append(period_summary)
                
                if period_vehicles:
                    consolidated_data["periodos"][period_info['nome']] = {
                        'info': period_info,
                        'veiculos': period_vehicles
                    }
            
            # Log estruturado do consolidado
            try:
                logger.info({
                    'event': 'consolidated_report_built',
                    'periodo': {'inicio': str(data_inicio), 'fim': str(data_fim)},
                    'cliente': cliente_obj.nome if cliente_obj else None,
                    'totais': {
                        'total_veiculos': consolidated_data["resumo_geral"]["total_veiculos"],
                        'km_total': consolidated_data["resumo_geral"]["km_total"],
                        'combustivel_total': consolidated_data["resumo_geral"]["combustivel_total"],
                        'vel_maxima_frota': consolidated_data["resumo_geral"]["vel_maxima_frota"]
                    },
                    'flags': {'CONSISTENT_SPEED_KM_ONLY': CONSISTENT_SPEED_KM_ONLY}
                })
            except Exception:
                pass
        
            return {
                'success': True,
                'data': consolidated_data,
                'total_km': total_km,
                'total_fuel': total_fuel,
                'message': f'Relatório consolidado gerado para {len(all_vehicles_data)} veículos'
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Erro ao gerar relatório consolidado: {str(e)}'
            }

if __name__ == "__main__":
    # Teste do analisador
    analyzer = TelemetryAnalyzer()
    print("Serviços de análise carregados com sucesso!")

# ... existing code ...

# ==============================
# LOGGING E FEATURE FLAGS GLOBAIS
# ==============================