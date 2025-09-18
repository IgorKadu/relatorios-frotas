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

from .models import Cliente, Veiculo, PosicaoHistorica, get_session
from .utils import get_fuel_consumption_estimate

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
            # Query para buscar dados
            query = self.session.query(PosicaoHistorica).join(Veiculo).filter(
                and_(
                    Veiculo.placa == placa,
                    PosicaoHistorica.data_evento >= data_inicio,
                    PosicaoHistorica.data_evento <= data_fim
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
                'total_registros': len(df),
                'km_total': df['odometro_periodo_km'].max() - df['odometro_periodo_km'].min() if len(df) > 0 else 0,
                'velocidade_maxima': df['velocidade_kmh'].max(),
                'velocidade_media': df[df['velocidade_kmh'] > 0]['velocidade_kmh'].mean(),
                'tempo_total_ligado': len(df[df['ligado']]),
                'tempo_em_movimento': len(df[df['em_movimento']]),
                'tempo_parado_ligado': len(df[(df['ligado']) & (~df['em_movimento'])]),
                'tempo_desligado': len(df[~df['ligado']])
            },
            'periodos': {
                # Horários Operacionais detalhados
                'operacional_manha': len(df[df['periodo_operacional'] == 'operacional_manha']),
                'operacional_meio_dia': len(df[df['periodo_operacional'] == 'operacional_meio_dia']),
                'operacional_tarde': len(df[df['periodo_operacional'] == 'operacional_tarde']),
                
                # Fora de Horário Operacional detalhados
                'fora_horario_manha': len(df[df['periodo_operacional'] == 'fora_horario_manha']),
                'fora_horario_tarde': len(df[df['periodo_operacional'] == 'fora_horario_tarde']),
                'fora_horario_noite': len(df[df['periodo_operacional'] == 'fora_horario_noite']),
                
                # Final de Semana
                'final_semana': len(df[df['periodo_operacional'] == 'final_semana']),
                
                # Totais calculados
                'total_operacional': len(df[df['periodo_operacional'].isin(['operacional_manha', 'operacional_meio_dia', 'operacional_tarde'])]),
                'total_fora_horario': len(df[df['periodo_operacional'].isin(['fora_horario_manha', 'fora_horario_tarde', 'fora_horario_noite'])]),
            },
            'conectividade': {
                'gps_ok': df['gps_status'].sum(),
                'gprs_ok': df['gprs_status'].sum(),
                'problemas_conexao': len(df) - min(df['gps_status'].sum(), df['gprs_status'].sum())
            }
        }
        
        # Estimativa de combustível
        if metrics['operacao']['km_total'] > 0:
            fuel_data = get_fuel_consumption_estimate(
                metrics['operacao']['km_total'],
                metrics['operacao']['velocidade_media'],
                cliente.consumo_medio_kmL if cliente else 12.0
            )
            metrics['combustivel'] = fuel_data
        
        # Eventos especiais
        eventos_especiais = df[df['tipo_evento'].str.contains('Excesso|Violado|Bloq', na=False, case=False)]
        metrics['eventos'] = {
            'total_eventos_especiais': len(eventos_especiais),
            'tipos_eventos': eventos_especiais['tipo_evento'].value_counts().to_dict() if not eventos_especiais.empty else {}
        }
        
        return metrics
    
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
        df_status['status_ignicao'] = df_status['ignicao'].astype(str).map(status_map)
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
        if df.empty or df[['latitude', 'longitude']].isna().all().all():
            return "<p>Dados de localização não disponíveis para gerar mapa.</p>"
        
        # Remove registros sem coordenadas válidas
        df_map = df.dropna(subset=['latitude', 'longitude'])
        
        if df_map.empty:
            return "<p>Dados de localização não disponíveis para gerar mapa.</p>"
        
        # Centro do mapa
        center_lat = df_map['latitude'].mean()
        center_lon = df_map['longitude'].mean()
        
        # Cria mapa
        m = folium.Map(
            location=[center_lat, center_lon],
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
        if df.empty or df[['latitude', 'longitude']].isna().all(axis=None):
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
                coords = periodo_data[['latitude', 'longitude']].values.tolist()
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
            color = period_colors.get(periodo, 'gray')
            
            # Popup com informações detalhadas
            popup_html = f"""
            <div style="width: 200px;">
                <b>Data/Hora:</b> {point['data_evento'].strftime('%d/%m/%Y %H:%M')}<br>
                <b>Velocidade:</b> {point['velocidade_kmh']} km/h<br>
                <b>Período:</b> {periodo}<br>
                <b>Status:</b> {point['ignicao']}<br>
                <b>Endereço:</b> {point.get('endereco', 'N/A')[:50]}...
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
        m.get_root().html.add_child(folium.Element(legend_html))
        
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
        
        # Gera gráficos
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
            'data_count': len(df)
        }

    def generate_consolidated_report(self, data_inicio: datetime, data_fim: datetime, cliente_nome: Optional[str] = None, reports_dir: Optional[str] = None) -> Dict:
        """
        Gera relatório consolidado com foco no cliente e rankings custo/benefício
        """
        try:
            session = get_session()
            
            # Filtra veículos por cliente se especificado
            if cliente_nome and cliente_nome != 'TODOS':
                vehicles = session.query(Veiculo).join(Cliente).filter(
                    Cliente.nome.ilike(f"%{cliente_nome}%")
                ).all()
                cliente_obj = session.query(Cliente).filter(
                    Cliente.nome.ilike(f"%{cliente_nome}%")
                ).first()
            else:
                # Pega todos os veículos e detecta cliente automaticamente
                vehicles = session.query(Veiculo).join(Cliente).all()
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
                    # Gera análise individual
                    df = self.analyzer.get_vehicle_data(str(vehicle.placa), data_inicio, data_fim)
                    
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
                            # Calcula a quilometragem correta para o período
                            if len(period_df) > 1:
                                # Usa diferença entre odômetros para calcular km percorridos
                                km_periodo = period_df['odometro_periodo_km'].max() - period_df['odometro_periodo_km'].min()
                            else:
                                # Para registros únicos, considera quilometragem proporcional
                                km_periodo = vehicle_data['km_total'] * (len(period_df) / len(df)) if len(df) > 0 else 0
                            
                            # Garante que km_periodo não seja zero se há consumo de combustível
                            combustivel_periodo_calc = vehicle_data['combustivel'] * (len(period_df) / len(df)) if len(df) > 0 else 0
                            if km_periodo == 0 and combustivel_periodo_calc > 0:
                                # Se há consumo mas km é zero, estima km baseado na eficiência média
                                km_periodo = combustivel_periodo_calc * vehicle_data['eficiencia']
                            
                            period_summary = {
                                'placa': vehicle_data['placa'],
                                'km_periodo': km_periodo,
                                'vel_max_periodo': period_df['velocidade_kmh'].max(),
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
                        period_summary = {
                            'placa': vehicle_data['placa'],
                            'km_periodo': len(period_df),
                            'vel_max_periodo': period_df['velocidade_kmh'].max() if not period_df.empty else 0,
                            'combustivel_periodo': vehicle_data['combustivel'] * (len(period_df) / len(df)) if len(df) > 0 else 0,
                            'eficiencia_periodo': vehicle_data['eficiencia']
                        }
                        period_vehicles.append(period_summary)
                
                if period_vehicles:
                    consolidated_data["periodos"][period_info['nome']] = {
                        'info': period_info,
                        'veiculos': period_vehicles
                    }
            
            # Agrupamento por dia (estrutura simplificada)
            all_dates = set()
            for vehicle_data in all_vehicles_data:
                df = vehicle_data['dataframe']
                if not df.empty:
                    dates = df['data_evento'].dt.date.unique()
                    all_dates.update(dates)
            
            for date in sorted(all_dates):
                date_str = date.strftime('%Y-%m-%d')
                daily_vehicles = []
                
                for vehicle_data in all_vehicles_data:
                    df = vehicle_data['dataframe']
                    daily_df = df[df['data_evento'].dt.date == date]
                    
                    if not daily_df.empty:
                        daily_summary = {
                            'placa': vehicle_data['placa'],
                            'km_dia': daily_df['odometro_periodo_km'].max() - daily_df['odometro_periodo_km'].min() if len(daily_df) > 1 else 0,
                            'vel_max': daily_df['velocidade_kmh'].max(),
                            'combustivel_dia': vehicle_data['combustivel'] * (len(daily_df) / len(df)) if len(df) > 0 else 0,
                            'eficiencia_dia': vehicle_data['eficiencia']
                        }
                        daily_vehicles.append(daily_summary)
                
                if daily_vehicles:
                    consolidated_data["por_dia"][date_str] = daily_vehicles
            
            # Ranking ÚNICO estilo campeonato (todos os veículos ordenados)
            sorted_by_score = sorted(all_vehicles_data, key=lambda x: x['score_custo_beneficio'], reverse=True)
            
            # Adiciona posição no ranking
            for i, vehicle in enumerate(sorted_by_score, 1):
                vehicle['posicao_ranking'] = i
                vehicle['categoria_ranking'] = 'top3' if i <= 3 else 'bottom3' if i > len(sorted_by_score) - 3 else 'normal'
            
            consolidated_data["ranking_campeonato"] = {
                'titulo': 'Ranking de Desempenho Custo/Benefício',
                'descricao': 'Classificação geral baseada em quilometragem (40%) + eficiência (40%) + controle de velocidade (20%)',
                'veiculos': sorted_by_score
            }
            
            # Mantém estruturas antigas para compatibilidade
            consolidated_data["ranking_melhores"] = [
                {
                    'categoria': 'Melhor Custo/Benefício',
                    'criterio': 'score_custo_beneficio',
                    'descricao': 'Alta quilometragem + Baixo consumo + Velocidades controladas',
                    'veiculos': sorted_by_score[:5]
                }
            ]
            
            consolidated_data["ranking_piores"] = [
                {
                    'categoria': 'Pior Custo/Benefício',
                    'criterio': 'score_custo_beneficio', 
                    'descricao': 'Baixa quilometragem + Alto consumo + Picos de velocidade',
                    'veiculos': sorted_by_score[-5:] if len(sorted_by_score) >= 5 else sorted_by_score[::-1]
                }
            ]
            
            # Detalhes dos veículos para tabela geral
            consolidated_data["detalhes_veiculos"] = all_vehicles_data
            
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