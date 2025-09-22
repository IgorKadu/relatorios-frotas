"""
Utilitários para leitura, limpeza e processamento de arquivos CSV de telemetria.
"""

import pandas as pd
import numpy as np
from datetime import datetime, time
from typing import Dict, List, Tuple, Optional, Any
import re
import os
from sqlalchemy.orm import Session
from .models import Cliente, Veiculo, PosicaoHistorica, get_session
from math import radians, sin, cos, asin, sqrt

def convert_numpy_types(obj: Any) -> Any:
    """
    Converte tipos numpy para tipos nativos do Python para serialização JSON
    """
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    elif pd.isna(obj):
        return None
    return obj

def haversine(lat1, lon1, lat2, lon2):
    """
    Calcula a distância entre dois pontos usando a fórmula de Haversine
    """
    R = 6371.0  # raio da Terra em km
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1))*cos(radians(lat2))*sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    return R * c  # distância em km

class CSVProcessor:
    """Classe para processar arquivos CSV de telemetria veicular"""
    
    def __init__(self):
        self.required_columns = [
            'Cliente', 'Placa', 'Ativo', 'Data', 'Data (GPRS)', 
            'Velocidade (Km)', 'Ignição', 'Motorista', 'GPS', 'Gprs',
            'Localização', 'Endereço', 'Tipo do Evento', 'Saida', 'Entrada',
            'Pacote', 'Odômetro do período  (Km)', 'Horímetro do período',
            'Horímetro embarcado', 'Odômetro embarcado (Km)', 'Bateria',
            'Imagem', 'Tensão', 'Bloqueado'
        ]
        
        # Definição dos períodos operacionais (padrão - fallback)
        self.periodos_operacionais = {
            'manha': (time(4, 0), time(7, 0)),
            'meio_dia': (time(10, 50), time(13, 0)),
            'tarde': (time(16, 50), time(19, 0))
        }
        self._cached_perfis = {}  # Cache de perfis por cliente
        
        # Parâmetros configuráveis
        self.speed_outlier_threshold = 220  # km/h
        self.trip_speed_threshold = 3  # km/h
        self.trip_min_duration_s = 60  # segundos
        self.gps_jump_distance_km = 500  # km
        self.aggregation_rule_days_for_summary = 7  # dias
    
    def detect_schema(self, df: pd.DataFrame) -> Dict:
        """
        Detecta automaticamente o schema de cada CSV.
        Para cada coluna, detecta tipo (timestamp, latitude, longitude, odometer, speed, ignition, 
        event, battery, vehicle_id, client_id, pagamento, estoque, etc.).
        """
        schema_detectado = {
            'arquivo': 'arquivo_csv',
            'colunas': []
        }
        
        for col in df.columns:
            # Ensure we're working with a Series
            col_data = df[col]
            if isinstance(col_data, pd.DataFrame):
                # If it's a DataFrame, take the first column
                col_data = col_data.iloc[:, 0]
            elif not isinstance(col_data, pd.Series):
                # If it's not a Series, convert it to one
                col_data = pd.Series(col_data, name=col)
                
            tipo_estimado = self._detect_column_type(col_data)
            exemplo_valor = col_data.iloc[0] if len(col_data) > 0 else None
            
            schema_detectado['colunas'].append({
                'nome_coluna': col,
                'tipo_estimado': tipo_estimado,
                'exemplo_valor': exemplo_valor
            })
        
        return schema_detectado
    
    def _detect_column_type(self, series: pd.Series) -> str:
        """
        Detecta o tipo de uma coluna específica
        """
        # Normaliza o nome da coluna para detecção
        col_name = str(series.name).lower().strip() if series.name else ''
        
        # Mapeamento de aliases para tipos
        aliases = {
            'timestamp': ['timestamp', 'time', 'data', 'dt', 'datetime'],
            'lat': ['lat', 'latitude'],
            'lon': ['lon', 'lng', 'longitude'],
            'odometer': ['odo', 'odometer', 'km', 'odômetro'],
            'speed': ['speed', 'velocidade', 'vel_km_h'],
            'ignition': ['ignition', 'ig', 'engine_status'],
            'vehicle_id': ['vehicle_id', 'id_veiculo', 'placa'],
            'client_id': ['client_id', 'cliente', 'id_cliente'],
            'pagamento': ['pagamento', 'valor'],
            'estoque': ['estoque']
        }
        
        # Verifica aliases primeiro
        for tipo, nomes in aliases.items():
            if any(alias in col_name for alias in nomes):
                return tipo
        
        # Se não encontrar por alias, tenta detecção automática
        sample_values = series.dropna().head(10)
        if len(sample_values) == 0:
            return 'unknown'
        
        # Verifica se parece com timestamp
        if self._looks_like_timestamp(sample_values):
            return 'timestamp'
        
        # Verifica se parece com número
        if self._looks_like_numeric(sample_values):
            # Verifica faixas específicas
            numeric_series = pd.to_numeric(sample_values, errors='coerce')
            # Filter out NaN values manually to avoid attribute errors
            mask = pd.notna(numeric_series)
            numeric_values = numeric_series[mask]
                
            if len(numeric_values) > 0:
                mean_val = float(numeric_values.mean())
                if 0 <= mean_val <= 90 and 'lat' in col_name:  # Latitude
                    return 'lat'
                elif -180 <= mean_val <= 180 and 'lon' in col_name:  # Longitude
                    return 'lon'
                elif mean_val >= 0 and ('speed' in col_name or 'velocidade' in col_name):  # Speed
                    return 'speed'
                elif mean_val >= 0 and ('odo' in col_name or 'km' in col_name):  # Odometer
                    return 'odometer'
                elif 0 <= mean_val <= 100 and ('bateria' in col_name or 'battery' in col_name):  # Battery
                    return 'battery'
                else:
                    return 'numeric'
        
        # Verifica se parece com booleano
        if self._looks_like_boolean(sample_values):
            return 'boolean'
        
        # Por padrão, retorna string
        return 'string'
    
    def _looks_like_timestamp(self, values: pd.Series) -> bool:
        """Verifica se os valores parecem timestamps"""
        formats_to_try = [
            '%d/%m/%Y %H:%M:%S',
            '%Y-%m-%d %H:%M:%S',
            '%d/%m/%Y',
            '%Y-%m-%d'
        ]
        
        for fmt in formats_to_try:
            try:
                pd.to_datetime(values.head(3), format=fmt)
                return True
            except:
                continue
        return False
    
    def _looks_like_numeric(self, values: pd.Series) -> bool:
        """Verifica se os valores parecem numéricos"""
        numeric_count = pd.to_numeric(values, errors='coerce').notna().sum()
        return numeric_count / len(values) > 0.8  # 80% dos valores são numéricos
    
    def _looks_like_boolean(self, values: pd.Series) -> bool:
        """Verifica se os valores parecem booleanos"""
        bool_values = {'0', '1', 'true', 'false', 'sim', 'não', 'yes', 'no'}
        unique_values = set(str(v).lower() for v in values.unique())
        return all(v in bool_values for v in unique_values)
    
    def map_columns_with_fallback(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Mapeia colunas com mecanismos de fallback
        """
        mapped_df = df.copy()
        mapping_info = {
            'original_to_mapped': {},
            'missing_columns': [],
            'fallbacks_applied': []
        }
        
        # Mapeamento de aliases
        column_aliases = {
            'timestamp': ['timestamp', 'time', 'data', 'dt', 'datetime'],
            'lat': ['lat', 'latitude'],
            'lon': ['lon', 'lng', 'longitude'],
            'odometer': ['odo', 'odometer', 'km', 'odômetro'],
            'speed': ['speed', 'velocidade', 'vel_km_h'],
            'ignition': ['ignition', 'ig', 'engine_status'],
            'vehicle_id': ['vehicle_id', 'id_veiculo', 'placa'],
            'client_id': ['client_id', 'cliente', 'id_cliente']
        }
        
        # Para cada tipo de coluna esperado, encontra a melhor correspondência
        for target_col, aliases in column_aliases.items():
            found_col = None
            for alias in aliases:
                # Procura por correspondência exata (case-insensitive)
                for col in df.columns:
                    if str(col).lower() == alias.lower():
                        found_col = col
                        break
                if found_col:
                    break
            
            if found_col:
                # Mapeia a coluna encontrada para o nome padrão
                mapped_df[target_col] = df[found_col]
                mapping_info['original_to_mapped'][found_col] = target_col
            else:
                # Coluna ausente
                mapping_info['missing_columns'].append(target_col)
                # Aplica fallbacks conforme necessário
                if target_col == 'odometer':
                    # Calcular distância via haversine entre pontos consecutivos
                    mapped_df['odometer'] = self._calculate_haversine_distance(df)
                    mapping_info['fallbacks_applied'].append('odometer: calculated via haversine')
                elif target_col == 'speed':
                    # Calcular velocidade instantânea como distância / delta_t
                    mapped_df['speed'] = self._calculate_instant_speed(df)
                    mapping_info['fallbacks_applied'].append('speed: calculated via distance/delta_t')
        
        return mapped_df, mapping_info
    
    def _calculate_haversine_distance(self, df: pd.DataFrame) -> pd.Series:
        """
        Calcula distância via haversine entre pontos consecutivos
        """
        if 'lat' not in df.columns or 'lon' not in df.columns:
            return pd.Series([0] * len(df))
        
        distances = [0]  # Primeiro ponto tem distância 0
        for i in range(1, len(df)):
            lat1, lon1 = df.iloc[i-1]['lat'], df.iloc[i-1]['lon']
            lat2, lon2 = df.iloc[i]['lat'], df.iloc[i]['lon']
            
            if pd.notna(lat1) and pd.notna(lon1) and pd.notna(lat2) and pd.notna(lon2):
                dist = haversine(lat1, lon1, lat2, lon2)
                distances.append(dist)
            else:
                distances.append(0)
        
        return pd.Series(distances)
    
    def _calculate_instant_speed(self, df: pd.DataFrame) -> pd.Series:
        """
        Calcula velocidade instantânea como distância / delta_t
        """
        if 'timestamp' not in df.columns:
            return pd.Series([0] * len(df))
        
        speeds = [0]  # Primeiro ponto tem velocidade 0
        for i in range(1, len(df)):
            # Calcula delta_t em horas
            timestamp1 = pd.to_datetime(df.iloc[i-1]['timestamp'])
            timestamp2 = pd.to_datetime(df.iloc[i]['timestamp'])
            
            if pd.notna(timestamp1) and pd.notna(timestamp2):
                delta_t_hours = (timestamp2 - timestamp1).total_seconds() / 3600
                
                # Se tiver odometer, usa a diferença
                if 'odometer' in df.columns:
                    odometer1 = df.iloc[i-1]['odometer']
                    odometer2 = df.iloc[i]['odometer']
                    if pd.notna(odometer1) and pd.notna(odometer2):
                        distance = abs(odometer2 - odometer1)
                        if delta_t_hours > 0:
                            speed = distance / delta_t_hours
                            speeds.append(speed)
                        else:
                            speeds.append(0)
                    else:
                        speeds.append(0)
                else:
                    speeds.append(0)
            else:
                speeds.append(0)
        
        return pd.Series(speeds)
    
    def apply_quality_rules(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Aplica regras de qualidade e saneamento (sanity checks)
        """
        df_clean = df.copy()
        quality_report = {
            'outliers_removed': 0,
            'duplicates_removed': 0,
            'gps_jumps_marked': 0,
            'speed_outliers_marked': 0,
            'anomalies_detected': [],
            'inconsistent_removed': 0
        }
        
        # Remover ou marcar como outlier pontos com:
        
        # 1. lat/lon fora do intervalo válido
        if 'lat' in df_clean.columns and 'lon' in df_clean.columns:
            invalid_coords = (
                (df_clean['lat'] < -90) | (df_clean['lat'] > 90) |
                (df_clean['lon'] < -180) | (df_clean['lon'] > 180)
            )
            quality_report['outliers_removed'] += invalid_coords.sum()
            df_clean = df_clean[~invalid_coords]
        
        # 2. Δt ≤ 0 entre pontos consecutivos (remover duplicatas exatas)
        if 'timestamp' in df_clean.columns:
            df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'])
            df_clean = df_clean.sort_values('timestamp')
            duplicates = df_clean.duplicated(subset=['timestamp'], keep='first')
            quality_report['duplicates_removed'] += duplicates.sum()
            df_clean = df_clean[~duplicates]
        
        # 3. deslocamento entre pontos > 500 km em Δt pequeno → possível salto GPS
        if 'lat' in df_clean.columns and 'lon' in df_clean.columns and 'timestamp' in df_clean.columns:
            df_clean['gps_jump'] = False
            for i in range(1, len(df_clean)):
                lat1, lon1 = df_clean.iloc[i-1]['lat'], df_clean.iloc[i-1]['lon']
                lat2, lon2 = df_clean.iloc[i]['lat'], df_clean.iloc[i]['lon']
                timestamp1 = df_clean.iloc[i-1]['timestamp']
                timestamp2 = df_clean.iloc[i]['timestamp']
                
                if all(pd.notna([lat1, lon1, lat2, lon2, timestamp1, timestamp2])):
                    distance = haversine(lat1, lon1, lat2, lon2)
                    delta_t_hours = (timestamp2 - timestamp1).total_seconds() / 3600
                    
                    # Se distância > 500km e delta_t < 1 hora, marca como salto GPS
                    if distance > self.gps_jump_distance_km and delta_t_hours < 1:
                        df_clean.loc[df_clean.index[i], 'gps_jump'] = True
                        quality_report['gps_jumps_marked'] += 1
        
        # 4. velocidade calculada > 220 km/h → marcar como outlier
        if 'speed' in df_clean.columns:
            speed_outliers = df_clean['speed'] > self.speed_outlier_threshold
            quality_report['speed_outliers_marked'] += speed_outliers.sum()
            df_clean['speed_outlier'] = speed_outliers
        
        # Adicionar cálculos de delta para validações
        if 'odometer' in df_clean.columns:
            df_clean['delta_km'] = df_clean['odometer'].diff().fillna(0)
        if 'timestamp' in df_clean.columns:
            df_clean['delta_t'] = df_clean['timestamp'].diff().dt.total_seconds().fillna(0) / 3600
        
        # 5. Detectar inconsistências lógicas
        inconsistent = pd.Series(False, index=df_clean.index)
        if 'delta_km' in df_clean.columns and 'speed' in df_clean.columns:
            rule1 = (df_clean['delta_km'] > 0) & (df_clean['speed'] == 0)
            inconsistent |= rule1
            rule2 = (df_clean['delta_km'] == 0) & (df_clean['speed'] > 0)
            inconsistent |= rule2
        if 'combustivel_litros' in df_clean.columns and 'delta_km' in df_clean.columns and 'speed' in df_clean.columns:
            rule3 = (df_clean['delta_km'] > 0) & (df_clean['speed'] > 0) & (df_clean['combustivel_litros'] == 0)
            inconsistent |= rule3
        quality_report['inconsistent_removed'] = inconsistent.sum()
        df_clean = df_clean[~inconsistent]
        
        # 6. Se total_km > 0 e max_speed_raw == 0 → recalcule max_speed
        # Esta verificação será feita após o cálculo das métricas
        
        return df_clean, quality_report
    
    def calculate_distance_and_speed(self, df: pd.DataFrame) -> Dict:
        """
        Calcula distância e velocidade recomendadas
        """
        metrics = {}
        
        # Distance total (por veículo por período)
        if 'odometer' in df.columns and len(df) > 0:
            odometer_valid = df['odometer'].notna()
            if odometer_valid.sum() > 0:
                # total_km = odometer.max() - odometer.min() (usar somente se odometer parecer confiável)
                odometer_values = df.loc[odometer_valid, 'odometer']
                total_km_odometer = odometer_values.max() - odometer_values.min()
                metrics['total_km_odometer'] = total_km_odometer
            else:
                metrics['total_km_odometer'] = 0
        else:
            metrics['total_km_odometer'] = 0
        
        # Se odometer não disponível ou não plausível, calcular via haversine
        if 'lat' in df.columns and 'lon' in df.columns and len(df) > 1:
            total_km_haversine = 0
            valid_points = df[['lat', 'lon']].dropna()
            for i in range(1, len(valid_points)):
                lat1, lon1 = valid_points.iloc[i-1]['lat'], valid_points.iloc[i-1]['lon']
                lat2, lon2 = valid_points.iloc[i]['lat'], valid_points.iloc[i]['lon']
                if all(pd.notna([lat1, lon1, lat2, lon2])):
                    total_km_haversine += haversine(lat1, lon1, lat2, lon2)
            metrics['total_km_haversine'] = total_km_haversine
        else:
            metrics['total_km_haversine'] = 0
        
        # Escolher a melhor distância
        # Priorizar odômetro quando disponível e plausível (>= 2 leituras válidas e delta não-negativo)
        odometer_valid = df['odometer'].notna() if 'odometer' in df.columns else pd.Series([], dtype=bool)
        if 'odometer' in df.columns and odometer_valid.sum() >= 2 and metrics['total_km_odometer'] >= 0:
            metrics['total_km'] = metrics['total_km_odometer']
            metrics['distance_source'] = 'odometer'
        else:
            metrics['total_km'] = metrics['total_km_haversine']
            metrics['distance_source'] = 'haversine'
        
        # Velocidade máxima
        if 'speed' in df.columns and len(df) > 0:
            speed_valid = df['speed'].notna()
            if speed_valid.sum() > 0:
                # Remover outliers (> 220 km/h)
                valid_speeds = df.loc[speed_valid, 'speed']
                valid_speeds = valid_speeds[valid_speeds <= self.speed_outlier_threshold]
                if len(valid_speeds) > 0:
                    max_speed_raw = valid_speeds.max()
                    metrics['max_speed_raw'] = max_speed_raw
                else:
                    metrics['max_speed_raw'] = 0
            else:
                metrics['max_speed_raw'] = 0
        else:
            metrics['max_speed_raw'] = 0
        
        # Se speed ausente ou zerado, calcular instant_speeds entre pontos
        if metrics.get('max_speed_raw', 0) == 0:
            # Calcular velocidades instantâneas
            instant_speeds = []
            for i in range(1, len(df)):
                if 'timestamp' in df.columns and 'lat' in df.columns and 'lon' in df.columns:
                    timestamp1 = pd.to_datetime(df.iloc[i-1]['timestamp'])
                    timestamp2 = pd.to_datetime(df.iloc[i]['timestamp'])
                    lat1, lon1 = df.iloc[i-1]['lat'], df.iloc[i-1]['lon']
                    lat2, lon2 = df.iloc[i]['lat'], df.iloc[i]['lon']
                    
                    if all(pd.notna([timestamp1, timestamp2, lat1, lon1, lat2, lon2])):
                        distance = haversine(lat1, lon1, lat2, lon2)
                        delta_t_hours = (timestamp2 - timestamp1).total_seconds() / 3600
                        if delta_t_hours > 0:
                            speed = distance / delta_t_hours
                            instant_speeds.append(speed)
            
            if instant_speeds:
                # Usar o percentil 95 (ou 99) de inst_speed como max_speed_estimada
                metrics['max_speed_instant_95'] = np.percentile(instant_speeds, 95)
                metrics['max_speed_instant_99'] = np.percentile(instant_speeds, 99)
                metrics['max_speed_estimada'] = metrics['max_speed_instant_95']
            else:
                metrics['max_speed_estimada'] = 0
        
        # Regras específicas para o problema "300KM rodado com 0 velocidade máxima"
        if metrics.get('total_km', 0) >= 20 and metrics.get('max_speed_raw', 0) == 0:
            # Recalcule max_speed a partir de inst_speed
            if 'max_speed_estimada' in metrics:
                metrics['max_speed'] = max(metrics['max_speed_estimada'], metrics.get('max_speed_raw', 0))
            else:
                metrics['max_speed'] = metrics.get('max_speed_raw', 0)
            
            # Se ainda for 0 ou < 5 km/h, marque como sensor de velocidade inativo
            if metrics['max_speed'] == 0 or metrics['max_speed'] < 5:
                metrics['sensor_issue'] = True
                metrics['max_speed'] = metrics.get('total_km', 0)  # Usar odometer como referência
                metrics['speed_source'] = 'odometer_based'
            else:
                metrics['sensor_issue'] = False
                metrics['speed_source'] = 'instant_speed'
        else:
            metrics['max_speed'] = metrics.get('max_speed_raw', 0)
            metrics['sensor_issue'] = False
            metrics['speed_source'] = 'raw_speed' if metrics.get('max_speed_raw', 0) > 0 else 'instant_speed'
        
        return metrics
    
    def detect_trips(self, df: pd.DataFrame) -> List[Dict]:
        """
        Detecta viagens (trips) e calcula métricas por viagem
        """
        trips = []
        if len(df) < 2:
            return trips
        
        # Converter timestamps
        if 'timestamp' in df.columns:
            df = df.copy()
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp').reset_index(drop=True)
        
        # Detectar início e fim de viagens
        in_trip = False
        trip_start_idx = None
        
        for i in range(len(df)):
            speed = df.iloc[i]['speed'] if 'speed' in df.columns else 0
            timestamp = df.iloc[i]['timestamp'] if 'timestamp' in df.columns else None
            
            if not in_trip and speed > self.trip_speed_threshold:
                # Potencial início de viagem
                in_trip = True
                trip_start_idx = i
            elif in_trip and speed <= self.trip_speed_threshold:
                # Potencial fim de viagem
                if trip_start_idx is not None and i > trip_start_idx:
                    # Verificar duração mínima
                    start_time = df.iloc[trip_start_idx]['timestamp']
                    end_time = df.iloc[i]['timestamp']

                    duration = (end_time - start_time).total_seconds()
                    
                    if duration >= self.trip_min_duration_s:
                        # Calcular distância da viagem
                        odo_delta = None
                        hav_cum = None
                        # 1) Calcular delta de odômetro, quando disponível e plausível
                        if 'odometer' in df.columns:
                            odo_start = df.iloc[trip_start_idx].get('odometer')
                            odo_end = df.iloc[i].get('odometer')
                            if pd.notna(odo_start) and pd.notna(odo_end):
                                delta = float(odo_end) - float(odo_start)
                                if delta >= 0:
                                    odo_delta = delta
                        # 2) Calcular haversine cumulativo
                        if 'lat' in df.columns and 'lon' in df.columns:
                            cumulative = 0.0
                            for j in range(trip_start_idx + 1, i + 1):
                                lat1 = df.iloc[j-1]['lat']
                                lon1 = df.iloc[j-1]['lon']
                                lat2 = df.iloc[j]['lat']
                                lon2 = df.iloc[j]['lon']
                                if all(pd.notna([lat1, lon1, lat2, lon2])):
                                    cumulative += haversine(lat1, lon1, lat2, lon2)
                            hav_cum = cumulative
                        # 3) Escolher fonte
                        if odo_delta is not None and odo_delta >= 0:
                            distance_km = odo_delta
                            distance_source = 'odometer'
                        elif hav_cum is not None:
                            distance_km = hav_cum
                            distance_source = 'haversine'
                        else:
                            distance_km = 0.0
                            distance_source = 'unknown'
                        # Aplicar threshold mínimo de deslocamento (> 100m)
                        if distance_km * 1000 > 100:
                            # Criar trip
                            trip = {
                                'start_time': start_time,
                                'end_time': end_time,
                                'duration': duration,
                                'distance_km': distance_km,
                                'avg_speed_moving': self._calculate_avg_moving_speed(df, trip_start_idx, i),
                                'max_speed_trip': self._calculate_max_speed_trip(df, trip_start_idx, i)
                            }
                            trips.append(trip)
                
                in_trip = False
                trip_start_idx = None
        
        return trips
    
    def _calculate_avg_moving_speed(self, df: pd.DataFrame, start_idx: int, end_idx: int) -> float:
        """Calcula velocidade média apenas em pontos com speed > 3"""
        speeds = []
        for i in range(start_idx, end_idx + 1):
            speed = df.iloc[i]['speed'] if 'speed' in df.columns else 0
            if speed > self.trip_speed_threshold:
                speeds.append(speed)
        return np.mean(speeds) if speeds else 0
    
    def _calculate_max_speed_trip(self, df: pd.DataFrame, start_idx: int, end_idx: int) -> float:
        """Calcula velocidade máxima na viagem"""
        speeds = []
        for i in range(start_idx, end_idx + 1):
            speed = df.iloc[i]['speed'] if 'speed' in df.columns else 0
            speeds.append(speed)
        return max(speeds) if speeds else 0
    
    def read_csv_file(self, file_path: str) -> pd.DataFrame:
        """
        Lê arquivo CSV e retorna DataFrame limpo e padronizado
        """
        try:
            # Tenta diferentes encodings
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(file_path, sep=';', encoding=encoding)
                    break
                except UnicodeDecodeError:
                    continue
            
            if df is None:
                raise ValueError(f"Não foi possível ler o arquivo {file_path} com nenhum encoding")
            
            # Limpa os nomes das colunas
            df.columns = df.columns.str.strip()
            
            # Verifica se tem as colunas necessárias
            missing_cols = [col for col in self.required_columns if col not in df.columns]
            if missing_cols:
                print(f"Aviso: Colunas faltando: {missing_cols}")
            
            return df
        
        except Exception as e:
            raise Exception(f"Erro ao ler arquivo CSV {file_path}: {str(e)}")
    
    def clean_and_parse_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpa e padroniza os dados do DataFrame
        """
        df_clean = df.copy()
        
        # Limpa e converte datas
        df_clean['Data'] = pd.to_datetime(df_clean['Data'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
        
        if 'Data (GPRS)' in df_clean.columns:
            df_clean['Data (GPRS)'] = pd.to_datetime(df_clean['Data (GPRS)'], format='%d/%m/%Y %H:%M:%S', errors='coerce')
        
        # Limpa velocidade
        df_clean['Velocidade (Km)'] = pd.to_numeric(df_clean['Velocidade (Km)'], errors='coerce').fillna(0)
        
        # Processa coordenadas
        if 'Localização' in df_clean.columns:
            coords = df_clean['Localização'].str.split(',', expand=True)
            if coords.shape[1] >= 2:
                df_clean['Latitude'] = pd.to_numeric(coords[0], errors='coerce')
                df_clean['Longitude'] = pd.to_numeric(coords[1], errors='coerce')
            else:
                df_clean['Latitude'] = np.nan
                df_clean['Longitude'] = np.nan
        
        # Limpa dados de odômetro
        if 'Odômetro do período  (Km)' in df_clean.columns:
            df_clean['Odometro_Periodo_Km'] = pd.to_numeric(df_clean['Odômetro do período  (Km)'], errors='coerce').fillna(0)
        
        if 'Odômetro embarcado (Km)' in df_clean.columns:
            df_clean['Odometro_Embarcado_Km'] = pd.to_numeric(df_clean['Odômetro embarcado (Km)'], errors='coerce').fillna(0)
        
        # Converte GPS e GPRS para booleano
        df_clean['GPS'] = df_clean['GPS'].astype(str).map({'1': True, '0': False}).fillna(True)
        df_clean['Gprs'] = df_clean['Gprs'].astype(str).map({'1': True, '0': False}).fillna(True)
        
        # Limpa dados de bateria
        if 'Bateria' in df_clean.columns:
            df_clean['Bateria_Pct'] = df_clean['Bateria'].str.extract(r'(\d+)').astype(float)
        
        # Limpa tensão
        if 'Tensão' in df_clean.columns:
            df_clean['Tensao_V'] = pd.to_numeric(df_clean['Tensão'], errors='coerce')
        
        # Converte bloqueado para booleano
        df_clean['Bloqueado'] = df_clean['Bloqueado'].astype(str).map({'1': True, '0': False}).fillna(False)
        
        # Remove linhas com data inválida
        df_clean = df_clean.dropna(subset=['Data'])
        
        return df_clean
    
    def load_perfis_cliente(self, cliente_id: int) -> Dict:
        """Carrega perfis de horário personalizados do cliente"""
        if cliente_id in self._cached_perfis:
            return self._cached_perfis[cliente_id]
            
        try:
            from .models import get_session, PerfilHorario
            session = get_session()
            try:
                perfis = session.query(PerfilHorario).filter_by(
                    cliente_id=cliente_id, 
                    ativo=True
                ).all()
                
                perfis_dict = {}
                for perfil in perfis:
                    perfis_dict[perfil.nome.lower().replace(' ', '_')] = {
                        'inicio': perfil.hora_inicio,
                        'fim': perfil.hora_fim,
                        'tipo': perfil.tipo_periodo,
                        'cor': perfil.cor_relatorio,
                        'nome': perfil.nome,
                        'descricao': perfil.descricao
                    }
                
                self._cached_perfis[cliente_id] = perfis_dict
                return perfis_dict
            finally:
                session.close()
        except Exception as e:
            print(f"Erro ao carregar perfis do cliente {cliente_id}: {e}")
            return {}

    def classify_operational_period(self, timestamp: datetime, cliente_id: Optional[int] = None) -> str:
        """
        Classifica um timestamp em período operacional usando perfis personalizados do cliente
        """
        if timestamp.weekday() >= 5:  # Sábado=5, Domingo=6
            return 'final_semana'
        
        current_time = timestamp.time()
        
        # Tenta usar perfis personalizados do cliente
        if cliente_id:
            perfis_cliente = self.load_perfis_cliente(cliente_id)
            if perfis_cliente:
                # Verifica períodos operacionais personalizados
                for nome_perfil, config in perfis_cliente.items():
                    if config['tipo'] == 'operacional':
                        inicio = config['inicio']
                        fim = config['fim']
                        
                        # Trata horário que cruza meia-noite (ex: 19:00 - 04:00)
                        if inicio <= fim:
                            if inicio <= current_time <= fim:
                                return nome_perfil
                        else:
                            if current_time >= inicio or current_time <= fim:
                                return nome_perfil
                
                # Se não encontrou período operacional, verifica outros tipos
                for nome_perfil, config in perfis_cliente.items():
                    if config['tipo'] in ['fora_horario', 'especial']:
                        inicio = config['inicio']
                        fim = config['fim']
                        
                        if inicio <= fim:
                            if inicio <= current_time <= fim:
                                return nome_perfil
                        else:
                            if current_time >= inicio or current_time <= fim:
                                return nome_perfil
                
                # Se não encontrou nenhum período, é fora do horário
                return 'fora_horario'
        
        # Fallback para períodos padrão se não houver perfis personalizados
        for periodo, (inicio, fim) in self.periodos_operacionais.items():
            if inicio <= current_time <= fim:
                return periodo
        
        return 'fora_horario'
    
    def calculate_metrics(self, df: pd.DataFrame) -> Dict:
        """
        Calcula métricas principais do DataFrame
        """
        if df.empty:
            return {}
        
        # Métricas básicas - conversão para tipos nativos Python
        metrics = {
            'total_registros': int(len(df)),
            'data_inicio': df['Data'].min().isoformat(),
            'data_fim': df['Data'].max().isoformat(),
            'velocidade_maxima': int(df['Velocidade (Km)'].max()),
            'velocidade_media': float(df['Velocidade (Km)'].mean()),
            'km_total': float(df['Odometro_Periodo_Km'].max() - df['Odometro_Periodo_Km'].min()) if 'Odometro_Periodo_Km' in df.columns else 0.0,
        }
        
        # Análise por estado da ignição
        ignicao_stats = df['Ignição'].value_counts()
        metrics['tempo_ligado'] = int(ignicao_stats.get('L', 0) + ignicao_stats.get('LP', 0) + ignicao_stats.get('LM', 0))
        metrics['tempo_desligado'] = int(ignicao_stats.get('D', 0))
        metrics['tempo_movimento'] = int(ignicao_stats.get('LM', 0))
        metrics['tempo_parado'] = int(ignicao_stats.get('LP', 0))
        
        # Análise por período operacional
        # Identifica cliente_id do DataFrame (assume que todos os registros são do mesmo cliente)
        cliente_id = None
        if 'Cliente' in df.columns and not df['Cliente'].empty:
            primeiro_cliente = df['Cliente'].iloc[0]
            # Aqui você pode implementar uma função para buscar cliente_id pelo nome
            # Por enquanto vamos usar um mapeamento simples ou None para fallback
            try:
                from .models import get_session, Cliente
                session = get_session()
                try:
                    cliente_obj = session.query(Cliente).filter_by(nome=primeiro_cliente).first()
                    if cliente_obj:
                        cliente_id = cliente_obj.id
                finally:
                    session.close()
            except Exception as e:
                print(f"Erro ao buscar cliente_id: {e}")
        
        df['periodo_operacional'] = df['Data'].apply(lambda x: self.classify_operational_period(x, cliente_id))
        periodo_stats = df['periodo_operacional'].value_counts()
        
        metrics['registros_manha'] = int(periodo_stats.get('manha', 0))
        metrics['registros_meio_dia'] = int(periodo_stats.get('meio_dia', 0))
        metrics['registros_tarde'] = int(periodo_stats.get('tarde', 0))
        metrics['registros_final_semana'] = int(periodo_stats.get('final_semana', 0))
        metrics['registros_fora_horario'] = int(periodo_stats.get('fora_horario', 0))
        
        # Análise de conectividade
        metrics['gps_ok'] = int(df['GPS'].sum())
        metrics['gprs_ok'] = int(df['Gprs'].sum())
        metrics['conectividade_problemas'] = int(len(df) - min(metrics['gps_ok'], metrics['gprs_ok']))
        
        # Eventos especiais
        eventos_especiais = df[df['Tipo do Evento'].str.contains('Excesso|Violado|Bloq', na=False, case=False)]
        metrics['eventos_especiais'] = int(len(eventos_especiais))
        
        return metrics
    
    def save_to_database(self, df: pd.DataFrame, client_name: str = None) -> bool:
        """
        Salva dados do DataFrame no banco de dados
        """
        session = get_session()
        
        try:
            # Busca ou cria cliente
            if client_name:
                cliente = session.query(Cliente).filter_by(nome=client_name).first()
            else:
                cliente = session.query(Cliente).filter_by(nome=df['Cliente'].iloc[0]).first()
            
            if not cliente:
                cliente = Cliente(
                    nome=client_name or df['Cliente'].iloc[0],
                    consumo_medio_kmL=12.0,
                    limite_velocidade=80
                )
                session.add(cliente)
                session.commit()
            
            # Processa cada linha do DataFrame
            for _, row in df.iterrows():
                # Busca ou cria veículo
                veiculo = session.query(Veiculo).filter_by(placa=row['Placa']).first()
                if not veiculo:
                    veiculo = Veiculo(
                        placa=row['Placa'],
                        ativo=row['Ativo'],
                        cliente_id=cliente.id
                    )
                    session.add(veiculo)
                    session.commit()
                
                # Cria registro de posição
                posicao = PosicaoHistorica(
                    veiculo_id=veiculo.id,
                    data_evento=row['Data'],
                    data_gprs=row.get('Data (GPRS)'),
                    velocidade_kmh=int(row['Velocidade (Km)']),
                    ignicao=row['Ignição'],
                    motorista=row.get('Motorista', ''),
                    gps_status=row['GPS'],
                    gprs_status=row['Gprs'],
                    latitude=row.get('Latitude'),
                    longitude=row.get('Longitude'),
                    endereco=row.get('Endereço', ''),
                    tipo_evento=row.get('Tipo do Evento', ''),
                    saida=row.get('Saida', ''),
                    entrada=row.get('Entrada', ''),
                    pacote=row.get('Pacote', ''),
                    odometro_periodo_km=row.get('Odometro_Periodo_Km', 0),
                    odometro_embarcado_km=row.get('Odometro_Embarcado_Km', 0),
                    horimetro_periodo=row.get('Horímetro do período', ''),
                    horimetro_embarcado=row.get('Horímetro embarcado', ''),
                    bateria_pct=row.get('Bateria_Pct'),
                    tensao_v=row.get('Tensao_V'),
                    bloqueado=row['Bloqueado'],
                    imagem=row.get('Imagem', '')
                )
                
                session.add(posicao)
            
            session.commit()
            return True
            
        except Exception as e:
            session.rollback()
            print(f"Erro ao salvar no banco: {str(e)}")
            return False
        finally:
            session.close()

def process_csv_files(directory_path: str) -> Dict:
    """
    Processa todos os arquivos CSV em um diretório
    """
    processor = CSVProcessor()
    results = {}
    
    csv_files = [f for f in os.listdir(directory_path) if f.endswith('.csv')]
    
    for csv_file in csv_files:
        file_path = os.path.join(directory_path, csv_file)
        print(f"Processando: {csv_file}")
        
        try:
            # Lê e processa arquivo
            df = processor.read_csv_file(file_path)
            df_clean = processor.clean_and_parse_data(df)
            
            # Calcula métricas
            metrics = processor.calculate_metrics(df_clean)
            
            # Salva no banco
            success = processor.save_to_database(df_clean)
            
            results[csv_file] = {
                'success': success,
                'metrics': convert_numpy_types(metrics),
                'records_processed': int(len(df_clean))
            }
            
        except Exception as e:
            results[csv_file] = {
                'success': False,
                'error': str(e),
                'records_processed': 0
            }
    
    return convert_numpy_types(results)

def get_fuel_consumption_estimate(km_traveled: float, avg_speed: float, vehicle_kmL: float = 12.0) -> Dict:
    """
    Estima consumo de combustível baseado em quilometragem e velocidade média
    """
    # Fator de correção baseado na velocidade média
    if avg_speed < 30:
        efficiency_factor = 0.8  # Trânsito urbano, menor eficiência
    elif avg_speed > 80:
        efficiency_factor = 0.85  # Velocidade alta, menor eficiência
    else:
        efficiency_factor = 1.0  # Velocidade ideal
    
    adjusted_kmL = vehicle_kmL * efficiency_factor
    fuel_consumed = km_traveled / adjusted_kmL if adjusted_kmL > 0 else 0
    
    return {
        'km_traveled': km_traveled,
        'fuel_consumed_liters': round(fuel_consumed, 2),
        'efficiency_kmL': round(adjusted_kmL, 2),
        'avg_speed': avg_speed
    }