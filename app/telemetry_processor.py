"""
Módulo para processamento avançado de dados de telemetria veicular com detecção automática de schema
e mecanismos de fallback robustos.
"""

import pandas as pd
import numpy as np
from datetime import datetime, time, timezone
from typing import Dict, List, Tuple, Optional, Any, Union
import re
import os
import json
import logging
from math import radians, sin, cos, asin, sqrt
from sqlalchemy.orm import Session
from .models import Cliente, Veiculo, PosicaoHistorica, get_session
from .utils import CSVProcessor

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    elif pd.isna(obj):
        return None
    return obj

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calcula a distância entre dois pontos usando a fórmula de Haversine
    """
    R = 6371.0  # raio da Terra em km
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    return R * c  # distância em km

class TelemetryProcessor:
    """Classe para processar arquivos CSV de telemetria veicular com detecção automática de schema"""
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Inicializa o processador de telemetria
        
        Args:
            config: Dicionário com parâmetros configuráveis
        """
        # Parâmetros configuráveis com valores padrão
        self.config = config or {}
        self.speed_outlier_threshold = self.config.get('speed_outlier_threshold', 220)  # km/h
        self.trip_speed_threshold = self.config.get('trip_speed_threshold', 3)  # km/h
        self.trip_min_duration_s = self.config.get('trip_min_duration_s', 60)  # segundos
        self.gps_jump_distance_km = self.config.get('gps_jump_distance_km', 500)  # km
        self.aggregation_rule_days_for_summary = self.config.get('aggregation_rule_days_for_summary', 7)  # dias
        
        # Definição dos períodos operacionais
        self.periodos_operacionais = {
            'manha': (time(4, 0), time(7, 0)),
            'meio_dia': (time(10, 50), time(13, 0)),
            'tarde': (time(16, 50), time(19, 0))
        }
    
    def detect_schema(self, df: pd.DataFrame, filename: str = 'arquivo_csv') -> Dict:
        """
        Detecta automaticamente o schema de cada CSV.
        Para cada coluna, detecta tipo (timestamp, latitude, longitude, odometer, speed, ignition, 
        event, battery, vehicle_id, client_id, pagamento, estoque, etc.).
        
        Args:
            df: DataFrame pandas com os dados do CSV
            filename: Nome do arquivo para identificação
            
        Returns:
            Dict com informações do schema detectado
        """
        schema_detectado = {
            'arquivo': filename,
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
        
        Args:
            series: Série pandas representando uma coluna
            
        Returns:
            str: Tipo estimado da coluna
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
            # Ensure we're working with a pandas Series
            if not isinstance(numeric_series, pd.Series):
                numeric_series = pd.Series(numeric_series)
            # Filter out NaN values
            numeric_values = numeric_series.dropna()
                
            if len(numeric_values) > 0:
                # Convert to numpy array to ensure proper handling
                numeric_array = np.array(numeric_values)
                mean_val = float(np.mean(numeric_array))
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
        """Verifica se os valores parecem timestamps (tolerante a '24:00:00')"""
        # Normaliza casos de "24:00:00" e tenta converter de forma tolerante
        sample = pd.Series(values.head(3))
        try:
            sample_norm = self._normalize_24h_in_series(sample)
            parsed = pd.to_datetime(sample_norm, errors='coerce')
            valid_ratio = float(pd.notna(parsed).mean()) if len(parsed) > 0 else 0.0
            return valid_ratio >= 0.67  # pelo menos 2 de 3 válidos
        except Exception:
            return False

    def _normalize_24h_in_series(self, series: pd.Series) -> pd.Series:
        """Normaliza strings de timestamp com hora '24' para o dia seguinte 00:MM:SS.
        - Mantém o restante da string (minutos, segundos, frações e timezone) quando possível.
        - Para valores não-string, retorna o valor original.
        """
        return series.apply(self._fix_24h_string)

    def _fix_24h_string(self, value: Any) -> Any:
        """Corrige um único valor de timestamp contendo ' 24:' ou 'T24:' para o dia seguinte.
        Retorna o valor original se não houver necessidade de correção ou em caso de falha de parsing.
        """
        try:
            if not isinstance(value, str):
                return value
            s = value.strip()
            if ' 24:' not in s and 'T24:' not in s:
                return value

            import re
            # ISO-like: YYYY-MM-DD[ T]24:MM:SS(.fff)?(Z|±HH:MM)?
            m_iso = re.match(r"^(\d{4}-\d{2}-\d{2})([ T])24:(\d{2}):(\d{2})(\.[0-9]+)?(Z|[+-]\d{2}:\d{2})?$", s)
            if m_iso:
                date_part, sep, mm, ss, frac, tz = m_iso.groups()
                base_date = pd.to_datetime(date_part, errors='coerce')
                if pd.isna(base_date):
                    return value
                new_date = base_date + pd.Timedelta(days=1)
                frac = frac or ''
                tz = tz or ''
                return f"{new_date.strftime('%Y-%m-%d')}{sep}00:{mm}:{ss}{frac}{tz}"

            # BR-like: DD/MM/YYYY 24:MM:SS(.fff)?(Z|±HH:MM)?
            m_br = re.match(r"^(\d{2})/(\d{2})/(\d{4})\s+24:(\d{2}):(\d{2})(\.[0-9]+)?(Z|[+-]\d{2}:\d{2})?$", s)
            if m_br:
                dd, mm, yyyy, mm2, ss, frac, tz = m_br.groups()
                date_part = f"{yyyy}-{mm}-{dd}"
                base_date = pd.to_datetime(date_part, errors='coerce')
                if pd.isna(base_date):
                    return value
                new_date = base_date + pd.Timedelta(days=1)
                frac = frac or ''
                tz = tz or ''
                # Formata de volta como DD/MM/YYYY 00:MM:SS mantendo frações/tz
                return f"{new_date.strftime('%d/%m/%Y')} 00:{mm2}:{ss}{frac}{tz}"

            # Se não casou nenhum padrão conhecido, retorna original
            return value
        except Exception:
            return value

    def apply_quality_rules(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Aplica regras de qualidade e saneamento (sanity checks)
        
        Args:
            df: DataFrame pandas com os dados
            
        Returns:
            Tuple com DataFrame limpo e relatório de qualidade
        """
        df_clean = df.copy()
        quality_report = {
            'outliers_removed': 0,
            'duplicates_removed': 0,
            'gps_jumps_marked': 0,
            'speed_outliers_marked': 0,
            'anomalies_detected': []
        }
        
        initial_rows = len(df_clean)
        
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
            # Normalizar valores com '24:00:00' antes do parsing
            df_clean['timestamp'] = self._normalize_24h_in_series(df_clean['timestamp'])
            # Converter de forma tolerante (valores inválidos viram NaT)
            df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'], errors='coerce')
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
        
        # 5. Se total_km > 0 e max_speed_raw == 0 → recalcule max_speed
        # Esta verificação será feita após o cálculo das métricas
        
        quality_report['anomalies_detected'].append({
            'type': 'quality_check_summary',
            'initial_rows': initial_rows,
            'final_rows': len(df_clean),
            'rows_removed': initial_rows - len(df_clean)
        })
        
        return df_clean, quality_report
    
    def map_columns_with_fallback(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Mapeia colunas para nomes padrão aplicando fallbacks quando necessário.
        Delegador para reutilizar a lógica robusta já existente em utils.CSVProcessor.
        """
        # Instancia o processador utilitário e delega o mapeamento
        processor = CSVProcessor()
        mapped_df, mapping_info = processor.map_columns_with_fallback(df)
        return mapped_df, mapping_info

    # NOVO: wrappers para delegar cálculos ao CSVProcessor
    def calculate_distance_and_speed(self, df: pd.DataFrame) -> Dict:
        """
        Calcula distância total, fonte da distância e métricas de velocidade.
        Delegado para utils.CSVProcessor para manter uma única fonte de verdade.
        """
        processor = CSVProcessor()
        # Alinha parâmetros de configuração para consistência entre classes
        processor.speed_outlier_threshold = self.speed_outlier_threshold
        processor.trip_speed_threshold = self.trip_speed_threshold
        processor.trip_min_duration_s = self.trip_min_duration_s
        processor.gps_jump_distance_km = self.gps_jump_distance_km
        processor.periodos_operacionais = self.periodos_operacionais
        return processor.calculate_distance_and_speed(df)

    def detect_trips(self, df: pd.DataFrame) -> List[Dict]:
        """
        Detecta viagens (trips) e calcula métricas por viagem.
        Delegado para utils.CSVProcessor para reaproveitar a implementação testada.
        """
        processor = CSVProcessor()
        # Alinha parâmetros de configuração para consistência entre classes
        processor.speed_outlier_threshold = self.speed_outlier_threshold
        processor.trip_speed_threshold = self.trip_speed_threshold
        processor.trip_min_duration_s = self.trip_min_duration_s
        processor.gps_jump_distance_km = self.gps_jump_distance_km
        processor.periodos_operacionais = self.periodos_operacionais
        return processor.detect_trips(df)

    def process_csv_file(self, file_path: str) -> Dict:
        """
        Processa um arquivo CSV completo com todas as etapas
        
        Args:
            file_path: Caminho para o arquivo CSV
            
        Returns:
            Dicionário com resultados do processamento
        """
        try:
            # 1. Ler arquivo CSV
            df = self._read_csv_file(file_path)
            
            # 2. Detectar schema
            schema = self.detect_schema(df, os.path.basename(file_path))
            
            # 3. Mapear colunas com fallback
            mapped_df, mapping_info = self.map_columns_with_fallback(df)
            
            # 4. Aplicar regras de qualidade
            clean_df, quality_report = self.apply_quality_rules(mapped_df)
            
            # 5. Calcular distância e velocidade
            distance_speed_metrics = self.calculate_distance_and_speed(clean_df)
            
            # 6. Detectar viagens
            trips = self.detect_trips(clean_df)
            
            # 7. Calcular métricas gerais
            general_metrics = self._calculate_general_metrics(clean_df)
            
            # 8. Preparar relatório de verificação
            verification_report = self._generate_verification_report(
                df, clean_df, schema, mapping_info, quality_report
            )
            
            return {
                'success': True,
                'schema': schema,
                'mapping_info': mapping_info,
                'quality_report': quality_report,
                'distance_speed_metrics': distance_speed_metrics,
                'trips': trips,
                'general_metrics': general_metrics,
                'verification_report': verification_report,
                'processed_data': clean_df.to_dict('records')
            }
            
        except Exception as e:
            logger.error(f"Error processing CSV file {file_path}: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def _read_csv_file(self, file_path: str) -> pd.DataFrame:
        """
        Lê arquivo CSV com tratamento de diferentes encodings
        
        Args:
            file_path: Caminho para o arquivo CSV
            
        Returns:
            DataFrame pandas com os dados
        """
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
        
        return df
    
    def _calculate_general_metrics(self, df: pd.DataFrame) -> Dict:
        """
        Calcula métricas gerais do DataFrame
        
        Args:
            df: DataFrame pandas com os dados
            
        Returns:
            Dicionário com métricas gerais
        """
        if df.empty:
            return {}
        
        metrics = {
            'total_rows': len(df),
            'valid_rows': len(df.dropna()),
            'start_time': df['timestamp'].min().isoformat() if 'timestamp' in df.columns else None,
            'end_time': df['timestamp'].max().isoformat() if 'timestamp' in df.columns else None,
            'total_trips': 0,  # Será preenchido posteriormente
            'total_distance_km': 0,  # Será preenchido posteriormente
            'max_speed_kmh': 0,  # Será preenchido posteriormente
            'avg_speed_kmh': 0,  # Será preenchido posteriormente
        }

        # Trata timestamps com tolerância a NaT e strings
        if 'timestamp' in df.columns:
            ts_series = pd.to_datetime(df['timestamp'], errors='coerce')
            if ts_series.notna().any():
                start_val = ts_series.min()
                end_val = ts_series.max()
                metrics['start_time'] = start_val.isoformat() if pd.notna(start_val) else None
                metrics['end_time'] = end_val.isoformat() if pd.notna(end_val) else None

        return metrics
    
    def _generate_verification_report(self, original_df: pd.DataFrame, clean_df: pd.DataFrame, 
                                    schema: Dict, mapping_info: Dict, quality_report: Dict) -> Dict:
        """
        Gera relatório de verificação para prevenção de alucinações
        
        Args:
            original_df: DataFrame original
            clean_df: DataFrame limpo
            schema: Schema detectado
            mapping_info: Informações de mapeamento
            quality_report: Relatório de qualidade
            
        Returns:
            Dicionário com relatório de verificação
        """
        verification_report = {
            'total_rows_read': len(original_df),
            'valid_rows': len(clean_df),
            'rows_removed': len(original_df) - len(clean_df),
            'outliers_detected': quality_report.get('outliers_removed', 0) + 
                               quality_report.get('speed_outliers_marked', 0) +
                               quality_report.get('gps_jumps_marked', 0),
            'duplicates_removed': quality_report.get('duplicates_removed', 0),
            'detected_schema': schema,
            'column_mapping': mapping_info,
            'applied_rules': {
                'speed_outlier_threshold': self.speed_outlier_threshold,
                'trip_speed_threshold': self.trip_speed_threshold,
                'trip_min_duration_s': self.trip_min_duration_s,
                'gps_jump_distance_km': self.gps_jump_distance_km
            },
            'checksum': self._calculate_checksum(clean_df)
        }
        
        return verification_report
    
    def _calculate_checksum(self, df: pd.DataFrame) -> str:
        """
        Calcula um checksum simples para rastreabilidade
        
        Args:
            df: DataFrame pandas com os dados
            
        Returns:
            String com o checksum
        """
        if df.empty:
            return "empty"
        
        # Calcula soma das IDs/contagem para rastreabilidade
        checksum_parts = []
        
        if 'vehicle_id' in df.columns:
            checksum_parts.append(str(df['vehicle_id'].nunique()))
        
        if 'timestamp' in df.columns:
            checksum_parts.append(str(len(df)))
        
        return "|".join(checksum_parts) if checksum_parts else "no_checksum"
    
    def save_to_database(self, df: pd.DataFrame, client_name: str = None) -> bool:
        """
        Salva dados do DataFrame no banco de dados
        
        Args:
            df: DataFrame pandas com os dados
            client_name: Nome do cliente (opcional)
            
        Returns:
            Boolean indicando sucesso ou falha
        """
        session = get_session()
        
        try:
            # Busca ou cria cliente
            if client_name:
                cliente = session.query(Cliente).filter_by(nome=client_name).first()
            else:
                cliente = session.query(Cliente).filter_by(nome=df['client_id'].iloc[0] if 'client_id' in df.columns else 'Unknown').first()
            
            if not cliente:
                cliente = Cliente(
                    nome=client_name or (df['client_id'].iloc[0] if 'client_id' in df.columns else 'Unknown'),
                    consumo_medio_kmL=12.0,
                    limite_velocidade=80
                )
                session.add(cliente)
                session.commit()
            
            # Processa cada linha do DataFrame
            for _, row in df.iterrows():
                # Busca ou cria veículo
                vehicle_id = row.get('vehicle_id', row.get('placa', 'Unknown'))
                veiculo = session.query(Veiculo).filter_by(placa=vehicle_id).first()
                if not veiculo:
                    veiculo = Veiculo(
                        placa=vehicle_id,
                        ativo='Ativo',  # Valor padrão
                        cliente_id=cliente.id
                    )
                    session.add(veiculo)
                    session.commit()
                
                # Cria registro de posição
                posicao = PosicaoHistorica(
                    veiculo_id=veiculo.id,
                    data_evento=row.get('timestamp'),
                    data_gprs=row.get('timestamp'),  # Usando o mesmo timestamp como fallback
                    velocidade_kmh=int(row.get('speed', 0)),
                    ignicao='L' if row.get('ignition', True) else 'D',  # Simplificação
                    motorista='',  # Valor padrão
                    gps_status=True,  # Valor padrão
                    gprs_status=True,  # Valor padrão
                    latitude=row.get('lat'),
                    longitude=row.get('lon'),
                    endereco='',  # Valor padrão
                    tipo_evento='',  # Valor padrão
                    saida='',  # Valor padrão
                    entrada='',  # Valor padrão
                    pacote='',  # Valor padrão
                    odometro_periodo_km=row.get('odometer', 0),
                    odometro_embarcado_km=row.get('odometer', 0),  # Usando o mesmo valor como fallback
                    horimetro_periodo='',  # Valor padrão
                    horimetro_embarcado='',  # Valor padrão
                    bateria_pct=None,  # Valor padrão
                    tensao_v=None,  # Valor padrão
                    bloqueado=False,  # Valor padrão
                    imagem=''  # Valor padrão
                )
                
                session.add(posicao)
            
            session.commit()
            return True
            
        except Exception as e:
            session.rollback()
            logger.error(f"Erro ao salvar no banco: {str(e)}")
            return False
        finally:
            session.close()
    
    def generate_outputs(self, processing_result: Dict, output_dir: str, base_filename: str) -> Dict:
        """
        Gera todos os outputs exigidos (PDF, JSON, CSV, logs)
        
        Args:
            processing_result: Resultado do processamento
            output_dir: Diretório de saída
            base_filename: Nome base para os arquivos
            
        Returns:
            Dicionário com caminhos dos arquivos gerados
        """
        output_paths = {}
        
        # 1. JSON com KPIs e dados agregados
        json_path = os.path.join(output_dir, f"Relatorio_{base_filename}.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(convert_numpy_types(processing_result), f, ensure_ascii=False, indent=2)
        output_paths['json'] = json_path
        
        # 2. CSV com anomalias_detectadas (linhas com problemas)
        if 'quality_report' in processing_result and processing_result['quality_report'].get('anomalies_detected'):
            csv_anomalies_path = os.path.join(output_dir, f"Anomalias_{base_filename}.csv")
            # Criar DataFrame com anomalias
            anomalies_data = processing_result['quality_report']['anomalies_detected']
            if anomalies_data:
                anomalies_df = pd.DataFrame(anomalies_data)
                anomalies_df.to_csv(csv_anomalies_path, sep=';', index=False)
                output_paths['anomalies_csv'] = csv_anomalies_path
        
        # 3. Log de processamento .txt com detalhes
        log_path = os.path.join(output_dir, f"Log_{base_filename}.txt")
        with open(log_path, 'w', encoding='utf-8') as f:
            f.write(f"Processamento concluído em {datetime.now().isoformat()}\n")
            f.write(f"Total de linhas lidas: {processing_result.get('verification_report', {}).get('total_rows_read', 0)}\n")
            f.write(f"Linhas válidas: {processing_result.get('verification_report', {}).get('valid_rows', 0)}\n")
            f.write(f"Pontos removidos: {processing_result.get('verification_report', {}).get('rows_removed', 0)}\n")
            f.write(f"Outliers detectados: {processing_result.get('verification_report', {}).get('outliers_detected', 0)}\n")
            f.write("\nMapeamento de colunas detectadas:\n")
            mapping_info = processing_result.get('mapping_info', {})
            for original, mapped in mapping_info.get('original_to_mapped', {}).items():
                f.write(f"  {original} → {mapped}\n")
            f.write("\nColunas ausentes:\n")
            for missing in mapping_info.get('missing_columns', []):
                f.write(f"  {missing}\n")
            f.write("\nFallbacks aplicados:\n")
            for fallback in mapping_info.get('fallbacks_applied', []):
                f.write(f"  {fallback}\n")
            f.write("\nRegras aplicadas:\n")
            rules = processing_result.get('verification_report', {}).get('applied_rules', {})
            for rule, value in rules.items():
                f.write(f"  {rule}: {value}\n")
            f.write(f"\nChecksum: {processing_result.get('verification_report', {}).get('checksum', 'N/A')}\n")
        output_paths['log'] = log_path
        
        # 4. Preparar dados para PDF em arquivo JSON separado
        pdf_data_path = os.path.join(output_dir, f"PDF_Data_{base_filename}.json")
        with open(pdf_data_path, 'w', encoding='utf-8') as f:
            json.dump(convert_numpy_types(processing_result), f, ensure_ascii=False, indent=2)
        output_paths['pdf_data'] = pdf_data_path
        
        return output_paths

# Função para uso standalone
def process_telemetry_csv(file_path: str, config: Optional[Dict] = None) -> Dict:
    """
    Processa um arquivo CSV de telemetria com a configuração padrão
    
    Args:
        file_path: Caminho para o arquivo CSV
        config: Configuração opcional
        
    Returns:
        Dicionário com resultados do processamento
    """
    processor = TelemetryProcessor(config)
    return processor.process_csv_file(file_path)

# Método de QA como função no módulo e monkey patch na classe

def run_qa_tests(self, processing_result: Dict) -> Dict:
    """Executa testes de QA sobre o resultado do processamento.
    Atualmente cobre a verificação de consistência de timezone (Teste 4).
    """
    qa_results: Dict[str, str] = {}
    
    # Teste 4: Consistência de timezone nos timestamps
    processed = processing_result.get('processed_data') or []
    if not processed:
        qa_results['test_4_timezone_consistency'] = 'skipped - no timestamps'
        return qa_results
    
    # Extrair timestamps
    tz_naive = 0
    tz_aware = 0
    offsets = set()
    for row in processed:
        ts = row.get('timestamp')
        if ts is None:
            continue
        ts_parsed = pd.to_datetime(ts, utc=False, errors='coerce')
        if pd.isna(ts_parsed):
            continue
        # Em pandas, timezone-aware possui tzinfo; naive não
        if getattr(ts_parsed, 'tz', None) is not None and ts_parsed.tz is not None:
            tz_aware += 1
            try:
                offsets.add(ts_parsed.utcoffset())
            except Exception:
                pass
        else:
            tz_naive += 1
    
    if tz_aware > 0 and tz_naive > 0:
        qa_results['test_4_timezone_consistency'] = 'failed - mixed timezone awareness'
    elif tz_aware > 0:
        if len(offsets) <= 1:
            qa_results['test_4_timezone_consistency'] = 'passed'
        else:
            qa_results['test_4_timezone_consistency'] = 'failed - multiple timezones detected'
    elif tz_naive > 0:
        # Todos timestamps sem timezone: aceitável se forem consistentes
        qa_results['test_4_timezone_consistency'] = 'passed'
    else:
        qa_results['test_4_timezone_consistency'] = 'skipped - no timestamps'
    
    return qa_results

# Atribuir à classe
TelemetryProcessor.run_qa_tests = run_qa_tests

if __name__ == "__main__":
    # Exemplo de uso
    print("TelemetryProcessor module loaded successfully")