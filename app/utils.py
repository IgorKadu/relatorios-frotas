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
        
        # Definição dos períodos operacionais
        self.periodos_operacionais = {
            'manha': (time(4, 0), time(7, 0)),
            'meio_dia': (time(10, 50), time(13, 0)),
            'tarde': (time(16, 50), time(19, 0))
        }
    
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
    
    def classify_operational_period(self, timestamp: datetime) -> str:
        """
        Classifica um timestamp em período operacional
        """
        if timestamp.weekday() >= 5:  # Sábado=5, Domingo=6
            return 'final_semana'
        
        current_time = timestamp.time()
        
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
        df['periodo_operacional'] = df['Data'].apply(self.classify_operational_period)
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

if __name__ == "__main__":
    # Teste do processador
    processor = CSVProcessor()
    print("Utilitários CSV carregados com sucesso!")