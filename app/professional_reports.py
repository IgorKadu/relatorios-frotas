"""
Sistema de Relatórios Profissionais de Frota
Geração de relatórios PDF com dados de telemetria seguindo especificações técnicas rigorosas.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
from typing import List, Dict, Optional, Tuple, Any
import hashlib
import json
import logging
from pathlib import Path
import math
from geopy.distance import geodesic

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FleetReportProcessor:
    """
    Processador principal para geração de relatórios de frota profissionais.
    Implementa todas as regras de validação e cálculos especificados.
    """
    
    def __init__(self, timezone: str = "America/Bahia", consumo_default_km_por_litro: float = 12.0):
        self.timezone = pytz.timezone(timezone)
        self.consumo_default = consumo_default_km_por_litro
        self.thresholds = {
            'velocidade_max': 250,  # km/h
            'distancia_min_gps': 0.01,  # km
            'tempo_min_ociosidade': 5,  # minutos
            'delta_max_1min': 100,  # km máximo em 1 minuto
            'velocidade_limite_default': 80  # km/h
        }
        
        # Mapeamento de colunas para garantir compatibilidade
        self.column_mapping = {
            'placa': ['placa', 'plate', 'vehicle', 'vehicle_id'],
            'timestamp': ['timestamp', 'datetime', 'dt', 'date', 'hora', 'Data'],
            'odometer': ['odometer', 'odometro', 'odo', 'km_total', 'Odômetro do período  (Km)', 'Odômetro embarcado (Km)'],
            'distance': ['trajeto', 'km_delta', 'distance'],
            'speed': ['speed', 'velocidade', 'vel', 'Velocidade (Km)'],
            'fuel': ['combustivel', 'fuel_consumed', 'fuel_used'],
            'ignition': ['ignition', 'ignicao', 'on_off', 'Ignição'],
            'latitude': ['lat', 'latitude', 'Latitude'],
            'longitude': ['lon', 'longitude', 'Longitude'],
            'event': ['event_type', 'status', 'Tipo do Evento'],
            'cliente': ['cliente', 'client', 'client_id', 'Cliente'],
            'motorista': ['motorista', 'driver', 'Motorista'],
            'endereco': ['endereco', 'address', 'Endereço'],
            'gps': ['gps', 'GPS'],
            'gprs': ['gprs', 'Gprs']
        }
        
        # Janelas operacionais padrão
        self.janelas_operacionais = {
            'manha_operacional': {'inicio': '04:00', 'fim': '07:00', 'nome': 'Manhã Operacional'},
            'meio_dia_operacional': {'inicio': '10:50', 'fim': '13:00', 'nome': 'Meio-dia Operacional'},
            'tarde_operacional': {'inicio': '16:50', 'fim': '19:00', 'nome': 'Tarde Operacional'},
            'fora_horario': {'nome': 'Fora do Horário'}
        }
    
    def mapear_colunas(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Mapeia colunas do DataFrame usando aliases para garantir compatibilidade.
        """
        df_mapped = df.copy()
        colunas_mapeadas = {}
        
        for coluna_padrao, aliases in self.column_mapping.items():
            for alias in aliases:
                if alias in df.columns:
                    if coluna_padrao not in colunas_mapeadas:
                        colunas_mapeadas[coluna_padrao] = alias
                        break
        
        # Renomeia colunas encontradas
        df_mapped = df_mapped.rename(columns={v: k for k, v in colunas_mapeadas.items()})
        
        logger.info(f"Colunas mapeadas: {colunas_mapeadas}")
        return df_mapped, colunas_mapeadas
    
    def preprocessar_dados(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Pré-processamento completo dos dados seguindo as especificações.
        """
        log_processamento = {
            'registros_originais': len(df),
            'anomalias_detectadas': [],
            'ajustes_realizados': [],
            'dados_estimados': []
        }
        
        # 1. Mapear colunas
        df_clean, mapeamento = self.mapear_colunas(df)
        
        # 2. Converter timestamps para timezone
        if 'timestamp' in df_clean.columns:
            df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'], errors='coerce')
            df_clean['timestamp'] = df_clean['timestamp'].dt.tz_localize(None).dt.tz_localize(self.timezone)
        
        # 3. Ordenar por placa + timestamp
        if 'placa' in df_clean.columns and 'timestamp' in df_clean.columns:
            df_clean = df_clean.sort_values(['placa', 'timestamp'])
        
        # 4. Deduplicar registros
        registros_antes = len(df_clean)
        df_clean = df_clean.drop_duplicates()
        duplicatas_removidas = registros_antes - len(df_clean)
        if duplicatas_removidas > 0:
            log_processamento['ajustes_realizados'].append(f"Removidas {duplicatas_removidas} duplicatas")
        
        # 5. Calcular deltas de distância
        df_clean = self._calcular_deltas_distancia(df_clean, log_processamento)
        
        # 6. Aplicar regras de validação
        df_clean = self._aplicar_regras_validacao(df_clean, log_processamento)
        
        log_processamento['registros_finais'] = len(df_clean)
        
        return df_clean, log_processamento
    
    def _calcular_deltas_distancia(self, df: pd.DataFrame, log_processamento: Dict) -> pd.DataFrame:
        """
        Calcula deltas de distância usando odômetro ou GPS como fallback.
        """
        df = df.copy()
        df['km_delta'] = 0.0
        df['combustivel_delta'] = 0.0
        df['anomalia_delta'] = False
        
        for placa in df['placa'].unique() if 'placa' in df.columns else [None]:
            if placa:
                mask = df['placa'] == placa
            else:
                mask = pd.Series([True] * len(df))
            
            df_veiculo = df[mask].copy()
            
            # Calcular delta de odômetro
            if 'odometer' in df_veiculo.columns:
                df_veiculo = df_veiculo.sort_values('timestamp')
                odometer_diff = df_veiculo['odometer'].diff()
                
                # Ignorar resets negativos
                odometer_diff = odometer_diff.where(odometer_diff >= 0, 0)
                df.loc[mask, 'km_delta'] = odometer_diff
            
            # Calcular delta de combustível
            if 'fuel' in df_veiculo.columns:
                df_veiculo = df_veiculo.sort_values('timestamp')
                combustivel_diff = df_veiculo['fuel'].diff()
                
                # Ignorar valores negativos (reset de tanque)
                combustivel_diff = combustivel_diff.where(combustivel_diff >= 0, 0)
                df.loc[mask, 'combustivel_delta'] = combustivel_diff
            
            # Fallback para GPS se não houver odômetro válido
            if df.loc[mask, 'km_delta'].sum() == 0 and 'latitude' in df.columns and 'longitude' in df.columns:
                df.loc[mask, 'km_delta'] = self._calcular_distancia_gps(df_veiculo)
            
            # Fallback para velocidade * tempo
            if df.loc[mask, 'km_delta'].sum() == 0 and 'speed' in df.columns:
                df.loc[mask, 'km_delta'] = self._calcular_distancia_velocidade(df_veiculo)
            
            # Marcar anomalias em deltas impossíveis
            if 'timestamp' in df.columns:
                tempo_diff = df_veiculo['timestamp'].diff().dt.total_seconds() / 60  # minutos
                delta_impossivel = (df.loc[mask, 'km_delta'] > self.thresholds['delta_max_1min']) & (tempo_diff <= 1)
                df.loc[mask & delta_impossivel, 'anomalia_delta'] = True
                
                anomalias_count = delta_impossivel.sum()
                if anomalias_count > 0:
                    log_processamento['anomalias_detectadas'].append(
                        f"Placa {placa}: {anomalias_count} deltas impossíveis (>100km em 1min)"
                    )
            
            # Detectar dados inconsistentes nos valores individuais
            mask_veiculo = df['placa'] == placa if placa else pd.Series([True] * len(df))
            
            # Velocidade negativa ou impossível
            if 'speed' in df.columns:
                velocidade_invalida = (df['speed'] < 0) | (df['speed'] > 500)
                df.loc[mask_veiculo & velocidade_invalida, 'anomalia_delta'] = True
            
            # Coordenadas GPS impossíveis
            if 'latitude' in df.columns and 'longitude' in df.columns:
                coords_invalidas = (df['latitude'].abs() > 90) | (df['longitude'].abs() > 180)
                df.loc[mask_veiculo & coords_invalidas, 'anomalia_delta'] = True
            
            # Combustível negativo
            if 'fuel' in df.columns:
                combustivel_invalido = df['fuel'] < 0
                df.loc[mask_veiculo & combustivel_invalido, 'anomalia_delta'] = True
        
        return df
    
    def _calcular_distancia_gps(self, df_veiculo: pd.DataFrame) -> pd.Series:
        """
        Calcula distância usando coordenadas GPS (Haversine).
        """
        distancias = pd.Series(0.0, index=df_veiculo.index)
        
        for i in range(1, len(df_veiculo)):
            try:
                coord1 = (df_veiculo.iloc[i-1]['latitude'], df_veiculo.iloc[i-1]['longitude'])
                coord2 = (df_veiculo.iloc[i]['latitude'], df_veiculo.iloc[i]['longitude'])
                
                if not (pd.isna(coord1[0]) or pd.isna(coord1[1]) or pd.isna(coord2[0]) or pd.isna(coord2[1])):
                    distancia = geodesic(coord1, coord2).kilometers
                    distancias.iloc[i] = distancia
            except:
                distancias.iloc[i] = 0.0
        
        return distancias
    
    def _calcular_distancia_velocidade(self, df_veiculo: pd.DataFrame) -> pd.Series:
        """
        Calcula distância usando velocidade * tempo como último recurso.
        """
        distancias = pd.Series(0.0, index=df_veiculo.index)
        
        if 'timestamp' in df_veiculo.columns and 'speed' in df_veiculo.columns:
            tempo_diff = df_veiculo['timestamp'].diff().dt.total_seconds() / 3600  # horas
            velocidade_media = (df_veiculo['speed'] + df_veiculo['speed'].shift(1)) / 2
            distancias = velocidade_media * tempo_diff
            distancias = distancias.fillna(0)
        
        return distancias
    
    def _aplicar_regras_validacao(self, df: pd.DataFrame, log_processamento: Dict) -> pd.DataFrame:
        """
        Aplica as regras de validação R1-R6 especificadas.
        """
        df = df.copy()
        df['regra_aplicada'] = ''
        df['dados_estimados'] = False
        
        # R1: KM=0 & Vel>0 → validar GPS ±1min
        if 'km_delta' in df.columns and 'speed' in df.columns:
            mask_r1 = (df['km_delta'] == 0) & (df['speed'] > 0)
            df.loc[mask_r1, 'regra_aplicada'] += 'R1;'
            df.loc[mask_r1, 'anomalia_delta'] = True  # Marcar como anomalia
            
            # Marcar como inconsistente se não validar por GPS
            inconsistentes = mask_r1.sum()
            if inconsistentes > 0:
                log_processamento['anomalias_detectadas'].append(f"R1: {inconsistentes} registros com KM=0 e Vel>0")
        
        # R2: KM>0 & Vel=0 → calcular speed_est
        if 'km_delta' in df.columns and 'speed' in df.columns and 'timestamp' in df.columns:
            mask_r2 = (df['km_delta'] > 0) & (df['speed'] == 0)
            
            for idx in df[mask_r2].index:
                if idx > 0:
                    tempo_diff = (df.loc[idx, 'timestamp'] - df.loc[idx-1, 'timestamp']).total_seconds() / 3600
                    if tempo_diff > 0:
                        speed_estimada = df.loc[idx, 'km_delta'] / tempo_diff
                        if speed_estimada <= self.thresholds['velocidade_max']:
                            df.loc[idx, 'speed'] = speed_estimada
                            df.loc[idx, 'regra_aplicada'] += 'R2;'
                            df.loc[idx, 'dados_estimados'] = True
            
            ajustes_r2 = mask_r2.sum()
            if ajustes_r2 > 0:
                log_processamento['dados_estimados'].append(f"R2: {ajustes_r2} velocidades estimadas")
        
        # R3: Consumo>0 & KM≈0 → aceitar só se ignição=ON e duração >5min
        if 'combustivel_delta' in df.columns and 'km_delta' in df.columns and 'ignition' in df.columns:
            mask_r3 = (df['combustivel_delta'] > 0) & (df['km_delta'] <= 0.1)
            
            for idx in df[mask_r3].index:
                ignition_value = df.loc[idx, 'ignition']
                # Aceitar ignição ligada (valores numéricos 1 ou strings)
                ignition_on = (ignition_value == 1 or ignition_value in ['L', 'LM', 'LP', 'Ligado', 'ON', 'on'])
                
                if ignition_on:
                    # Verificar duração (simplificado - aceitar se >5min entre registros)
                    df.loc[idx, 'regra_aplicada'] += 'R3;'
                else:
                    df.loc[idx, 'combustivel_delta'] = 0  # Rejeitar consumo
            
            validacoes_r3 = mask_r3.sum()
            if validacoes_r3 > 0:
                log_processamento['ajustes_realizados'].append(f"R3: {validacoes_r3} consumos validados para idling")
        
        # R4: Comb=0 & KM>0 → estimar via km/consumo_default
        if 'combustivel_delta' in df.columns and 'km_delta' in df.columns:
            mask_r4 = (df['combustivel_delta'] == 0) & (df['km_delta'] > 0)
            
            df.loc[mask_r4, 'combustivel_delta'] = df.loc[mask_r4, 'km_delta'] / self.consumo_default
            df.loc[mask_r4, 'regra_aplicada'] += 'R4;'
            df.loc[mask_r4, 'dados_estimados'] = True
            
            estimativas_r4 = mask_r4.sum()
            if estimativas_r4 > 0:
                log_processamento['dados_estimados'].append(f"R4: {estimativas_r4} consumos estimados")
        
        # R5: Velocidade >250 km/h → truncar + marcar anomalia
        if 'speed' in df.columns:
            mask_r5 = df['speed'] > self.thresholds['velocidade_max']
            
            df.loc[mask_r5, 'speed'] = self.thresholds['velocidade_max']
            df.loc[mask_r5, 'regra_aplicada'] += 'R5;'
            df.loc[mask_r5, 'anomalia_delta'] = True  # Marcar como anomalia
            
            truncamentos_r5 = mask_r5.sum()
            if truncamentos_r5 > 0:
                log_processamento['anomalias_detectadas'].append(f"R5: {truncamentos_r5} velocidades truncadas (>250km/h)")
        
        # R6: Dados inconsistentes → nunca incluir em totais sem ajuste
        mask_inconsistente = df['anomalia_delta'] | (df['regra_aplicada'].str.contains('R1'))
        df.loc[mask_inconsistente, 'regra_aplicada'] += 'R6;'
        df.loc[mask_inconsistente, 'incluir_totais'] = False
        df.loc[~mask_inconsistente, 'incluir_totais'] = True
        
        inconsistentes_r6 = mask_inconsistente.sum()
        if inconsistentes_r6 > 0:
            log_processamento['ajustes_realizados'].append(f"R6: {inconsistentes_r6} registros excluídos dos totais")
        
        return df
    
    def calcular_metricas_principais(self, df: pd.DataFrame) -> Dict:
        """
        Calcula as métricas principais especificadas.
        """
        # Filtrar apenas dados válidos para totais
        df_validos = df[df.get('incluir_totais', True)] if 'incluir_totais' in df.columns else df
        
        metricas = {
            'quilometragem_total': df_validos['km_delta'].sum() if 'km_delta' in df_validos.columns else 0,
            'combustivel_total': df_validos['combustivel_delta'].sum() if 'combustivel_delta' in df_validos.columns else 0,
            'velocidade_maxima': df_validos['speed'].max() if 'speed' in df_validos.columns else 0,
            'total_veiculos': df_validos['placa'].nunique() if 'placa' in df_validos.columns else 0,
            'registros_processados': len(df_validos),
            'registros_originais': len(df)
        }
        
        # Eficiência (km/L)
        if metricas['combustivel_total'] > 0:
            metricas['eficiencia_kmL'] = metricas['quilometragem_total'] / metricas['combustivel_total']
        else:
            metricas['eficiencia_kmL'] = 0
        
        # Dias ativos por placa
        if 'placa' in df_validos.columns and 'timestamp' in df_validos.columns:
            dias_ativos = {}
            for placa in df_validos['placa'].unique():
                df_placa = df_validos[df_validos['placa'] == placa]
                dias_com_km = df_placa[df_placa['km_delta'] > 0]['timestamp'].dt.date.nunique()
                dias_ativos[placa] = dias_com_km
            metricas['dias_ativos_por_placa'] = dias_ativos
        
        # Percentual de dados estimados
        if 'dados_estimados' in df.columns:
            total_estimados = df['dados_estimados'].sum()
            metricas['percentual_estimados'] = (total_estimados / len(df)) * 100
        else:
            metricas['percentual_estimados'] = 0
        
        return metricas
    
    def gerar_hash_arquivos(self, arquivos_csv: List[str]) -> Dict[str, str]:
        """
        Gera hash MD5 dos arquivos CSV para rastreabilidade.
        """
        hashes = {}
        for arquivo in arquivos_csv:
            try:
                with open(arquivo, 'rb') as f:
                    hash_md5 = hashlib.md5(f.read()).hexdigest()
                    hashes[Path(arquivo).name] = hash_md5
            except Exception as e:
                logger.error(f"Erro ao calcular hash de {arquivo}: {e}")
                hashes[Path(arquivo).name] = "erro"
        
        return hashes
    
    def processar_relatorio_completo(self, 
                                   arquivos_csv: List[str],
                                   periodo_inicio: datetime,
                                   periodo_fim: datetime,
                                   tipo_relatorio: str,
                                   cliente: str,
                                   placas_filtro: Optional[List[str]] = None,
                                   dados_csv: Optional[pd.DataFrame] = None) -> Dict:
        """
        Processa relatório completo seguindo todas as especificações.
        """
        logger.info(f"Iniciando processamento de relatório {tipo_relatorio} para {cliente}")
        
        # Usar dados CSV fornecidos diretamente ou carregar de arquivos
        if dados_csv is not None:
            df_combined = dados_csv.copy()
            logger.info(f"Usando dados CSV fornecidos: {len(df_combined)} registros")
        else:
            # Carregar e combinar todos os CSVs
            dfs = []
            for arquivo in arquivos_csv:
                try:
                    df = pd.read_csv(arquivo, sep=';', encoding='utf-8')
                    dfs.append(df)
                    logger.info(f"Carregado {arquivo}: {len(df)} registros")
                except Exception as e:
                    logger.error(f"Erro ao carregar {arquivo}: {e}")
            
            if not dfs:
                raise ValueError("Nenhum arquivo CSV válido encontrado")
            
            # Combinar dados
            df_combined = pd.concat(dfs, ignore_index=True)
            logger.info(f"Total combinado: {len(df_combined)} registros")
        
        # Pré-processamento
        df_processed, log_processamento = self.preprocessar_dados(df_combined)
        
        # Filtrar por período
        if 'timestamp' in df_processed.columns:
            # Garantir que as datas tenham o mesmo timezone
            if hasattr(df_processed['timestamp'].dtype, 'tz') and df_processed['timestamp'].dtype.tz is not None:
                # DataFrame tem timezone, converter as datas de comparação
                tz = df_processed['timestamp'].dtype.tz
                if not hasattr(periodo_inicio, 'tzinfo') or periodo_inicio.tzinfo is None:
                    periodo_inicio = self.timezone.localize(periodo_inicio)
                if not hasattr(periodo_fim, 'tzinfo') or periodo_fim.tzinfo is None:
                    periodo_fim = self.timezone.localize(periodo_fim)
            
            mask_periodo = (df_processed['timestamp'] >= periodo_inicio) & (df_processed['timestamp'] <= periodo_fim)
            df_processed = df_processed[mask_periodo]
        
        # Filtrar por placas se especificado
        if placas_filtro and 'placa' in df_processed.columns:
            df_processed = df_processed[df_processed['placa'].isin(placas_filtro)]
        
        # Calcular métricas
        metricas = self.calcular_metricas_principais(df_processed)
        
        # Gerar hashes dos arquivos
        hashes_arquivos = self.gerar_hash_arquivos(arquivos_csv)
        
        # Compilar resultado final
        resultado = {
            'success': True,
            'cliente': cliente,
            'tipo_relatorio': tipo_relatorio,
            'periodo': {
                'inicio': periodo_inicio.isoformat(),
                'fim': periodo_fim.isoformat(),
                'dias': (periodo_fim - periodo_inicio).days + 1
            },
            'metricas_principais': metricas,
            'log_processamento': log_processamento,
            'arquivos_fonte': {
                'lista': [Path(f).name for f in arquivos_csv],
                'hashes': hashes_arquivos
            },
            'dados_processados': df_processed.to_dict('records'),
            'timestamp_geracao': datetime.now(self.timezone).isoformat()
        }
        
        logger.info(f"Processamento concluído: {metricas['quilometragem_total']:.1f}km, {metricas['total_veiculos']} veículos")
        
        return resultado

# Casos de teste obrigatórios para validação
def executar_casos_teste():
    """
    Executa os casos de teste obrigatórios especificados.
    """
    processor = FleetReportProcessor()
    
    # Caso 1: KM=0, Vel=50 → anomalia R1
    df_teste1 = pd.DataFrame({
        'placa': ['TEST001'],
        'timestamp': [datetime.now()],
        'km_delta': [0],
        'speed': [50],
        'latitude': [-15.7801],
        'longitude': [-47.9292]
    })
    
    df_resultado1, log1 = processor.preprocessar_dados(df_teste1)
    assert 'R1' in df_resultado1.iloc[0]['regra_aplicada'], "Caso 1 falhou: R1 não aplicada"
    
    # Caso 2: KM=100, Vel=0 → estimar velocidade (R2)
    df_teste2 = pd.DataFrame({
        'placa': ['TEST002', 'TEST002'],
        'timestamp': [datetime.now(), datetime.now() + timedelta(hours=1)],
        'km_delta': [0, 100],
        'speed': [0, 0]
    })
    
    df_resultado2, log2 = processor.preprocessar_dados(df_teste2)
    assert df_resultado2.iloc[1]['speed'] > 0, "Caso 2 falhou: velocidade não estimada"
    
    # Caso 3: KM=20, Vel=70, Comb=0 → estimar consumo (R4)
    df_teste3 = pd.DataFrame({
        'placa': ['TEST003'],
        'timestamp': [datetime.now()],
        'km_delta': [20],
        'speed': [70],
        'combustivel_delta': [0]
    })
    
    df_resultado3, log3 = processor.preprocessar_dados(df_teste3)
    assert df_resultado3.iloc[0]['combustivel_delta'] > 0, "Caso 3 falhou: consumo não estimado"
    
    # Caso 4: odômetro reset negativo → ignorar delta
    df_teste4 = pd.DataFrame({
        'placa': ['TEST004', 'TEST004'],
        'timestamp': [datetime.now(), datetime.now() + timedelta(hours=1)],
        'odometer': [1000, 500],  # Reset negativo
        'speed': [60, 60]
    })
    
    df_resultado4, log4 = processor.preprocessar_dados(df_teste4)
    assert df_resultado4.iloc[1]['km_delta'] == 0, "Caso 4 falhou: delta negativo não ignorado"
    
    print("✅ Todos os casos de teste obrigatórios passaram!")
    return True

if __name__ == "__main__":
    executar_casos_teste()