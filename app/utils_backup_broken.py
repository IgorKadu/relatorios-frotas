    def _looks_like_numeric(self, values: pd.Series) -> bool:
        """Verifica se os valores parecem numéricos"""
        try:
            numeric_count = pd.to_numeric(values, errors='coerce').notna().sum()
            return numeric_count / len(values) > 0.8  # 80% dos valores são numéricos
        except:
            return False

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
            'anomalies_detected': []
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
        
        # 5. Se total_km > 0 e max_speed_raw == 0 → recalcule max_speed
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
        if metrics['total_km_odometer'] > 0 and metrics['total_km_haversine'] > 0:
            # Se ambos estiverem disponíveis, usar o odômetro como principal
            metrics['total_km'] = metrics['total_km_odometer']
            metrics['distance_source'] = 'odometer'
        elif metrics['total_km_odometer'] > 0:
            # Apenas odômetro disponível
            metrics['total_km'] = metrics['total_km_odometer']
            metrics['distance_source'] = 'odometer_only'
        elif metrics['total_km_haversine'] > 0:
            # Apenas haversine disponível
            metrics['total_km'] = metrics['total_km_haversine']
            metrics['distance_source'] = 'haversine_only'
        else:
            # Nenhum disponível
            metrics['total_km'] = 0
            metrics['distance_source'] = 'none'
        
        # Velocidade máxima (por veículo por período)
        if 'speed' in df.columns and len(df) > 0:
            valid_speeds = df['speed'].notna()
            if valid_speeds.sum() > 0:
                max_speed_raw = df.loc[valid_speeds, 'speed'].max()
                metrics['max_speed_raw'] = max_speed_raw
            else:
                metrics['max_speed_raw'] = 0
        else:
            metrics['max_speed_raw'] = 0
        
        # Se max_speed_raw == 0 e total_km > 0 → recalcular max_speed
        if metrics['total_km'] > 0 and metrics['max_speed_raw'] == 0:
            max_speed_recalculated = metrics['total_km'] / (len(df) - 1)  # Assume 1 ponto por segundo
            metrics['max_speed'] = max_speed_recalculated
            metrics['max_speed_source'] = 'recalculated'
        else:
            metrics['max_speed'] = metrics['max_speed_raw']
            metrics['max_speed_source'] = 'raw'
        
        return metrics
    
    def detect_and_store_trips(self, df: pd.DataFrame, session: Session) -> List[Dict]:
        """
        Detecta viagens e armazena no banco de dados
        """
        trips = []
        trip_start = None
        trip_end = None
        trip_speeds = []
        trip_distances = []
        
        for i in range(1, len(df)):
            lat1, lon1 = df.iloc[i-1]['lat'], df.iloc[i-1]['lon']
            lat2, lon2 = df.iloc[i]['lat'], df.iloc[i]['lon']
            timestamp1 = pd.to_datetime(df.iloc[i-1]['timestamp'])
            timestamp2 = pd.to_datetime(df.iloc[i]['timestamp'])
            speed = df.iloc[i]['speed']
            
            if pd.notna(lat1) and pd.notna(lon1) and pd.notna(lat2) and pd.notna(lon2) and pd.notna(timestamp1) and pd.notna(timestamp2) and pd.notna(speed):
                duration = (timestamp2 - timestamp1).total_seconds()
                distance = haversine(lat1, lon1, lat2, lon2)
                
                if speed > self.trip_speed_threshold and duration >= self.trip_min_duration_s:
                    if trip_start is None:
                        trip_start = timestamp1
                    trip_end = timestamp2
                    trip_speeds.append(speed)
                    trip_distances.append(distance)
                else:
                    if trip_start is not None:
                        trip = {
                            'start_time': trip_start,
                            'end_time': trip_end,
                            'total_distance': sum(trip_distances),
                            'average_speed': sum(trip_speeds) / len(trip_speeds),
                            'max_speed': max(trip_speeds)
                        }
                        trips.append(trip)
                        trip_start = None
                        trip_end = None
                        trip_speeds = []
                        trip_distances = []
        
        # Adicionar a última viagem se houver
        if trip_start is not None:
            trip = {
                'start_time': trip_start,
                'end_time': trip_end,
                'total_distance': sum(trip_distances),
                'average_speed': sum(trip_speeds) / len(trip_speeds),
                'max_speed': max(trip_speeds)
            }
            trips.append(trip)
        
        # Armazenar viagens no banco de dados
        for trip in trips:
            start_time = trip['start_time']
            end_time = trip['end_time']
            total_distance = trip['total_distance']
            average_speed = trip['average_speed']
            max_speed = trip['max_speed']
            
            posicao_inicial = df[df['timestamp'] == start_time]
            posicao_final = df[df['timestamp'] == end_time]
            
            if not posicao_inicial.empty and not posicao_final.empty:
                lat_inicial = posicao_inicial['lat'].iloc[0]
                lon_inicial = posicao_inicial['lon'].iloc[0]
                lat_final = posicao_final['lat'].iloc[0]
                lon_final = posicao_final['lon'].iloc[0]
                
                posicao_historica = PosicaoHistorica(
                    lat_inicial=lat_inicial,
                    lon_inicial=lon_inicial,
                    lat_final=lat_final,
                    lon_final=lon_final,
                    timestamp_inicial=start_time,
                    timestamp_final=end_time,
                    distancia_total=total_distance,
                    velocidade_media=average_speed,
                    velocidade_maxima=max_speed
                )
                session.add(posicao_historica)
                session.commit()
        
        return trips
    
    def generate_summary(self, df: pd.DataFrame) -> Dict:
        """
        Gera um resumo das métricas calculadas
        """
        summary = {}
        summary['total_km'] = self.calculate_distance_and_speed(df)['total_km']
        summary['max_speed'] = self.calculate_distance_and_speed(df)['max_speed']
        summary['trip_count'] = len(self.detect_and_store_trips(df, get_session()))
        
        return summary