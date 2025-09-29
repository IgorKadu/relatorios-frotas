"""
Gerador de PDF Profissional para Relatórios de Frota
Implementa a estrutura completa especificada com layout profissional.
"""

from reportlab.lib.pagesizes import A4, letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch, cm
from reportlab.lib.colors import Color, black, white, blue, red, green, grey
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image
from reportlab.platypus.flowables import HRFlowable
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT, TA_JUSTIFY
from reportlab.graphics.shapes import Drawing, Rect, String
from reportlab.graphics.charts.linecharts import HorizontalLineChart
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.charts.piecharts import Pie
from reportlab.graphics.widgets.markers import makeMarker
from reportlab.graphics import renderPDF

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import matplotlib.pyplot as plt
import seaborn as sns
import io
import base64
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class ProfessionalPDFGenerator:
    """
    Gerador de PDF profissional seguindo especificações técnicas rigorosas.
    """
    
    def __init__(self):
        self.page_width, self.page_height = A4
        self.margin = 2*cm
        self.content_width = self.page_width - 2*self.margin
        
        # Cores corporativas
        self.colors = {
            'primary': Color(0.2, 0.4, 0.8),      # Azul principal
            'secondary': Color(0.8, 0.9, 1.0),    # Azul claro
            'accent': Color(0.9, 0.6, 0.1),       # Laranja
            'success': Color(0.2, 0.7, 0.3),      # Verde
            'warning': Color(0.9, 0.7, 0.1),      # Amarelo
            'danger': Color(0.8, 0.2, 0.2),       # Vermelho
            'text': Color(0.2, 0.2, 0.2),         # Cinza escuro
            'light_grey': Color(0.95, 0.95, 0.95) # Cinza claro
        }
        
        # Estilos de texto
        self.styles = getSampleStyleSheet()
        self._setup_custom_styles()
    
    def _setup_custom_styles(self):
        """Configura estilos personalizados para o relatório."""
        
        # Título principal
        self.styles.add(ParagraphStyle(
            name='MainTitle',
            parent=self.styles['Title'],
            fontSize=24,
            spaceAfter=30,
            textColor=self.colors['primary'],
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        ))
        
        # Subtítulo
        self.styles.add(ParagraphStyle(
            name='Subtitle',
            parent=self.styles['Normal'],
            fontSize=16,
            spaceAfter=20,
            textColor=self.colors['text'],
            alignment=TA_CENTER,
            fontName='Helvetica'
        ))
        
        # Cabeçalho de seção
        self.styles.add(ParagraphStyle(
            name='SectionHeader',
            parent=self.styles['Heading1'],
            fontSize=18,
            spaceAfter=15,
            spaceBefore=25,
            textColor=self.colors['primary'],
            fontName='Helvetica-Bold',
            borderWidth=0,
            borderColor=self.colors['primary'],
            borderPadding=5
        ))
        
        # Cabeçalho de subseção
        self.styles.add(ParagraphStyle(
            name='SubsectionHeader',
            parent=self.styles['Heading2'],
            fontSize=14,
            spaceAfter=10,
            spaceBefore=15,
            textColor=self.colors['text'],
            fontName='Helvetica-Bold'
        ))
        
        # Texto de KPI
        self.styles.add(ParagraphStyle(
            name='KPIValue',
            parent=self.styles['Normal'],
            fontSize=20,
            textColor=self.colors['primary'],
            fontName='Helvetica-Bold',
            alignment=TA_CENTER
        ))
        
        # Texto de KPI label
        self.styles.add(ParagraphStyle(
            name='KPILabel',
            parent=self.styles['Normal'],
            fontSize=10,
            textColor=self.colors['text'],
            fontName='Helvetica',
            alignment=TA_CENTER
        ))
        
        # Rodapé
        self.styles.add(ParagraphStyle(
            name='Footer',
            parent=self.styles['Normal'],
            fontSize=8,
            textColor=colors.grey,
            fontName='Helvetica',
            alignment=TA_CENTER
        ))
    
    def gerar_relatorio_completo(self, dados_processados: Dict, output_path: str) -> str:
        """
        Gera o relatório PDF completo seguindo a estrutura especificada.
        """
        logger.info(f"Gerando relatório PDF: {output_path}")
        
        # Criar documento
        doc = SimpleDocTemplate(
            output_path,
            pagesize=A4,
            rightMargin=self.margin,
            leftMargin=self.margin,
            topMargin=self.margin,
            bottomMargin=self.margin
        )
        
        # Construir conteúdo
        story = []
        
        # 1. Cabeçalho
        self._add_header(story, dados_processados)
        
        # 2. Dados Gerais do Veículo
        self._add_general_data(story, dados_processados)
        
        # 3. Desempenho do Veículo
        self._add_vehicle_performance(story, dados_processados)
        
        # 4. Desempenho Diário por Horário
        self._add_daily_performance(story, dados_processados)
        
        # 5. Consolidados por Período
        self._add_consolidated_analysis(story, dados_processados)
        
        # 6. Seção de Anomalias
        self._add_anomalies_section(story, dados_processados)
        
        # 7. Rodapé
        self._add_footer(story, dados_processados)
        
        # Gerar PDF
        doc.build(story)
        logger.info(f"Relatório PDF gerado com sucesso: {output_path}")
        
        return output_path
    
    def _add_header(self, story: List, dados: Dict):
        """Adiciona cabeçalho com logo, título e KPIs principais."""
        
        # Título principal
        titulo = f"Relatório de Frota – {dados['cliente']}"
        if 'placas_filtro' in dados and dados['placas_filtro']:
            titulo += f" – {', '.join(dados['placas_filtro'])}"
        
        story.append(Paragraph(titulo, self.styles['MainTitle']))
        
        # Subtítulo com tipo e período
        periodo_inicio = datetime.fromisoformat(dados['periodo']['inicio']).strftime('%d/%m/%Y')
        periodo_fim = datetime.fromisoformat(dados['periodo']['fim']).strftime('%d/%m/%Y')
        
        subtitulo = f"Dados {dados['tipo_relatorio'].title()}<br/>"
        subtitulo += f"Período: {periodo_inicio} a {periodo_fim} ({dados['periodo']['dias']} dias)"
        
        story.append(Paragraph(subtitulo, self.styles['Subtitle']))
        story.append(Spacer(1, 20))
        
        # KPIs principais em cards
        metricas = dados['metricas_principais']
        
        kpi_data = [
            ['Total de Veículos', f"{metricas['total_veiculos']}", 'veículos'],
            ['Quilometragem Total', f"{metricas['quilometragem_total']:.1f}", 'km'],
            ['Velocidade Máxima', f"{metricas['velocidade_maxima']:.0f}", 'km/h'],
            ['Eficiência Média', f"{metricas['eficiencia_kmL']:.1f}", 'km/L']
        ]
        
        # Criar tabela de KPIs
        kpi_table_data = []
        for i in range(0, len(kpi_data), 2):
            row = []
            for j in range(2):
                if i + j < len(kpi_data):
                    label, value, unit = kpi_data[i + j]
                    cell_content = f"<para align=center><b>{value}</b> {unit}<br/><font size=8>{label}</font></para>"
                    row.append(Paragraph(cell_content, self.styles['Normal']))
                else:
                    row.append("")
            kpi_table_data.append(row)
        
        kpi_table = Table(kpi_table_data, colWidths=[self.content_width/2]*2)
        kpi_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), self.colors['secondary']),
            ('BORDER', (0, 0), (-1, -1), 1, self.colors['primary']),
            ('PADDING', (0, 0), (-1, -1), 15),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))
        
        story.append(kpi_table)
        story.append(Spacer(1, 30))
    
    def _add_general_data(self, story: List, dados: Dict):
        """Adiciona seção de dados gerais do veículo."""
        
        story.append(Paragraph("2. Dados Gerais do Veículo", self.styles['SectionHeader']))
        
        metricas = dados['metricas_principais']
        log_proc = dados['log_processamento']
        
        # Tabela de métricas gerais
        general_data = [
            ['Métrica', 'Valor'],
            ['Total de Veículos', f"{metricas['total_veiculos']}"],
            ['Quilometragem Total', f"{metricas['quilometragem_total']:.1f} km"],
            ['Combustível Total Estimado', f"{metricas['combustivel_total']:.1f} L"],
            ['Média de KM por Veículo', f"{metricas['quilometragem_total']/max(metricas['total_veiculos'], 1):.1f} km"],
            ['Velocidade Máxima da Frota', f"{metricas['velocidade_maxima']:.0f} km/h"],
            ['Consumo Médio Esperado', f"{metricas['eficiencia_kmL']:.1f} km/L"],
            ['Registros Processados', f"{metricas['registros_processados']:,}"],
            ['Registros Originais', f"{metricas['registros_originais']:,}"]
        ]
        
        # Adicionar aviso se muitos dados estimados
        if metricas['percentual_estimados'] > 30:
            general_data.append(['⚠️ Dados Estimados', f"{metricas['percentual_estimados']:.1f}% (Alto)"])
        else:
            general_data.append(['Dados Estimados', f"{metricas['percentual_estimados']:.1f}%"])
        
        table = Table(general_data, colWidths=[self.content_width*0.6, self.content_width*0.4])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), self.colors['primary']),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BACKGROUND', (0, 1), (-1, -1), colors.white),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('PADDING', (0, 0), (-1, -1), 8),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))
        
        story.append(table)
        story.append(Spacer(1, 20))
    
    def _add_vehicle_performance(self, story: List, dados: Dict):
        """Adiciona tabela de desempenho por veículo."""
        
        story.append(Paragraph("3. Desempenho do Veículo", self.styles['SectionHeader']))
        
        # Processar dados por veículo
        df = pd.DataFrame(dados['dados_processados'])
        
        if 'placa' in df.columns:
            vehicle_data = [['Placa', 'KM', 'Vel.Máx', 'Combustível', 'Eficiência', 'Dias Ativos', 'Observações']]
            
            for placa in df['placa'].unique():
                df_veiculo = df[df['placa'] == placa]
                df_validos = df_veiculo[df_veiculo.get('incluir_totais', True)]
                
                km_total = df_validos['km_delta'].sum() if 'km_delta' in df_validos.columns else 0
                vel_max = df_validos['speed'].max() if 'speed' in df_validos.columns else 0
                combustivel = df_validos['combustivel_delta'].sum() if 'combustivel_delta' in df_validos.columns else 0
                eficiencia = km_total / combustivel if combustivel > 0 else 0
                
                # Dias ativos
                if 'timestamp' in df_validos.columns:
                    dias_ativos = df_validos[df_validos['km_delta'] > 0]['timestamp'].dt.date.nunique()
                else:
                    dias_ativos = 0
                
                # Observações
                observacoes = []
                if df_veiculo['dados_estimados'].any() if 'dados_estimados' in df_veiculo.columns else False:
                    observacoes.append("Dados estimados")
                if df_veiculo['anomalia_delta'].any() if 'anomalia_delta' in df_veiculo.columns else False:
                    observacoes.append("Anomalias detectadas")
                
                vehicle_data.append([
                    placa,
                    f"{km_total:.1f}",
                    f"{vel_max:.0f}",
                    f"{combustivel:.1f}",
                    f"{eficiencia:.1f}",
                    str(dias_ativos),
                    "; ".join(observacoes) if observacoes else "Normal"
                ])
            
            table = Table(vehicle_data, colWidths=[
                self.content_width*0.15,  # Placa
                self.content_width*0.12,  # KM
                self.content_width*0.12,  # Vel.Máx
                self.content_width*0.12,  # Combustível
                self.content_width*0.12,  # Eficiência
                self.content_width*0.12,  # Dias Ativos
                self.content_width*0.25   # Observações
            ])
            
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), self.colors['primary']),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('PADDING', (0, 0), (-1, -1), 6),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ]))
            
            story.append(table)
        else:
            story.append(Paragraph("Dados de veículos não disponíveis.", self.styles['Normal']))
        
        story.append(Spacer(1, 20))
    
    def _add_daily_performance(self, story: List, dados: Dict):
        """Adiciona análise de desempenho diário por horário."""
        
        story.append(Paragraph("4. Desempenho Diário por Horário", self.styles['SectionHeader']))
        
        df = pd.DataFrame(dados['dados_processados'])
        
        if 'timestamp' in df.columns:
            # Análise por janelas operacionais
            janelas = {
                'Manhã Operacional (04:00-07:00)': (4, 7),
                'Meio-dia Operacional (10:50-13:00)': (10.83, 13),
                'Tarde Operacional (16:50-19:00)': (16.83, 19),
                'Fora do Horário': None
            }
            
            df['hora'] = pd.to_datetime(df['timestamp']).dt.hour + pd.to_datetime(df['timestamp']).dt.minute/60
            
            janela_data = [['Janela Operacional', 'KM Total', 'Registros', 'Vel. Média', 'Tempo Ativo']]
            
            for janela_nome, horario in janelas.items():
                if horario:
                    inicio, fim = horario
                    mask = (df['hora'] >= inicio) & (df['hora'] <= fim)
                else:
                    # Fora do horário = tudo que não está nas outras janelas
                    mask_manha = (df['hora'] >= 4) & (df['hora'] <= 7)
                    mask_meio = (df['hora'] >= 10.83) & (df['hora'] <= 13)
                    mask_tarde = (df['hora'] >= 16.83) & (df['hora'] <= 19)
                    mask = ~(mask_manha | mask_meio | mask_tarde)
                
                df_janela = df[mask]
                
                km_total = df_janela['km_delta'].sum() if 'km_delta' in df_janela.columns else 0
                registros = len(df_janela)
                vel_media = df_janela['speed'].mean() if 'speed' in df_janela.columns and len(df_janela) > 0 else 0
                
                # Tempo ativo (registros com movimento)
                if 'km_delta' in df_janela.columns:
                    tempo_ativo = len(df_janela[df_janela['km_delta'] > 0])
                else:
                    tempo_ativo = 0
                
                janela_data.append([
                    janela_nome,
                    f"{km_total:.1f} km",
                    f"{registros:,}",
                    f"{vel_media:.1f} km/h",
                    f"{tempo_ativo} reg."
                ])
            
            table = Table(janela_data, colWidths=[
                self.content_width*0.35,  # Janela
                self.content_width*0.15,  # KM
                self.content_width*0.15,  # Registros
                self.content_width*0.15,  # Vel. Média
                self.content_width*0.20   # Tempo Ativo
            ])
            
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), self.colors['primary']),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('PADDING', (0, 0), (-1, -1), 8),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ]))
            
            story.append(table)
            
            # Análise de final de semana
            story.append(Spacer(1, 15))
            story.append(Paragraph("Final de Semana", self.styles['SubsectionHeader']))
            
            df['dia_semana'] = pd.to_datetime(df['timestamp']).dt.dayofweek
            df_weekend = df[df['dia_semana'].isin([5, 6])]  # Sábado e Domingo
            
            if len(df_weekend) > 0:
                km_weekend = df_weekend['km_delta'].sum() if 'km_delta' in df_weekend.columns else 0
                registros_weekend = len(df_weekend)
                
                weekend_text = f"Atividade no final de semana: {km_weekend:.1f} km em {registros_weekend:,} registros."
                story.append(Paragraph(weekend_text, self.styles['Normal']))
            else:
                story.append(Paragraph("Nenhuma atividade registrada no final de semana.", self.styles['Normal']))
        
        story.append(Spacer(1, 20))
    
    def _add_consolidated_analysis(self, story: List, dados: Dict):
        """Adiciona análise consolidada por período."""
        
        story.append(Paragraph("5. Consolidados por Período", self.styles['SectionHeader']))
        
        df = pd.DataFrame(dados['dados_processados'])
        
        if 'timestamp' in df.columns:
            # Análise por semana ISO
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['semana_iso'] = df['timestamp'].dt.isocalendar().week
            df['ano'] = df['timestamp'].dt.year
            
            semana_data = [['Semana ISO', 'KM Total', 'Vel. Máxima', 'Registros', 'Veículos Ativos']]
            
            for (ano, semana) in df.groupby(['ano', 'semana_iso']).groups.keys():
                df_semana = df[(df['ano'] == ano) & (df['semana_iso'] == semana)]
                
                km_total = df_semana['km_delta'].sum() if 'km_delta' in df_semana.columns else 0
                vel_max = df_semana['speed'].max() if 'speed' in df_semana.columns else 0
                registros = len(df_semana)
                veiculos_ativos = df_semana['placa'].nunique() if 'placa' in df_semana.columns else 0
                
                semana_data.append([
                    f"{ano}-W{semana:02d}",
                    f"{km_total:.1f} km",
                    f"{vel_max:.0f} km/h",
                    f"{registros:,}",
                    str(veiculos_ativos)
                ])
            
            table = Table(semana_data, colWidths=[
                self.content_width*0.20,  # Semana
                self.content_width*0.20,  # KM
                self.content_width*0.20,  # Vel. Máxima
                self.content_width*0.20,  # Registros
                self.content_width*0.20   # Veículos
            ])
            
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), self.colors['primary']),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('PADDING', (0, 0), (-1, -1), 8),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ]))
            
            story.append(table)
        
        story.append(Spacer(1, 20))
    
    def _add_anomalies_section(self, story: List, dados: Dict):
        """Adiciona seção de anomalias detectadas."""
        
        story.append(Paragraph("6. Seção de Anomalias", self.styles['SectionHeader']))
        
        log_proc = dados['log_processamento']
        df = pd.DataFrame(dados['dados_processados'])
        
        # Tabela de anomalias
        anomaly_data = [['Placa', 'Data/Hora', 'Tipo de Anomalia', 'Valores', 'Regra Aplicada', 'Nível de Confiança']]
        
        # Processar anomalias dos dados
        if 'regra_aplicada' in df.columns:
            df_anomalias = df[df['regra_aplicada'].str.len() > 0]
            
            for _, row in df_anomalias.iterrows():
                placa = row.get('placa', 'N/A')
                timestamp = row.get('timestamp', 'N/A')
                if timestamp != 'N/A':
                    timestamp = pd.to_datetime(timestamp).strftime('%d/%m/%Y %H:%M')
                
                regras = row['regra_aplicada'].split(';')
                regras = [r for r in regras if r]
                
                tipo_anomalia = []
                if 'R1' in regras:
                    tipo_anomalia.append("KM=0 & Vel>0")
                if 'R2' in regras:
                    tipo_anomalia.append("Velocidade estimada")
                if 'R3' in regras:
                    tipo_anomalia.append("Consumo em idling")
                if 'R4' in regras:
                    tipo_anomalia.append("Consumo estimado")
                if 'R5' in regras:
                    tipo_anomalia.append("Velocidade truncada")
                
                valores = f"KM: {row.get('km_delta', 0):.1f}, Vel: {row.get('speed', 0):.0f}"
                nivel_confianca = "Baixa" if row.get('dados_estimados', False) else "Alta"
                
                anomaly_data.append([
                    placa,
                    timestamp,
                    "; ".join(tipo_anomalia),
                    valores,
                    "; ".join(regras),
                    nivel_confianca
                ])
        
        # Se não há anomalias específicas, mostrar resumo do log
        if len(anomaly_data) == 1:
            for anomalia in log_proc.get('anomalias_detectadas', []):
                anomaly_data.append([
                    "Múltiplas",
                    "Período completo",
                    anomalia,
                    "Vários",
                    "Sistema",
                    "Média"
                ])
        
        if len(anomaly_data) > 1:
            table = Table(anomaly_data, colWidths=[
                self.content_width*0.12,  # Placa
                self.content_width*0.18,  # Data/Hora
                self.content_width*0.25,  # Tipo
                self.content_width*0.15,  # Valores
                self.content_width*0.15,  # Regra
                self.content_width*0.15   # Confiança
            ])
            
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), self.colors['danger']),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('PADDING', (0, 0), (-1, -1), 6),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ]))
            
            story.append(table)
        else:
            story.append(Paragraph("✅ Nenhuma anomalia significativa detectada nos dados.", self.styles['Normal']))
        
        story.append(Spacer(1, 20))
    
    def _add_footer(self, story: List, dados: Dict):
        """Adiciona rodapé com informações de geração."""
        
        story.append(PageBreak())
        
        # Informações de geração
        timestamp_geracao = datetime.fromisoformat(dados['timestamp_geracao']).strftime('%d/%m/%Y %H:%M')
        footer_text = f"Relatório gerado em {timestamp_geracao} – Fonte: CSVs"
        
        story.append(Paragraph(footer_text, self.styles['Footer']))
        story.append(Spacer(1, 10))
        
        # Lista de arquivos fonte
        story.append(Paragraph("Arquivos CSV utilizados:", self.styles['SubsectionHeader']))
        
        arquivos_fonte = dados['arquivos_fonte']
        
        arquivo_data = [['Arquivo', 'Hash MD5']]
        for arquivo in arquivos_fonte['lista']:
            hash_md5 = arquivos_fonte['hashes'].get(arquivo, 'N/A')
            arquivo_data.append([arquivo, hash_md5])
        
        table = Table(arquivo_data, colWidths=[self.content_width*0.6, self.content_width*0.4])
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), self.colors['light_grey']),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('PADDING', (0, 0), (-1, -1), 6),
        ]))
        
        story.append(table)
        
        # Log de processamento resumido
        story.append(Spacer(1, 15))
        story.append(Paragraph("Resumo do Processamento:", self.styles['SubsectionHeader']))
        
        log_proc = dados['log_processamento']
        resumo_text = f"""
        • Registros originais: {log_proc['registros_originais']:,}<br/>
        • Registros finais: {log_proc['registros_finais']:,}<br/>
        • Anomalias detectadas: {len(log_proc['anomalias_detectadas'])}<br/>
        • Ajustes realizados: {len(log_proc['ajustes_realizados'])}<br/>
        • Dados estimados: {len(log_proc['dados_estimados'])}
        """
        
        story.append(Paragraph(resumo_text, self.styles['Normal']))

def gerar_relatorio_pdf_completo(dados_processados: Dict, output_path: str) -> str:
    """
    Função principal para gerar relatório PDF completo.
    """
    generator = ProfessionalPDFGenerator()
    return generator.gerar_relatorio_completo(dados_processados, output_path)

if __name__ == "__main__":
    # Teste básico
    dados_teste = {
        'cliente': 'Cliente Teste',
        'tipo_relatorio': 'semanal',
        'periodo': {
            'inicio': '2025-01-01T00:00:00',
            'fim': '2025-01-07T23:59:59',
            'dias': 7
        },
        'metricas_principais': {
            'total_veiculos': 3,
            'quilometragem_total': 1500.5,
            'velocidade_maxima': 85,
            'eficiencia_kmL': 12.5,
            'combustivel_total': 120.0,
            'registros_processados': 1000,
            'registros_originais': 1050,
            'percentual_estimados': 15.2
        },
        'log_processamento': {
            'registros_originais': 1050,
            'registros_finais': 1000,
            'anomalias_detectadas': ['Teste anomalia'],
            'ajustes_realizados': ['Teste ajuste'],
            'dados_estimados': ['Teste estimativa']
        },
        'arquivos_fonte': {
            'lista': ['teste.csv'],
            'hashes': {'teste.csv': 'abc123def456'}
        },
        'dados_processados': [
            {
                'placa': 'TEST001',
                'timestamp': '2025-01-01T08:00:00',
                'km_delta': 50.0,
                'speed': 60,
                'regra_aplicada': '',
                'dados_estimados': False,
                'incluir_totais': True
            }
        ],
        'timestamp_geracao': datetime.now().isoformat()
    }
    
    output_file = "teste_relatorio.pdf"
    gerar_relatorio_pdf_completo(dados_teste, output_file)
    print(f"Relatório de teste gerado: {output_file}")