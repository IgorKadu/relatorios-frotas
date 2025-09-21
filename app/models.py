"""
Modelos de dados para o sistema de relatórios de telemetria veicular.
"""

from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text, ForeignKey, Time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy import create_engine
from datetime import datetime, time
import os

Base = declarative_base()

class Cliente(Base):
    """Modelo para armazenar dados dos clientes"""
    __tablename__ = 'clientes'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    nome = Column(String(255), nullable=False, unique=True)
    consumo_medio_kmL = Column(Float, default=12.0)  # km/L padrão
    limite_velocidade = Column(Integer, default=80)  # km/h
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relacionamentos
    veiculos = relationship("Veiculo", back_populates="cliente")
    perfis_horario = relationship("PerfilHorario", back_populates="cliente")

class PerfilHorario(Base):
    """Modelo para armazenar perfis de horário personalizados por cliente"""
    __tablename__ = 'perfis_horario'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    cliente_id = Column(Integer, ForeignKey('clientes.id'), nullable=False)
    nome = Column(String(100), nullable=False)  # Ex: "Manhã", "Meio-dia", "Tarde"
    descricao = Column(String(255))  # Ex: "Período matutino"
    hora_inicio = Column(Time, nullable=False)  # Ex: 04:00
    hora_fim = Column(Time, nullable=False)     # Ex: 07:00
    tipo_periodo = Column(String(50), default='operacional')  # operacional, fora_horario, especial
    ativo = Column(Boolean, default=True)
    cor_relatorio = Column(String(7), default='#28a745')  # Cor hexadecimal para relatórios
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relacionamentos
    cliente = relationship("Cliente", back_populates="perfis_horario")

class Veiculo(Base):
    """Modelo para armazenar dados dos veículos"""
    __tablename__ = 'veiculos'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    placa = Column(String(20), nullable=False, unique=True)
    ativo = Column(String(50), nullable=False)  # Código interno
    cliente_id = Column(Integer, ForeignKey('clientes.id'), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relacionamentos
    cliente = relationship("Cliente", back_populates="veiculos")
    posicoes = relationship("PosicaoHistorica", back_populates="veiculo")

class PosicaoHistorica(Base):
    """Modelo para armazenar dados de posições históricas dos veículos"""
    __tablename__ = 'posicoes_historicas'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    veiculo_id = Column(Integer, ForeignKey('veiculos.id'), nullable=False)
    
    # Dados temporais
    data_evento = Column(DateTime, nullable=False)
    data_gprs = Column(DateTime, nullable=True)
    
    # Dados de velocidade e ignição
    velocidade_kmh = Column(Integer, default=0)
    ignicao = Column(String(2))  # 'L' = ligado, 'D' = desligado, 'LP' = ligado parado, 'LM' = ligado movimento
    motorista = Column(String(255))
    
    # Dados de conectividade
    gps_status = Column(Boolean, default=True)
    gprs_status = Column(Boolean, default=True)
    
    # Dados de localização
    latitude = Column(Float)
    longitude = Column(Float)
    endereco = Column(Text)
    
    # Dados do evento
    tipo_evento = Column(String(100))
    saida = Column(String(50))  # Sensores digitais
    entrada = Column(String(50))  # Sensores digitais
    pacote = Column(String(50))
    
    # Dados de odômetro e horímetro
    odometro_periodo_km = Column(Float, default=0.0)
    odometro_embarcado_km = Column(Float, default=0.0)
    horimetro_periodo = Column(String(20))  # HH:MM:SS
    horimetro_embarcado = Column(String(20))  # HH:MM:SS
    
    # Dados elétricos
    bateria_pct = Column(Integer)
    tensao_v = Column(Float)
    bloqueado = Column(Boolean, default=False)
    
    # Metadados
    imagem = Column(Text)  # Campo para anexos
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relacionamentos
    veiculo = relationship("Veiculo", back_populates="posicoes")

class RelatorioGerado(Base):
    """Modelo para armazenar histórico de relatórios gerados"""
    __tablename__ = 'relatorios_gerados'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    cliente_id = Column(Integer, ForeignKey('clientes.id'), nullable=False)
    veiculo_id = Column(Integer, ForeignKey('veiculos.id'), nullable=True)
    
    # Dados do relatório
    nome_arquivo = Column(String(255), nullable=False)
    caminho_arquivo = Column(String(500), nullable=False)
    data_inicio = Column(DateTime, nullable=False)
    data_fim = Column(DateTime, nullable=False)
    
    # Métricas do relatório
    total_registros = Column(Integer, default=0)
    km_total = Column(Float, default=0.0)
    tempo_ligado_horas = Column(Float, default=0.0)
    velocidade_maxima = Column(Integer, default=0)
    
    # Metadados
    tamanho_arquivo_mb = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relacionamentos
    cliente = relationship("Cliente")
    veiculo = relationship("Veiculo")

# Configuração do banco de dados
def get_database_url():
    """Retorna a URL do banco de dados"""
    db_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'telemetria.db')
    return f"sqlite:///{db_path}"

def create_database_engine():
    """Cria e retorna o engine do banco de dados"""
    database_url = get_database_url()
    engine = create_engine(database_url, echo=False)
    return engine

def create_tables():
    """Cria todas as tabelas no banco de dados"""
    engine = create_database_engine()
    Base.metadata.create_all(engine)
    return engine

def get_session():
    """Retorna uma sessão do banco de dados"""
    engine = create_database_engine()
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()

# Função para inicializar o banco
def init_database():
    """Inicializa o banco de dados com dados padrão"""
    engine = create_tables()
    session = get_session()
    
    try:
        # Verifica se já existem clientes
        cliente_existe = session.query(Cliente).first()
        if not cliente_existe:
            # Cria cliente padrão baseado nos dados CSV
            cliente_jandaia = Cliente(
                nome="JANDAIA",
                consumo_medio_kmL=12.0,
                limite_velocidade=80
            )
            session.add(cliente_jandaia)
            session.commit()
            print("Cliente padrão JANDAIA criado.")
            
            # Cria perfis de horário padrão para o cliente JANDAIA
            perfis_padrao = [
                PerfilHorario(
                    cliente_id=cliente_jandaia.id,
                    nome="Manhã",
                    descricao="Período operacional matutino",
                    hora_inicio=time(4, 0),
                    hora_fim=time(7, 0),
                    tipo_periodo="operacional",
                    cor_relatorio="#28a745"
                ),
                PerfilHorario(
                    cliente_id=cliente_jandaia.id,
                    nome="Meio-dia",
                    descricao="Período operacional do meio-dia",
                    hora_inicio=time(10, 50),
                    hora_fim=time(13, 0),
                    tipo_periodo="operacional",
                    cor_relatorio="#17a2b8"
                ),
                PerfilHorario(
                    cliente_id=cliente_jandaia.id,
                    nome="Tarde",
                    descricao="Período operacional vespertino",
                    hora_inicio=time(16, 50),
                    hora_fim=time(19, 0),
                    tipo_periodo="operacional",
                    cor_relatorio="#007bff"
                ),
                PerfilHorario(
                    cliente_id=cliente_jandaia.id,
                    nome="Fora Horário Manhã",
                    descricao="Fora do horário operacional matutino",
                    hora_inicio=time(7, 0),
                    hora_fim=time(10, 50),
                    tipo_periodo="fora_horario",
                    cor_relatorio="#ffc107"
                ),
                PerfilHorario(
                    cliente_id=cliente_jandaia.id,
                    nome="Fora Horário Tarde",
                    descricao="Fora do horário operacional vespertino",
                    hora_inicio=time(13, 0),
                    hora_fim=time(16, 50),
                    tipo_periodo="fora_horario",
                    cor_relatorio="#fd7e14"
                ),
                PerfilHorario(
                    cliente_id=cliente_jandaia.id,
                    nome="Fora Horário Noite",
                    descricao="Fora do horário operacional noturno",
                    hora_inicio=time(19, 0),
                    hora_fim=time(4, 0),
                    tipo_periodo="fora_horario",
                    cor_relatorio="#6f42c1"
                )
            ]
            
            for perfil in perfis_padrao:
                session.add(perfil)
            session.commit()
            print("Perfis de horário padrão criados.")
        
        session.close()
        return True
    except Exception as e:
        session.rollback()
        session.close()
        print(f"Erro ao inicializar banco: {e}")
        return False

if __name__ == "__main__":
    # Inicializa o banco quando executado diretamente
    init_database()
    print("Banco de dados inicializado com sucesso!")