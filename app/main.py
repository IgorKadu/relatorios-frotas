"""
API FastAPI principal para o sistema de relatórios de telemetria veicular.
"""

from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Depends, Query
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
from datetime import datetime, timedelta
from typing import Optional, List, Optional
import os
import shutil
import tempfile
from pathlib import Path

from .models import init_database, get_session, Cliente, Veiculo, PosicaoHistorica, RelatorioGerado
from .utils import CSVProcessor, convert_numpy_types
from .services import ReportGenerator, TelemetryAnalyzer
from .reports import generate_consolidated_vehicle_report
# Removed old generate_vehicle_report - now uses standardized consolidated generation

# Inicialização da aplicação
app = FastAPI(
    title="Sistema de Relatórios de Telemetria Veicular",
    description="API para processamento e análise de dados de telemetria veicular",
    version="1.0.0"
)

# Configuração de diretórios
BASE_DIR = Path(__file__).parent.parent
STATIC_DIR = BASE_DIR / "frontend" / "static"
TEMPLATES_DIR = BASE_DIR / "frontend" / "templates"
UPLOAD_DIR = BASE_DIR / "data" / "uploads"
REPORTS_DIR = BASE_DIR / "reports"

# Cria diretórios necessários
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
STATIC_DIR.mkdir(parents=True, exist_ok=True)
TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)

# Configuração de arquivos estáticos e templates
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Inicialização do banco de dados
@app.on_event("startup")
async def startup_event():
    """Inicializa o banco de dados na inicialização da aplicação"""
    try:
        init_database()
        print("✅ Banco de dados inicializado com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao inicializar banco de dados: {e}")

# Rotas principais
@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """Página inicial da aplicação"""
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/health")
async def health_check():
    """Verificação de saúde da API"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0"
    }

# Rotas para gerenciamento de clientes
@app.get("/api/clientes")
async def listar_clientes():
    """Lista todos os clientes cadastrados"""
    session = get_session()
    try:
        clientes = session.query(Cliente).all()
        return [
            {
                "id": cliente.id,
                "nome": cliente.nome,
                "consumo_medio_kmL": cliente.consumo_medio_kmL,
                "limite_velocidade": cliente.limite_velocidade,
                "created_at": cliente.created_at.isoformat()
            }
            for cliente in clientes
        ]
    finally:
        session.close()

@app.post("/api/clientes")
async def criar_cliente(
    nome: str = Form(...),
    consumo_medio_kmL: float = Form(12.0),
    limite_velocidade: int = Form(80)
):
    """Cria um novo cliente"""
    session = get_session()
    try:
        # Verifica se cliente já existe
        cliente_existe = session.query(Cliente).filter_by(nome=nome).first()
        if cliente_existe:
            raise HTTPException(status_code=400, detail="Cliente já existe")
        
        # Cria novo cliente
        cliente = Cliente(
            nome=nome,
            consumo_medio_kmL=consumo_medio_kmL,
            limite_velocidade=limite_velocidade
        )
        session.add(cliente)
        session.commit()
        
        return {
            "success": True,
            "message": "Cliente criado com sucesso",
            "cliente_id": cliente.id
        }
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        session.close()

# Rotas para gerenciamento de veículos
@app.get("/api/veiculos")
async def listar_veiculos():
    """Lista todos os veículos cadastrados"""
    session = get_session()
    try:
        veiculos = session.query(Veiculo).join(Cliente).all()
        return [
            {
                "id": veiculo.id,
                "placa": veiculo.placa,
                "ativo": veiculo.ativo,
                "cliente": veiculo.cliente.nome,
                "cliente_id": veiculo.cliente_id,
                "created_at": veiculo.created_at.isoformat()
            }
            for veiculo in veiculos
        ]
    finally:
        session.close()

@app.get("/api/veiculos/{placa}")
async def obter_veiculo(placa: str):
    """Obtém informações de um veículo específico"""
    session = get_session()
    try:
        veiculo = session.query(Veiculo).filter_by(placa=placa).first()
        if not veiculo:
            raise HTTPException(status_code=404, detail="Veículo não encontrado")
        
        return {
            "id": veiculo.id,
            "placa": veiculo.placa,
            "ativo": veiculo.ativo,
            "cliente": veiculo.cliente.nome,
            "cliente_id": veiculo.cliente_id,
            "created_at": veiculo.created_at.isoformat()
        }
    finally:
        session.close()

# Rotas para limpeza de dados
@app.delete("/api/database/clear")
async def clear_database():
    """Limpa todos os dados do banco de dados (exceto clientes)"""
    session = get_session()
    try:
        # Remove todas as posições históricas
        session.query(PosicaoHistorica).delete()
        
        # Remove todos os veículos
        session.query(Veiculo).delete()
        
        # Remove todos os relatórios gerados
        session.query(RelatorioGerado).delete()
        
        session.commit()
        
        return convert_numpy_types({
            "success": True,
            "message": "Banco de dados limpo com sucesso!",
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        session.rollback()
        raise HTTPException(status_code=500, detail=f"Erro ao limpar banco: {str(e)}")
    finally:
        session.close()

# Rotas para upload e processamento de CSV
@app.post("/api/upload-csv")
async def upload_csv(
    files: List[UploadFile] = File(...),
    cliente_nome: Optional[str] = Form(None)
):
    """Upload e processamento de arquivos CSV"""
    try:
        processor = CSVProcessor()
        results = {}
        
        for file in files:
            if not file.filename.endswith('.csv'):
                continue
            
            # Salva arquivo temporariamente
            temp_path = UPLOAD_DIR / file.filename
            with open(temp_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
            
            try:
                # Processa arquivo
                df = processor.read_csv_file(str(temp_path))
                df_clean = processor.clean_and_parse_data(df)
                
                # Calcula métricas
                metrics = processor.calculate_metrics(df_clean)
                
                # Salva no banco
                success = processor.save_to_database(df_clean, cliente_nome or "Cliente Padrão")
                
                results[file.filename] = {
                    "success": success,
                    "records_processed": int(len(df_clean)),
                    "metrics": convert_numpy_types(metrics)
                }
                
            except Exception as e:
                results[file.filename] = {
                    "success": False,
                    "error": str(e)
                }
            finally:
                # Remove arquivo temporário
                if temp_path.exists():
                    temp_path.unlink()
        
        return convert_numpy_types({
            "success": True,
            "message": f"Processados {len(files)} arquivos",
            "results": results
        })
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Rotas para análise e relatórios
@app.get("/api/analise/{placa}/mapa-detalhado")
async def gerar_mapa_detalhado(
    placa: str,
    data_inicio: str,
    data_fim: str
):
    """Gera mapa detalhado de rotas com dados operacionais"""
    try:
        # Validação de entrada
        if not placa or not placa.strip():
            raise HTTPException(status_code=400, detail="Placa é obrigatória")
            
        # Converte datas
        try:
            dt_inicio = datetime.fromisoformat(data_inicio.replace('Z', '+00:00'))
            dt_fim = datetime.fromisoformat(data_fim.replace('Z', '+00:00'))
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Formato de data inválido: {str(e)}")
        
        # Validação de período
        if dt_inicio >= dt_fim:
            raise HTTPException(status_code=400, detail="Data de início deve ser anterior à data de fim")
            
        # Verifica se o veículo existe no banco
        session = get_session()
        try:
            veiculo = session.query(Veiculo).filter(Veiculo.placa == placa.upper()).first()
            if not veiculo:
                raise HTTPException(status_code=404, detail=f"Veículo com placa {placa} não encontrado")
        finally:
            session.close()
        
        # Gera análise com mapa detalhado
        analyzer = TelemetryAnalyzer()
        df = analyzer.get_vehicle_data(placa, dt_inicio, dt_fim)
        
        if df.empty:
            return {
                'success': False,
                'message': 'Nenhum dado encontrado para o período especificado.'
            }
        
        # Gera métricas e mapas
        metrics = analyzer.generate_summary_metrics(df, placa)
        detailed_map = analyzer.create_detailed_route_map(df)
        regular_map = analyzer.create_route_map(df)
        
        # Gera gráficos adicionais
        speed_chart = analyzer.create_speed_chart(df)
        periods_chart = analyzer.create_operational_periods_chart(df)
        ignition_chart = analyzer.create_ignition_status_chart(df)
        
        # Análise de combustível
        fuel_analysis = analyzer.create_fuel_consumption_analysis(metrics)
        
        return {
            'success': True,
            'metrics': convert_numpy_types(metrics),
            'detailed_map': detailed_map,
            'regular_map': regular_map,
            'charts': {
                'speed_chart': speed_chart,
                'periods_chart': periods_chart,
                'ignition_chart': ignition_chart
            },
            'fuel_analysis': fuel_analysis,
            'data_count': len(df)
        }
        
    except HTTPException:
        raise  # Re-raise HTTP exceptions
    except Exception as e:
        # Log the error for debugging
        import traceback
        print(f"Erro na geração do mapa detalhado: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Erro interno do servidor: {str(e)}")

@app.get("/api/analise/{placa}")
async def gerar_analise(
    placa: str,
    data_inicio: str = Query(..., description="Data inicial no formato YYYY-MM-DD ou ISO8601"),
    data_fim: str = Query(..., description="Data final no formato YYYY-MM-DD ou ISO8601")
):
    """Gera análise completa de um veículo"""
    try:
        # Validação de entrada
        if not placa or not placa.strip():
            raise HTTPException(status_code=400, detail="Placa é obrigatória")
            
        # Converte datas de forma robusta (ISO ou YYYY-MM-DD)
        try:
            def _parse_date(s: str) -> datetime:
                s = (s or "").strip()
                if not s:
                    raise ValueError("Data vazia")
                # Normaliza sufixo Z
                st = s.replace('Z', '+00:00')
                try:
                    return datetime.fromisoformat(st)
                except Exception:
                    # Fallback para YYYY-MM-DD
                    try:
                        d = datetime.strptime(s, "%Y-%m-%d")
                        return d
                    except Exception as e2:
                        raise ValueError(f"Formato de data inválido: {s}") from e2
            dt_inicio = _parse_date(data_inicio)
            dt_fim = _parse_date(data_fim)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Formato de data inválido: {str(e)}")
        
        # Validação de período
        if dt_inicio >= dt_fim:
            raise HTTPException(status_code=400, detail="Data de início deve ser anterior à data de fim")
        
        # Verifica se o veículo existe no banco
        session = get_session()
        try:
            veiculo = session.query(Veiculo).filter(Veiculo.placa == placa.upper()).first()
            if not veiculo:
                raise HTTPException(status_code=404, detail=f"Veículo com placa {placa} não encontrado")
        finally:
            session.close()
        
        # Gera análise
        generator = ReportGenerator()
        result = generator.generate_complete_analysis(placa.upper(), dt_inicio, dt_fim)
        return JSONResponse(content=convert_numpy_types(result))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"Erro na geração da análise: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Erro interno do servidor: {str(e)}")

@app.post("/api/relatorio/{placa}")
async def gerar_relatorio_pdf(
    placa: str,
    data_inicio: str = Form(...),
    data_fim: str = Form(...)
):
    """Gera relatório PDF padronizado para qualquer filtro (veículo individual ou todos)"""
    try:
        # Converte datas
        dt_inicio = datetime.fromisoformat(data_inicio.replace('Z', '+00:00'))
        dt_fim = datetime.fromisoformat(data_fim.replace('Z', '+00:00'))
        
        # SEMPRE usa a estrutura consolidada padronizada - independente do filtro
        from .reports import generate_consolidated_vehicle_report
        
        if placa.upper() == 'TODOS':
            # Relatório para todos os veículos
            result = generate_consolidated_vehicle_report(
                dt_inicio, dt_fim, str(REPORTS_DIR), cliente_nome=None
            )
        else:
            # Relatório para veículo individual usando mesma estrutura padronizada
            result = generate_consolidated_vehicle_report(
                dt_inicio, dt_fim, str(REPORTS_DIR), vehicle_filter=placa
            )
        
        if not result['success']:
            raise HTTPException(status_code=500, detail=result.get('error', 'Erro ao gerar relatório'))
        
        return {
            "success": True,
            "message": "Relatório gerado com sucesso",
            "file_path": result['file_path'],
            "file_size_mb": result['file_size_mb'],
            "download_url": f"/api/download/{Path(result['file_path']).name}"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Formato de data inválido: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/download/{filename}")
async def download_relatorio(filename: str):
    """Download de relatório PDF"""
    file_path = REPORTS_DIR / filename
    
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="Arquivo não encontrado")
    
    return FileResponse(
        path=str(file_path),
        filename=filename,
        media_type='application/pdf'
    )

@app.delete("/api/relatorios/clear")
async def clear_reports_history():
    """Limpa o histórico de relatórios gerados"""
    try:
        deleted_count = 0
        for file_path in REPORTS_DIR.glob("*.pdf"):
            try:
                file_path.unlink()
                deleted_count += 1
            except Exception as e:
                print(f"Erro ao deletar {file_path}: {e}")
                
        return {
            "success": True,
            "message": f"Histórico limpo com sucesso! {deleted_count} relatório(s) removido(s).",
            "deleted_count": deleted_count
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao limpar histórico: {str(e)}")

@app.get("/api/relatorios")
async def listar_relatorios(veiculo: Optional[str] = None, data: Optional[str] = None):
    """Lista todos os relatórios gerados com filtros opcionais"""
    try:
        reports = []
        for file_path in REPORTS_DIR.glob("*.pdf"):
            stat = file_path.stat()
            filename = file_path.name
            created_at = datetime.fromtimestamp(stat.st_ctime)
            
            # Extrair placa do nome do arquivo (formato: PLACA_YYYYMMDD_HHMMSS.pdf)
            try:
                placa_from_file = filename.split('_')[0] if '_' in filename else None
            except:
                placa_from_file = None
                
            # Aplicar filtros
            include_file = True
            
            # Filtro por veículo
            if veiculo and placa_from_file:
                if placa_from_file.upper() != veiculo.upper():
                    include_file = False
                    
            # Filtro por data
            if data and include_file:
                try:
                    filter_date = datetime.strptime(data, "%Y-%m-%d").date()
                    file_date = created_at.date()
                    if file_date != filter_date:
                        include_file = False
                except ValueError:
                    pass  # Ignora filtro de data se formato inválido
                    
            if include_file:
                reports.append({
                    "id": filename.replace('.pdf', ''),
                    "filename": filename,
                    "placa": placa_from_file,
                    "size_mb": round(stat.st_size / (1024 * 1024), 2),
                    "created_at": created_at.isoformat(),
                    "download_url": f"/api/download/{filename}"
                })
        
        # Ordena por data de criação (mais recente primeiro)
        reports.sort(key=lambda x: x['created_at'], reverse=True)
        
        return reports
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Rotas para dashboard
@app.get("/api/dashboard/resumo")
async def dashboard_resumo():
    """Retorna resumo para dashboard"""
    session = get_session()
    try:
        # Estatísticas básicas
        total_clientes = session.query(Cliente).count()
        total_veiculos = session.query(Veiculo).count()
        total_registros = session.query(PosicaoHistorica).count()
        
        # Últimos registros (últimos 7 dias)
        data_limite = datetime.now() - timedelta(days=7)
        registros_recentes = session.query(PosicaoHistorica).filter(
            PosicaoHistorica.data_evento >= data_limite
        ).count()
        
        # Relatórios gerados
        total_relatorios = len(list(REPORTS_DIR.glob("*.pdf")))
        
        return {
            "total_clientes": total_clientes,
            "total_veiculos": total_veiculos,
            "total_registros": total_registros,
            "registros_ultimos_7_dias": registros_recentes,
            "total_relatorios": total_relatorios,
            "timestamp": datetime.now().isoformat()
        }
        
    finally:
        session.close()

@app.get("/api/dashboard/atividade-recente")
async def dashboard_atividade():
    """Retorna atividade recente para dashboard"""
    session = get_session()
    try:
        # Últimos 10 registros
        registros = session.query(PosicaoHistorica).join(Veiculo).order_by(
            PosicaoHistorica.data_evento.desc()
        ).limit(10).all()
        
        atividades = []
        for registro in registros:
            atividades.append({
                "placa": registro.veiculo.placa,
                "data_evento": registro.data_evento.isoformat(),
                "velocidade": registro.velocidade_kmh,
                "endereco": registro.endereco[:50] + "..." if len(registro.endereco) > 50 else registro.endereco,
                "tipo_evento": registro.tipo_evento
            })
        
        return atividades
        
    finally:
        session.close()

# Middleware para CORS (se necessário)
from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Em produção, especificar origins específicos
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Novo endpoint para relatório aprimorado
@app.post("/api/generate-enhanced-report")
async def generate_enhanced_report(
    placa: str = Form(...),
    data_inicio: str = Form(...),
    data_fim: str = Form(...)
):
    """Gera relatório PDF aprimorado com estrutura melhorada (diário/semanal/mensal)"""
    try:
        # Validar e parsear datas
        try:
            data_inicio_dt = datetime.strptime(data_inicio, "%Y-%m-%d")
            data_fim_dt = datetime.strptime(data_fim, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="Formato de data inválido. Use YYYY-MM-DD")
        
        # Validar período
        if data_inicio_dt > data_fim_dt:
            raise HTTPException(status_code=400, detail="Data de início deve ser anterior à data de fim")
        
        if (data_fim_dt - data_inicio_dt).days > 365:
            raise HTTPException(status_code=400, detail="Período máximo permitido é de 365 dias")
        
        # Garantir que o diretório existe
        os.makedirs(REPORTS_DIR, exist_ok=True)
        
        # Gerar relatório aprimorado
        from .reports import PDFReportGenerator
        generator = PDFReportGenerator()
        result = generator.generate_enhanced_pdf_report(
            placa=placa,
            data_inicio=data_inicio_dt,
            data_fim=data_fim_dt,
            output_path=str(REPORTS_DIR)
        )
        
        if result['success']:
            return {
                "success": True,
                "message": f"Relatório aprimorado gerado com sucesso - Análise {result['analysis_type']}",
                "file_path": result['file_path'],
                "filename": result['filename'],
                "file_size_mb": result['file_size_mb'],
                "analysis_type": result['analysis_type'],
                "period_days": result['period_days'],
                "data_quality": result['data_quality'],
                "download_url": f"/api/download/{result['filename']}"
            }
        else:
            raise HTTPException(status_code=500, detail=result.get('error', 'Erro desconhecido'))
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro interno: {str(e)}")

# Função utilitária (não endpoint) para geração de relatório consolidado
# Permite uso programático sem subir o servidor FastAPI
async def gerar_relatorio_consolidado(
    data_inicio: str,
    data_fim: str,
    cliente_nome: Optional[str] = None,
    vehicle_filter: Optional[str] = None,
    output_dir: Optional[str] = None
):
    """
    Gera relatório consolidado (todos os veículos ou um veículo específico) em PDF.

    Args:
        data_inicio: Data inicial no formato YYYY-MM-DD
        data_fim: Data final no formato YYYY-MM-DD
        cliente_nome: Nome do cliente para filtrar (opcional)
        vehicle_filter: Placa para relatório individual (opcional)
        output_dir: Diretório de saída (opcional; padrão usa pasta reports do projeto)

    Returns:
        Dict com resultado da geração do PDF (success, file_path, file_size_mb, mode, ...)
    """
    try:
        start_dt = datetime.strptime(data_inicio, "%Y-%m-%d")
        end_dt = datetime.strptime(data_fim, "%Y-%m-%d")

        target_output_dir = output_dir or str(REPORTS_DIR)

        result = generate_consolidated_vehicle_report(
            start_dt,
            end_dt,
            output_dir=target_output_dir,
            cliente_nome=cliente_nome,
            vehicle_filter=vehicle_filter
        )
        return result
    except Exception as e:
        return {"success": False, "error": f"Erro ao gerar relatório consolidado: {e}"}

