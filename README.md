# Sistema de Relatórios de Telemetria Veicular

## 📋 Descrição

Sistema completo para processamento e análise de dados de telemetria veicular, transformando arquivos CSV brutos em relatórios estruturados PDF com insights personalizados para clientes.

## 🎯 Funcionalidades

### Core Features
- ✅ **Processamento de CSV**: Importação automática de arquivos de telemetria
- ✅ **Análise Inteligente**: Geração de insights baseados em períodos operacionais
- ✅ **Relatórios PDF**: Criação de relatórios profissionais e personalizados
- ✅ **Dashboard Web**: Interface moderna para monitoramento e gestão
- ✅ **API REST**: Endpoints completos para integração

### Recursos Avançados
- 🗺️ **Mapas de Trajeto**: Visualização interativa das rotas percorridas
- 📊 **Gráficos Dinâmicos**: Análise visual de velocidade, consumo e operação
- ⛽ **Estimativa de Combustível**: Cálculos baseados em velocidade e eficiência
- 🚨 **Alertas de Segurança**: Detecção de excesso de velocidade e eventos
- 📱 **Interface Responsiva**: Acesso via desktop, tablet e mobile

## 🏗️ Arquitetura

```
relatorios-frotas/
├── app/
│   ├── main.py         # API FastAPI principal
│   ├── models.py       # Modelos SQLAlchemy
│   ├── services.py     # Serviços de análise
│   ├── reports.py      # Gerador de PDF
│   ├── utils.py        # Utilitários CSV
│   └── __init__.py     # Inicialização
├── frontend/
│   ├── templates/      # Templates HTML
│   └── static/         # CSS, JS, assets
├── data/               # CSVs e banco SQLite
├── reports/            # PDFs gerados
└── requirements.txt    # Dependências
```

## 🚀 Instalação e Configuração

### Pré-requisitos
- Python 3.11+
- pip (gerenciador de pacotes Python)

### Passo a Passo

1. **Clone ou baixe o projeto**
   ```bash
   cd relatorios-frotas
   ```

2. **Instale as dependências**
   ```bash
   pip install -r requirements.txt
   ```

3. **Inicialize o banco de dados**
   ```bash
   python -m app.models
   ```

4. **Execute o servidor**
   ```bash
   uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
   ```

5. **Acesse a aplicação**
   ```
   http://localhost:8000
   ```

## 📊 Períodos Operacionais

O sistema analisa os dados considerando os seguintes períodos:

| Período | Horário |
|---------|---------|
| **Manhã** | 04:00 - 07:00 |
| **Meio-dia** | 10:50 - 13:00 |
| **Tarde** | 16:50 - 19:00 |
| **Final de Semana** | Sábado e Domingo (todo período) |
| **Fora de Horário** | Demais horários |

## 📁 Estrutura dos Dados CSV

### Colunas Obrigatórias
- `Cliente`: Nome do cliente
- `Placa`: Identificação do veículo
- `Ativo`: Código interno
- `Data`: Data/hora do evento (DD/MM/YYYY HH:mm:ss)
- `Velocidade (Km)`: Velocidade em km/h
- `Ignição`: Status (L=ligado, D=desligado, LP=ligado parado, LM=ligado movimento)
- `Localização`: Coordenadas (latitude,longitude)
- `Endereço`: Endereço formatado

### Exemplo de Dados
```csv
Cliente;Placa;Ativo;Data;Velocidade (Km);Ignição;Localização;Endereço
JANDAIA;TFE-6D41;TFE-6D41;15/09/2025 19:13:32;26;LM;-17.040746,-50.151721;Avenida A - 394 - Jandaia - GO
```

## 🔄 Fluxo de Trabalho

### 1. Upload de Dados
- Acesse a aba "Upload CSV"
- Selecione um ou mais arquivos CSV
- Opcionalmente especifique o nome do cliente
- Clique em "Processar Arquivos"

### 2. Análise de Dados
- Vá para a aba "Análise"
- Selecione o veículo e período
- Visualize métricas, gráficos e insights

### 3. Geração de Relatórios
- Acesse "Relatórios"
- Escolha veículo e período
- Gere e baixe o PDF profissional

## 📈 Métricas e Insights

### Indicadores Principais
- **Quilometragem Total**: Distância percorrida no período
- **Velocidade Máxima/Média**: Análise de velocidade
- **Tempo Operacional**: Ligado, movimento, parado, desligado
- **Consumo de Combustível**: Estimativa baseada em eficiência
- **Conectividade**: Status GPS/GPRS

### Insights Automatizados
- 🚨 **Alertas de Velocidade**: Excesso acima de 80 km/h
- ⛽ **Eficiência de Combustível**: Análise de consumo
- 📊 **Utilização do Veículo**: Percentual em movimento
- 📡 **Problemas de Conectividade**: Falhas GPS/GPRS
- 🕒 **Operação Fora de Horário**: Uso em períodos não comerciais

## 🛠️ API Endpoints

### Principais Rotas

#### Dashboard
- `GET /api/dashboard/resumo` - Estatísticas gerais
- `GET /api/dashboard/atividade-recente` - Últimas atividades

#### Veículos
- `GET /api/veiculos` - Listar veículos
- `GET /api/veiculos/{placa}` - Dados de um veículo

#### Processamento
- `POST /api/upload-csv` - Upload de arquivos CSV
- `GET /api/analise/{placa}` - Análise de um veículo

#### Relatórios
- `POST /api/relatorio/{placa}` - Gerar relatório PDF
- `GET /api/relatorios` - Listar relatórios
- `GET /api/download/{filename}` - Download de relatório

## 🎨 Interface Web

### Dashboard
- **Cards de Estatísticas**: Totais de clientes, veículos, registros
- **Atividade Recente**: Últimos eventos de telemetria
- **Lista de Veículos**: Veículos cadastrados

### Upload de CSV
- **Seleção Múltipla**: Upload de vários arquivos
- **Validação**: Verificação automática de formato
- **Progresso**: Feedback visual do processamento

### Análise Dinâmica
- **Filtros de Período**: Seleção flexível de datas
- **Gráficos Interativos**: Velocidade, períodos operacionais
- **Mapas de Rota**: Visualização do trajeto percorrido
- **Insights Contextuais**: Recomendações automáticas

### Geração de Relatórios
- **Seleção de Veículo**: Lista de veículos disponíveis
- **Configuração de Período**: Datas início e fim
- **Download Automático**: PDF gerado e baixado

## 🔧 Tecnologias Utilizadas

### Backend
- **FastAPI**: Framework web moderno e rápido
- **SQLAlchemy**: ORM para banco de dados
- **SQLite**: Banco de dados local
- **Pandas**: Processamento de dados
- **ReportLab**: Geração de PDFs

### Frontend
- **Bootstrap 5**: Framework CSS responsivo
- **JavaScript ES6+**: Funcionalidades interativas
- **Axios**: Cliente HTTP
- **Font Awesome**: Ícones

### Visualização
- **Plotly**: Gráficos interativos
- **Matplotlib**: Gráficos estáticos
- **Folium**: Mapas interativos

## 📊 Relatórios PDF

### Estrutura do Relatório
1. **Capa**: Logo, cliente, veículo, período
2. **Sumário Executivo**: Métricas principais e insights
3. **Análise Operacional**: Períodos, conectividade
4. **Consumo de Combustível**: Eficiência e recomendações
5. **Recomendações**: Plano de ação personalizado

### Características
- Design profissional e limpo
- Gráficos e tabelas integrados
- Insights destacados por categoria
- Recomendações específicas por cliente

## 🔒 Considerações de Segurança

- Dados armazenados localmente (SQLite)
- Validação de entrada de dados
- Sanitização de arquivos CSV
- Controle de acesso por IP (configurável)

## 🚀 Próximos Passos

### Melhorias Planejadas
- [ ] Autenticação e autorização
- [ ] Suporte a PostgreSQL
- [ ] Exportação para Excel
- [ ] API de integração com frotas
- [ ] Alertas em tempo real
- [ ] Dashboard executivo

### Integrações Futuras
- [ ] Power BI connector
- [ ] WhatsApp notifications
- [ ] Email automático de relatórios
- [ ] Backup em nuvem

## 📞 Suporte

### Logs e Debugging
Os logs da aplicação são exibidos no console durante a execução. Para debugging detalhado, modifique o nível de log em `main.py`.

### Problemas Comuns

**Erro de importação CSV:**
- Verifique se o arquivo está no formato correto
- Confirme se as colunas obrigatórias estão presentes
- Teste com um arquivo menor primeiro

**Relatório não gerado:**
- Verifique se existem dados para o período
- Confirme se o veículo existe no sistema
- Consulte os logs para detalhes do erro

**Interface não carrega:**
- Verifique se o servidor está rodando na porta 8000
- Confirme se todas as dependências estão instaladas
- Teste em um navegador diferente

## 📄 Licença

Este projeto foi desenvolvido para automatizar o processamento de telemetria veicular e geração de relatórios profissionais.

---

**Desenvolvido com ❤️ para otimizar a gestão de frotas**