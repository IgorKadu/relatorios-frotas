// Sistema de Telemetria Veicular - Frontend JavaScript

class TelemetriaApp {
    constructor() {
        this.currentSection = 'dashboard';
        this.veiculos = [];
        this.loadingModal = new bootstrap.Modal(document.getElementById('loadingModal'));
        
        this.init();
    }
    
    init() {
        this.setupNavigation();
        this.setupForms();
        this.loadDashboard();
        this.loadVeiculos();
        this.setupDateDefaults();
    }
    
    // Configuração da navegação
    setupNavigation() {
        const navLinks = document.querySelectorAll('.navbar-nav .nav-link');
        
        navLinks.forEach(link => {
            link.addEventListener('click', async (e) => {
                e.preventDefault();
                const section = link.getAttribute('href').substring(1);
                await this.showSection(section);
                
                // Update active nav link
                navLinks.forEach(l => l.classList.remove('active'));
                link.classList.add('active');
            });
        });
    }
    
    // Mostrar seção específica
    async showSection(sectionId) {
        const sections = document.querySelectorAll('.content-section');
        sections.forEach(section => {
            section.style.display = 'none';
        });
        
        const targetSection = document.getElementById(sectionId);
        if (targetSection) {
            targetSection.style.display = 'block';
            this.currentSection = sectionId;
        }
        
        // Carregar seção específica
        switch(sectionId) {
            case 'dashboard':
                this.loadDashboard();
                break;
            case 'relatorios':
                console.log('Loading reports section');
                await this.loadVeiculos(); // Load vehicles first
                await this.loadRelatorios();
                this.populateFilterVeiculoSelect();
                this.populateVeiculoSelects(); // Also populate the report generation select
                break;
            case 'analise':
                this.populateVeiculoSelects();
                break;
            case 'perfis-horario':
                await this.showPerfisHorario();
                break;
        }
    }
    
    // Configuração dos formulários
    setupForms() {
        // Upload form
        document.getElementById('upload-form').addEventListener('submit', (e) => {
            e.preventDefault();
            this.handleUpload();
        });
        
        // Database cleanup button
        const clearDbBtn = document.getElementById('clear-database-btn');
        if (clearDbBtn) {
            console.log('Database cleanup button found, attaching event listener');
            clearDbBtn.addEventListener('click', (e) => {
                e.preventDefault();
                console.log('Database cleanup button clicked');
                this.clearDatabase();
            });
        } else {
            console.error('Database cleanup button not found!');
        }
        
        // Reports filter buttons
        const applyFiltersBtn = document.getElementById('apply-filters-btn');
        if (applyFiltersBtn) {
            console.log('Apply filters button found, attaching event listener');
            applyFiltersBtn.addEventListener('click', (e) => {
                e.preventDefault();
                console.log('Apply filters button clicked');
                this.applyReportsFilters();
            });
        } else {
            console.error('Apply filters button not found!');
        }
        
        const clearFiltersBtn = document.getElementById('clear-filters-btn');
        if (clearFiltersBtn) {
            console.log('Clear filters button found, attaching event listener');
            clearFiltersBtn.addEventListener('click', (e) => {
                e.preventDefault();
                console.log('Clear filters button clicked');
                this.clearReportsFilters();
            });
        } else {
            console.error('Clear filters button not found!');
        }
        
        // Clear reports history button
        const clearReportsBtn = document.getElementById('clear-reports-btn');
        if (clearReportsBtn) {
            console.log('Clear reports button found, attaching event listener');
            clearReportsBtn.addEventListener('click', (e) => {
                e.preventDefault();
                console.log('Clear reports button clicked');
                this.clearReportsHistory();
            });
        } else {
            console.error('Clear reports button not found!');
        }
        
        // Relatório form
        document.getElementById('relatorio-form').addEventListener('submit', (e) => {
            e.preventDefault();
            this.handleRelatorioGeneration();
        });
        
        // Análise form
        document.getElementById('analise-form').addEventListener('submit', (e) => {
            e.preventDefault();
            this.handleAnalise();
        });
    }
    
    // Configurar datas padrão (últimos 30 dias)
    setupDateDefaults() {
        const hoje = new Date();
        const mes_passado = new Date();
        mes_passado.setDate(hoje.getDate() - 30);
        
        const hojeStr = hoje.toISOString().split('T')[0];
        const mesPassadoStr = mes_passado.toISOString().split('T')[0];
        
        // Set default dates
        document.getElementById('data-inicio').value = mesPassadoStr;
        document.getElementById('data-fim').value = hojeStr;
        document.getElementById('analise-inicio').value = mesPassadoStr;
        document.getElementById('analise-fim').value = hojeStr;
    }
    
    // Carregar dashboard
    async loadDashboard() {
        try {
            const response = await axios.get('/api/dashboard/resumo');
            const data = response.data;
            
            // Update stats cards
            document.getElementById('total-clientes').textContent = data.total_clientes.toLocaleString();
            document.getElementById('total-veiculos').textContent = data.total_veiculos.toLocaleString();
            document.getElementById('total-registros').textContent = data.total_registros.toLocaleString();
            document.getElementById('total-relatorios').textContent = data.total_relatorios.toLocaleString();
            
            // Load recent activity
            await this.loadAtividadeRecente();
            await this.loadVeiculosLista();
            
        } catch (error) {
            this.showError('Erro ao carregar dashboard: ' + error.message);
        }
    }
    
    // Carregar atividade recente
    async loadAtividadeRecente() {
        try {
            const response = await axios.get('/api/dashboard/atividade-recente');
            const atividades = response.data;
            
            const container = document.getElementById('atividade-recente');
            
            if (atividades.length === 0) {
                container.innerHTML = '<p class="text-muted">Nenhuma atividade recente.</p>';
                return;
            }
            
            let html = '';
            atividades.forEach(atividade => {
                const data = new Date(atividade.data_evento).toLocaleString();
                html += `
                    <div class="activity-item">
                        <div class="d-flex justify-content-between">
                            <strong>${atividade.placa}</strong>
                            <span class="activity-time">${data}</span>
                        </div>
                        <div class="activity-location">${atividade.endereco}</div>
                        <div class="d-flex justify-content-between">
                            <span>Velocidade: ${atividade.velocidade} km/h</span>
                            <small class="text-muted">${atividade.tipo_evento}</small>
                        </div>
                    </div>
                `;
            });
            
            container.innerHTML = html;
            
        } catch (error) {
            document.getElementById('atividade-recente').innerHTML = 
                '<p class="text-danger">Erro ao carregar atividades.</p>';
        }
    }
    
    // Carregar lista de veículos para dashboard
    async loadVeiculosLista() {
        try {
            const response = await axios.get('/api/veiculos');
            const veiculos = response.data;
            
            const container = document.getElementById('veiculos-lista');
            
            if (veiculos.length === 0) {
                container.innerHTML = '<p class="text-muted">Nenhum veículo cadastrado.</p>';
                return;
            }
            
            let html = '';
            veiculos.forEach(veiculo => {
                html += `
                    <div class="vehicle-item">
                        <div class="vehicle-plate">${veiculo.placa}</div>
                        <div class="vehicle-client">${veiculo.cliente}</div>
                        <div class="text-muted small">Cadastrado em: ${new Date(veiculo.created_at).toLocaleDateString()}</div>
                    </div>
                `;
            });
            
            container.innerHTML = html;
            
        } catch (error) {
            document.getElementById('veiculos-lista').innerHTML = 
                '<p class="text-danger">Erro ao carregar veículos.</p>';
        }
    }
    
    // Carregar veículos para selects
    async loadVeiculos() {
        try {
            const response = await axios.get('/api/veiculos');
            this.veiculos = response.data;
        } catch (error) {
            console.error('Erro ao carregar veículos:', error);
        }
    }
    
    // Popular select de veículos para filtros
    populateFilterVeiculoSelect() {
        console.log('Populating filter vehicle select, vehicles count:', this.veiculos.length);
        const select = document.getElementById('filter-veiculo');
        if (!select) {
            console.error('filter-veiculo select not found');
            return;
        }
        
        // Clear existing options (except first)
        while (select.children.length > 1) {
            select.removeChild(select.lastChild);
        }
        
        // Add vehicle options
        this.veiculos.forEach(veiculo => {
            const option = document.createElement('option');
            option.value = veiculo.placa;
            option.textContent = `${veiculo.placa} - ${veiculo.cliente}`;
            select.appendChild(option);
        });
        
        console.log(`Populated filter select with ${this.veiculos.length} vehicles`);
    }
    
    // Aplicar filtros nos relatórios
    async applyReportsFilters() {
        try {
            console.log('Applying reports filters');
            const veiculo = document.getElementById('filter-veiculo').value;
            const data = document.getElementById('filter-data').value;
            console.log('Filter values:', { veiculo, data });
            
            await this.loadRelatorios(veiculo, data);
        } catch (error) {
            console.error('Error applying filters:', error);
            this.showError('Erro ao aplicar filtros: ' + error.message);
        }
    }
    
    // Limpar filtros dos relatórios
    async clearReportsFilters() {
        try {
            console.log('Clearing reports filters');
            const filterVeiculo = document.getElementById('filter-veiculo');
            const filterData = document.getElementById('filter-data');
            
            if (filterVeiculo) {
                filterVeiculo.value = '';
                console.log('Vehicle filter cleared');
            } else {
                console.error('filter-veiculo element not found');
            }
            
            if (filterData) {
                filterData.value = '';
                console.log('Date filter cleared');
            } else {
                console.error('filter-data element not found');
            }
            
            await this.loadRelatorios();
            console.log('Reports reloaded without filters');
        } catch (error) {
            console.error('Error clearing filters:', error);
            this.showError('Erro ao limpar filtros: ' + error.message);
        }
    }
    
    // Limpar histórico de relatórios
    async clearReportsHistory() {
        try {
            console.log('Clearing reports history');
            
            // Confirmação
            if (!confirm('⚠️ Atenção: Esta ação irá excluir TODOS os relatórios salvos. Deseja continuar?')) {
                console.log('Reports history clear cancelled');
                return;
            }
            
            this.showLoading(true);
            
            const response = await axios.delete('/api/relatorios/clear');
            console.log('Clear reports API response:', response);
            const data = response.data;
            
            if (data.success) {
                this.showSuccess(`✅ ${data.message}`);
                // Recarrega a lista de relatórios
                await this.loadRelatorios();
            } else {
                throw new Error(data.message || 'Erro ao limpar histórico');
            }
            
        } catch (error) {
            console.error('Error clearing reports history:', error);
            this.showError('Erro ao limpar histórico: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }
    
    // Popular selects de veículos
    populateVeiculoSelects() {
        console.log('Populating vehicle selects, vehicles count:', this.veiculos.length);
        const selects = ['veiculo-placa', 'analise-veiculo'];
        
        selects.forEach(selectId => {
            const select = document.getElementById(selectId);
            if (!select) {
                console.warn(`Select element ${selectId} not found`);
                return;
            }
            
            console.log(`Populating select: ${selectId}`);
            
            // Clear existing options (except first)
            while (select.children.length > 1) {
                select.removeChild(select.lastChild);
            }
            
            // Add "All Vehicles" option for report generation
            if (selectId === 'veiculo-placa') {
                const allOption = document.createElement('option');
                allOption.value = 'TODOS';
                allOption.textContent = '📊 Todos os Veículos (Relatório Consolidado)';
                select.appendChild(allOption);
            }
            
            // Add vehicle options
            this.veiculos.forEach(veiculo => {
                const option = document.createElement('option');
                option.value = veiculo.placa;
                option.textContent = `${veiculo.placa} - ${veiculo.cliente}`;
                select.appendChild(option);
            });
            
            console.log(`Populated ${selectId} with ${this.veiculos.length} vehicles`);
        });
    }
    
    // Handle database cleanup
    async clearDatabase() {
        console.log('clearDatabase function called');
        
        // Show confirmation dialog
        if (!confirm('⚠️ ATENÇÃO: Esta ação irá excluir TODOS os dados dos veículos e registros de telemetria do banco de dados. Esta ação NÃO pode ser desfeita. Deseja continuar?')) {
            console.log('First confirmation cancelled');
            return;
        }
        
        // Second confirmation
        if (!confirm('Confirma a exclusão de TODOS os dados? Digite "CONFIRMAR" na próxima janela para prosseguir.')) {
            console.log('Second confirmation cancelled');
            return;
        }
        
        const confirmation = prompt('Digite "CONFIRMAR" para excluir todos os dados:');
        if (confirmation !== 'CONFIRMAR') {
            console.log('Final confirmation cancelled or invalid:', confirmation);
            alert('Operação cancelada.');
            return;
        }
        
        console.log('All confirmations passed, making API call');
        this.showLoading(true);
        
        try {
            const response = await axios.delete('/api/database/clear');
            console.log('API response:', response);
            const data = response.data;
            
            if (data.success) {
                this.showSuccess('✅ Banco de dados limpo com sucesso! ' + data.message);
                
                // Reload all data
                await this.loadDashboard();
                await this.loadVeiculos();
                this.populateVeiculoSelects();
                
                // Clear any displayed results
                document.getElementById('upload-result').innerHTML = '';
                document.getElementById('analise-resultado').innerHTML = '';
                
            } else {
                throw new Error(data.message || 'Erro na limpeza do banco');
            }
            
        } catch (error) {
            console.error('Error during database cleanup:', error);
            this.showError('Erro ao limpar banco de dados: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }
    
    // Handle upload de CSV
    async handleUpload() {
        const form = document.getElementById('upload-form');
        const formData = new FormData(form);
        const resultDiv = document.getElementById('upload-result');
        
        this.showLoading(true);
        resultDiv.innerHTML = '<div class="spinner-border spinner-border-sm"></div> Processando...';
        
        try {
            const response = await axios.post('/api/upload-csv', formData, {
                headers: {
                    'Content-Type': 'multipart/form-data'
                }
            });
            
            const data = response.data;
            
            if (data.success) {
                let html = '<div class="alert alert-success">Arquivos processados com sucesso!</div>';
                
                Object.entries(data.results).forEach(([filename, result]) => {
                    if (result.success) {
                        html += `
                            <div class="border rounded p-2 mb-2">
                                <strong>${filename}</strong>
                                <br><small>Registros: ${result.records_processed}</small>
                            </div>
                        `;
                    } else {
                        html += `
                            <div class="border border-danger rounded p-2 mb-2">
                                <strong>${filename}</strong>
                                <br><small class="text-danger">Erro: ${result.error}</small>
                            </div>
                        `;
                    }
                });
                
                resultDiv.innerHTML = html;
                
                // Reset form and reload data
                form.reset();
                await this.loadDashboard();
                await this.loadVeiculos();
                
            } else {
                throw new Error(data.message || 'Erro no processamento');
            }
            
        } catch (error) {
            resultDiv.innerHTML = `<div class="alert alert-danger">Erro: ${error.message}</div>`;
        } finally {
            this.showLoading(false);
        }
    }
    
    // Handle geração de relatório
    async handleRelatorioGeneration() {
        const form = document.getElementById('relatorio-form');
        const formData = new FormData(form);
        
        this.showLoading(true);
        
        try {
            const response = await axios.post(`/api/relatorio/${formData.get('placa')}`, formData);
            const data = response.data;
            
            if (data.success) {
                this.showSuccess(`Relatório gerado com sucesso! Tamanho: ${data.file_size_mb} MB`);
                
                // Download automatically
                window.open(data.download_url, '_blank');
                
                // Reload reports list
                await this.loadRelatorios();
                
            } else {
                throw new Error(data.message || 'Erro na geração');
            }
            
        } catch (error) {
            this.showError('Erro ao gerar relatório: ' + error.message);
        } finally {
            this.showLoading(false);
        }
    }
    
    // Handle análise
    async handleAnalise() {
        const form = document.getElementById('analise-form');
        const formData = new FormData(form);
        const resultDiv = document.getElementById('analise-resultado');
        
        this.showLoading(true);
        resultDiv.innerHTML = '<div class="text-center"><div class="spinner-border"></div></div>';
        
        try {
            const placa = formData.get('placa');
            const dataInicio = formData.get('data_inicio');
            const dataFim = formData.get('data_fim');
            
            const response = await axios.get(`/api/analise/${placa}`, {
                params: {
                    data_inicio: dataInicio + 'T00:00:00',
                    data_fim: dataFim + 'T23:59:59'
                }
            });
            
            const data = response.data;
            
            if (data.success) {
                this.renderAnalysisResults(data, resultDiv);
            } else {
                throw new Error(data.message || 'Erro na análise');
            }
            
        } catch (error) {
            resultDiv.innerHTML = `<div class="alert alert-danger">Erro: ${error.message}</div>`;
        } finally {
            this.showLoading(false);
        }
    }
    
    // Renderizar resultados da análise
    renderAnalysisResults(data, container) {
        const metrics = data.metrics;
        const insights = data.insights;
        
        let html = '';
        
        // Metrics summary
        if (metrics.operacao) {
            html += `
                <div class="row mb-4">
                    <div class="col-md-3">
                        <div class="metric-card">
                            <div class="metric-value">${metrics.operacao.km_total.toFixed(2)}</div>
                            <div class="metric-label">Km Percorridos</div>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="metric-card">
                            <div class="metric-value">${metrics.operacao.velocidade_maxima}</div>
                            <div class="metric-label">Vel. Máxima (km/h)</div>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="metric-card">
                            <div class="metric-value">${metrics.operacao.velocidade_media.toFixed(1)}</div>
                            <div class="metric-label">Vel. Média (km/h)</div>
                        </div>
                    </div>
                    <div class="col-md-3">
                        <div class="metric-card">
                            <div class="metric-value">${metrics.operacao.tempo_em_movimento}</div>
                            <div class="metric-label">Tempo Movimento</div>
                        </div>
                    </div>
                </div>
            `;
        }
        
        // Insights
        if (insights && insights.length > 0) {
            html += '<h5>Insights e Recomendações:</h5>';
            insights.forEach(insight => {
                let className = 'info';
                if (insight.includes('🚨') || insight.includes('⚠️')) className = 'danger';
                else if (insight.includes('✅')) className = 'success';
                else if (insight.includes('⛽') || insight.includes('📊')) className = 'warning';
                
                html += `<div class="insight-item ${className}">${insight}</div>`;
            });
        }
        
        // Charts
        if (data.charts) {
            if (data.charts.speed_chart) {
                html += '<div class="chart-container">' + data.charts.speed_chart + '</div>';
            }
            if (data.charts.route_map) {
                html += '<div class="chart-container">' + data.charts.route_map + '</div>';
            }
        }
        
        container.innerHTML = html;
    }
    
    // Carregar lista de relatórios
    async loadRelatorios(filterVeiculo = '', filterData = '') {
        try {
            console.log('Loading relatorios with filters:', { filterVeiculo, filterData });
            let url = '/api/relatorios';
            const params = new URLSearchParams();
            
            if (filterVeiculo) {
                params.append('veiculo', filterVeiculo);
            }
            if (filterData) {
                params.append('data', filterData);
            }
            
            if (params.toString()) {
                url += '?' + params.toString();
            }
            
            console.log('Making request to:', url);
            const response = await axios.get(url);
            const relatorios = response.data;
            console.log('Received reports:', relatorios);
            
            const container = document.getElementById('relatorios-lista');
            
            if (relatorios.length === 0) {
                container.innerHTML = '<p class="text-muted">Nenhum relatório encontrado.</p>';
                return;
            }
            
            let html = '';
            relatorios.forEach(relatorio => {
                const data = new Date(relatorio.created_at).toLocaleString();
                const downloadUrl = relatorio.download_url || `/api/relatorio/download/${relatorio.id}`;
                
                html += `
                    <div class="report-item">
                        <div class="report-info">
                            <h6>${relatorio.filename || relatorio.placa + '_relatorio.pdf'}</h6>
                            <div class="report-meta">
                                Veículo: ${relatorio.placa || 'N/A'}<br>
                                Criado em: ${data}<br>
                                Tamanho: ${relatorio.size_mb || 'N/A'} MB
                            </div>
                        </div>
                        <div>
                            <a href="${downloadUrl}" class="btn btn-sm btn-primary" target="_blank">
                                <i class="fas fa-download me-1"></i>Download
                            </a>
                        </div>
                    </div>
                `;
            });
            
            container.innerHTML = html;
            console.log('Reports list updated successfully');
            
        } catch (error) {
            console.error('Error loading reports:', error);
            document.getElementById('relatorios-lista').innerHTML = 
                '<p class="text-danger">Erro ao carregar relatórios: ' + error.message + '</p>';
        }
    }
    
    // Utility methods
    showLoading(show) {
        if (show) {
            this.loadingModal.show();
        } else {
            this.loadingModal.hide();
        }
    }
    
    showSuccess(message) {
        this.showAlert(message, 'success');
    }
    
    showError(message) {
        this.showAlert(message, 'danger');
    }
    
    showAlert(message, type) {
        const alertDiv = document.createElement('div');
        alertDiv.className = `alert alert-${type} alert-dismissible fade show`;
        alertDiv.innerHTML = `
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        `;
        
        // Insert at top of current section
        const currentSectionEl = document.getElementById(this.currentSection);
        currentSectionEl.insertBefore(alertDiv, currentSectionEl.firstChild);
        
        // Auto-remove after 5 seconds
        setTimeout(() => {
            if (alertDiv.parentNode) {
                alertDiv.remove();
            }
        }, 5000);
    }
    
    // ====== PERFIS DE HORÁRIO ======
    
    async showPerfisHorario() {
        await this.loadClientes();
        this.populateClientePerfisSelect();
        this.setupPerfisEventListeners();
    }
    
    populateClientePerfisSelect() {
        const select = document.getElementById('cliente-perfil-select');
        select.innerHTML = '<option value="">Selecione um cliente...</option>';
        
        this.clientes.forEach(cliente => {
            const option = document.createElement('option');
            option.value = cliente.id;
            option.textContent = cliente.nome;
            select.appendChild(option);
        });
    }
    
    setupPerfisEventListeners() {
        // Cliente selection change
        document.getElementById('cliente-perfil-select').addEventListener('change', (e) => {
            const clienteId = e.target.value;
            const novoPerfil = document.getElementById('novo-perfil-btn');
            const container = document.getElementById('perfis-container');
            
            if (clienteId) {
                novoPerfil.disabled = false;
                this.loadPerfilsHorario(clienteId);
                container.style.display = 'block';
            } else {
                novoPerfil.disabled = true;
                container.style.display = 'none';
            }
        });
        
        // Novo perfil button
        document.getElementById('novo-perfil-btn').addEventListener('click', () => {
            this.openPerfilModal();
        });
        
        // Perfil form submission
        document.getElementById('perfil-form').addEventListener('submit', (e) => {
            e.preventDefault();
            this.handlePerfilSubmit();
        });
        
        // Color picker sync
        document.getElementById('perfil-cor').addEventListener('input', (e) => {
            document.getElementById('perfil-cor-hex').value = e.target.value;
        });
        
        document.getElementById('perfil-cor-hex').addEventListener('input', (e) => {
            document.getElementById('perfil-cor').value = e.target.value;
        });
    }
    
    async loadPerfilsHorario(clienteId) {
        try {
            const response = await axios.get(`/api/perfis-horario/${clienteId}`);
            this.renderPerfisHorario(response.data);
        } catch (error) {
            console.error('Erro ao carregar perfis:', error);
            this.showToast('Erro ao carregar perfis de horário', 'error');
        }
    }
    
    renderPerfisHorario(perfis) {
        const container = document.getElementById('perfis-list');
        const count = document.getElementById('perfis-count');
        
        count.textContent = `${perfis.length} perfis`;
        
        if (perfis.length === 0) {
            container.innerHTML = `
                <div class="text-center text-muted py-4">
                    <i class="fas fa-clock fa-3x mb-3"></i>
                    <p>Nenhum perfil de horário configurado para este cliente.</p>
                    <p>Clique em "Criar Novo Perfil" para começar.</p>
                </div>
            `;
            return;
        }
        
        const perfisHtml = perfis.map(perfil => `
            <div class="card mb-3 ${!perfil.ativo ? 'opacity-50' : ''}">
                <div class="card-body">
                    <div class="row align-items-center">
                        <div class="col-md-2">
                            <div class="d-flex align-items-center">
                                <div style="width: 20px; height: 20px; background-color: ${perfil.cor_relatorio}; border-radius: 3px; margin-right: 10px;"></div>
                                <strong>${perfil.nome}</strong>
                            </div>
                        </div>
                        <div class="col-md-3">
                            <small class="text-muted">Horário:</small><br>
                            <span class="badge bg-secondary">${perfil.hora_inicio} - ${perfil.hora_fim}</span>
                        </div>
                        <div class="col-md-2">
                            <small class="text-muted">Tipo:</small><br>
                            <span class="badge ${perfil.tipo_periodo === 'operacional' ? 'bg-success' : perfil.tipo_periodo === 'fora_horario' ? 'bg-warning' : 'bg-info'}">${this.formatTipoPeriodo(perfil.tipo_periodo)}</span>
                        </div>
                        <div class="col-md-3">
                            <small class="text-muted">Descrição:</small><br>
                            <span>${perfil.descricao || 'Sem descrição'}</span>
                        </div>
                        <div class="col-md-2 text-end">
                            <div class="btn-group" role="group">
                                <button class="btn btn-outline-primary btn-sm" onclick="telemetriaApp.editPerfil(${perfil.id})" title="Editar">
                                    <i class="fas fa-edit"></i>
                                </button>
                                <button class="btn btn-outline-${perfil.ativo ? 'warning' : 'success'} btn-sm" 
                                        onclick="telemetriaApp.togglePerfil(${perfil.id})" 
                                        title="${perfil.ativo ? 'Desativar' : 'Ativar'}">
                                    <i class="fas fa-${perfil.ativo ? 'pause' : 'play'}"></i>
                                </button>
                                <button class="btn btn-outline-danger btn-sm" onclick="telemetriaApp.deletePerfil(${perfil.id})" title="Excluir">
                                    <i class="fas fa-trash"></i>
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        `).join('');
        
        container.innerHTML = perfisHtml;
    }
    
    formatTipoPeriodo(tipo) {
        switch(tipo) {
            case 'operacional': return 'Operacional';
            case 'fora_horario': return 'Fora do Horário';
            case 'especial': return 'Especial';
            default: return tipo;
        }
    }
    
    openPerfilModal(perfilData = null) {
        const modal = new bootstrap.Modal(document.getElementById('perfilModal'));
        const title = document.getElementById('perfilModalTitle');
        const form = document.getElementById('perfil-form');
        
        if (perfilData) {
            // Editing existing perfil
            title.innerHTML = '<i class="fas fa-edit me-2"></i>Editar Perfil de Horário';
            this.populatePerfilForm(perfilData);
        } else {
            // Creating new perfil
            title.innerHTML = '<i class="fas fa-plus me-2"></i>Novo Perfil de Horário';
            form.reset();
            const clienteId = document.getElementById('cliente-perfil-select').value;
            document.getElementById('perfil-cliente-id').value = clienteId;
            document.getElementById('perfil-cor').value = '#28a745';
            document.getElementById('perfil-cor-hex').value = '#28a745';
            document.getElementById('perfil-ativo').checked = true;
        }
        
        modal.show();
    }
    
    populatePerfilForm(perfil) {
        document.getElementById('perfil-id').value = perfil.id;
        document.getElementById('perfil-cliente-id').value = perfil.cliente_id;
        document.getElementById('perfil-nome').value = perfil.nome;
        document.getElementById('perfil-descricao').value = perfil.descricao || '';
        document.getElementById('perfil-hora-inicio').value = perfil.hora_inicio;
        document.getElementById('perfil-hora-fim').value = perfil.hora_fim;
        document.getElementById('perfil-tipo').value = perfil.tipo_periodo;
        document.getElementById('perfil-cor').value = perfil.cor_relatorio;
        document.getElementById('perfil-cor-hex').value = perfil.cor_relatorio;
        document.getElementById('perfil-ativo').checked = perfil.ativo;
    }
    
    async handlePerfilSubmit() {
        const formData = new FormData();
        const perfilId = document.getElementById('perfil-id').value;
        
        formData.append('cliente_id', document.getElementById('perfil-cliente-id').value);
        formData.append('nome', document.getElementById('perfil-nome').value);
        formData.append('descricao', document.getElementById('perfil-descricao').value);
        formData.append('hora_inicio', document.getElementById('perfil-hora-inicio').value);
        formData.append('hora_fim', document.getElementById('perfil-hora-fim').value);
        formData.append('tipo_periodo', document.getElementById('perfil-tipo').value);
        formData.append('cor_relatorio', document.getElementById('perfil-cor-hex').value);
        formData.append('ativo', document.getElementById('perfil-ativo').checked);
        
        try {
            let response;
            if (perfilId) {
                // Update existing perfil
                response = await axios.put(`/api/perfis-horario/${perfilId}`, formData);
            } else {
                // Create new perfil
                response = await axios.post('/api/perfis-horario', formData);
            }
            
            if (response.data.success) {
                this.showSuccess(response.data.message);
                const modal = bootstrap.Modal.getInstance(document.getElementById('perfilModal'));
                modal.hide();
                
                // Reload perfis
                const clienteId = document.getElementById('cliente-perfil-select').value;
                this.loadPerfilsHorario(clienteId);
            }
        } catch (error) {
            console.error('Erro ao salvar perfil:', error);
            const message = error.response?.data?.detail || 'Erro ao salvar perfil';
            this.showError(message);
        }
    }
    
    async editPerfil(perfilId) {
        try {
            const clienteId = document.getElementById('cliente-perfil-select').value;
            const response = await axios.get(`/api/perfis-horario/${clienteId}`);
            const perfil = response.data.find(p => p.id === perfilId);
            
            if (perfil) {
                this.openPerfilModal(perfil);
            }
        } catch (error) {
            console.error('Erro ao carregar perfil:', error);
            this.showError('Erro ao carregar dados do perfil');
        }
    }
    
    async togglePerfil(perfilId) {
        try {
            const response = await axios.patch(`/api/perfis-horario/${perfilId}/toggle`);
            
            if (response.data.success) {
                this.showSuccess(response.data.message);
                
                // Reload perfis
                const clienteId = document.getElementById('cliente-perfil-select').value;
                this.loadPerfilsHorario(clienteId);
            }
        } catch (error) {
            console.error('Erro ao alterar status do perfil:', error);
            this.showError('Erro ao alterar status do perfil');
        }
    }
    
    async deletePerfil(perfilId) {
        if (!confirm('Tem certeza que deseja excluir este perfil de horário?')) {
            return;
        }
        
        try {
            const response = await axios.delete(`/api/perfis-horario/${perfilId}`);
            
            if (response.data.success) {
                this.showSuccess(response.data.message);
                
                // Reload perfis
                const clienteId = document.getElementById('cliente-perfil-select').value;
                this.loadPerfilsHorario(clienteId);
            }
        } catch (error) {
            console.error('Erro ao excluir perfil:', error);
            this.showError('Erro ao excluir perfil');
        }
    }
}

// Initialize app when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    window.telemetriaApp = new TelemetriaApp();
});