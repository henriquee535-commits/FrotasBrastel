import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from datetime import datetime, timedelta
import io
import plotly.express as px
import plotly.graph_objects as go
import math

# ==============================================================================
# 1. CONFIGURAÇÃO GERAL E ESTILIZAÇÃO DA INTERFACE
# ==============================================================================
st.set_page_config(page_title="ERP Frota Brastel", layout="wide", page_icon="🚙")

st.markdown("""
<style>
    /* Estilização Avançada para Dashboards e Cards */
    .reportview-container .main .block-container{ max-width: 100%; padding-top: 2rem; }
    .metric-card { 
        background-color: #ffffff; padding: 20px; border-radius: 8px; 
        border-left: 6px solid #0052cc; box-shadow: 0 2px 4px rgba(0,0,0,0.1); 
        margin-bottom: 15px; transition: transform 0.2s;
    }
    .metric-card:hover { transform: translateY(-3px); box-shadow: 0 4px 8px rgba(0,0,0,0.15); }
    h1, h2, h3 { color: #2c3e50; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
    .stButton>button { border-radius: 6px; font-weight: 600; }
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# 2. CONEXÃO AO BANCO DE DADOS E FUNÇÕES CORE
# ==============================================================================
DATABASE_URL = st.secrets.get("DATABASE_URL")

@contextmanager
def get_conn():
    """Gerenciador de contexto para ligação segura com PostgreSQL via psycopg2."""
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def execute_query(query, params=None, fetch=False):
    """Executa queries de forma isolada e segura."""
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute(query, params)
            if fetch:
                return c.fetchall()
            return None

def registrar_log(acao, tabela, registro_id, detalhes, usuario="Admin"):
    """Motor de auditoria do sistema. Regista qualquer mudança estrutural."""
    q = """INSERT INTO historico_movimentacoes 
           (tipo_acao, tabela_afetada, registro_identificador, detalhes, usuario) 
           VALUES (%s, %s, %s, %s, %s)"""
    execute_query(q, (acao, tabela, str(registro_id), detalhes, usuario))

# ==============================================================================
# 3. MIGRAÇÃO E AUTO-CURA NUCLEAR DO BANCO DE DADOS (SCHEMA)
# ==============================================================================
def db_migration():
    """
    Constrói a estrutura relacional do ERP de Frota. 
    Possui sistema de auto-cura: recria as tabelas se estiverem desatualizadas.
    """
    needs_wipe = False
    
    # 1. TESTE DE VERSÃO: Verifica se a tabela já possui a coluna nova
    with get_conn() as conn:
        with conn.cursor() as c:
            try:
                c.execute("SELECT custo_total FROM manutencoes LIMIT 1")
            except Exception:
                # Se der erro, a base de dados está na versão antiga.
                conn.rollback()
                needs_wipe = True

    # 2. LIMPEZA (Se necessário, apaga as tabelas antigas em cascata)
    if needs_wipe:
        with get_conn() as conn:
            with conn.cursor() as c:
                tabelas = ["diario_bordo", "manutencoes", "sinistros", "multas", "veiculos", 
                           "condutores", "locadoras", "centros_custo", "categorias_veiculo", 
                           "dre_rateios", "historico_movimentacoes"]
                for tab in tabelas:
                    try:
                        c.execute(f"DROP TABLE IF EXISTS {tab} CASCADE")
                    except Exception:
                        conn.rollback()

    # 3. CRIAÇÃO DA ESTRUTURA OFICIAL ATUALIZADA
    queries = [
        "CREATE TABLE IF NOT EXISTS centros_custo (nome TEXT PRIMARY KEY)",
        "CREATE TABLE IF NOT EXISTS locadoras (nome TEXT PRIMARY KEY)",
        "CREATE TABLE IF NOT EXISTS categorias_veiculo (nome TEXT PRIMARY KEY)",
        
        """CREATE TABLE IF NOT EXISTS historico_movimentacoes (
            id SERIAL PRIMARY KEY, data_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            tipo_acao TEXT, tabela_afetada TEXT, registro_identificador TEXT, 
            detalhes TEXT, usuario TEXT
        )""",
        
        """CREATE TABLE IF NOT EXISTS veiculos (
            id SERIAL PRIMARY KEY, placa TEXT UNIQUE NOT NULL, modelo TEXT, 
            categoria TEXT, tipo_frota TEXT CHECK (tipo_frota IN ('Alugada Mensal', 'Alugada Diária', 'Própria', 'Colaborador', 'Indefinido')),
            locadora TEXT REFERENCES locadoras(nome), cc_atual TEXT REFERENCES centros_custo(nome), 
            valor_mensal NUMERIC(15,2) DEFAULT 0, km_franquia INTEGER DEFAULT 0,
            ano_fabricacao INTEGER, tag_pedagio TEXT, cartao_combustivel TEXT,
            status TEXT DEFAULT 'Disponível', km_atual INTEGER DEFAULT 0,
            data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""",
        
        """CREATE TABLE IF NOT EXISTS condutores (
            id SERIAL PRIMARY KEY, nome TEXT NOT NULL, cnh TEXT UNIQUE, 
            validade_cnh DATE, cc_padrao TEXT REFERENCES centros_custo(nome),
            status TEXT DEFAULT 'Ativo'
        )""",
        
        """CREATE TABLE IF NOT EXISTS diario_bordo (
            id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id), 
            condutor_id INTEGER REFERENCES condutores(id), cc_viagem TEXT REFERENCES centros_custo(nome), 
            km_saida INTEGER NOT NULL, data_saida TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            km_retorno INTEGER, data_retorno TIMESTAMP, status TEXT DEFAULT 'Em Andamento', observacoes TEXT
        )""",
        
        """CREATE TABLE IF NOT EXISTS multas (
            id SERIAL PRIMARY KEY, veiculo_placa TEXT, condutor_id INTEGER REFERENCES condutores(id),
            ait TEXT UNIQUE, data_infracao DATE, hora_infracao TEXT, codigo_infracao TEXT,
            descricao TEXT, categoria_infracao TEXT, pontuacao INTEGER DEFAULT 0,
            valor NUMERIC(10,2) DEFAULT 0, status_pagamento TEXT DEFAULT 'A Pagar', status_recurso TEXT DEFAULT 'Não Iniciado'
        )""",
        
        """CREATE TABLE IF NOT EXISTS manutencoes (
            id SERIAL PRIMARY KEY, veiculo_placa TEXT, condutor_solicitante TEXT,
            tipo TEXT CHECK (tipo IN ('PREVENTIVA', 'CORRETIVA', 'REVISÃO', 'OUTROS')), 
            data_solicitacao DATE, data_agendada DATE, data_liberacao DATE, 
            km_manutencao INTEGER, descricao TEXT, custo_total NUMERIC(10,2) DEFAULT 0, status TEXT DEFAULT 'PENDENTE'
        )""",
        
        """CREATE TABLE IF NOT EXISTS sinistros (
            id SERIAL PRIMARY KEY, veiculo_placa TEXT, condutor TEXT, data_sinistro DATE, 
            local_sinistro TEXT, boletim_ocorrencia TEXT, gravidade TEXT,
            terceiros_envolvidos BOOLEAN DEFAULT FALSE, descricao_dano TEXT, 
            custo_interno NUMERIC(10,2) DEFAULT 0, status_reparo TEXT DEFAULT 'Aguardando Análise', observacoes TEXT
        )""",
        
        """CREATE TABLE IF NOT EXISTS dre_rateios (
            id SERIAL PRIMARY KEY, competencia VARCHAR(7), cc_nome TEXT,
            custo_alocacao NUMERIC(15,2), data_processamento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )"""
    ]
    with get_conn() as conn:
        with conn.cursor() as c:
            for q in queries:
                c.execute(q)

# Tenta criar as tabelas antes de iniciar a aplicação
try:
    db_migration()
except Exception as e:
    st.error(f"Erro na inicialização da Base de Dados: {e}")

# ==============================================================================
# 4. FUNÇÕES UTILITÁRIAS E HELPERS
# ==============================================================================
def fuso_br(): 
    """Retorna a data e hora atual no fuso do Brasil (UTC-3)."""
    return datetime.utcnow() - timedelta(hours=3)

def parse_date(date_str):
    """Converte strings de data variadas para formato YYYY-MM-DD com segurança."""
    if pd.isna(date_str) or not str(date_str).strip(): return None
    try:
        return pd.to_datetime(date_str).date()
    except:
        return None

def format_currency(value):
    """Formata float para moeda BRL."""
    if value is None: return "R$ 0,00"
    return f"R$ {value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def gerar_excel_bytes(df):
    """Converte um DataFrame Pandas em bytes de um ficheiro Excel para download."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Relatorio_Frota')
    return output.getvalue()

# Carregamento de Listas Base para os formulários
try:
    lista_cc = [r['nome'] for r in execute_query("SELECT nome FROM centros_custo ORDER BY nome", fetch=True)]
    lista_locadoras = [r['nome'] for r in execute_query("SELECT nome FROM locadoras ORDER BY nome", fetch=True)]
except:
    lista_cc = []
    lista_locadoras = []

# ==============================================================================
# 5. ESTRUTURA DE NAVEGAÇÃO MULTI-PÁGINAS (SIDEBAR)
# ==============================================================================
st.sidebar.image("https://cdn-icons-png.flaticon.com/512/3085/3085330.png", width=100)
st.sidebar.title("ERP Frota")
st.sidebar.markdown("---")

menu = st.sidebar.radio("Navegação do Sistema:", [
    "📊 Dashboard Gerencial", 
    "📋 Portaria (Diário de Bordo)", 
    "🛠️ Operação: Manutenções",
    "🚨 Operação: Sinistros & Multas", 
    "🔄 Movimentação & Logística", 
    "💰 Controladoria: Rateio DRE", 
    "⚙️ ETL / Importação em Massa",
    "🗂️ Cadastros e Relatórios"
])

st.sidebar.markdown("---")
st.sidebar.caption(f"Sessão iniciada em: {fuso_br().strftime('%d/%m/%Y %H:%M')}")

# ==============================================================================
# MÓDULO 1: DASHBOARD GERENCIAL (Análise Gráfica)
# ==============================================================================
if menu == "📊 Dashboard Gerencial":
    st.title("Painel de Controlo da Frota")
    
    try:
        kpi_query = """
            SELECT 
                (SELECT COUNT(*) FROM veiculos) as total_frota,
                (SELECT COUNT(*) FROM veiculos WHERE status='Em Uso') as em_uso,
                (SELECT COUNT(*) FROM veiculos WHERE status='Manutenção') as em_manutencao,
                (SELECT COALESCE(SUM(valor), 0) FROM multas WHERE status_pagamento='A Pagar') as multas_valor,
                (SELECT COUNT(*) FROM manutencoes WHERE status='PENDENTE' OR status='EM ANDAMENTO') as manut_pendentes
        """
        kpis = execute_query(kpi_query, fetch=True)[0]
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown(f"<div class='metric-card'><h3>🚗 Total Frota</h3><h2>{kpis['total_frota']}</h2></div>", unsafe_allow_html=True)
        with col2:
            st.markdown(f"<div class='metric-card'><h3>🛣️ Em Uso (Rua)</h3><h2>{kpis['em_uso']}</h2></div>", unsafe_allow_html=True)
        with col3:
            st.markdown(f"<div class='metric-card'><h3>🔧 Manutenções Abertas</h3><h2>{kpis['manut_pendentes']}</h2></div>", unsafe_allow_html=True)
        with col4:
            st.markdown(f"<div class='metric-card'><h3>💸 Multas Pendentes</h3><h2>{format_currency(kpis['multas_valor'])}</h2></div>", unsafe_allow_html=True)

        st.markdown("---")
        
        g_col1, g_col2 = st.columns(2)
        with g_col1:
            st.subheader("Distribuição por Tipo de Frota")
            df_tipo = pd.DataFrame(execute_query("SELECT tipo_frota, COUNT(*) as qtd FROM veiculos GROUP BY tipo_frota", fetch=True))
            if not df_tipo.empty:
                fig_tipo = px.pie(df_tipo, values='qtd', names='tipo_frota', hole=0.4, color_discrete_sequence=px.colors.qualitative.Pastel)
                st.plotly_chart(fig_tipo, use_container_width=True)
            else:
                st.info("Sem dados de frota suficientes.")

        with g_col2:
            st.subheader("Veículos por Locadora / Propriedade")
            df_loc = pd.DataFrame(execute_query("SELECT locadora, COUNT(*) as qtd FROM veiculos GROUP BY locadora", fetch=True))
            if not df_loc.empty:
                fig_loc = px.bar(df_loc, x='locadora', y='qtd', text='qtd', color='locadora', template="plotly_white")
                fig_loc.update_traces(textposition='outside')
                st.plotly_chart(fig_loc, use_container_width=True)
            else:
                st.info("Sem dados de locadoras suficientes.")

        st.subheader("Atenção Requerida (Manutenções Recentes)")
        df_atencao = pd.DataFrame(execute_query("SELECT veiculo_placa, tipo, data_solicitacao, status FROM manutencoes WHERE status != 'CONCLUÍDA' ORDER BY data_solicitacao DESC LIMIT 10", fetch=True))
        if not df_atencao.empty:
            st.dataframe(df_atencao, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Erro ao carregar Dashboard: {e}")

# ==============================================================================
# MÓDULO 2: PORTARIA E DIÁRIO DE BORDO
# ==============================================================================
elif menu == "📋 Portaria (Diário de Bordo)":
    st.title("Controlo de Pátio e Logística")
    aba_saida, aba_retorno, aba_historico = st.tabs(["🚀 Despachar Veículo", "📥 Receber Veículo", "📜 Histórico de Movimentação"])
    
    v_disp = execute_query("SELECT id, placa, modelo, km_atual FROM veiculos WHERE status='Disponível' ORDER BY placa", fetch=True)
    c_atv = execute_query("SELECT id, nome, cnh, validade_cnh FROM condutores WHERE status='Ativo' ORDER BY nome", fetch=True)
    
    with aba_saida:
        st.subheader("Registo de Saída")
        with st.form("form_saida"):
            c1, c2 = st.columns(2)
            v_sel = c1.selectbox("Selecione o Veículo:", [f"{v['id']} | {v['placa']} - {v['modelo']} (KM: {v['km_atual']})" for v in v_disp]) if v_disp else None
            c_sel = c2.selectbox("Motorista Responsável:", [f"{c['id']} | {c['nome']}" for c in c_atv]) if c_atv else None
            
            c3, c4 = st.columns(2)
            km_saida = c3.number_input("Odómetro de Saída (KM):", min_value=0, step=1)
            cc_viagem = c4.selectbox("Centro de Custo da Viagem:", lista_cc)
            obs_saida = st.text_input("Observações (Avarias pré-existentes, etc):")
            
            submit_saida = st.form_submit_button("Liberar Veículo", type="primary")
            
            if submit_saida and v_sel and c_sel:
                vid = int(v_sel.split(" | ")[0])
                cid = int(c_sel.split(" | ")[0])
                
                motorista = next(c for c in c_atv if c['id'] == cid)
                if motorista['validade_cnh'] and motorista['validade_cnh'] < fuso_br().date():
                    st.error(f"⚠️ BLOQUEIO: A CNH do condutor {motorista['nome']} está vencida desde {motorista['validade_cnh']}!")
                else:
                    try:
                        q_ins = """INSERT INTO diario_bordo (veiculo_id, condutor_id, cc_viagem, km_saida, observacoes) 
                                   VALUES (%s, %s, %s, %s, %s)"""
                        execute_query(q_ins, (vid, cid, cc_viagem, km_saida, obs_saida))
                        execute_query("UPDATE veiculos SET status='Em Uso', km_atual=%s WHERE id=%s", (km_saida, vid))
                        st.success("Veículo liberado com sucesso!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro ao registar: {e}")

    with aba_retorno:
        st.subheader("Registo de Retorno")
        em_andamento = execute_query("""
            SELECT db.id, v.id as vid, v.placa, c.nome, db.km_saida, db.data_saida 
            FROM diario_bordo db 
            JOIN veiculos v ON db.veiculo_id = v.id 
            JOIN condutores c ON db.condutor_id = c.id 
            WHERE db.status='Em Andamento'
        """, fetch=True)
        
        if not em_andamento:
            st.info("Não há veículos em trânsito no momento.")
        else:
            with st.form("form_retorno"):
                db_sel = st.selectbox("Viagem em Andamento:", [f"{v['id']} | {v['placa']} (Cond: {v['nome']} | Saiu: {v['km_saida']}km)" for v in em_andamento])
                km_retorno = st.number_input("Odómetro de Retorno (KM):", min_value=0, step=1)
                
                if st.form_submit_button("Finalizar Viagem"):
                    idb = int(db_sel.split(" | ")[0])
                    viagem_info = next(v for v in em_andamento if v['id'] == idb)
                    
                    if km_retorno < viagem_info['km_saida']:
                        st.error("Erro: O KM de retorno não pode ser inferior ao KM de saída.")
                    else:
                        execute_query("UPDATE diario_bordo SET km_retorno=%s, data_retorno=%s, status='Concluído' WHERE id=%s", (km_retorno, fuso_br(), idb))
                        execute_query("UPDATE veiculos SET status='Disponível', km_atual=%s WHERE id=%s", (km_retorno, viagem_info['vid']))
                        st.success("Retorno registado com sucesso!")
                        st.rerun()

    with aba_historico:
        st.subheader("Últimas 100 Movimentações")
        hist = execute_query("""
            SELECT v.placa, c.nome as condutor, db.cc_viagem, db.data_saida, db.data_retorno, 
                   db.km_saida, db.km_retorno, (db.km_retorno - db.km_saida) as km_rodado, db.status 
            FROM diario_bordo db 
            JOIN veiculos v ON db.veiculo_id = v.id 
            JOIN condutores c ON db.condutor_id = c.id 
            ORDER BY db.data_saida DESC LIMIT 100
        """, fetch=True)
        if hist:
            st.dataframe(pd.DataFrame(hist), use_container_width=True, hide_index=True)

# ==============================================================================
# MÓDULO 3: OPERAÇÃO - MANUTENÇÕES
# ==============================================================================
elif menu == "🛠️ Operação: Manutenções":
    st.title("Gestão de Manutenção de Frota")
    aba_lista, aba_nova = st.tabs(["📋 Histórico e Acompanhamento", "➕ Abrir O.S. (Ordem de Serviço)"])
    
    with aba_lista:
        st.subheader("Painel de Manutenções")
        manuts = execute_query("SELECT id, veiculo_placa, tipo, data_solicitacao, data_liberacao, descricao, custo_total, status FROM manutencoes ORDER BY data_solicitacao DESC", fetch=True)
        if manuts:
            df_m = pd.DataFrame(manuts)
            st.dataframe(df_m, use_container_width=True, hide_index=True)
            
            pendentes = df_m[df_m['status'] != 'CONCLUÍDA']
            if not pendentes.empty:
                st.markdown("---")
                st.subheader("Baixar Manutenção Pendente")
                with st.form("baixa_manut"):
                    m_sel = st.selectbox("Selecione a Ordem de Serviço:", pendentes['id'].astype(str) + " - Placa: " + pendentes['veiculo_placa'])
                    custo_final = st.number_input("Custo Total da Manutenção (R$):", min_value=0.0)
                    if st.form_submit_button("Finalizar O.S."):
                        id_m = int(m_sel.split(" - ")[0])
                        execute_query("UPDATE manutencoes SET status='CONCLUÍDA', data_liberacao=%s, custo_total=%s WHERE id=%s", (fuso_br().date(), custo_final, id_m))
                        placa = pendentes[pendentes['id'] == id_m]['veiculo_placa'].values[0]
                        execute_query("UPDATE veiculos SET status='Disponível' WHERE placa=%s", (placa,))
                        st.success("Ordem de serviço finalizada e veículo liberado!")
                        st.rerun()

    with aba_nova:
        st.subheader("Abertura de Solicitação")
        with st.form("nova_manutencao"):
            placas = execute_query("SELECT placa FROM veiculos ORDER BY placa", fetch=True)
            col_a, col_b = st.columns(2)
            placa_sel = col_a.selectbox("Veículo (Placa):", [p['placa'] for p in placas]) if placas else col_a.text_input("Placa:")
            tipo_m = col_b.selectbox("Tipo de Intervenção:", ["PREVENTIVA", "CORRETIVA", "REVISÃO", "OUTROS"])
            
            col_c, col_d = st.columns(2)
            dt_solic = col_c.date_input("Data da Solicitação:")
            km_atual_m = col_d.number_input("KM Atual (Opcional):", min_value=0)
            
            desc_m = st.text_area("Descrição do Defeito / Serviço Necessário:")
            
            if st.form_submit_button("Gravar Solicitação"):
                q_ins = """INSERT INTO manutencoes (veiculo_placa, tipo, data_solicitacao, km_manutencao, descricao, status) 
                           VALUES (%s, %s, %s, %s, %s, 'PENDENTE')"""
                execute_query(q_ins, (placa_sel, tipo_m, dt_solic, km_atual_m, desc_m))
                execute_query("UPDATE veiculos SET status='Manutenção' WHERE placa=%s", (placa_sel,))
                st.success("Manutenção registada. Veículo bloqueado para uso.")
                st.rerun()

# ==============================================================================
# MÓDULO 4: OPERAÇÃO - SINISTROS E MULTAS
# ==============================================================================
elif menu == "🚨 Operação: Sinistros & Multas":
    st.title("Controlo de Infrações e Acidentes")
    aba_multas, aba_sinistros = st.tabs(["📄 Gestão de Multas", "💥 Gestão de Sinistros"])
    
    with aba_multas:
        st.subheader("Infrações de Trânsito")
        with st.expander("➕ Lançar Nova Multa Manualmente"):
            with st.form("form_multa_manual"):
                c1, c2, c3 = st.columns(3)
                m_placa = c1.text_input("Placa do Veículo:")
                m_ait = c2.text_input("Número do AIT:")
                m_dt = c3.date_input("Data da Infração:")
                
                c4, c5 = st.columns(2)
                m_val = c4.number_input("Valor da Multa R$:", min_value=0.0)
                m_pontos = c5.number_input("Pontuação (CNH):", min_value=0, max_value=21, step=1)
                
                m_desc = st.text_input("Descrição / Local:")
                if st.form_submit_button("Registar Multa") and m_placa and m_ait:
                    q = "INSERT INTO multas (veiculo_placa, ait, data_infracao, valor, pontuacao, descricao) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT(ait) DO NOTHING"
                    execute_query(q, (m_placa.upper(), m_ait, m_dt, m_val, m_pontos, m_desc))
                    st.success("Multa guardada na base.")
                    st.rerun()

        df_multas = pd.DataFrame(execute_query("SELECT veiculo_placa, ait, data_infracao, valor, pontuacao, status_pagamento FROM multas ORDER BY data_infracao DESC", fetch=True))
        if not df_multas.empty:
            st.dataframe(df_multas, use_container_width=True, hide_index=True)

    with aba_sinistros:
        st.subheader("Acidentes e Avarias (B.O.)")
        with st.expander("➕ Comunicar Novo Sinistro"):
            with st.form("form_sinistro"):
                c1, c2 = st.columns(2)
                s_placa = c1.text_input("Placa:")
                s_dt = c2.date_input("Data do Ocorrido:")
                
                c3, c4 = st.columns(2)
                s_bo = c3.text_input("Número do B.O.:")
                s_gravidade = c4.selectbox("Gravidade:", ["Leve (Apenas Avaria)", "Média", "Grave (Perda Total / Vítimas)"])
                
                s_terceiros = st.checkbox("Envolve Terceiros?")
                s_desc = st.text_area("Dinâmica do Acidente:")
                
                if st.form_submit_button("Registar Sinistro"):
                    q = "INSERT INTO sinistros (veiculo_placa, data_sinistro, boletim_ocorrencia, gravidade, terceiros_envolvidos, descricao_dano) VALUES (%s, %s, %s, %s, %s, %s)"
                    execute_query(q, (s_placa.upper(), s_dt, s_bo, s_gravidade, s_terceiros, s_desc))
                    st.success("Sinistro registado na base.")
                    st.rerun()
                    
        df_sin = pd.DataFrame(execute_query("SELECT veiculo_placa, data_sinistro, boletim_ocorrencia, gravidade, status_reparo FROM sinistros ORDER BY data_sinistro DESC", fetch=True))
        if not df_sin.empty:
            st.dataframe(df_sin, use_container_width=True, hide_index=True)

# ==============================================================================
# MÓDULO 5: TRANSFERÊNCIAS E AUDITORIA (LOG)
# ==============================================================================
elif menu == "🔄 Movimentação & Logística":
    st.title("Logística Interna e Auditoria")
    aba_transf, aba_auditoria = st.tabs(["🔄 Realocar Veículo (Centro de Custo)", "🛡️ Log de Auditoria"])
    
    with aba_transf:
        st.subheader("Transferência de Imobilizado/Frota")
        vs = execute_query("SELECT id, placa, cc_atual, tipo_frota FROM veiculos ORDER BY placa", fetch=True)
        if vs:
            with st.form("form_transferencia"):
                vt_sel = st.selectbox("Selecione o Veículo:", [f"{v['id']} | {v['placa']} - Atual: {v['cc_atual']}" for v in vs])
                novo_cc = st.selectbox("Novo Centro de Custo de Destino:", lista_cc)
                motivo = st.text_area("Justificativa para a transferência:")
                
                if st.form_submit_button("Efetivar Transferência", type="primary"):
                    if not motivo.strip():
                        st.error("A justificativa é obrigatória.")
                    else:
                        vid, placa = vt_sel.split(" | ")[0], vt_sel.split(" | ")[1].split(" - ")[0]
                        cc_antigo = vt_sel.split("Atual: ")[1]
                        
                        execute_query("UPDATE veiculos SET cc_atual=%s WHERE id=%s", (novo_cc, vid))
                        detalhe_log = f"De: [{cc_antigo}] Para: [{novo_cc}]. Motivo: {motivo}"
                        registrar_log("TRANSFERENCIA_CC", "veiculos", placa, detalhe_log)
                        
                        st.success(f"Veículo {placa} transferido com sucesso!")
                        st.rerun()

    with aba_auditoria:
        st.subheader("Rasto de Alterações no Sistema")
        logs = execute_query("SELECT data_hora, tipo_acao, tabela_afetada, registro_identificador, detalhes, usuario FROM historico_movimentacoes ORDER BY data_hora DESC LIMIT 500", fetch=True)
        if logs:
            st.dataframe(pd.DataFrame(logs), use_container_width=True, hide_index=True)

# ==============================================================================
# MÓDULO 6: RATEIO DRE (Análise de Custos)
# ==============================================================================
elif menu == "💰 Controladoria: Rateio DRE":
    st.title("Processamento de Custos para DRE")
    st.markdown("Este módulo calcula a proporção do custo fixo mensal de cada veículo e distribui matematicamente pelos Centros de Custo que o utilizaram com base na quilometragem rodada.")
    
    with st.form("form_dre"):
        c1, c2 = st.columns(2)
        hj = fuso_br().date()
        dt_inicio = c1.date_input("Período Inicial:", hj.replace(day=1))
        dt_fim = c2.date_input("Período Final:", hj)
        
        if st.form_submit_button("Rodar Processamento de Custo (Run Rateio)", type="primary"):
            query_rateio = """
                SELECT 
                    v.placa, 
                    COALESCE(v.valor_mensal, 0) as custo_mensal, 
                    db.cc_viagem as centro_custo, 
                    SUM(db.km_retorno - db.km_saida) as km_rodado_no_cc
                FROM diario_bordo db 
                JOIN veiculos v ON db.veiculo_id = v.id 
                WHERE db.status='Concluído' 
                  AND DATE(db.data_retorno) BETWEEN %s AND %s 
                GROUP BY v.placa, v.valor_mensal, db.cc_viagem
            """
            viagens = execute_query(query_rateio, (dt_inicio, dt_fim), fetch=True)
            
            if not viagens:
                st.warning("Não foram encontrados dados de viagens concluídas para o período.")
            else:
                df_rateio = pd.DataFrame(viagens)
                df_rateio['km_rodado_no_cc'] = pd.to_numeric(df_rateio['km_rodado_no_cc'])
                
                km_total_carro = df_rateio.groupby('placa')['km_rodado_no_cc'].sum().reset_index().rename(columns={'km_rodado_no_cc':'km_total_mes'})
                
                df_final = pd.merge(df_rateio, km_total_carro, on='placa')
                df_final = df_final[df_final['km_total_mes'] > 0]
                df_final['Fator_Proporcao'] = df_final['km_rodado_no_cc'] / df_final['km_total_mes']
                df_final['Custo_Rateado_R$'] = df_final['Fator_Proporcao'] * pd.to_numeric(df_final['custo_mensal'])
                
                st.subheader("Detalhamento por Veículo x CC")
                st.dataframe(df_final.style.format({'Custo_Rateado_R$': '{:.2f}', 'Fator_Proporcao': '{:.2%}'}), use_container_width=True)
                
                st.subheader("Resumo Contabilístico (Exportação DRE)")
                dre_resumo = df_final.groupby('centro_custo')['Custo_Rateado_R$'].sum().reset_index()
                
                c_graf, c_tab = st.columns([2, 1])
                with c_graf:
                    fig_dre = px.bar(dre_resumo, x='centro_custo', y='Custo_Rateado_R$', title="Impacto Financeiro por CC")
                    st.plotly_chart(fig_dre, use_container_width=True)
                with c_tab:
                    st.dataframe(dre_resumo.style.format({'Custo_Rateado_R$': 'R$ {:.2f}'}), use_container_width=True)

# ==============================================================================
# MÓDULO 7: IMPORTAÇÃO EM MASSA (ETL / DATA PIPELINE)
# ==============================================================================
elif menu == "⚙️ ETL / Importação em Massa":
    st.title("Motor de Importação de Planilhas (ETL)")
    st.info("Algoritmo inteligente de leitura. Faça o upload das suas bases e o sistema classificará e atualizará a base de dados automaticamente, preservando a integridade referencial.")
    
    uploaded_files = st.file_uploader("Arraste e solte ficheiros CSV/XLSX", type=["csv", "xlsx"], accept_multiple_files=True)
    
    if uploaded_files and st.button("Processar Lote de Ficheiros", type="primary"):
        progress_bar = st.progress(0)
        total_files = len(uploaded_files)
        
        for i, file in enumerate(uploaded_files):
            st.write(f"🔄 **A analisar:** `{file.name}`")
            try:
                if file.name.endswith('.csv'):
                    df = pd.read_csv(file, on_bad_lines='skip')
                else:
                    df = pd.read_excel(file)
                
                df = df.fillna('')
                cols = [str(c).upper().strip() for c in df.columns]
                linhas_processadas = 0
                
                # PIPELINE 1: BASE DE VEÍCULOS
                if 'PLACA' in cols and 'MODELO' in cols and 'Nº AUTO DE INFRAÇÃO (AIT)' not in cols:
                    st.caption("Tipo detetado: Tabela de Veículos")
                    col_placa = df.columns[cols.index('PLACA')]
                    col_mod = df.columns[cols.index('MODELO')]
                    col_cc = df.columns[cols.index('CENTRO DE CUSTO')] if 'CENTRO DE CUSTO' in cols else None
                    col_loc = df.columns[cols.index('LOCADORA')] if 'LOCADORA' in cols else None
                    col_valor = df.columns[cols.index('VALOR')] if 'VALOR' in cols else None
                    
                    t_frota = 'Indefinido'
                    if 'Mensal' in file.name: t_frota = 'Alugada Mensal'
                    elif 'Diária' in file.name: t_frota = 'Alugada Diária'
                    elif 'Própria' in file.name: t_frota = 'Própria'
                    elif 'Colaborador' in file.name: t_frota = 'Colaborador'
                    
                    for _, row in df.iterrows():
                        placa = str(row[col_placa]).strip().upper()
                        if not placa or len(placa) < 7: continue
                        
                        cc = str(row[col_cc]).strip() if col_cc and str(row[col_cc]).strip() else 'GERAL'
                        loc = str(row[col_loc]).strip() if col_loc and str(row[col_loc]).strip() else 'PRÓPRIA'
                        
                        try: val = float(str(row[col_valor]).replace(',','.'))
                        except: val = 0.0
                        
                        execute_query("INSERT INTO centros_custo (nome) VALUES (%s) ON CONFLICT DO NOTHING", (cc,))
                        execute_query("INSERT INTO locadoras (nome) VALUES (%s) ON CONFLICT DO NOTHING", (loc,))
                        
                        q_upsert = """
                            INSERT INTO veiculos (placa, modelo, tipo_frota, locadora, cc_atual, valor_mensal)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (placa) DO UPDATE SET 
                                tipo_frota = EXCLUDED.tipo_frota,
                                locadora = EXCLUDED.locadora,
                                cc_atual = EXCLUDED.cc_atual,
                                valor_mensal = EXCLUDED.valor_mensal,
                                data_atualizacao = CURRENT_TIMESTAMP
                        """
                        execute_query(q_upsert, (placa, str(row[col_mod]), t_frota, loc, cc, val))
                        linhas_processadas += 1
                        
                    registrar_log("ETL_IMPORT", "veiculos", file.name, f"{linhas_processadas} registos processados.")
                    st.success(f"✅ Sucesso. {linhas_processadas} veículos consolidados.")

                # PIPELINE 2: MULTAS DE TRÂNSITO
                elif 'Nº AUTO DE INFRAÇÃO (AIT)' in cols:
                    st.caption("Tipo detetado: Tabela de Multas")
                    col_placa = df.columns[cols.index('PLACA')]
                    col_ait = df.columns[cols.index('Nº AUTO DE INFRAÇÃO (AIT)')]
                    col_valor = df.columns[cols.index('VALOR DA MULTA')] if 'VALOR DA MULTA' in cols else None
                    col_dt = df.columns[cols.index('DATA DA INFRAÇÃO')] if 'DATA DA INFRAÇÃO' in cols else None
                    col_desc = df.columns[cols.index('DESCRIÇÃO DA INFRAÇÃO')] if 'DESCRIÇÃO DA INFRAÇÃO' in cols else None
                    
                    for _, row in df.iterrows():
                        ait = str(row[col_ait]).strip()
                        if not ait: continue
                        placa = str(row[col_placa]).upper().strip()
                        
                        try: val = float(str(row[col_valor]).replace(',','.'))
                        except: val = 0.0
                        
                        dt_inf = parse_date(row[col_dt]) if col_dt else None
                        desc = str(row[col_desc]) if col_desc else ""
                        
                        execute_query("""INSERT INTO multas (veiculo_placa, ait, data_infracao, valor, descricao) 
                                         VALUES (%s, %s, %s, %s, %s) ON CONFLICT(ait) DO NOTHING""", 
                                      (placa, ait, dt_inf, val, desc))
                        linhas_processadas += 1
                    st.success(f"✅ Sucesso. {linhas_processadas} multas processadas.")

                # PIPELINE 3: MANUTENÇÕES
                elif 'TIPO DE MANUTENÇÃO' in cols:
                    st.caption("Tipo detetado: Tabela de Manutenção")
                    col_placa = df.columns[cols.index('PLACA')]
                    col_tipo = df.columns[cols.index('TIPO DE MANUTENÇÃO')]
                    col_status = df.columns[cols.index('STATUS FINAL')] if 'STATUS FINAL' in cols else None
                    col_km = df.columns[cols.index('KM DO VEÍCULO')] if 'KM DO VEÍCULO' in cols else None
                    
                    for _, row in df.iterrows():
                        placa = str(row[col_placa]).upper().strip()
                        if not placa: continue
                        
                        status = str(row[col_status]).upper().strip() if col_status else 'PENDENTE'
                        try: km = int(str(row[col_km]).replace('.','').replace(',',''))
                        except: km = 0
                        
                        execute_query("INSERT INTO manutencoes (veiculo_placa, tipo, km_manutencao, status) VALUES (%s, %s, %s, %s)", 
                                      (placa, str(row[col_tipo]), km, status))
                        linhas_processadas += 1
                    st.success(f"✅ Sucesso. {linhas_processadas} O.S. importadas.")

                # PIPELINE 4: SINISTROS
                elif 'BOLETIM DE OCORRÊNCIA Nº' in cols:
                    st.caption("Tipo detetado: Tabela de Sinistros")
                    col_placa = df.columns[cols.index('PLACA')]
                    col_bo = df.columns[cols.index('BOLETIM DE OCORRÊNCIA Nº')]
                    col_desc = df.columns[cols.index('DESCRIÇÃO DO SINISTRO')] if 'DESCRIÇÃO DO SINISTRO' in cols else None
                    
                    for _, row in df.iterrows():
                        placa = str(row[col_placa]).upper().strip()
                        bo = str(row[col_bo]).strip()
                        if not placa or not bo: continue
                        
                        desc = str(row[col_desc]) if col_desc else ""
                        execute_query("INSERT INTO sinistros (veiculo_placa, boletim_ocorrencia, descricao_dano) VALUES (%s, %s, %s)", 
                                      (placa, bo, desc))
                        linhas_processadas += 1
                    st.success(f"✅ Sucesso. {linhas_processadas} sinistros importados.")

                else:
                    st.warning(f"⚠️ Padrão de colunas não reconhecido para o ficheiro: {file.name}. Verifique o cabeçalho.")

            except Exception as e:
                st.error(f"Erro ao processar {file.name}: {str(e)}")
            
            progress_bar.progress((i + 1) / total_files)
        
        st.info("🎉 Processamento em lote concluído!")

# ==============================================================================
# MÓDULO 8: CADASTROS BÁSICOS E RELATÓRIOS
# ==============================================================================
elif menu == "🗂️ Cadastros e Relatórios":
    st.title("Administração de Dados")
    aba_cad_motorista, aba_export = st.tabs(["👷 Cadastro de Motoristas", "💾 Exportar Relatórios (Excel)"])
    
    with aba_cad_motorista:
        st.subheader("Gerir Condutores")
        with st.form("form_condutor"):
            c1, c2 = st.columns(2)
            n_nome = c1.text_input("Nome Completo:")
            n_cnh = c2.text_input("Número CNH:")
            
            c3, c4 = st.columns(2)
            n_val = c3.date_input("Validade CNH:")
            n_cc = c4.selectbox("Centro de Custo Padrão:", lista_cc)
            
            if st.form_submit_button("Guardar Motorista"):
                q = "INSERT INTO condutores (nome, cnh, validade_cnh, cc_padrao) VALUES (%s, %s, %s, %s) ON CONFLICT(cnh) DO UPDATE SET validade_cnh = EXCLUDED.validade_cnh"
                execute_query(q, (n_nome, n_cnh, n_val, n_cc))
                st.success("Motorista registado/atualizado com sucesso.")
                st.rerun()
                
        df_cond = pd.DataFrame(execute_query("SELECT nome, cnh, validade_cnh, status FROM condutores ORDER BY nome", fetch=True))
        if not df_cond.empty:
            st.dataframe(df_cond, use_container_width=True, hide_index=True)

    with aba_export:
        st.subheader("Extração de Dados")
        st.write("Faça o download da base de dados consolidada em formato Excel para análises externas.")
        
        df_completo = pd.DataFrame(execute_query("SELECT * FROM veiculos", fetch=True))
        if not df_completo.empty:
            excel_data = gerar_excel_bytes(df_completo)
            st.download_button(
                label="📥 Descarregar Base de Veículos Consolidada (XLSX)",
                data=excel_data,
                file_name=f"Base_Frota_Brastel_{fuso_br().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
