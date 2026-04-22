import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from datetime import datetime

# ══════════════════════════════════════════════════════════════════════════════
# 1. CONFIGURAÇÃO E CONEXÃO
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Gestão de Frota", layout="wide", page_icon="🚙")

# Para rodar localmente, garanta que a DATABASE_URL esteja no .streamlit/secrets.toml
DATABASE_URL = st.secrets["DATABASE_URL"]

@contextmanager
def get_conn():
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

# ══════════════════════════════════════════════════════════════════════════════
# 2. ARQUITETURA DO BANCO DE DADOS (OS PILARES)
# ══════════════════════════════════════════════════════════════════════════════
def init_db():
    with get_conn() as conn:
        with conn.cursor() as c:
            # Tabela de Veículos (O 'Estoque')
            c.execute('''
                CREATE TABLE IF NOT EXISTS veiculos (
                    id SERIAL PRIMARY KEY,
                    placa TEXT UNIQUE NOT NULL,
                    modelo TEXT NOT NULL,
                    cc_fixo TEXT NOT NULL,
                    tipo_contrato TEXT CHECK (tipo_contrato IN ('Próprio', 'Locação Mensal', 'Locação Diária')),
                    status TEXT DEFAULT 'Disponível' CHECK (status IN ('Disponível', 'Em Uso', 'Manutenção', 'Inativo'))
                )
            ''')
            
            # Tabela de Condutores (Os 'Colaboradores')
            c.execute('''
                CREATE TABLE IF NOT EXISTS condutores (
                    id SERIAL PRIMARY KEY,
                    nome TEXT NOT NULL,
                    cnh TEXT UNIQUE NOT NULL,
                    validade_cnh DATE NOT NULL,
                    cc_padrao TEXT NOT NULL,
                    status TEXT DEFAULT 'Ativo' CHECK (status IN ('Ativo', 'Bloqueado'))
                )
            ''')
            
            # Tabela Diário de Bordo (A 'Movimentação / RDM-CGM')
            c.execute('''
                CREATE TABLE IF NOT EXISTS diario_bordo (
                    id SERIAL PRIMARY KEY,
                    veiculo_id INTEGER REFERENCES veiculos(id),
                    condutor_id INTEGER REFERENCES condutores(id),
                    cc_viagem TEXT NOT NULL,
                    data_saida TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    km_saida INTEGER NOT NULL,
                    data_retorno TIMESTAMP,
                    km_retorno INTEGER,
                    status TEXT DEFAULT 'Em Andamento' CHECK (status IN ('Em Andamento', 'Concluído'))
                )
            ''')
            
            # Tabela de Custos Financeiros (O Motor do Rateio / DRE)
            c.execute('''
                CREATE TABLE IF NOT EXISTS despesas_frota (
                    id SERIAL PRIMARY KEY,
                    diario_id INTEGER REFERENCES diario_bordo(id), -- Pode ser nulo se for custo fixo mensal
                    veiculo_id INTEGER REFERENCES veiculos(id),
                    tipo_despesa TEXT CHECK (tipo_despesa IN ('Combustível', 'Manutenção', 'Multa', 'Locação', 'Seguro/IPVA')),
                    valor NUMERIC(10, 2) NOT NULL,
                    cc_pagador TEXT NOT NULL,
                    data_competencia DATE NOT NULL
                )
            ''')
init_db()

# ══════════════════════════════════════════════════════════════════════════════
# 3. HELPERS DE CONSULTA
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_data(ttl=60)
def carregar_veiculos(apenas_disponiveis=False):
    with get_conn() as conn:
        with conn.cursor() as c:
            query = "SELECT * FROM veiculos"
            if apenas_disponiveis:
                query += " WHERE status = 'Disponível'"
            c.execute(query)
            return pd.DataFrame(c.fetchall())

@st.cache_data(ttl=60)
def carregar_condutores(apenas_ativos=True):
    with get_conn() as conn:
        with conn.cursor() as c:
            query = "SELECT * FROM condutores WHERE validade_cnh >= CURRENT_DATE" if apenas_ativos else "SELECT * FROM condutores"
            c.execute(query)
            return pd.DataFrame(c.fetchall())

# ══════════════════════════════════════════════════════════════════════════════
# 4. INTERFACE E ROTEAMENTO DE MÓDULOS
# ══════════════════════════════════════════════════════════════════════════════
st.sidebar.title("🚙 Menu Frota")
modulo = st.sidebar.radio("Navegação:", [
    "📋 Diário de Bordo (Pátio)", 
    "💰 Lançamento de Despesas", 
    "📊 Rateio Mensal (DRE)", 
    "⚙️ Cadastros Base"
])
st.sidebar.divider()

# Lista de Centros de Custo (Mock - idealmente vindo do DB)
LISTA_CC = ["Comercial", "Engenharia", "Diretoria", "Operações", "TI"]

# ──────────────────────────────────────────────────────────────────────────────
if modulo == "📋 Diário de Bordo (Pátio)":
    st.title("Controle de Pátio: Saídas e Retornos")
    aba1, aba2 = st.tabs(["🚀 Registrar Saída (Check-out)", "📥 Registrar Retorno (Check-in)"])
    
    with aba1:
        st.subheader("Liberar Veículo")
        df_v = carregar_veiculos(apenas_disponiveis=True)
        df_c = carregar_condutores(apenas_ativos=True)
        
        if df_v.empty or df_c.empty:
            st.warning("Cadastre veículos disponíveis e condutores válidos primeiro.")
        else:
            with st.form("form_saida"):
                c1, c2 = st.columns(2)
                veiculo_sel = c1.selectbox("Veículo:", df_v['placa'] + " - " + df_v['modelo'])
                condutor_sel = c2.selectbox("Condutor:", df_c['nome'] + " (CNH válida)")
                
                c3, c4 = st.columns(2)
                km_saida = c3.number_input("Hodômetro Atual (KM):", min_value=0, step=1)
                cc_viagem = c4.selectbox("Centro de Custo Responsável pela Viagem:", LISTA_CC)
                
                if st.form_submit_button("Liberar Veículo", type="primary"):
                    # Aqui entraremos com a lógica SQL de INSERT no diario_bordo e UPDATE no status do veiculo
                    st.info("Lógica de banco a ser implementada na nossa lapidação!")

    with aba2:
        st.subheader("Receber Veículo")
        # Aqui buscaremos os veículos com status 'Em Uso'
        st.write("Selecione o veículo retornando, informe o KM final, combustível e check-list de avarias.")
        # Todo: Lógica de Check-in

# ──────────────────────────────────────────────────────────────────────────────
elif modulo == "💰 Lançamento de Despesas":
    st.title("Registro de Custos Diretos")
    st.write("Lançamento de Combustível, Multas e Manutenções Corretivas.")
    
    with st.form("form_despesa"):
        tipo = st.selectbox("Tipo de Despesa:", ["Combustível", "Manutenção", "Multa", "Locação Extra"])
        c1, c2, c3 = st.columns(3)
        valor = c1.number_input("Valor (R$):", min_value=0.01, format="%.2f")
        data_comp = c2.date_input("Data da Competência:")
        cc_pagador = c3.selectbox("Alocar para o CC:", LISTA_CC)
        
        st.text_area("Observações / Justificativa:")
        
        if st.form_submit_button("Registrar Custo"):
            st.info("Lógica de banco a ser implementada na nossa lapidação!")

# ──────────────────────────────────────────────────────────────────────────────
elif modulo == "📊 Rateio Mensal (DRE)":
    st.title("Fechamento Contábil e Rateio")
    st.markdown("""
    Nesta tela, vamos construir o motor do rateio:
    1. O script vai somar todos os custos Fixos (Locação, IPVA).
    2. Vai calcular a proporção de KM rodado por cada Centro de Custo no mês selecionado.
    3. Vai distribuir o custo fixo usando a proporção do KM e somar aos custos diretos (combustível da viagem específica).
    """)
    mes_fechamento = st.selectbox("Mês de Competência:", ["04/2026", "05/2026", "06/2026"])
    
    if st.button("🔄 Simular Rateio", type="primary"):
        st.info("Aqui vamos gerar o DataFrame com a DRE formatada pronta para exportação.")

# ──────────────────────────────────────────────────────────────────────────────
elif modulo == "⚙️ Cadastros Base":
    st.title("Cadastros Fundamentais")
    aba_v, aba_c = st.tabs(["Carros", "Motoristas"])
    
    with aba_v:
        # Tabela e form simplificado de veículos
        st.dataframe(carregar_veiculos(), use_container_width=True)
        
    with aba_c:
        # Tabela e form simplificado de condutores
        st.dataframe(carregar_condutores(apenas_ativos=False), use_container_width=True)
