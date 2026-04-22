import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

st.set_page_config(page_title="Gestão de Frota V2", layout="wide", page_icon="🚙")

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
# 1. INIT DB: NOVAS TABELAS (CCs, Multas, Avarias, Transferências)
# ══════════════════════════════════════════════════════════════════════════════
def init_db():
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute('CREATE TABLE IF NOT EXISTS centros_custo (nome TEXT PRIMARY KEY)')
            
            c.execute('''
                CREATE TABLE IF NOT EXISTS veiculos (
                    id SERIAL PRIMARY KEY, placa TEXT UNIQUE NOT NULL, modelo TEXT NOT NULL,
                    cc_atual TEXT REFERENCES centros_custo(nome),
                    tipo_contrato TEXT, custo_fixo_mensal NUMERIC(10,2) DEFAULT 0,
                    status TEXT DEFAULT 'Disponível'
                )
            ''')
            
            c.execute('''
                CREATE TABLE IF NOT EXISTS condutores (
                    id SERIAL PRIMARY KEY, nome TEXT NOT NULL, cnh TEXT UNIQUE NOT NULL,
                    validade_cnh DATE NOT NULL, cc_padrao TEXT REFERENCES centros_custo(nome)
                )
            ''')
            
            c.execute('''
                CREATE TABLE IF NOT EXISTS diario_bordo (
                    id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
                    condutor_id INTEGER REFERENCES condutores(id), cc_viagem TEXT REFERENCES centros_custo(nome),
                    km_saida INTEGER NOT NULL, km_retorno INTEGER, data_retorno DATE, status TEXT DEFAULT 'Em Andamento'
                )
            ''')

            c.execute('''
                CREATE TABLE IF NOT EXISTS multas (
                    id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
                    condutor_id INTEGER REFERENCES condutores(id), data_infracao DATE NOT NULL,
                    valor NUMERIC(10,2) NOT NULL, descricao TEXT, status TEXT DEFAULT 'A Pagar'
                )
            ''')

            c.execute('''
                CREATE TABLE IF NOT EXISTS avarias (
                    id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
                    condutor_relacionado INTEGER REFERENCES condutores(id), data_registro DATE NOT NULL,
                    descricao TEXT NOT NULL, custo_estimado NUMERIC(10,2)
                )
            ''')

            c.execute('''
                CREATE TABLE IF NOT EXISTS transferencias_cc (
                    id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
                    cc_origem TEXT, cc_destino TEXT REFERENCES centros_custo(nome),
                    data_transferencia DATE DEFAULT CURRENT_DATE, km_transferencia INTEGER NOT NULL
                )
            ''')
init_db()

# Funções de Cache
@st.cache_data(ttl=10)
def carregar_dados(tabela):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute(f"SELECT * FROM {tabela}")
            return pd.DataFrame(c.fetchall())

@st.cache_data(ttl=10)
def lista_ccs():
    df = carregar_dados("centros_custo")
    return df['nome'].tolist() if not df.empty else []

df_veiculos = carregar_dados("veiculos")
df_condutores = carregar_dados("condutores")
ccs = lista_ccs()

# ══════════════════════════════════════════════════════════════════════════════
# 2. ROTEAMENTO
# ══════════════════════════════════════════════════════════════════════════════
st.sidebar.title("🚙 Frota Brastel")
modulo = st.sidebar.radio("Ir para:", [
    "📋 Diário de Bordo", "🚨 Multas e Avarias", "🔄 Transferência de CC", 
    "📊 Rateio Mensal (DRE)", "⚙️ Administração"
])

# ──────────────────────────────────────────────────────────────────────────────
if modulo == "⚙️ Administração":
    st.title("⚙️ Configurações e Cadastros")
    tab1, tab2, tab3 = st.tabs(["🏢 Centros de Custo", "👷 Condutores / 🚙 Veículos", "⚠️ Zona de Perigo"])

    with tab1:
        cc_novo = st.text_input("Novo Centro de Custo:")
        if st.button("Cadastrar CC") and cc_novo:
            with get_conn() as conn:
                with conn.cursor() as c: c.execute("INSERT INTO centros_custo (nome) VALUES (%s) ON CONFLICT DO NOTHING", (cc_novo,))
            st.success(f"{cc_novo} cadastrado!"); st.rerun()
        if ccs: st.write("CCs Cadastrados:", ccs)

    with tab2:
        if not ccs: st.warning("Cadastre um CC primeiro.")
        else:
            with st.form("f_cond"):
                st.subheader("Novo Condutor")
                c1, c2, c3, c4 = st.columns(4)
                nome = c1.text_input("Nome:"); cnh = c2.text_input("CNH:"); val = c3.date_input("Validade:"); cc_c = c4.selectbox("CC Padrão:", ccs)
                if st.form_submit_button("Salvar Condutor"):
                    with get_conn() as conn:
                        with conn.cursor() as c: c.execute("INSERT INTO condutores (nome, cnh, validade_cnh, cc_padrao) VALUES (%s,%s,%s,%s)", (nome, cnh, val, cc_c))
                    st.success("Salvo!"); st.rerun()

            with st.form("f_veic"):
                st.subheader("Novo Veículo")
                v1, v2, v3, v4 = st.columns(4)
                placa = v1.text_input("Placa:"); mod = v2.text_input("Modelo:"); cc_v = v3.selectbox("CC Fixo:", ccs); custo = v4.number_input("Mensalidade (R$):", min_value=0.0)
                if st.form_submit_button("Salvar Veículo"):
                    with get_conn() as conn:
                        with conn.cursor() as c: c.execute("INSERT INTO veiculos (placa, modelo, cc_atual, custo_fixo_mensal) VALUES (%s,%s,%s,%s)", (placa.upper(), mod, cc_v, custo))
                    st.success("Salvo!"); st.rerun()

    with tab3:
        st.error("Área de Limpeza do Banco de Dados (FASE DE TESTES)")
        confirmar = st.checkbox("Tenho certeza que quero apagar TODOS os dados do sistema.")
        if confirmar and st.button("🚨 ZERAR TUDO", type="primary"):
            with get_conn() as conn:
                with conn.cursor() as c:
                    # A ordem importa por conta das Foreign Keys
                    tabelas = ["transferencias_cc", "avarias", "multas", "diario_bordo", "veiculos", "condutores", "centros_custo"]
                    for t in tabelas: c.execute(f"TRUNCATE TABLE {t} CASCADE")
            st.success("Banco de dados zerado com sucesso!"); st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
elif modulo == "🚨 Multas e Avarias":
    st.title("Ocorrências")
    aba_m, aba_a = st.tabs(["💸 Multas", "💥 Avarias"])
    
    if df_veiculos.empty or df_condutores.empty: st.warning("Cadastre veículos e condutores.")
    else:
        with aba_m:
            with st.form("f_multa"):
                c1, c2 = st.columns(2)
                veic_m = c1.selectbox("Veículo (Placa):", df_veiculos['id'].astype(str) + " - " + df_veiculos['placa'])
                cond_m = c2.selectbox("Condutor Infrator:", df_condutores['id'].astype(str) + " - " + df_condutores['nome'])
                v1, v2 = st.columns(2); data_m = v1.date_input("Data da Infração:"); valor_m = v2.number_input("Valor (R$):")
                desc_m = st.text_input("Descrição / Local:")
                if st.form_submit_button("Registrar Multa"):
                    vid, cid = veic_m.split(" - ")[0], cond_m.split(" - ")[0]
                    with get_conn() as conn:
                        with conn.cursor() as c: c.execute("INSERT INTO multas (veiculo_id, condutor_id, data_infracao, valor, descricao) VALUES (%s,%s,%s,%s,%s)", (vid, cid, data_m, valor_m, desc_m))
                    st.success("Multa registrada!")

        with aba_a:
            with st.form("f_avaria"):
                c1, c2 = st.columns(2)
                veic_a = c1.selectbox("Veículo Avariado:", df_veiculos['id'].astype(str) + " - " + df_veiculos['placa'])
                cond_a = c2.selectbox("Condutor Relacionado (Opcional):", ["Nenhum"] + (df_condutores['id'].astype(str) + " - " + df_condutores['nome']).tolist())
                v1, v2 = st.columns(2); data_a = v1.date_input("Data do Registro:"); valor_a = v2.number_input("Custo Estimado (R$):")
                desc_a = st.text_area("Detalhes da Avaria:")
                if st.form_submit_button("Registrar Avaria"):
                    vid = veic_a.split(" - ")[0]
                    cid = None if cond_a == "Nenhum" else cond_a.split(" - ")[0]
                    with get_conn() as conn:
                        with conn.cursor() as c: c.execute("INSERT INTO avarias (veiculo_id, condutor_relacionado, data_registro, descricao, custo_estimado) VALUES (%s,%s,%s,%s,%s)", (vid, cid, data_a, desc_a, valor_a))
                    st.success("Avaria registrada!")

# ──────────────────────────────────────────────────────────────────────────────
elif modulo == "🔄 Transferência de CC":
    st.title("Transferência de Frota entre Centros de Custo")
    if df_veiculos.empty: st.warning("Sem veículos.")
    else:
        with st.form("f_transf"):
            v_sel = st.selectbox("Selecione o Veículo:", df_veiculos['placa'] + " (Atual: " + df_veiculos['cc_atual'] + ")")
            cc_novo = st.selectbox("Transferir para o CC:", ccs)
            km_transf = st.number_input("KM Exato no momento da transferência:", min_value=0)
            
            if st.form_submit_button("Efetivar Transferência", type="primary"):
                placa = v_sel.split(" ")[0]
                veiculo = df_veiculos[df_veiculos['placa'] == placa].iloc[0]
                if veiculo['cc_atual'] == cc_novo: st.error("O veículo já pertence a este CC.")
                else:
                    with get_conn() as conn:
                        with conn.cursor() as c:
                            c.execute("INSERT INTO transferencias_cc (veiculo_id, cc_origem, cc_destino, km_transferencia) VALUES (%s,%s,%s,%s)", (veiculo['id'], veiculo['cc_atual'], cc_novo, km_transf))
                            c.execute("UPDATE veiculos SET cc_atual = %s WHERE id = %s", (cc_novo, veiculo['id']))
                    st.success(f"Veículo {placa} transferido para {cc_novo}!"); st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
elif modulo == "📊 Rateio Mensal (DRE)":
    st.title("Simulador de Rateio (Protótipo)")
    st.write("A lógica estrutural: Custo Fixo do Veículo proporcional ao KM rodado por CC.")
    
    # Gerando dados fictícios para demonstrar o cálculo se o banco estiver vazio
    st.info("Demonstração da lógica (Mockup Data)")
    dados_viagens = pd.DataFrame({
        'Veículo': ['ABC-1234', 'ABC-1234', 'ABC-1234'],
        'CC_Viagem': ['Comercial', 'Comercial', 'Engenharia'],
        'KM_Rodado': [200, 300, 500] # Total rodado: 1000 KM
    })
    custo_fixo_veiculo = 2000.00 # Ex: Locação mensal

    # Cálculo do Rateio
    resumo_km = dados_viagens.groupby(['Veículo', 'CC_Viagem'])['KM_Rodado'].sum().reset_index()
    total_km_veiculo = resumo_km.groupby('Veículo')['KM_Rodado'].sum().reset_index().rename(columns={'KM_Rodado': 'KM_Total'})
    
    rateio = pd.merge(resumo_km, total_km_veiculo, on='Veículo')
    rateio['% de Uso'] = (rateio['KM_Rodado'] / rateio['KM_Total'])
    rateio['Custo Alocado (R$)'] = rateio['% de Uso'] * custo_fixo_veiculo
    rateio['% de Uso'] = (rateio['% de Uso'] * 100).round(2).astype(str) + "%"

    st.dataframe(rateio, use_container_width=True)

# ──────────────────────────────────────────────────────────────────────────────
elif modulo == "📋 Diário de Bordo":
    st.title("Pátio")
    st.write("Módulo em construção para a próxima iteração.")
