import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from datetime import datetime, timedelta
import io

# ══════════════════════════════════════════════════════════════════════════════
# 1. CONFIGURAÇÃO GLOBAL
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="ERP Frota Brastel", layout="wide", page_icon="🚙")

st.markdown("""
<style>
    .metric-card { background-color: #f8f9fa; padding: 20px; border-radius: 10px; border-left: 5px solid #0052cc; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# 2. CONEXÃO SEGURA E AUTO-CURA DO BANCO (BULLETPROOF)
# ══════════════════════════════════════════════════════════════════════════════
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

def execute_query(query, params=None, fetch=False):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute(query, params)
            if fetch: return c.fetchall()
            return None

def setup_database():
    """Cria tabelas se não existirem e insere colunas faltantes sem destruir dados."""
    queries_criacao = [
        'CREATE TABLE IF NOT EXISTS centros_custo (nome TEXT PRIMARY KEY)',
        '''CREATE TABLE IF NOT EXISTS veiculos (
            id SERIAL PRIMARY KEY, placa TEXT UNIQUE NOT NULL, modelo TEXT NOT NULL,
            cc_atual TEXT REFERENCES centros_custo(nome), custo_fixo_mensal NUMERIC(10,2) DEFAULT 0
        )''',
        '''CREATE TABLE IF NOT EXISTS condutores (
            id SERIAL PRIMARY KEY, nome TEXT NOT NULL, cnh TEXT UNIQUE NOT NULL,
            validade_cnh DATE NOT NULL, cc_padrao TEXT REFERENCES centros_custo(nome)
        )''',
        '''CREATE TABLE IF NOT EXISTS diario_bordo (
            id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
            condutor_id INTEGER REFERENCES condutores(id), cc_viagem TEXT REFERENCES centros_custo(nome),
            km_saida INTEGER NOT NULL, data_saida TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            km_retorno INTEGER, data_retorno TIMESTAMP
        )''',
        '''CREATE TABLE IF NOT EXISTS multas (
            id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
            condutor_id INTEGER REFERENCES condutores(id), data_infracao DATE NOT NULL,
            valor NUMERIC(10,2) NOT NULL, descricao TEXT
        )''',
        '''CREATE TABLE IF NOT EXISTS avarias (
            id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
            condutor_relacionado INTEGER REFERENCES condutores(id), data_registro DATE NOT NULL,
            descricao TEXT NOT NULL, custo_estimado NUMERIC(10,2)
        )''',
        '''CREATE TABLE IF NOT EXISTS transferencias_cc (
            id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
            cc_origem TEXT, cc_destino TEXT REFERENCES centros_custo(nome),
            data_transferencia TIMESTAMP DEFAULT CURRENT_TIMESTAMP, km_transferencia INTEGER NOT NULL
        )'''
    ]
    
    # 1. Garante que as tabelas base existem
    with get_conn() as conn:
        with conn.cursor() as c:
            for q in queries_criacao:
                c.execute(q)

    # 2. Patching Cirúrgico: Adiciona colunas novas caso a tabela seja de uma versão antiga
    # Abrimos conexões individuais para cada alteração. Assim, se a coluna já existir e gerar erro,
    # ele ignora silenciosamente e continua arrumando o resto do banco.
    colunas_novas = [
        ("veiculos", "status", "TEXT DEFAULT 'Disponível'"),
        ("veiculos", "km_atual", "INTEGER DEFAULT 0"),
        ("condutores", "status", "TEXT DEFAULT 'Ativo'"),
        ("diario_bordo", "status", "TEXT DEFAULT 'Em Andamento'"),
        ("multas", "status_pagamento", "TEXT DEFAULT 'A Pagar'"),
        ("avarias", "status", "TEXT DEFAULT 'Pendente'")
    ]

    for tabela, coluna, definicao in colunas_novas:
        try:
            with get_conn() as conn_patch:
                with conn_patch.cursor() as c_patch:
                    c_patch.execute(f"ALTER TABLE {tabela} ADD COLUMN {coluna} {definicao}")
        except Exception:
            pass # Se der erro, é porque a coluna já existe. Segue a vida!

# Executa a blindagem sempre que o app inicia
setup_database()

# ══════════════════════════════════════════════════════════════════════════════
# 3. FUNÇÕES GERAIS DE DATA E UI
# ══════════════════════════════════════════════════════════════════════════════
def fuso_br(): return datetime.now() - timedelta(hours=3)

def format_data(val, inc_hora=False):
    fmt = '%d/%m/%Y %H:%M' if inc_hora else '%d/%m/%Y'
    return pd.to_datetime(val, errors='coerce').dt.strftime(fmt)

def gerar_xls(df):
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='xlsxwriter') as w: df.to_excel(w, index=False)
    return out.getvalue()

try:
    df_cc = pd.DataFrame(execute_query("SELECT nome FROM centros_custo ORDER BY nome", fetch=True))
    lista_cc = df_cc['nome'].tolist() if not df_cc.empty else []
except: lista_cc = []

# ══════════════════════════════════════════════════════════════════════════════
# 4. MENU LATERAL E NAVEGAÇÃO
# ══════════════════════════════════════════════════════════════════════════════
st.sidebar.title("🚙 Frota Brastel")
menu = st.sidebar.radio("Navegue:", ["📊 Painel", "📋 Portaria", "🚨 Multas & Danos", "🔄 Transf. Frota", "💰 Rateio Mensal", "⚙️ Cadastros Base"])

# ──────────────────────────────────────────────────────────────────────────────
# DASHBOARD
# ──────────────────────────────────────────────────────────────────────────────
if menu == "📊 Painel":
    st.title("Visão da Frota")
    try:
        q_kpi = "SELECT (SELECT COUNT(*) FROM veiculos) as v_tot, (SELECT COUNT(*) FROM veiculos WHERE status='Em Uso') as v_uso, (SELECT COUNT(*) FROM condutores WHERE status='Ativo') as c_atv, (SELECT COALESCE(SUM(valor),0) FROM multas WHERE status_pagamento='A Pagar') as m_pend"
        kpi = execute_query(q_kpi, fetch=True)[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Veículos", kpi['v_tot']); c2.metric("Na Rua", kpi['v_uso'])
        c3.metric("Motoristas Ativos", kpi['c_atv']); c4.metric("Multas em Aberto", f"R$ {kpi['m_pend']:.2f}")
        
        st.subheader("Carros em Operação Agora")
        ativos = execute_query("SELECT v.placa, c.nome, db.cc_viagem, db.km_saida, db.data_saida FROM diario_bordo db JOIN veiculos v ON db.veiculo_id = v.id JOIN condutores c ON db.condutor_id = c.id WHERE db.status='Em Andamento' ORDER BY db.data_saida DESC", fetch=True)
        if ativos:
            df_a = pd.DataFrame(ativos)
            df_a['data_saida'] = format_data(df_a['data_saida'], True)
            st.dataframe(df_a, hide_index=True, use_container_width=True)
        else: st.info("Pátio completo. Todos os veículos estão disponíveis.")
    except Exception as e: st.error(f"Erro ao carregar Dashboard. Detalhes: {e}")

# ──────────────────────────────────────────────────────────────────────────────
# PORTARIA (DIÁRIO DE BORDO)
# ──────────────────────────────────────────────────────────────────────────────
elif menu == "📋 Portaria":
    st.title("Controle de Pátio")
    aba_s, aba_r, aba_h = st.tabs(["🚀 Saída", "📥 Retorno", "📜 Histórico"])
    
    with aba_s:
        v_disp = execute_query("SELECT id, placa, modelo, km_atual FROM veiculos WHERE status='Disponível' ORDER BY placa", fetch=True)
        c_atv = execute_query("SELECT id, nome, cnh, validade_cnh FROM condutores WHERE status='Ativo' ORDER BY nome", fetch=True)
        if not v_disp or not c_atv: st.warning("Cadastre carros/motoristas para poder liberar veículos.")
        else:
            with st.form("fs"):
                c1, c2 = st.columns(2)
                v_s = c1.selectbox("Carro:", [f"{v['id']} | {v['placa']} (KM: {v['km_atual']})" for v in v_disp])
                c_s = c2.selectbox("Motorista:", [f"{c['id']} | {c['nome']} (CNH: {c['cnh']})" for c in c_atv])
                km_s = st.number_input("KM Inicial:", min_value=0, step=1)
                cc_v = st.selectbox("CC da Viagem:", lista_cc)
                if st.form_submit_button("Liberar"):
                    vid, cid = int(v_s.split(" | ")[0]), int(c_s.split(" | ")[0])
                    dc = next(c for c in c_atv if c['id']==cid)
                    if dc['validade_cnh'] < fuso_br().date(): st.error("⛔ CNH VENCIDA!")
                    else:
                        execute_query("INSERT INTO diario_bordo (veiculo_id, condutor_id, cc_viagem, km_saida, data_saida) VALUES (%s,%s,%s,%s,%s)", (vid, cid, cc_v, km_s, fuso_br()))
                        execute_query("UPDATE veiculos SET status='Em Uso', km_atual=%s WHERE id=%s", (km_s, vid))
                        st.success("Saída Ok! Carro bloqueado para outras viagens."); st.rerun()

    with aba_r:
        em_and = execute_query("SELECT db.id, v.id as vid, v.placa, c.nome, db.km_saida FROM diario_bordo db JOIN veiculos v ON db.veiculo_id=v.id JOIN condutores c ON db.condutor_id=c.id WHERE db.status='Em Andamento'", fetch=True)
        if not em_and: st.info("Sem viagens pendentes.")
        else:
            with st.form("fr"):
                db_s = st.selectbox("Viagem Retornando:", [f"{v['id']} | {v['placa']} - {v['nome']} (Saiu c/ {v['km_saida']} KM)" for v in em_and])
                km_r = st.number_input("KM Final (Chegada):", min_value=0, step=1)
                if st.form_submit_button("Registrar Retorno"):
                    idb = int(db_s.split(" | ")[0])
                    dv = next(v for v in em_and if v['id']==idb)
                    if km_r < dv['km_saida']: st.error("⛔ O KM Final não pode ser menor que o Inicial!")
                    else:
                        execute_query("UPDATE diario_bordo SET km_retorno=%s, data_retorno=%s, status='Concluído' WHERE id=%s", (km_r, fuso_br(), idb))
                        execute_query("UPDATE veiculos SET status='Disponível', km_atual=%s WHERE id=%s", (km_r, dv['vid']))
                        st.success("Retorno Ok! Carro livre no pátio."); st.rerun()

    with aba_h:
        h = execute_query("SELECT db.id, v.placa, c.nome, db.cc_viagem, db.status, db.km_saida, db.km_retorno, (db.km_retorno-db.km_saida) as rodado, db.data_saida, db.data_retorno FROM diario_bordo db JOIN veiculos v ON db.veiculo_id=v.id JOIN condutores c ON db.condutor_id=c.id ORDER BY db.data_saida DESC LIMIT 100", fetch=True)
        if h:
            dfh = pd.DataFrame(h)
            dfh['data_saida'] = format_data(dfh['data_saida'], True)
            dfh['data_retorno'] = format_data(dfh['data_retorno'], True).replace('NaT', 'Em trânsito')
            st.dataframe(dfh, hide_index=True, use_container_width=True)

# ──────────────────────────────────────────────────────────────────────────────
# OCORRÊNCIAS
# ──────────────────────────────────────────────────────────────────────────────
elif menu == "🚨 Multas & Danos":
    st.title("Multas e Avarias")
    vt = execute_query("SELECT id, placa FROM veiculos ORDER BY placa", fetch=True)
    ct = execute_query("SELECT id, nome FROM condutores ORDER BY nome", fetch=True)
    a_m, a_a = st.tabs(["💸 Multas", "🛠️ Avarias"])
    
    with a_m:
        with st.form("fm"):
            vm = st.selectbox("Carro Envolvido:", [f"{v['id']} | {v['placa']}" for v in vt]) if vt else None
            cm = st.selectbox("Motorista Infrator:", [f"{c['id']} | {c['nome']}" for c in ct]) if ct else None
            c1, c2, c3 = st.columns(3)
            dt = c1.date_input("Data da Infração:", format="DD/MM/YYYY")
            vl = c2.number_input("Valor da Multa (R$):", min_value=0.0)
            ds = c3.text_input("Local / Descrição:")
            if st.form_submit_button("Lançar Multa") and vm and cm:
                execute_query("INSERT INTO multas (veiculo_id, condutor_id, data_infracao, valor, descricao) VALUES (%s,%s,%s,%s,%s)", (int(vm.split(" | ")[0]), int(cm.split(" | ")[0]), dt, vl, ds))
                st.success("Multa registrada com sucesso!"); st.rerun()

        mf = execute_query("SELECT m.data_infracao, v.placa, c.nome, m.valor, m.descricao FROM multas m JOIN veiculos v ON m.veiculo_id = v.id JOIN condutores c ON m.condutor_id = c.id", fetch=True)
        if mf:
            dfm = pd.DataFrame(mf)
            dfm['data_infracao'] = format_data(dfm['data_infracao'])
            st.dataframe(dfm, hide_index=True, use_container_width=True)

    with a_a:
        with st.form("fa"):
            va = st.selectbox("Carro Danificado:", [f"{v['id']} | {v['placa']}" for v in vt]) if vt else None
            ca = st.selectbox("Motorista Responsável:", ["Nenhum"] + [f"{c['id']} | {c['nome']}" for c in ct]) if ct else None
            c1, c2 = st.columns(2)
            da = c1.date_input("Data Constatação:", format="DD/MM/YYYY")
            vla = c2.number_input("Custo de Reparo Estimado/Real (R$):", min_value=0.0)
            dsa = st.text_input("Qual foi o dano?")
            if st.form_submit_button("Lançar Avaria") and va:
                idv = int(va.split(" | ")[0]); idc = int(ca.split(" | ")[0]) if ca!="Nenhum" else None
                execute_query("INSERT INTO avarias (veiculo_id, condutor_relacionado, data_registro, descricao, custo_estimado) VALUES (%s,%s,%s,%s,%s)", (idv, idc, da, dsa, vla))
                st.success("Avaria registrada no dossiê do veículo."); st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
# TRANSFERÊNCIA
# ──────────────────────────────────────────────────────────────────────────────
elif menu == "🔄 Transf. Frota":
    st.title("Mudar CC Fixo do Veículo")
    vs = execute_query("SELECT id, placa, cc_atual, km_atual FROM veiculos ORDER BY placa", fetch=True)
    if vs:
        with st.form("ft"):
            vt_s = st.selectbox("Veículo a transferir:", [f"{v['id']} | {v['placa']} (Atualmente em: {v['cc_atual']})" for v in vs])
            ccn = st.selectbox("Transferir para o Novo CC:", lista_cc)
            kmt = st.number_input("KM Exato do veículo na hora da transferência:", min_value=0, step=1)
            if st.form_submit_button("Efetivar Mudança"):
                iv = int(vt_s.split(" | ")[0])
                vbd = next(v for v in vs if v['id']==iv)
                if vbd['cc_atual'] == ccn: st.warning("O veículo já está neste CC.")
                elif kmt < vbd['km_atual']: st.error(f"O KM informado é menor que o registrado ({vbd['km_atual']}).")
                else:
                    execute_query("INSERT INTO transferencias_cc (veiculo_id, cc_origem, cc_destino, km_transferencia) VALUES (%s,%s,%s,%s)", (iv, vbd['cc_atual'], ccn, kmt))
                    execute_query("UPDATE veiculos SET cc_atual=%s, km_atual=%s WHERE id=%s", (ccn, kmt, iv))
                    st.success("Veículo transferido!"); st.rerun()
    else: st.warning("Cadastre veículos primeiro.")

# ──────────────────────────────────────────────────────────────────────────────
# RATEIO
# ──────────────────────────────────────────────────────────────────────────────
elif menu == "💰 Rateio Mensal":
    st.title("DRE - Rateio por Proporção de Uso (KM)")
    with st.form("fdre"):
        d1, d2 = st.columns(2)
        hj = fuso_br().date()
        di = d1.date_input("Início da Competência:", hj.replace(day=1), format="DD/MM/YYYY")
        dfim = d2.date_input("Fim da Competência:", hj, format="DD/MM/YYYY")
        if st.form_submit_button("Rodar Processamento Contábil", type="primary"):
            qr = "SELECT v.placa, v.custo_fixo_mensal as custo, db.cc_viagem as cc, SUM(db.km_retorno-db.km_saida) as rodado FROM diario_bordo db JOIN veiculos v ON db.veiculo_id=v.id WHERE db.status='Concluído' AND DATE(db.data_retorno) BETWEEN %s AND %s GROUP BY v.placa, v.custo_fixo_mensal, db.cc_viagem"
            vgs = execute_query(qr, (di, dfim), fetch=True)
            if not vgs: st.warning("Nenhuma viagem concluída neste período para ratear.")
            else:
                dfr = pd.DataFrame(vgs)
                dfr['rodado'] = pd.to_numeric(dfr['rodado'])
                kt = dfr.groupby('placa')['rodado'].sum().reset_index().rename(columns={'rodado':'kmtot'})
                dc = pd.merge(dfr, kt, on='placa')
                dc = dc[dc['kmtot']>0] # Previne divisão por zero
                dc['%_Uso'] = (dc['rodado']/dc['kmtot'] * 100).round(2).astype(str) + "%"
                dc['Rateio_R$'] = (dc['rodado']/dc['kmtot']) * pd.to_numeric(dc['custo'])
                
                st.subheader("1. Memória de Cálculo Detalhada")
                st.dataframe(dc[['placa', 'cc', 'rodado', 'kmtot', '%_Uso', 'custo', 'Rateio_R$']], hide_index=True, use_container_width=True)
                
                st.subheader("2. Resumo DRE por Centro de Custo")
                st.dataframe(dc.groupby('cc')['Rateio_R$'].sum().reset_index(), hide_index=True)
                st.download_button("📥 Exportar Relatório DRE (Excel)", gerar_xls(dc), "Rateio_Mensal.xlsx")

# ──────────────────────────────────────────────────────────────────────────────
# ADMINISTRAÇÃO E CADASTROS
# ──────────────────────────────────────────────────────────────────────────────
elif menu == "⚙️ Cadastros Base":
    st.title("Configuração Inicial")
    a_cc, a_c, a_v, a_z = st.tabs(["🏢 Setores/CC", "👷 Pessoas (Motoristas)", "🚙 Veículos da Frota", "⚠️ Wipe Database"])
    
    with a_cc:
        st.write("Insira múltiplos Centros de Custo (um por linha):")
        cm = st.text_area("Lista de CCs", placeholder="Engenharia\nComercial\nOperações")
        if st.button("Salvar CCs em Lote") and cm:
            for l in cm.split('\n'):
                if l.strip(): execute_query("INSERT INTO centros_custo (nome) VALUES (%s) ON CONFLICT DO NOTHING", (l.strip(),))
            st.success("Lista processada!"); st.rerun()

    with a_c:
        st.write("📥 **Importação Múltipla via Excel:** Colunas obrigatórias: `nome`, `cnh`, `validade_cnh`, `cc_padrao`")
        upc = st.file_uploader("Subir XLS Condutores", type=["xlsx"])
        if upc and st.button("Gravar Excel Pessoas"):
            d = pd.read_excel(upc)
            for _, r in d.iterrows():
                execute_query("INSERT INTO centros_custo (nome) VALUES (%s) ON CONFLICT DO NOTHING", (str(r['cc_padrao']),))
                execute_query("INSERT INTO condutores (nome,
