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
# 2. CONEXÃO E AUTO-CURA NUCLEAR DO BANCO DE DADOS
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

def db_migration():
    """Validação Nuclear: Se faltar QUALQUER coluna das novas tabelas, ele recria o banco."""
    needs_wipe = False
    
    # 1. Fase de Teste das Colunas Críticas
    with get_conn() as conn:
        with conn.cursor() as c:
            try:
                # Tenta puxar dados das colunas que deram problema nas versões anteriores
                c.execute("SELECT status_pagamento FROM multas LIMIT 1")
                c.execute("SELECT km_atual FROM veiculos LIMIT 1")
                c.execute("SELECT status FROM condutores LIMIT 1")
            except Exception:
                # Se QUALQUER uma der erro (UndefinedColumn ou UndefinedTable), o banco está velho/corrompido.
                conn.rollback()
                needs_wipe = True

    # 2. Fase de Limpeza (Se necessário)
    if needs_wipe:
        with get_conn() as conn:
            with conn.cursor() as c:
                tabelas = ["transferencias_cc", "avarias", "multas", "diario_bordo", "veiculos", "condutores", "centros_custo"]
                for tab in tabelas:
                    try:
                        c.execute(f"DROP TABLE IF EXISTS {tab} CASCADE")
                    except Exception:
                        conn.rollback()

    # 3. Fase de Criação da Estrutura Oficial (Sempre executada para garantir)
    queries = [
        "CREATE TABLE IF NOT EXISTS centros_custo (nome TEXT PRIMARY KEY)",
        """CREATE TABLE IF NOT EXISTS veiculos (
            id SERIAL PRIMARY KEY, placa TEXT UNIQUE NOT NULL, modelo TEXT NOT NULL,
            cc_atual TEXT REFERENCES centros_custo(nome), custo_fixo_mensal NUMERIC(10,2) DEFAULT 0,
            status TEXT DEFAULT 'Disponível', km_atual INTEGER DEFAULT 0
        )""",
        """CREATE TABLE IF NOT EXISTS condutores (
            id SERIAL PRIMARY KEY, nome TEXT NOT NULL, cnh TEXT UNIQUE NOT NULL,
            validade_cnh DATE NOT NULL, cc_padrao TEXT REFERENCES centros_custo(nome),
            status TEXT DEFAULT 'Ativo'
        )""",
        """CREATE TABLE IF NOT EXISTS diario_bordo (
            id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
            condutor_id INTEGER REFERENCES condutores(id), cc_viagem TEXT REFERENCES centros_custo(nome),
            km_saida INTEGER NOT NULL, data_saida TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            km_retorno INTEGER, data_retorno TIMESTAMP,
            status TEXT DEFAULT 'Em Andamento'
        )""",
        """CREATE TABLE IF NOT EXISTS multas (
            id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
            condutor_id INTEGER REFERENCES condutores(id), data_infracao DATE NOT NULL,
            valor NUMERIC(10,2) NOT NULL, descricao TEXT, status_pagamento TEXT DEFAULT 'A Pagar'
        )""",
        """CREATE TABLE IF NOT EXISTS avarias (
            id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
            condutor_relacionado INTEGER REFERENCES condutores(id), data_registro DATE NOT NULL,
            descricao TEXT NOT NULL, custo_estimado NUMERIC(10,2), status TEXT DEFAULT 'Pendente'
        )""",
        """CREATE TABLE IF NOT EXISTS transferencias_cc (
            id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
            cc_origem TEXT, cc_destino TEXT REFERENCES centros_custo(nome),
            data_transferencia TIMESTAMP DEFAULT CURRENT_TIMESTAMP, km_transferencia INTEGER NOT NULL
        )"""
    ]
    with get_conn() as conn:
        with conn.cursor() as c:
            for q in queries:
                c.execute(q)

# BLINDAGEM: Executa isso antes de renderizar qualquer coisa na tela.
db_migration()

# ══════════════════════════════════════════════════════════════════════════════
# 3. FUNÇÕES GERAIS DE UI
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
except Exception: 
    lista_cc = []

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
        q_kpi = """
            SELECT 
                (SELECT COUNT(*) FROM veiculos) as v_tot, 
                (SELECT COUNT(*) FROM veiculos WHERE status='Em Uso') as v_uso, 
                (SELECT COUNT(*) FROM condutores WHERE status='Ativo') as c_atv, 
                (SELECT COALESCE(SUM(valor),0) FROM multas WHERE status_pagamento='A Pagar') as m_pend
        """
        kpi = execute_query(q_kpi, fetch=True)[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Veículos", kpi['v_tot'])
        c2.metric("Na Rua", kpi['v_uso'])
        c3.metric("Motoristas Ativos", kpi['c_atv'])
        c4.metric("Multas em Aberto", f"R$ {kpi['m_pend']:.2f}")
        
        st.subheader("Carros em Operação Agora")
        q_ativos = """
            SELECT v.placa, c.nome, db.cc_viagem, db.km_saida, db.data_saida 
            FROM diario_bordo db 
            JOIN veiculos v ON db.veiculo_id = v.id 
            JOIN condutores c ON db.condutor_id = c.id 
            WHERE db.status='Em Andamento' ORDER BY db.data_saida DESC
        """
        ativos = execute_query(q_ativos, fetch=True)
        if ativos:
            df_a = pd.DataFrame(ativos)
            df_a['data_saida'] = format_data(df_a['data_saida'], True)
            st.dataframe(df_a, hide_index=True, use_container_width=True)
        else: st.info("Pátio completo.")
    except Exception as e: st.error(f"Erro ao carregar Dashboard. ({e})")

# ──────────────────────────────────────────────────────────────────────────────
# PORTARIA (DIÁRIO DE BORDO)
# ──────────────────────────────────────────────────────────────────────────────
elif menu == "📋 Portaria":
    st.title("Pátio")
    aba_s, aba_r, aba_h = st.tabs(["🚀 Saída", "📥 Retorno", "📜 Histórico"])
    
    with aba_s:
        v_disp = execute_query("SELECT id, placa, modelo, km_atual FROM veiculos WHERE status='Disponível' ORDER BY placa", fetch=True)
        c_atv = execute_query("SELECT id, nome, cnh, validade_cnh FROM condutores WHERE status='Ativo' ORDER BY nome", fetch=True)
        if not v_disp or not c_atv: st.warning("Cadastre carros e motoristas ativos para operar.")
        else:
            with st.form("fs"):
                c1, c2 = st.columns(2)
                v_s = c1.selectbox("Carro:", [f"{v['id']} | {v['placa']} (KM: {v['km_atual']})" for v in v_disp])
                c_s = c2.selectbox("Motorista:", [f"{c['id']} | {c['nome']} (CNH: {c['cnh']})" for c in c_atv])
                km_s = st.number_input("KM Inicial:", min_value=0, step=1)
                cc_v = st.selectbox("CC da Viagem:", lista_cc)
                
                if st.form_submit_button("Liberar"):
                    vid = int(v_s.split(" | ")[0])
                    cid = int(c_s.split(" | ")[0])
                    dc = next(c for c in c_atv if c['id'] == cid)
                    if dc['validade_cnh'] < fuso_br().date(): st.error("CNH VENCIDA!")
                    else:
                        q_ins = "INSERT INTO diario_bordo (veiculo_id, condutor_id, cc_viagem, km_saida, data_saida) VALUES (%s, %s, %s, %s, %s)"
                        execute_query(q_ins, (vid, cid, cc_v, km_s, fuso_br()))
                        q_upd = "UPDATE veiculos SET status='Em Uso', km_atual=%s WHERE id=%s"
                        execute_query(q_upd, (km_s, vid))
                        st.success("Saída Ok!"); st.rerun()

    with aba_r:
        em_and = execute_query("SELECT db.id, v.id as vid, v.placa, c.nome, db.km_saida FROM diario_bordo db JOIN veiculos v ON db.veiculo_id=v.id JOIN condutores c ON db.condutor_id=c.id WHERE db.status='Em Andamento'", fetch=True)
        if not em_and: st.info("Sem viagens pendentes.")
        else:
            with st.form("fr"):
                db_s = st.selectbox("Viagem:", [f"{v['id']} | {v['placa']} - {v['nome']} (Saiu c/ {v['km_saida']})" for v in em_and])
                km_r = st.number_input("KM Final:", min_value=0, step=1)
                if st.form_submit_button("Retornar"):
                    idb = int(db_s.split(" | ")[0])
                    dv = next(v for v in em_and if v['id'] == idb)
                    if km_r < dv['km_saida']: st.error("KM Final Menor que Inicial!")
                    else:
                        q_upd_db = "UPDATE diario_bordo SET km_retorno=%s, data_retorno=%s, status='Concluído' WHERE id=%s"
                        execute_query(q_upd_db, (km_r, fuso_br(), idb))
                        q_upd_v = "UPDATE veiculos SET status='Disponível', km_atual=%s WHERE id=%s"
                        execute_query(q_upd_v, (km_r, dv['vid']))
                        st.success("Retorno Ok!"); st.rerun()

    with aba_h:
        q_hist = """
            SELECT db.id, v.placa, c.nome, db.cc_viagem, db.status, db.km_saida, db.km_retorno, 
                   (db.km_retorno - db.km_saida) as rodado, db.data_saida, db.data_retorno 
            FROM diario_bordo db JOIN veiculos v ON db.veiculo_id=v.id JOIN condutores c ON db.condutor_id=c.id 
            ORDER BY db.data_saida DESC LIMIT 100
        """
        h = execute_query(q_hist, fetch=True)
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
    
    a_m, a_a = st.tabs(["Multas", "Avarias"])
    
    with a_m:
        with st.form("fm"):
            vm = st.selectbox("Carro:", [f"{v['id']} | {v['placa']}" for v in vt]) if vt else None
            cm = st.selectbox("Motorista:", [f"{c['id']} | {c['nome']}" for c in ct]) if ct else None
            dt = st.date_input("Data:", format="DD/MM/YYYY")
            vl = st.number_input("Valor:", min_value=0.0)
            ds = st.text_input("Local:")
            
            if st.form_submit_button("Lançar Multa") and vm and cm:
                q_ins_m = "INSERT INTO multas (veiculo_id, condutor_id, data_infracao, valor, descricao) VALUES (%s, %s, %s, %s, %s)"
                id_v = int(vm.split(" | ")[0])
                id_c = int(cm.split(" | ")[0])
                execute_query(q_ins_m, (id_v, id_c, dt, vl, ds))
                st.success("Lançado!"); st.rerun()

    with a_a:
        with st.form("fa"):
            va = st.selectbox("Carro:", [f"{v['id']} | {v['placa']}" for v in vt]) if vt else None
            ca = st.selectbox("Responsável:", ["Nenhum"] + [f"{c['id']} | {c['nome']}" for c in ct]) if ct else None
            da = st.date_input("Data Constatação:", format="DD/MM/YYYY")
            vla = st.number_input("Custo Reparo:", min_value=0.0)
            dsa = st.text_input("Dano:")
            
            if st.form_submit_button("Lançar Avaria") and va:
                idv = int(va.split(" | ")[0])
                idc = int(ca.split(" | ")[0]) if ca != "Nenhum" else None
                q_ins_a = "INSERT INTO avarias (veiculo_id, condutor_relacionado, data_registro, descricao, custo_estimado) VALUES (%s, %s, %s, %s, %s)"
                execute_query(q_ins_a, (idv, idc, da, dsa, vla))
                st.success("Lançado!"); st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
# TRANSFERÊNCIA
# ──────────────────────────────────────────────────────────────────────────────
elif menu == "🔄 Transf. Frota":
    st.title("Mudar CC do Veículo")
    vs = execute_query("SELECT id, placa, cc_atual, km_atual FROM veiculos ORDER BY placa", fetch=True)
    if vs:
        with st.form("ft"):
            vt_s = st.selectbox("Veículo:", [f"{v['id']} | {v['placa']} ({v['cc_atual']})" for v in vs])
            ccn = st.selectbox("Novo CC:", lista_cc)
            kmt = st.number_input("KM da Transf.:", min_value=0, step=1)
            
            if st.form_submit_button("Transferir"):
                iv = int(vt_s.split(" | ")[0])
                vbd = next(v for v in vs if v['id'] == iv)
                if kmt < vbd['km_atual']: st.error("KM Inválido.")
                else:
                    q_transf = "INSERT INTO transferencias_cc (veiculo_id, cc_origem, cc_destino, km_transferencia) VALUES (%s, %s, %s, %s)"
                    execute_query(q_transf, (iv, vbd['cc_atual'], ccn, kmt))
                    q_upd_v = "UPDATE veiculos SET cc_atual=%s, km_atual=%s WHERE id=%s"
                    execute_query(q_upd_v, (ccn, kmt, iv))
                    st.success("Feito!"); st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
# RATEIO
# ──────────────────────────────────────────────────────────────────────────────
elif menu == "💰 Rateio Mensal":
    st.title("DRE - Rateio por Uso (KM)")
    with st.form("fdre"):
        d1, d2 = st.columns(2)
        hj = fuso_br().date()
        di = d1.date_input("Início:", hj.replace(day=1), format="DD/MM/YYYY")
        dfim = d2.date_input("Fim:", hj, format="DD/MM/YYYY")
        
        if st.form_submit_button("Rodar Rateio", type="primary"):
            qr = """
                SELECT v.placa, v.custo_fixo_mensal as custo, db.cc_viagem as cc, SUM(db.km_retorno - db.km_saida) as rodado 
                FROM diario_bordo db JOIN veiculos v ON db.veiculo_id = v.id 
                WHERE db.status='Concluído' AND DATE(db.data_retorno) BETWEEN %s AND %s 
                GROUP BY v.placa, v.custo_fixo_mensal, db.cc_viagem
            """
            vgs = execute_query(qr, (di, dfim), fetch=True)
            if not vgs: st.warning("Nada no período.")
            else:
                dfr = pd.DataFrame(vgs)
                dfr['rodado'] = pd.to_numeric(dfr['rodado'])
                kt = dfr.groupby('placa')['rodado'].sum().reset_index().rename(columns={'rodado':'kmtot'})
                dc = pd.merge(dfr, kt, on='placa')
                dc = dc[dc['kmtot'] > 0]
                dc['Rateio_R$'] = (dc['rodado'] / dc['kmtot']) * pd.to_numeric(dc['custo'])
                
                st.dataframe(dc[['placa', 'cc', 'rodado', 'kmtot', 'custo', 'Rateio_R$']], hide_index=True, use_container_width=True)
                st.subheader("Custo Total por CC")
                st.dataframe(dc.groupby('cc')['Rateio_R$'].sum().reset_index(), hide_index=True)

# ──────────────────────────────────────────────────────────────────────────────
# ADMINISTRAÇÃO E CADASTROS
# ──────────────────────────────────────────────────────────────────────────────
elif menu == "⚙️ Cadastros Base":
    st.title("Gestão Base")
    a_cc, a_c, a_v = st.tabs(["🏢 CCs", "👷 Pessoas", "🚙 Carros"])
    
    with a_cc:
        st.write("Insira um por linha:")
        cm = st.text_area("Lista de CCs")
        if st.button("Salvar CCs") and cm:
            for l in cm.split('\n'):
                if l.strip(): execute_query("INSERT INTO centros_custo (nome) VALUES (%s) ON CONFLICT DO NOTHING", (l.strip(),))
            st.success("Ok!"); st.rerun()

    with a_c:
        st.write("Colunas Excel: nome, cnh, validade_cnh, cc_padrao")
        upc = st.file_uploader("Subir XLS Condutores", type=["xlsx"])
        if upc and st.button("Salvar Excel Pessoas"):
            d = pd.read_excel(upc)
            for _, r in d.iterrows():
                execute_query("INSERT INTO centros_custo (nome) VALUES (%s) ON CONFLICT DO NOTHING", (str(r['cc_padrao']),))
                execute_query(
                    "INSERT INTO condutores (nome, cnh, validade_cnh, cc_padrao) VALUES (%s, %s, %s, %s) ON CONFLICT(cnh) DO NOTHING", 
                    (r['nome'], str(r['cnh']), r['validade_cnh'], str(r['cc_padrao']))
                )
            st.success("Ok!"); st.rerun()

    with a_v:
        st.write("Colunas Excel: placa, modelo, cc_atual, custo_fixo_mensal")
        upv = st.file_uploader("Subir XLS Carros", type=["xlsx"])
        if upv and st.button("Salvar Excel Carros"):
            d = pd.read_excel(upv)
            for _, r in d.iterrows():
                execute_query("INSERT INTO centros_custo (nome) VALUES (%s) ON CONFLICT DO NOTHING", (str(r['cc_atual']),))
                execute_query(
                    "INSERT INTO veiculos (placa, modelo, cc_atual, custo_fixo_mensal) VALUES (%s, %s, %s, %s) ON CONFLICT(placa) DO NOTHING", 
                    (str(r['placa']).upper(), r['modelo'], str(r['cc_atual']), r['custo_fixo_mensal'])
                )
            st.success("Ok!"); st.rerun()
