import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import io

st.set_page_config(page_title="Frota Brastel", layout="wide", page_icon="🚙")

# ══════════════════════════════════════════════════════════════════════════════
# 1. BANCO DE DADOS E CONEXÃO
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

def init_db():
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute('CREATE TABLE IF NOT EXISTS centros_custo (nome TEXT PRIMARY KEY)')
            c.execute('''
                CREATE TABLE IF NOT EXISTS veiculos (
                    id SERIAL PRIMARY KEY, placa TEXT UNIQUE NOT NULL, modelo TEXT NOT NULL,
                    cc_atual TEXT REFERENCES centros_custo(nome),
                    tipo_contrato TEXT, custo_fixo_mensal NUMERIC(10,2) DEFAULT 0,
                    status TEXT DEFAULT 'Disponível' CHECK (status IN ('Disponível', 'Em Uso', 'Manutenção', 'Inativo'))
                )
            ''')
            c.execute('''
                CREATE TABLE IF NOT EXISTS condutores (
                    id SERIAL PRIMARY KEY, nome TEXT NOT NULL, cnh TEXT UNIQUE NOT NULL,
                    validade_cnh DATE NOT NULL, cc_padrao TEXT REFERENCES centros_custo(nome),
                    status TEXT DEFAULT 'Ativo'
                )
            ''')
            c.execute('''
                CREATE TABLE IF NOT EXISTS diario_bordo (
                    id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
                    condutor_id INTEGER REFERENCES condutores(id), cc_viagem TEXT REFERENCES centros_custo(nome),
                    km_saida INTEGER NOT NULL, data_saida TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    km_retorno INTEGER, data_retorno TIMESTAMP,
                    status TEXT DEFAULT 'Em Andamento' CHECK (status IN ('Em Andamento', 'Concluído'))
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

@st.cache_data(ttl=5)
def fetch_data(query, params=None):
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute(query, params)
            return pd.DataFrame(c.fetchall())

# Carregamento de dados globais
df_ccs = fetch_data("SELECT nome FROM centros_custo ORDER BY nome")
lista_ccs = df_ccs['nome'].tolist() if not df_ccs.empty else []
df_veic = fetch_data("SELECT * FROM veiculos")
df_cond = fetch_data("SELECT * FROM condutores WHERE status = 'Ativo'")

# ══════════════════════════════════════════════════════════════════════════════
# 2. NAVEGAÇÃO LATERAL
# ══════════════════════════════════════════════════════════════════════════════
st.sidebar.title("🚙 Menu Operacional")
modulo = st.sidebar.radio("Navegação:", [
    "📋 Pátio (Check-in/Out)", "🚨 Ocorrências (Multas/Avarias)", 
    "🔄 Transferências", "📊 Rateio Mensal (DRE)", "⚙️ Cadastros e Massa"
])
st.sidebar.divider()

# ──────────────────────────────────────────────────────────────────────────────
# MÓDULO: CADASTROS E IMPORTAÇÃO EM MASSA
# ──────────────────────────────────────────────────────────────────────────────
if modulo == "⚙️ Cadastros e Massa":
    st.title("⚙️ Cadastros e Estrutura")
    aba_cc, aba_cond, aba_veic, aba_perigo = st.tabs(["🏢 Centros de Custo", "👷 Condutores", "🚙 Veículos", "⚠️ Zerar BD"])

    with aba_cc:
        st.subheader("Cadastro em Massa de Centros de Custo")
        ccs_massa = st.text_area("Cole os CCs (um por linha):", height=150, placeholder="Comercial\nEngenharia\nTI")
        if st.button("Salvar CCs"):
            novos = [x.strip() for x in ccs_massa.split('\n') if x.strip()]
            with get_conn() as conn:
                with conn.cursor() as c:
                    for nc in novos: c.execute("INSERT INTO centros_custo (nome) VALUES (%s) ON CONFLICT DO NOTHING", (nc,))
            st.success(f"{len(novos)} CCs processados!"); st.cache_data.clear(); st.rerun()
        st.dataframe(df_ccs, use_container_width=True)

    with aba_cond:
        st.subheader("Importar Condutores (Excel)")
        st.write("Colunas necessárias: **nome**, **cnh**, **validade_cnh** (YYYY-MM-DD), **cc_padrao**")
        arquivo = st.file_uploader("Anexe a planilha (.xlsx):", type=["xlsx"])
        if arquivo and st.button("Processar Planilha"):
            df_up = pd.read_excel(arquivo)
            if not {'nome', 'cnh', 'validade_cnh', 'cc_padrao'}.issubset(df_up.columns):
                st.error("Formato inválido. Verifique o nome das colunas.")
            else:
                with get_conn() as conn:
                    with conn.cursor() as c:
                        for _, row in df_up.iterrows():
                            # Se o CC não existir, cria na hora
                            c.execute("INSERT INTO centros_custo (nome) VALUES (%s) ON CONFLICT DO NOTHING", (str(row['cc_padrao']).strip(),))
                            c.execute("""
                                INSERT INTO condutores (nome, cnh, validade_cnh, cc_padrao) 
                                VALUES (%s, %s, %s, %s) ON CONFLICT (cnh) DO NOTHING
                            """, (row['nome'], str(row['cnh']), row['validade_cnh'], str(row['cc_padrao']).strip()))
                st.success("Condutores importados com sucesso!"); st.cache_data.clear(); st.rerun()
        
        st.divider()
        st.subheader("Cadastro Individual")
        with st.form("f_cond"):
            c1, c2, c3, c4 = st.columns(4)
            nome = c1.text_input("Nome:")
            cnh = c2.text_input("CNH:")
            val = c3.date_input("Validade:")
            cc_c = c4.selectbox("CC:", lista_ccs) if lista_ccs else c4.text_input("CC:")
            if st.form_submit_button("Cadastrar Condutor") and nome and cnh:
                with get_conn() as conn:
                    with conn.cursor() as c: c.execute("INSERT INTO condutores (nome, cnh, validade_cnh, cc_padrao) VALUES (%s,%s,%s,%s)", (nome, cnh, val, cc_c))
                st.success("Salvo!"); st.cache_data.clear(); st.rerun()
        st.dataframe(df_cond, use_container_width=True)

    with aba_veic:
        with st.form("f_veic"):
            v1, v2, v3, v4 = st.columns(4)
            placa = v1.text_input("Placa:")
            mod = v2.text_input("Modelo:")
            cc_v = v3.selectbox("CC Atual:", lista_ccs) if lista_ccs else v3.text_input("CC Atual:")
            custo = v4.number_input("Custo Fixo/Mensalidade (R$):", min_value=0.0)
            if st.form_submit_button("Cadastrar Veículo") and placa:
                with get_conn() as conn:
                    with conn.cursor() as c: c.execute("INSERT INTO veiculos (placa, modelo, cc_atual, custo_fixo_mensal) VALUES (%s,%s,%s,%s)", (placa.upper(), mod, cc_v, custo))
                st.success("Salvo!"); st.cache_data.clear(); st.rerun()
        st.dataframe(df_veic, use_container_width=True)

    with aba_perigo:
        st.error("⚠️ DELETAR TODO O BANCO DE DADOS")
        if st.checkbox("Confirmo que desejo apagar tudo."):
            if st.button("🚨 EXECUTAR LIMPEZA", type="primary"):
                with get_conn() as conn:
                    with conn.cursor() as c:
                        for tbl in ["transferencias_cc", "avarias", "multas", "diario_bordo", "veiculos", "condutores", "centros_custo"]:
                            c.execute(f"TRUNCATE TABLE {tbl} CASCADE")
                st.success("Banco formatado!"); st.cache_data.clear(); st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
# MÓDULO: PÁTIO (DIÁRIO DE BORDO)
# ──────────────────────────────────────────────────────────────────────────────
elif modulo == "📋 Pátio (Check-in/Out)":
    st.title("Controle de Portaria")
    aba_out, aba_in, aba_hist = st.tabs(["🚀 Liberar Saída", "📥 Registrar Retorno", "📜 Histórico"])

    with aba_out:
        df_disponivel = df_veic[df_veic['status'] == 'Disponível'] if not df_veic.empty else pd.DataFrame()
        if df_disponivel.empty or df_cond.empty:
            st.warning("Não há veículos disponíveis ou condutores cadastrados.")
        else:
            with st.form("form_saida"):
                c1, c2 = st.columns(2)
                v_sel = c1.selectbox("Veículo:", df_disponivel['id'].astype(str) + " - " + df_disponivel['placa'])
                cond_sel = c2.selectbox("Condutor:", df_cond['id'].astype(str) + " - " + df_cond['nome'])
                c3, c4 = st.columns(2)
                km_s = c3.number_input("KM de Saída:", min_value=0, step=1)
                cc_viagem = c4.selectbox("CC da Viagem (Rateio):", lista_ccs)
                
                if st.form_submit_button("Registrar Saída", type="primary"):
                    vid = int(v_sel.split(" - ")[0])
                    cid = int(cond_sel.split(" - ")[0])
                    with get_conn() as conn:
                        with conn.cursor() as c:
                            c.execute("INSERT INTO diario_bordo (veiculo_id, condutor_id, cc_viagem, km_saida) VALUES (%s,%s,%s,%s)", (vid, cid, cc_viagem, km_s))
                            c.execute("UPDATE veiculos SET status = 'Em Uso' WHERE id = %s", (vid,))
                    st.success("Veículo liberado!"); st.cache_data.clear(); st.rerun()

    with aba_in:
        viagens_abertas = fetch_data("""
            SELECT db.id, db.km_saida, v.placa, c.nome as condutor 
            FROM diario_bordo db 
            JOIN veiculos v ON db.veiculo_id = v.id 
            JOIN condutores c ON db.condutor_id = c.id 
            WHERE db.status = 'Em Andamento'
        """)
        if viagens_abertas.empty:
            st.info("Nenhum veículo rodando no momento.")
        else:
            with st.form("form_retorno"):
                v_aberto = st.selectbox("Selecione o Veículo Retornando:", viagens_abertas['id'].astype(str) + " - " + viagens_abertas['placa'] + " (" + viagens_abertas['condutor'] + ")")
                km_r = st.number_input("KM de Retorno:", min_value=0, step=1)
                if st.form_submit_button("Registrar Retorno", type="primary"):
                    id_viagem = int(v_aberto.split(" - ")[0])
                    km_s_val = viagens_abertas[viagens_abertas['id'] == id_viagem]['km_saida'].iloc[0]
                    
                    if km_r < km_s_val:
                        st.error(f"O KM de Retorno ({km_r}) não pode ser menor que o de Saída ({km_s_val}).")
                    else:
                        with get_conn() as conn:
                            with conn.cursor() as c:
                                c.execute("UPDATE diario_bordo SET km_retorno = %s, data_retorno = NOW(), status = 'Concluído' WHERE id = %s RETURNING veiculo_id", (km_r, id_viagem))
                                vid = c.fetchone()['veiculo_id']
                                c.execute("UPDATE veiculos SET status = 'Disponível' WHERE id = %s", (vid,))
                        st.success("Retorno registrado com sucesso!"); st.cache_data.clear(); st.rerun()

    with aba_hist:
        st.dataframe(fetch_data("SELECT * FROM diario_bordo ORDER BY id DESC LIMIT 50"), use_container_width=True)

# ──────────────────────────────────────────────────────────────────────────────
# MÓDULO: RATEIO (DRE)
# ──────────────────────────────────────────────────────────────────────────────
elif modulo == "📊 Rateio Mensal (DRE)":
    st.title("Fechamento e Rateio Financeiro")
    
    # 1. Busca todas as viagens concluídas
    query_rateio = """
        SELECT 
            v.placa, 
            v.custo_fixo_mensal, 
            db.cc_viagem, 
            SUM(db.km_retorno - db.km_saida) as km_rodado
        FROM diario_bordo db
        JOIN veiculos v ON db.veiculo_id = v.id
        WHERE db.status = 'Concluído'
        GROUP BY v.placa, v.custo_fixo_mensal, db.cc_viagem
    """
    df_viagens = fetch_data(query_rateio)
    
    if df_viagens.empty:
        st.warning("Não há viagens concluídas para processar o rateio.")
    else:
        # Lógica de Distribuição
        df_viagens['km_rodado'] = pd.to_numeric(df_viagens['km_rodado'])
        df_viagens['custo_fixo_mensal'] = pd.to_numeric(df_viagens['custo_fixo_mensal'])
        
        # Calcula KM total de cada carro
        km_total_carro = df_viagens.groupby('placa')['km_rodado'].sum().reset_index().rename(columns={'km_rodado': 'km_total'})
        
        # Junta os DataFrames
        df_rateio = pd.merge(df_viagens, km_total_carro, on='placa')
        
        # Encontra a proporção e o custo final
        df_rateio['prop_uso'] = df_rateio['km_rodado'] / df_rateio['km_total']
        df_rateio['custo_rateado'] = df_rateio['prop_uso'] * df_rateio['custo_fixo_mensal']
        
        # Formatação para exibição
        df_exibicao = df_rateio[['placa', 'cc_viagem', 'km_rodado', 'km_total', 'custo_fixo_mensal', 'custo_rateado']].copy()
        df_exibicao['% de Uso'] = (df_rateio['prop_uso'] * 100).round(2).astype(str) + '%'
        df_exibicao['custo_rateado'] = df_exibicao['custo_rateado'].round(2)
        
        st.subheader("Detalhamento por Veículo")
        st.dataframe(df_exibicao, use_container_width=True)
        
        st.subheader("Visão DRE: Custo Fixo Consolidado por Centro de Custo")
        dre = df_exibicao.groupby('cc_viagem')['custo_rateado'].sum().reset_index()
        dre.columns = ['Centro de Custo', 'Total Alocado (R$)']
        st.dataframe(dre, use_container_width=True)

# ──────────────────────────────────────────────────────────────────────────────
# MÓDULOS DE OCORRÊNCIAS E TRANSFERÊNCIAS (Mantidos Simplificados para Autonomia)
# ──────────────────────────────────────────────────────────────────────────────
elif modulo == "🚨 Ocorrências (Multas/Avarias)":
    st.title("Ocorrências")
    aba_m, aba_a = st.tabs(["💸 Multas", "💥 Avarias"])
    with aba_m: st.info("Formulários de infração prontos para receberem a lógica de insert, semelhante ao pátio.")
    with aba_a: st.info("Formulários de avarias prontos para receberem a lógica de insert, semelhante ao pátio.")

elif modulo == "🔄 Transferências":
    st.title("Transferência de Frota entre Centros de Custo")
    if not df_veic.empty:
        with st.form("f_transf"):
            v_sel = st.selectbox("Selecione o Veículo:", df_veic['placa'] + " (Atual: " + df_veic['cc_atual'] + ")")
            cc_novo = st.selectbox("Transferir para o CC:", lista_ccs)
            km_transf = st.number_input("KM Exato na transferência:", min_value=0)
            if st.form_submit_button("Efetivar"):
                placa = v_sel.split(" ")[0]
                veiculo = df_veic[df_veic['placa'] == placa].iloc[0]
                with get_conn() as conn:
                    with conn.cursor() as c:
                        c.execute("INSERT INTO transferencias_cc (veiculo_id, cc_origem, cc_destino, km_transferencia) VALUES (%s,%s,%s,%s)", (veiculo['id'], veiculo['cc_atual'], cc_novo, km_transf))
                        c.execute("UPDATE veiculos SET cc_atual = %s WHERE id = %s", (cc_novo, veiculo['id']))
                st.success("Transferido!"); st.cache_data.clear(); st.rerun()
