import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from datetime import datetime

# ══════════════════════════════════════════════════════════════════════════════
# 1. INFRAESTRUTURA E BANCO
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
                    custo_fixo_mensal NUMERIC(10,2) DEFAULT 0,
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
                    valor NUMERIC(10,2) NOT NULL, descricao TEXT
                )
            ''')
            c.execute('''
                CREATE TABLE IF NOT EXISTS avarias (
                    id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
                    data_registro DATE NOT NULL, descricao TEXT NOT NULL, custo_estimado NUMERIC(10,2)
                )
            ''')
init_db()

# ══════════════════════════════════════════════════════════════════════════════
# 2. MÓDULO DE PÁTIO (DIÁRIO DE BORDO)
# ══════════════════════════════════════════════════════════════════════════════
st.sidebar.title("🚙 Frota Brastel")
modulo = st.sidebar.radio("Navegação:", ["📋 Pátio", "🚨 Ocorrências", "📊 Rateio DRE", "⚙️ Cadastros"])

df_ccs = pd.DataFrame()
with get_conn() as conn:
    with conn.cursor() as c:
        c.execute("SELECT nome FROM centros_custo ORDER BY nome")
        lista_ccs = [r['nome'] for r in c.fetchall()]

if modulo == "📋 Pátio":
    st.title("Controle de Pátio")
    aba_out, aba_in = st.tabs(["🚀 Saída", "📥 Retorno"])
    
    with aba_out:
        with get_conn() as conn:
            with conn.cursor() as c:
                c.execute("SELECT id, placa, modelo FROM veiculos WHERE status = 'Disponível'")
                veics_off = c.fetchall()
                c.execute("SELECT id, nome FROM condutores WHERE status = 'Ativo'")
                conds = c.fetchall()
        
        if not veics_off or not conds: st.warning("Cadastre veículos disponíveis e condutores.")
        else:
            with st.form("f_saida"):
                v = st.selectbox("Veículo:", [f"{r['id']} - {r['placa']}" for r in veics_off])
                cond = st.selectbox("Condutor:", [f"{r['id']} - {r['nome']}" for r in conds])
                km_s = st.number_input("KM Saída:", min_value=0)
                cc_v = st.selectbox("CC Responsável:", lista_ccs)
                if st.form_submit_button("Liberar"):
                    vid, cid = v.split(" - ")[0], cond.split(" - ")[0]
                    with get_conn() as conn:
                        with conn.cursor() as c:
                            c.execute("INSERT INTO diario_bordo (veiculo_id, condutor_id, cc_viagem, km_saida) VALUES (%s,%s,%s,%s)", (vid, cid, cc_v, km_s))
                            c.execute("UPDATE veiculos SET status = 'Em Uso' WHERE id = %s", (vid,))
                    st.success("Saída registrada!"); st.rerun()

    with aba_in:
        with get_conn() as conn:
            with conn.cursor() as c:
                c.execute("SELECT db.id, v.placa, db.km_saida FROM diario_bordo db JOIN veiculos v ON db.veiculo_id = v.id WHERE db.status = 'Em Andamento'")
                em_uso = c.fetchall()
        
        if not em_uso: st.info("Nenhum veículo em trânsito.")
        else:
            with st.form("f_retorno"):
                viagem = st.selectbox("Viagem Ativa:", [f"{r['id']} - {r['placa']} (Iniciou em: {r['km_saida']} KM)" for r in em_uso])
                km_r = st.number_input("KM Retorno:", min_value=0)
                if st.form_submit_button("Encerrar Viagem"):
                    vid_db = viagem.split(" - ")[0]
                    with get_conn() as conn:
                        with conn.cursor() as c:
                            c.execute("UPDATE diario_bordo SET km_retorno = %s, data_retorno = NOW(), status = 'Concluído' WHERE id = %s RETURNING veiculo_id", (km_r, vid_db))
                            v_id = c.fetchone()['veiculo_id']
                            c.execute("UPDATE veiculos SET status = 'Disponível' WHERE id = %s", (v_id,))
                    st.success("Retorno registrado!"); st.rerun()

# ══════════════════════════════════════════════════════════════════════════════
# 3. MÓDULO DE OCORRÊNCIAS (MULTAS E AVARIAS)
# ══════════════════════════════════════════════════════════════════════════════
elif modulo == "🚨 Ocorrências":
    st.title("Multas e Avarias")
    aba_m, aba_a = st.tabs(["💸 Multas", "🛠️ Avarias"])
    
    with aba_m:
        with st.form("f_multa"):
            placa_m = st.text_input("Placa do Veículo:")
            cnh_m = st.text_input("CNH do Motorista:")
            vlr = st.number_input("Valor:", min_value=0.0)
            data_m = st.date_input("Data da Infração:")
            if st.form_submit_button("Lançar Multa"):
                with get_conn() as conn:
                    with conn.cursor() as c:
                        c.execute("SELECT id FROM veiculos WHERE placa = %s", (placa_m.upper(),))
                        vid = c.fetchone(); c.execute("SELECT id FROM condutores WHERE cnh = %s", (cnh_m,))
                        cid = c.fetchone()
                        if vid and cid:
                            c.execute("INSERT INTO multas (veiculo_id, condutor_id, valor, data_infracao) VALUES (%s,%s,%s,%s)", (vid['id'], cid['id'], vlr, data_m))
                            st.success("Multa lançada!"); st.rerun()
                        else: st.error("Veículo ou Condutor não localizado.")

    with aba_a:
        with st.form("f_avaria"):
            placa_a = st.text_input("Placa:")
            desc = st.text_area("Descrição do Dano:")
            vlr_est = st.number_input("Custo Estimado:", min_value=0.0)
            if st.form_submit_button("Lançar Avaria"):
                with get_conn() as conn:
                    with conn.cursor() as c:
                        c.execute("SELECT id FROM veiculos WHERE placa = %s", (placa_a.upper(),))
                        vid = c.fetchone()
                        if vid:
                            c.execute("INSERT INTO avarias (veiculo_id, data_registro, descricao, custo_estimado) VALUES (%s, NOW(), %s, %s)", (vid['id'], desc, vlr_est))
                            st.success("Avaria registrada!")
                        else: st.error("Placa não encontrada.")

# ══════════════════════════════════════════════════════════════════════════════
# 4. MÓDULO DE RATEIO (DRE)
# ══════════════════════════════════════════════════════════════════════════════
elif modulo == "📊 Rateio DRE":
    st.title("Rateio Mensal Proporcional")
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute("""
                SELECT v.placa, v.custo_fixo_mensal, db.cc_viagem, SUM(db.km_retorno - db.km_saida) as km_total_cc
                FROM diario_bordo db JOIN veiculos v ON db.veiculo_id = v.id
                WHERE db.status = 'Concluído' GROUP BY v.placa, v.custo_fixo_mensal, db.cc_viagem
            """)
            dados = c.fetchall()
    
    if not dados: st.info("Sem viagens concluídas para processar o rateio.")
    else:
        df = pd.DataFrame(dados)
        df['km_total_carro'] = df.groupby('placa')['km_total_cc'].transform('sum')
        df['percentual'] = df['km_total_cc'] / df['km_total_carro']
        df['custo_alocado'] = df['percentual'] * df['custo_fixo_mensal'].astype(float)
        st.dataframe(df[['placa', 'cc_viagem', 'km_total_cc', 'custo_alocado']], use_container_width=True)
        
        st.subheader("Resumo por Centro de Custo")
        st.write(df.groupby('cc_viagem')['custo_alocado'].sum())

# ══════════════════════════════════════════════════════════════════════════════
# 5. CADASTROS (INDIVIDUAL E MASSA)
# ══════════════════════════════════════════════════════════════════════════════
elif modulo == "⚙️ Cadastros":
    st.title("Gestão de Cadastros")
    aba_cc, aba_p, aba_z = st.tabs(["🏢 Centros de Custo", "👷 Pessoas/Veículos", "🗑️ Limpeza"])
    
    with aba_cc:
        cc_massa = st.text_area("Cole os CCs (um por linha):")
        if st.button("Salvar CCs em Massa"):
            with get_conn() as conn:
                with conn.cursor() as c:
                    for linha in cc_massa.split('\n'):
                        if linha.strip(): c.execute("INSERT INTO centros_custo (nome) VALUES (%s) ON CONFLICT DO NOTHING", (linha.strip(),))
            st.success("Processado!"); st.rerun()

    with aba_p:
        st.subheader("Novos Veículos")
        with st.form("f_v"):
            c1, c2, c3, c4 = st.columns(4)
            p = c1.text_input("Placa"); m = c2.text_input("Modelo"); cc = c3.selectbox("CC", lista_ccs); cf = c4.number_input("Custo Fixo")
            if st.form_submit_button("Salvar Veículo"):
                with get_conn() as conn:
                    with conn.cursor() as c: c.execute("INSERT INTO veiculos (placa, modelo, cc_atual, custo_fixo_mensal) VALUES (%s,%s,%s,%s)", (p.upper(), m, cc, cf))
                st.success("Salvo!")

    with aba_z:
        if st.checkbox("Confirmar exclusão total de dados"):
            if st.button("LIMPAR TUDO"):
                with get_conn() as conn:
                    with conn.cursor() as c:
                        for t in ["avarias", "multas", "diario_bordo", "veiculos", "condutores", "centros_custo"]:
                            c.execute(f"TRUNCATE TABLE {t} CASCADE")
                st.success("Banco de dados resetado."); st.rerun()
