import streamlit as st
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from datetime import datetime, timedelta
import io

# ══════════════════════════════════════════════════════════════════════════════
# 1. CONFIGURAÇÃO GLOBAL E ESTILO
# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="ERP Frota Brastel", layout="wide", page_icon="🚙")

st.markdown("""
<style>
    .metric-card { background-color: #f8f9fa; padding: 20px; border-radius: 10px; border-left: 5px solid #0052cc; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# 2. CONEXÃO E ARQUITETURA DE BANCO DE DADOS (COM AUTO-CURA)
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
    """Executa queries de forma segura e padronizada."""
    with get_conn() as conn:
        with conn.cursor() as c:
            c.execute(query, params)
            if fetch:
                return c.fetchall()
            return None

def db_migration():
    """Verifica a versão do banco. Se for a antiga (sem as colunas novas), formata e recria automaticamente."""
    with get_conn() as conn:
        with conn.cursor() as c:
            # AUTO-HEALING: Verifica se a coluna 'status' existe na tabela 'diario_bordo'
            c.execute("SELECT column_name FROM information_schema.columns WHERE table_name='diario_bordo' AND column_name='status'")
            if not c.fetchone():
                # Se não existir, significa que é o banco V1. Vamos formatar para a V2 automaticamente.
                tabelas = ["transferencias_cc", "avarias", "multas", "diario_bordo", "veiculos", "condutores", "centros_custo"]
                for tab in tabelas:
                    c.execute(f"DROP TABLE IF EXISTS {tab} CASCADE")
            
            # Recriação oficial das tabelas com a estrutura V2
            c.execute('CREATE TABLE IF NOT EXISTS centros_custo (nome TEXT PRIMARY KEY)')
            
            c.execute('''CREATE TABLE IF NOT EXISTS veiculos (
                id SERIAL PRIMARY KEY, placa TEXT UNIQUE NOT NULL, modelo TEXT NOT NULL,
                cc_atual TEXT REFERENCES centros_custo(nome), custo_fixo_mensal NUMERIC(10,2) DEFAULT 0,
                status TEXT DEFAULT 'Disponível', km_atual INTEGER DEFAULT 0
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS condutores (
                id SERIAL PRIMARY KEY, nome TEXT NOT NULL, cnh TEXT UNIQUE NOT NULL,
                validade_cnh DATE NOT NULL, cc_padrao TEXT REFERENCES centros_custo(nome),
                status TEXT DEFAULT 'Ativo'
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS diario_bordo (
                id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
                condutor_id INTEGER REFERENCES condutores(id), cc_viagem TEXT REFERENCES centros_custo(nome),
                km_saida INTEGER NOT NULL, data_saida TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                km_retorno INTEGER, data_retorno TIMESTAMP,
                status TEXT DEFAULT 'Em Andamento'
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS multas (
                id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
                condutor_id INTEGER REFERENCES condutores(id), data_infracao DATE NOT NULL,
                valor NUMERIC(10,2) NOT NULL, descricao TEXT, status_pagamento TEXT DEFAULT 'A Pagar'
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS avarias (
                id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
                condutor_relacionado INTEGER REFERENCES condutores(id), data_registro DATE NOT NULL,
                descricao TEXT NOT NULL, custo_estimado NUMERIC(10,2), status TEXT DEFAULT 'Pendente'
            )''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS transferencias_cc (
                id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
                cc_origem TEXT, cc_destino TEXT REFERENCES centros_custo(nome),
                data_transferencia TIMESTAMP DEFAULT CURRENT_TIMESTAMP, km_transferencia INTEGER NOT NULL
            )''')

# Executa a migração/cura toda vez que o app inicia
db_migration()

# ══════════════════════════════════════════════════════════════════════════════
# 3. HELPERS E FUNÇÕES GERAIS
# ══════════════════════════════════════════════════════════════════════════════
def gerar_excel(df, nome_arquivo="exportacao.xlsx"):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Dados')
    return output.getvalue()

def fuso_br():
    return datetime.now() - timedelta(hours=3)

def formatar_data_br(serie_pandas, com_hora=False):
    """Formata série do Pandas para padrão Brasileiro DD/MM/YYYY"""
    formato = '%d/%m/%Y %H:%M' if com_hora else '%d/%m/%Y'
    return pd.to_datetime(serie_pandas, errors='coerce').dt.strftime(formato)

try:
    df_ccs = pd.DataFrame(execute_query("SELECT nome FROM centros_custo ORDER BY nome", fetch=True))
    lista_ccs = df_ccs['nome'].tolist() if not df_ccs.empty else []
except Exception:
    lista_ccs = []

# ══════════════════════════════════════════════════════════════════════════════
# 4. NAVEGAÇÃO LATERAL E MÓDULOS
# ══════════════════════════════════════════════════════════════════════════════
st.sidebar.title("🚙 ERP Frota")
modulo = st.sidebar.radio("Navegação Operacional:", [
    "📈 Dashboard Gerencial",
    "📋 Pátio e Operação",
    "🚨 Multas e Avarias",
    "🔄 Transferência de Veículos",
    "📊 Fechamento DRE (Rateio)",
    "⚙️ Administração e Cadastros"
])
st.sidebar.divider()
st.sidebar.caption("Data Base: " + fuso_br().strftime("%d/%m/%Y"))

# ──────────────────────────────────────────────────────────────────────────────
# MÓDULO 1: DASHBOARD
# ──────────────────────────────────────────────────────────────────────────────
if modulo == "📈 Dashboard Gerencial":
    st.title("Visão Geral da Frota")
    
    kpis = execute_query("""
        SELECT 
            (SELECT COUNT(*) FROM veiculos) as total_veic,
            (SELECT COUNT(*) FROM veiculos WHERE status = 'Em Uso') as veic_uso,
            (SELECT COUNT(*) FROM condutores WHERE status = 'Ativo') as cond_ativos,
            (SELECT SUM(valor) FROM multas WHERE status_pagamento = 'A Pagar') as multas_pendentes
    """, fetch=True)[0]
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total de Veículos", kpis['total_veic'])
    c2.metric("Veículos na Rua", kpis['veic_uso'])
    c3.metric("Condutores Ativos", kpis['cond_ativos'])
    c4.metric("Multas a Pagar", f"R$ {kpis['multas_pendentes'] or 0:.2f}")
    
    st.divider()
    st.subheader("Veículos Atualmente na Rua")
    viagens_ativas = execute_query("""
        SELECT v.placa, v.modelo, c.nome as condutor, db.cc_viagem, db.km_saida, db.data_saida 
        FROM diario_bordo db 
        JOIN veiculos v ON db.veiculo_id = v.id JOIN condutores c ON db.condutor_id = c.id 
        WHERE db.status = 'Em Andamento' ORDER BY db.data_saida DESC
    """, fetch=True)
    
    if viagens_ativas:
        df_va = pd.DataFrame(viagens_ativas)
        df_va['data_saida'] = formatar_data_br(df_va['data_saida'], com_hora=True)
        df_va.rename(columns={'data_saida': 'Data/Hora Saída', 'km_saida': 'KM Saída'}, inplace=True)
        st.dataframe(df_va, use_container_width=True, hide_index=True)
    else:
        st.info("Todos os veículos estão no pátio no momento.")

# ──────────────────────────────────────────────────────────────────────────────
# MÓDULO 2: PÁTIO (DIÁRIO DE BORDO)
# ──────────────────────────────────────────────────────────────────────────────
elif modulo == "📋 Pátio e Operação":
    st.title("Controle de Portaria")
    aba_saida, aba_retorno, aba_historico = st.tabs(["🚀 Liberar Saída", "📥 Registrar Retorno", "📜 Histórico Completo"])
    
    with aba_saida:
        veiculos_disp = execute_query("SELECT id, placa, modelo, cc_atual, km_atual FROM veiculos WHERE status = 'Disponível' ORDER BY placa", fetch=True)
        condutores_ativos = execute_query("SELECT id, nome, cnh, validade_cnh FROM condutores WHERE status = 'Ativo' ORDER BY nome", fetch=True)
        
        if not veiculos_disp:
            st.warning("Nenhum veículo disponível no pátio.")
        elif not condutores_ativos:
            st.warning("Nenhum condutor ativo cadastrado.")
        else:
            with st.form("form_saida"):
                c1, c2 = st.columns(2)
                v_sel = c1.selectbox("Selecione o Veículo:", [f"{v['id']} | {v['placa']} - {v['modelo']} (KM: {v['km_atual']})" for v in veiculos_disp])
                cond_sel = c2.selectbox("Selecione o Condutor:", [f"{c['id']} | {c['nome']} (CNH: {c['cnh']})" for c in condutores_ativos])
                
                c3, c4 = st.columns(2)
                km_saida = c3.number_input("KM de Saída (Hodômetro):", min_value=0, step=1)
                cc_viagem = c4.selectbox("Centro de Custo Responsável:", lista_ccs)
                
                if st.form_submit_button("Liberar Veículo", type="primary"):
                    vid = int(v_sel.split(" | ")[0])
                    cid = int(cond_sel.split(" | ")[0])
                    
                    dados_condutor = next(c for c in condutores_ativos if c['id'] == cid)
                    if dados_condutor['validade_cnh'] < fuso_br().date():
                        st.error(f"⛔ OPERAÇÃO BLOQUEADA: A CNH de {dados_condutor['nome']} está vencida desde {dados_condutor['validade_cnh'].strftime('%d/%m/%Y')}!")
                    else:
                        execute_query("INSERT INTO diario_bordo (veiculo_id, condutor_id, cc_viagem, km_saida, data_saida) VALUES (%s,%s,%s,%s,%s)", (vid, cid, cc_viagem, km_saida, fuso_br()))
                        execute_query("UPDATE veiculos SET status = 'Em Uso', km_atual = %s WHERE id = %s", (km_saida, vid))
                        st.success("✅ Saída registrada e veículo bloqueado para novos usos."); st.rerun()

    with aba_retorno:
        em_andamento = execute_query("""
            SELECT db.id as db_id, v.id as v_id, v.placa, c.nome as condutor, db.km_saida, db.data_saida 
            FROM diario_bordo db JOIN veiculos v ON db.veiculo_id = v.id JOIN condutores c ON db.condutor_id = c.id 
            WHERE db.status = 'Em Andamento'
        """, fetch=True)
        
        if not em_andamento:
            st.info("Não há viagens aguardando retorno.")
        else:
            with st.form("form_retorno"):
                viagem_sel = st.selectbox("Selecione o veículo retornando:", [f"{v['db_id']} | {v['placa']} - Motorista: {v['condutor']} (Saiu com {v['km_saida']} KM)" for v in em_andamento])
                km_retorno = st.number_input("KM de Retorno Exato:", min_value=0, step=1)
                
                if st.form_submit_button("Registrar Retorno", type="primary"):
                    db_id = int(viagem_sel.split(" | ")[0])
                    dados_viagem = next(v for v in em_andamento if v['db_id'] == db_id)
                    
                    if km_retorno < dados_viagem['km_saida']:
                        st.error(f"⛔ Erro: O KM de retorno ({km_retorno}) não pode ser menor que o KM de saída ({dados_viagem['km_saida']}).")
                    else:
                        execute_query("UPDATE diario_bordo SET km_retorno = %s, data_retorno = %s, status = 'Concluído' WHERE id = %s", (km_retorno, fuso_br(), db_id))
                        execute_query("UPDATE veiculos SET status = 'Disponível', km_atual = %s WHERE id = %s", (km_retorno, dados_viagem['v_id']))
                        st.success("✅ Retorno registrado com sucesso."); st.rerun()

    with aba_historico:
        hist_viagens = execute_query("""
            SELECT db.id, v.placa, c.nome as condutor, db.cc_viagem, db.status,
                   db.km_saida, db.km_retorno, (db.km_retorno - db.km_saida) as km_rodado,
                   db.data_saida, db.data_retorno 
            FROM diario_bordo db JOIN veiculos v ON db.veiculo_id = v.id JOIN condutores c ON db.condutor_id = c.id 
            ORDER BY db.data_saida DESC LIMIT 200
        """, fetch=True)
        
        if hist_viagens:
            df_hist = pd.DataFrame(hist_viagens)
            df_hist['data_saida'] = formatar_data_br(df_hist['data_saida'], com_hora=True)
            df_hist['data_retorno'] = formatar_data_br(df_hist['data_retorno'], com_hora=True).replace('NaT', 'Em trânsito')
            st.dataframe(df_hist, use_container_width=True, hide_index=True)
            st.download_button("📥 Exportar Relatório", gerar_excel(df_hist, "Historico"), "Historico_Patio.xlsx")

# ──────────────────────────────────────────────────────────────────────────────
# MÓDULO 3: OCORRÊNCIAS
# ──────────────────────────────────────────────────────────────────────────────
elif modulo == "🚨 Multas e Avarias":
    st.title("Gestão de Ocorrências")
    aba_multas, aba_avarias = st.tabs(["💸 Registro de Multas", "🛠️ Registro de Avarias"])
    
    veiculos_todos = execute_query("SELECT id, placa, modelo FROM veiculos ORDER BY placa", fetch=True)
    condutores_todos = execute_query("SELECT id, nome, cnh FROM condutores ORDER BY nome", fetch=True)
    
    with aba_multas:
        with st.form("form_multa", clear_on_submit=True):
            c1, c2 = st.columns(2)
            v_m = c1.selectbox("Veículo da Infração:", [f"{v['id']} | {v['placa']}" for v in veiculos_todos]) if veiculos_todos else None
            c_m = c2.selectbox("Condutor Autuado:", [f"{c['id']} | {c['nome']} ({c['cnh']})" for c in condutores_todos]) if condutores_todos else None
            
            c3, c4, c5 = st.columns([1, 1, 2])
            dt_m = c3.date_input("Data da Infração:", format="DD/MM/YYYY")
            vlr_m = c4.number_input("Valor da Multa (R$):", min_value=0.0, format="%.2f")
            desc_m = c5.text_input("Local/Descrição da Infração:")
            
            if st.form_submit_button("Lançar Multa", type="primary") and v_m and c_m:
                vid, cid = int(v_m.split(" | ")[0]), int(c_m.split(" | ")[0])
                execute_query("INSERT INTO multas (veiculo_id, condutor_id, data_infracao, valor, descricao) VALUES (%s,%s,%s,%s,%s)", (vid, cid, dt_m, vlr_m, desc_m))
                st.success("✅ Multa registrada."); st.rerun()
                
        st.subheader("Multas Registradas")
        multas_cadastradas = execute_query("""
            SELECT m.id, v.placa, c.nome as condutor, m.data_infracao, m.valor, m.descricao, m.status_pagamento 
            FROM multas m JOIN veiculos v ON m.veiculo_id = v.id JOIN condutores c ON m.condutor_id = c.id ORDER BY m.data_infracao DESC
        """, fetch=True)
        if multas_cadastradas:
            df_multas = pd.DataFrame(multas_cadastradas)
            df_multas['data_infracao'] = formatar_data_br(df_multas['data_infracao'])
            st.dataframe(df_multas, use_container_width=True, hide_index=True)

    with aba_avarias:
        with st.form("form_avaria", clear_on_submit=True):
            c1, c2 = st.columns(2)
            v_a = c1.selectbox("Veículo Avariado:", [f"{v['id']} | {v['placa']}" for v in veiculos_todos]) if veiculos_todos else None
            c_a = c2.selectbox("Condutor Responsável:", ["Nenhum"] + [f"{c['id']} | {c['nome']}" for c in condutores_todos]) if condutores_todos else None
            
            c3, c4 = st.columns(2)
            dt_a = c3.date_input("Data da Constatação:", format="DD/MM/YYYY")
            vlr_a = c4.number_input("Custo de Reparo Estimado (R$):", min_value=0.0, format="%.2f")
            desc_a = st.text_area("Detalhes da Avaria:")
            
            if st.form_submit_button("Registrar Avaria", type="primary") and v_a:
                vid = int(v_a.split(" | ")[0])
                cid = int(c_a.split(" | ")[0]) if c_a != "Nenhum" else None
                execute_query("INSERT INTO avarias (veiculo_id, condutor_relacionado, data_registro, descricao, custo_estimado) VALUES (%s,%s,%s,%s,%s)", (vid, cid, dt_a, desc_a, vlr_a))
                st.success("✅ Avaria registrada."); st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
# MÓDULO 4: TRANSFERÊNCIAS
# ──────────────────────────────────────────────────────────────────────────────
elif modulo == "🔄 Transferência de Veículos":
    st.title("Transferência entre Centros de Custo")
    
    veiculos = execute_query("SELECT id, placa, cc_atual, km_atual FROM veiculos ORDER BY placa", fetch=True)
    if not veiculos:
        st.warning("Sem veículos para transferir.")
    else:
        with st.form("form_transf"):
            v_sel = st.selectbox("Veículo:", [f"{v['id']} | {v['placa']} (Atual: {v['cc_atual']})" for v in veiculos])
            cc_novo = st.selectbox("Novo Centro de Custo:", lista_ccs)
            km_transf = st.number_input("KM do veículo na transferência:", min_value=0, step=1)
            
            if st.form_submit_button("Efetivar Transferência", type="primary"):
                vid = int(v_sel.split(" | ")[0])
                v_dados = next(v for v in veiculos if v['id'] == vid)
                
                if v_dados['cc_atual'] == cc_novo:
                    st.error("⛔ Veículo já pertence a este CC.")
                elif km_transf < (v_dados['km_atual'] or 0):
                    st.error(f"⛔ KM informado menor que o último registrado ({v_dados['km_atual']}).")
                else:
                    execute_query("INSERT INTO transferencias_cc (veiculo_id, cc_origem, cc_destino, km_transferencia) VALUES (%s,%s,%s,%s)", (vid, v_dados['cc_atual'], cc_novo, km_transf))
                    execute_query("UPDATE veiculos SET cc_atual = %s, km_atual = %s WHERE id = %s", (cc_novo, km_transf, vid))
                    st.success(f"✅ Transferido para {cc_novo}."); st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
# MÓDULO 5: RATEIO E DRE
# ──────────────────────────────────────────────────────────────────────────────
elif modulo == "📊 Fechamento DRE (Rateio)":
    st.title("Motor de Rateio Financeiro (DRE)")
    
    with st.form("form_filtro_dre"):
        c1, c2 = st.columns(2)
        hoje = fuso_br().date()
        dt_inicio = c1.date_input("Data Inicial:", value=hoje.replace(day=1), format="DD/MM/YYYY")
        dt_fim = c2.date_input("Data Final:", value=hoje, format="DD/MM/YYYY")
        gerar = st.form_submit_button("Processar DRE", type="primary")
    
    if gerar:
        query_rateio = """
            SELECT 
                v.placa, v.custo_fixo_mensal, db.cc_viagem, 
                SUM(db.km_retorno - db.km_saida) as km_rodado
            FROM diario_bordo db JOIN veiculos v ON db.veiculo_id = v.id
            WHERE db.status = 'Concluído' AND DATE(db.data_retorno) BETWEEN %s AND %s
            GROUP BY v.placa, v.custo_fixo_mensal, db.cc_viagem
        """
        viagens_periodo = execute_query(query_rateio, (dt_inicio, dt_fim), fetch=True)
        
        if not viagens_periodo:
            st.warning("Nenhuma viagem concluída neste período.")
        else:
            df_dre = pd.DataFrame(viagens_periodo)
            df_dre['km_rodado'] = pd.to_numeric(df_dre['km_rodado'])
            df_dre['custo_fixo_mensal'] = pd.to_numeric(df_dre['custo_fixo_mensal'])
            
            km_total_por_carro = df_dre.groupby('placa')['km_rodado'].sum().reset_index().rename(columns={'km_rodado': 'km_total_carro'})
            df_calc = pd.merge(df_dre, km_total_por_carro, on='placa')
            df_calc = df_calc[df_calc['km_total_carro'] > 0]
            
            df_calc['Proporcao_Uso'] = df_calc['km_rodado'] / df_calc['km_total_carro']
            df_calc['Custo_Rateado'] = df_calc['Proporcao_Uso'] * df_calc['custo_fixo_mensal']
            
            df_exibicao = df_calc[['placa', 'cc_viagem', 'km_rodado', 'km_total_carro', 'custo_fixo_mensal', 'Custo_Rateado']].copy()
            df_exibicao['% de Uso'] = (df_calc['Proporcao_Uso'] * 100).round(2).astype(str) + '%'
            df_exibicao.rename(columns={'placa': 'Placa', 'cc_viagem': 'Centro de Custo', 'km_rodado': 'KM Rodado (CC)', 'km_total_carro': 'KM Total Veículo', 'custo_fixo_mensal': 'Custo Base (R$)', 'Custo_Rateado': 'Custo Alocado (R$)'}, inplace=True)
            
            st.subheader("1. Memória de Cálculo por Veículo")
            st.dataframe(df_exibicao, use_container_width=True, hide_index=True)
            
            st.subheader("2. DRE Consolidada")
            dre_consolidada = df_exibicao.groupby('Centro de Custo')['Custo Alocado (R$)'].sum().reset_index()
            st.dataframe(dre_consolidada, use_container_width=True, hide_index=True)
            
            st.download_button("📥 Baixar DRE (Excel)", gerar_excel(df_exibicao, "Rateio"), "Rateio_Frota.xlsx")

# ──────────────────────────────────────────────────────────────────────────────
# MÓDULO 6: ADMINISTRAÇÃO E CADASTROS
# ──────────────────────────────────────────────────────────────────────────────
elif modulo == "⚙️ Administração e Cadastros":
    st.title("Gestão de Cadastros Base")
    aba_cc, aba_cond, aba_veic, aba_danger = st.tabs(["🏢 Centros de Custo", "👷 Condutores", "🚙 Veículos", "⚠️ Formatar Sistema"])
    
    with aba_cc:
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Cadastro Individual")
            novo_cc = st.text_input("Nome do Setor/CC:")
            if st.button("Salvar Centro de Custo") and novo_cc:
                execute_query("INSERT INTO centros_custo (nome) VALUES (%s) ON CONFLICT DO NOTHING", (novo_cc.strip(),))
                st.success("Salvo!"); st.rerun()
        with c2:
            st.subheader("Importação em Massa")
            cc_massa = st.text_area("Cole a lista (um CC por linha):")
            if st.button("Salvar Lista") and cc_massa:
                for linha in cc_massa.split('\n'):
                    if linha.strip():
                        execute_query("INSERT INTO centros_custo (nome) VALUES (%s) ON CONFLICT DO NOTHING", (linha.strip(),))
                st.success("Processado!"); st.rerun()
        st.write("CCs Existentes:", lista_ccs)

    with aba_cond:
        st.subheader("Importar Planilha (Excel)")
        st.write("Colunas necessárias: **nome**, **cnh**, **validade_cnh** (YYYY-MM-DD), **cc_padrao**")
        up_cond = st.file_uploader("Subir Condutores", type=["xlsx", "xls"])
        if up_cond and st.button("Processar Upload Condutores"):
            df_up = pd.read_excel(up_cond)
            if not {'nome', 'cnh', 'validade_cnh', 'cc_padrao'}.issubset(df_up.columns):
                st.error("Colunas faltando no Excel.")
            else:
                for _, r in df_up.iterrows():
                    cc_p = str(r['cc_padrao']).strip()
                    execute_query("INSERT INTO centros_custo (nome) VALUES (%s) ON CONFLICT DO NOTHING", (cc_p,))
                    execute_query("INSERT INTO condutores (nome, cnh, validade_cnh, cc_padrao) VALUES (%s,%s,%s,%s) ON CONFLICT (cnh) DO NOTHING", (r['nome'], str(r['cnh']), r['validade_cnh'], cc_p))
                st.success("Condutores importados!"); st.rerun()
        
        st.divider()
        st.subheader("Cadastro Manual")
        with st.form("fc"):
            col1, col2, col3, col4 = st.columns(4)
            n_c = col1.text_input("Nome Completo")
            c_c = col2.text_input("CNH")
            v_c = col3.date_input("Validade", format="DD/MM/YYYY")
            cc_c = col4.selectbox("CC Vinculado", lista_ccs) if lista_ccs else col4.text_input("CC Vinculado")
            if st.form_submit_button("Cadastrar") and n_c:
                execute_query("INSERT INTO condutores (nome, cnh, validade_cnh, cc_padrao) VALUES (%s,%s,%s,%s)", (n_c, c_c, v_c, cc_c))
                st.success("Cadastrado!"); st.rerun()

        df_conds = pd.DataFrame(execute_query("SELECT id, nome, cnh, validade_cnh, status FROM condutores", fetch=True))
        if not df_conds.empty:
            df_conds['validade_cnh'] = formatar_data_br(df_conds['validade_cnh'])
            st.dataframe(df_conds, use_container_width=True, hide_index=True)

    with aba_veic:
        st.subheader("Importar Planilha (Excel)")
        st.write("Colunas necessárias: **placa**, **modelo**, **cc_atual**, **custo_fixo_mensal**")
        up_veic = st.file_uploader("Subir Veículos", type=["xlsx", "xls"])
        if up_veic and st.button("Processar Upload Veículos"):
            df_uv = pd.read_excel(up_veic)
            if not {'placa', 'modelo', 'cc_atual', 'custo_fixo_mensal'}.issubset(df_uv.columns):
                st.error("Colunas inválidas.")
            else:
                for _, r in df_uv.iterrows():
                    cc_v = str(r['cc_atual']).strip()
                    execute_query("INSERT INTO centros_custo (nome) VALUES (%s) ON CONFLICT DO NOTHING", (cc_v,))
                    execute_query("INSERT INTO veiculos (placa, modelo, cc_atual, custo_fixo_mensal) VALUES (%s,%s,%s,%s) ON CONFLICT (placa) DO NOTHING", (str(r['placa']).upper(), r['modelo'], cc_v, r['custo_fixo_mensal']))
                st.success("Veículos importados!"); st.rerun()

        st.divider()
        st.subheader("Cadastro Manual")
        with st.form("fv"):
            col1, col2, col3, col4 = st.columns(4)
            p_v = col1.text_input("Placa")
            m_v = col2.text_input("Modelo")
            c_v = col3.selectbox("CC Fixo", lista_ccs) if lista_ccs else col3.text_input("CC Fixo")
            cf_v = col4.number_input("Mensalidade (R$)", min_value=0.0)
            if st.form_submit_button("Cadastrar") and p_v:
                execute_query("INSERT INTO veiculos (placa, modelo, cc_atual, custo_fixo_mensal) VALUES (%s,%s,%s,%s)", (p_v.upper(), m_v, c_v, cf_v))
                st.success("Cadastrado!"); st.rerun()

        df_vs = pd.DataFrame(execute_query("SELECT placa, modelo, cc_atual, custo_fixo_mensal, status, km_atual FROM veiculos", fetch=True))
        if not df_vs.empty:
            st.dataframe(df_vs, use_container_width=True, hide_index=True)

    with aba_danger:
        st.error("⚠️ FORMATAR E RESETAR BANCO DE DADOS (Zerar Todo o Sistema)")
        st.write("Esta ação limpará completamente o banco de dados e recriará as estruturas zeradas.")
        
        with st.form("form_reset"):
            confirmacao = st.text_input("Digite 'CONFIRMAR' em maiúsculo para liberar a formatação:")
            if st.form_submit_button("🚨 EXECUTAR FORMAT C:", type="primary"):
                if confirmacao == "CONFIRMAR":
                    tabelas = ["transferencias_cc", "avarias", "multas", "diario_bordo", "veiculos", "condutores", "centros_custo"]
                    with get_conn() as conn:
                        with conn.cursor() as c:
                            for tab in tabelas:
                                c.execute(f"DROP TABLE IF EXISTS {tab} CASCADE")
                    db_migration() 
                    st.success("✅ Sistema zerado com sucesso! Recarregue a página.")
                else:
                    st.error("Palavra de segurança incorreta.")
