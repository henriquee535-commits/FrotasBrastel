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
    .status-uso { color: #d9534f; font-weight: bold; }
    .status-disp { color: #5cb85c; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# 2. CONEXÃO E ARQUITETURA DE BANCO DE DADOS (ROBUSTA)
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
    """Garante que as tabelas e as colunas existam, atualizando schemas antigos automaticamente."""
    queries_criacao = [
        'CREATE TABLE IF NOT EXISTS centros_custo (nome TEXT PRIMARY KEY)',
        
        '''CREATE TABLE IF NOT EXISTS veiculos (
            id SERIAL PRIMARY KEY, placa TEXT UNIQUE NOT NULL, modelo TEXT NOT NULL,
            cc_atual TEXT REFERENCES centros_custo(nome), custo_fixo_mensal NUMERIC(10,2) DEFAULT 0,
            status TEXT DEFAULT 'Disponível'
        )''',
        
        '''CREATE TABLE IF NOT EXISTS condutores (
            id SERIAL PRIMARY KEY, nome TEXT NOT NULL, cnh TEXT UNIQUE NOT NULL,
            validade_cnh DATE NOT NULL, cc_padrao TEXT REFERENCES centros_custo(nome)
        )''',
        
        '''CREATE TABLE IF NOT EXISTS diario_bordo (
            id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
            condutor_id INTEGER REFERENCES condutores(id), cc_viagem TEXT REFERENCES centros_custo(nome),
            km_saida INTEGER NOT NULL, data_saida TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            km_retorno INTEGER, data_retorno TIMESTAMP,
            status TEXT DEFAULT 'Em Andamento'
        )''',
        
        '''CREATE TABLE IF NOT EXISTS multas (
            id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
            condutor_id INTEGER REFERENCES condutores(id), data_infracao DATE NOT NULL,
            valor NUMERIC(10,2) NOT NULL, descricao TEXT, status_pagamento TEXT DEFAULT 'A Pagar'
        )''',
        
        '''CREATE TABLE IF NOT EXISTS avarias (
            id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
            condutor_relacionado INTEGER REFERENCES condutores(id), data_registro DATE NOT NULL,
            descricao TEXT NOT NULL, custo_estimado NUMERIC(10,2), status TEXT DEFAULT 'Pendente'
        )''',
        
        '''CREATE TABLE IF NOT EXISTS transferencias_cc (
            id SERIAL PRIMARY KEY, veiculo_id INTEGER REFERENCES veiculos(id),
            cc_origem TEXT, cc_destino TEXT REFERENCES centros_custo(nome),
            data_transferencia TIMESTAMP DEFAULT CURRENT_TIMESTAMP, km_transferencia INTEGER NOT NULL
        )'''
    ]
    
    with get_conn() as conn:
        with conn.cursor() as c:
            for q in queries_criacao:
                c.execute(q)
            
            # Migrações Automáticas (Adiciona colunas faltantes em bancos que já existiam)
            colunas_verificar = [
                ("condutores", "status", "TEXT DEFAULT 'Ativo'"),
                ("veiculos", "km_atual", "INTEGER DEFAULT 0"),
                ("multas", "vencimento", "DATE"),
            ]
            
            for tabela, coluna, tipo in colunas_verificar:
                try:
                    c.execute(f"ALTER TABLE {tabela} ADD COLUMN {coluna} {tipo}")
                except psycopg2.errors.DuplicateColumn:
                    conn.rollback() # Ignora se a coluna já existir

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

# Buscas transacionais (sem cache para refletir dados em tempo real)
df_ccs = pd.DataFrame(execute_query("SELECT nome FROM centros_custo ORDER BY nome", fetch=True))
lista_ccs = df_ccs['nome'].tolist() if not df_ccs.empty else []

# ══════════════════════════════════════════════════════════════════════════════
# 4. NAVEGAÇÃO LATERAL
# ══════════════════════════════════════════════════════════════════════════════
st.sidebar.image("https://cdn-icons-png.flaticon.com/512/3204/3204073.png", width=80)
st.sidebar.title("ERP Frota")
modulo = st.sidebar.radio("Navegação Operacional:", [
    "📈 Dashboard Gerencial",
    "📋 Pátio e Operação",
    "🚨 Multas e Avarias",
    "🔄 Transferência de Veículos",
    "📊 Fechamento DRE (Rateio)",
    "⚙️ Administração e Cadastros"
])
st.sidebar.divider()
st.sidebar.caption("v2.0 - Sistema de Governança")

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
        df_va['data_saida'] = pd.to_datetime(df_va['data_saida']).dt.strftime('%d/%m/%Y %H:%M')
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
                cc_viagem = c4.selectbox("Centro de Custo que pagará a viagem:", lista_ccs)
                
                if st.form_submit_button("Liberar Veículo", type="primary"):
                    vid = int(v_sel.split(" | ")[0])
                    cid = int(cond_sel.split(" | ")[0])
                    
                    # Trava de Segurança: Verificar validade da CNH
                    dados_condutor = next(c for c in condutores_ativos if c['id'] == cid)
                    if dados_condutor['validade_cnh'] < fuso_br().date():
                        st.error(f"⛔ OPERAÇÃO BLOQUEADA: A CNH de {dados_condutor['nome']} está vencida desde {dados_condutor['validade_cnh'].strftime('%d/%m/%Y')}!")
                    else:
                        try:
                            execute_query("INSERT INTO diario_bordo (veiculo_id, condutor_id, cc_viagem, km_saida, data_saida) VALUES (%s,%s,%s,%s,%s)", (vid, cid, cc_viagem, km_saida, fuso_br()))
                            execute_query("UPDATE veiculos SET status = 'Em Uso', km_atual = %s WHERE id = %s", (km_saida, vid))
                            st.success("✅ Saída registrada e veículo bloqueado para novos usos."); st.rerun()
                        except Exception as e:
                            st.error(f"Erro no banco de dados: {e}")

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
                
                if st.form_submit_button("Registrar Retorno e Disponibilizar Veículo", type="primary"):
                    db_id = int(viagem_sel.split(" | ")[0])
                    dados_viagem = next(v for v in em_andamento if v['db_id'] == db_id)
                    
                    if km_retorno < dados_viagem['km_saida']:
                        st.error(f"⛔ Erro: O KM de retorno ({km_retorno}) não pode ser menor que o KM de saída ({dados_viagem['km_saida']}). Verifique o painel do carro.")
                    else:
                        execute_query("UPDATE diario_bordo SET km_retorno = %s, data_retorno = %s, status = 'Concluído' WHERE id = %s", (km_retorno, fuso_br(), db_id))
                        execute_query("UPDATE veiculos SET status = 'Disponível', km_atual = %s WHERE id = %s", (km_retorno, dados_viagem['v_id']))
                        st.success("✅ Retorno registrado com sucesso. Veículo disponível no pátio."); st.rerun()

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
            df_hist['data_saida'] = pd.to_datetime(df_hist['data_saida']).dt.strftime('%d/%m/%Y %H:%M')
            df_hist['data_retorno'] = pd.to_datetime(df_hist['data_retorno']).dt.strftime('%d/%m/%Y %H:%M').replace('NaT', 'Em trânsito')
            st.dataframe(df_hist, use_container_width=True, hide_index=True)
            st.download_button("📥 Exportar Relatório Excel", gerar_excel(df_hist, "Historico_Patio"), "Historico_Patio.xlsx")

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
            dt_m = c3.date_input("Data da Infração:")
            vlr_m = c4.number_input("Valor da Multa (R$):", min_value=0.0, format="%.2f")
            desc_m = c5.text_input("Local/Descrição da Infração:")
            
            if st.form_submit_button("Lançar Multa no Sistema", type="primary") and v_m and c_m:
                vid, cid = int(v_m.split(" | ")[0]), int(c_m.split(" | ")[0])
                execute_query("INSERT INTO multas (veiculo_id, condutor_id, data_infracao, valor, descricao) VALUES (%s,%s,%s,%s,%s)", (vid, cid, dt_m, vlr_m, desc_m))
                st.success("✅ Multa registrada. Ela constará no rateio/dashboard."); st.rerun()
                
        st.subheader("Multas Registradas")
        df_multas = pd.DataFrame(execute_query("""
            SELECT m.id, v.placa, c.nome as condutor, m.data_infracao, m.valor, m.descricao, m.status_pagamento 
            FROM multas m JOIN veiculos v ON m.veiculo_id = v.id JOIN condutores c ON m.condutor_id = c.id ORDER BY m.data_infracao DESC
        """, fetch=True))
        if not df_multas.empty:
            df_multas['data_infracao'] = pd.to_datetime(df_multas['data_infracao']).dt.strftime('%d/%m/%Y')
            st.dataframe(df_multas, use_container_width=True, hide_index=True)

    with aba_avarias:
        with st.form("form_avaria", clear_on_submit=True):
            c1, c2 = st.columns(2)
            v_a = c1.selectbox("Veículo Avariado:", [f"{v['id']} | {v['placa']}" for v in veiculos_todos]) if veiculos_todos else None
            c_a = c2.selectbox("Condutor Responsável (Se aplicável):", ["Nenhum"] + [f"{c['id']} | {c['nome']}" for c in condutores_todos]) if condutores_todos else None
            
            c3, c4 = st.columns(2)
            dt_a = c3.date_input("Data do Ocorrido/Constatação:")
            vlr_a = c4.number_input("Custo de Reparo Estimado/Real (R$):", min_value=0.0, format="%.2f")
            desc_a = st.text_area("Detalhes da Avaria (Ex: Para-choque dianteiro amassado):")
            
            if st.form_submit_button("Registrar Avaria", type="primary") and v_a:
                vid = int(v_a.split(" | ")[0])
                cid = int(c_a.split(" | ")[0]) if c_a != "Nenhum" else None
                execute_query("INSERT INTO avarias (veiculo_id, condutor_relacionado, data_registro, descricao, custo_estimado) VALUES (%s,%s,%s,%s,%s)", (vid, cid, dt_a, desc_a, vlr_a))
                st.success("✅ Avaria registrada no dossiê do veículo."); st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
# MÓDULO 4: TRANSFERÊNCIAS
# ──────────────────────────────────────────────────────────────────────────────
elif modulo == "🔄 Transferência de Veículos":
    st.title("Transferência entre Centros de Custo")
    st.write("Mude o CC 'Dono' do veículo. Isso é vital para calcular o rateio correto de frotas que mudam de setor.")
    
    veiculos = execute_query("SELECT id, placa, cc_atual, km_atual FROM veiculos ORDER BY placa", fetch=True)
    if not veiculos:
        st.warning("Sem veículos para transferir.")
    else:
        with st.form("form_transf"):
            v_sel = st.selectbox("Veículo a ser transferido:", [f"{v['id']} | {v['placa']} (Atual: {v['cc_atual']})" for v in veiculos])
            cc_novo = st.selectbox("Novo Centro de Custo:", lista_ccs)
            km_transf = st.number_input("KM Exato do veículo no momento da transferência:", min_value=0, step=1)
            
            if st.form_submit_button("Efetivar Transferência", type="primary"):
                vid = int(v_sel.split(" | ")[0])
                v_dados = next(v for v in veiculos if v['id'] == vid)
                
                if v_dados['cc_atual'] == cc_novo:
                    st.error("⛔ O veículo já pertence a este Centro de Custo.")
                elif km_transf < (v_dados['km_atual'] or 0):
                    st.error(f"⛔ O KM informado é menor que o último KM registrado no banco ({v_dados['km_atual']}).")
                else:
                    execute_query("INSERT INTO transferencias_cc (veiculo_id, cc_origem, cc_destino, km_transferencia) VALUES (%s,%s,%s,%s)", (vid, v_dados['cc_atual'], cc_novo, km_transf))
                    execute_query("UPDATE veiculos SET cc_atual = %s, km_atual = %s WHERE id = %s", (cc_novo, km_transf, vid))
                    st.success(f"✅ Veículo transferido para {cc_novo}. Histórico gravado para auditoria."); st.rerun()

# ──────────────────────────────────────────────────────────────────────────────
# MÓDULO 5: RATEIO E DRE
# ──────────────────────────────────────────────────────────────────────────────
elif modulo == "📊 Fechamento DRE (Rateio)":
    st.title("Motor de Rateio Financeiro (DRE)")
    st.write("Distribui o Custo Fixo de cada veículo com base no uso (% de KM rodado por CC) no período selecionado.")
    
    with st.form("form_filtro_dre"):
        c1, c2 = st.columns(2)
        dt_inicio = c1.date_input("Data Inicial:")
        dt_fim = c2.date_input("Data Final:")
        gerar = st.form_submit_button("Processar DRE", type="primary")
    
    if gerar:
        # Busca viagens concluídas no período
        query_rateio = """
            SELECT 
                v.placa, v.custo_fixo_mensal, db.cc_viagem, 
                SUM(db.km_retorno - db.km_saida) as km_rodado
            FROM diario_bordo db
            JOIN veiculos v ON db.veiculo_id = v.id
            WHERE db.status = 'Concluído' 
              AND DATE(db.data_retorno) BETWEEN %s AND %s
            GROUP BY v.placa, v.custo_fixo_mensal, db.cc_viagem
        """
        viagens_periodo = execute_query(query_rateio, (dt_inicio, dt_fim), fetch=True)
        
        if not viagens_periodo:
            st.warning("Nenhuma viagem concluída e registrada neste período.")
        else:
            df_dre = pd.DataFrame(viagens_periodo)
            df_dre['km_rodado'] = pd.to_numeric(df_dre['km_rodado'])
            df_dre['custo_fixo_mensal'] = pd.to_numeric(df_dre['custo_fixo_mensal'])
            
            # Matemática do Rateio
            km_total_por_carro = df_dre.groupby('placa')['km_rodado'].sum().reset_index().rename(columns={'km_rodado': 'km_total_carro'})
            df_calc = pd.merge(df_dre, km_total_por_carro, on='placa')
            
            # Prevenindo divisão por zero (se carro rodou 0 km mas concluiu viagem com mesmo KM)
            df_calc = df_calc[df_calc['km_total_carro'] > 0]
            
            df_calc['Proporcao_Uso'] = df_calc['km_rodado'] / df_calc['km_total_carro']
            df_calc['Custo_Rateado'] = df_calc['Proporcao_Uso'] * df_calc['custo_fixo_mensal']
            
            # Formatação
            df_exibicao = df_calc[['placa', 'cc_viagem', 'km_rodado', 'km_total_carro', 'custo_fixo_mensal', 'Custo_Rateado']].copy()
            df_exibicao['% de Uso'] = (df_calc['Proporcao_Uso'] * 100).round(2).astype(str) + '%'
            df_exibicao.rename(columns={'placa': 'Placa', 'cc_viagem': 'Centro de Custo', 'km_rodado': 'KM Rodado (CC)', 'km_total_carro': 'KM Total Veículo', 'custo_fixo_mensal': 'Custo Base (R$)', 'Custo_Rateado': 'Custo Alocado (R$)'}, inplace=True)
            
            st.subheader("1. Memória de Cálculo por Veículo")
            st.dataframe(df_exibicao, use_container_width=True, hide_index=True)
            
            st.subheader("2. DRE Consolidada (Custo Total por Centro de Custo)")
            dre_consolidada = df_exibicao.groupby('Centro de Custo')['Custo Alocado (R$)'].sum().reset_index()
            st.dataframe(dre_consolidada, use_container_width=True, hide_index=True)
            
            st.download_button("📥 Baixar Relatório Contábil (Excel)", gerar_excel(df_exibicao, "Rateio"), "Rateio_Frota.xlsx")

# ──────────────────────────────────────────────────────────────────────────────
# MÓDULO 6: ADMINISTRAÇÃO E CADASTROS
# ──────────────────────────────────────────────────────────────────────────────
elif modulo == "⚙️ Administração e Cadastros":
    st.title("Gestão de Cadastros Base")
    aba_cc, aba_cond, aba_veic, aba_danger = st.tabs(["🏢 Centros de Custo", "👷 Condutores", "🚙 Veículos", "⚠️ Config Avançadas"])
    
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
        st.write("Planilha deve ter as colunas: **nome**, **cnh**, **validade_cnh** (YYYY-MM-DD), **cc_padrao**")
        up_cond = st.file_uploader("Subir Condutores", type=["xlsx", "xls"])
        if up_cond and st.button("Processar Upload Condutores"):
            df_up = pd.read_excel(up_cond)
            colunas_req = {'nome', 'cnh', 'validade_cnh', 'cc_padrao'}
            if not colunas_req.issubset(df_up.columns):
                st.error(f"Erro: Colunas faltando. O Excel precisa ter exatamente estas: {', '.join(colunas_req)}")
            else:
                for _, r in df_up.iterrows():
                    cc_p = str(r['cc_padrao']).strip()
                    execute_query("INSERT INTO centros_custo (nome) VALUES (%s) ON CONFLICT DO NOTHING", (cc_p,))
                    execute_query("INSERT INTO condutores (nome, cnh, validade_cnh, cc_padrao) VALUES (%s,%s,%s,%s) ON CONFLICT (cnh) DO NOTHING", (r['nome'], str(r['cnh']), r['validade_cnh'], cc_p))
                st.success("Condutores importados com sucesso!"); st.rerun()
        
        st.divider()
        st.subheader("Cadastro Manual")
        with st.form("fc"):
            col1, col2, col3, col4 = st.columns(4)
            n_c = col1.text_input("Nome Completo")
            c_c = col2.text_input("CNH (Somente números)")
            v_c = col3.date_input("Validade CNH")
            cc_c = col4.selectbox("CC Vinculado", lista_ccs) if lista_ccs else col4.text_input("CC Vinculado")
            if st.form_submit_button("Cadastrar") and n_c:
                execute_query("INSERT INTO condutores (nome, cnh, validade_cnh, cc_padrao) VALUES (%s,%s,%s,%s)", (n_c, c_c, v_c, cc_c))
                st.success("Cadastrado!"); st.rerun()

        st.dataframe(pd.DataFrame(execute_query("SELECT id, nome, cnh, validade_cnh, status FROM condutores", fetch=True)), use_container_width=True, hide_index=True)

    with aba_veic:
        st.subheader("Importar Planilha (Excel)")
        st.write("Planilha deve ter as colunas: **placa**, **modelo**, **cc_atual**, **custo_fixo_mensal**")
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
                st.success("Veículos importados com sucesso!"); st.rerun()

        st.divider()
        st.subheader("Cadastro Manual")
        with st.form("fv"):
            col1, col2, col3, col4 = st.columns(4)
            p_v = col1.text_input("Placa")
            m_v = col2.text_input("Modelo")
            c_v = col3.selectbox("CC Fixo", lista_ccs) if lista_ccs else col3.text_input("CC Fixo")
            cf_v = col4.number_input("Custo/Mensalidade (R$)", min_value=0.0)
            if st.form_submit_button("Cadastrar") and p_v:
                execute_query("INSERT INTO veiculos (placa, modelo, cc_atual, custo_fixo_mensal) VALUES (%s,%s,%s,%s)", (p_v.upper(), m_v, c_v, cf_v))
                st.success("Cadastrado!"); st.rerun()

        st.dataframe(pd.DataFrame(execute_query("SELECT placa, modelo, cc_atual, custo_fixo_mensal, status, km_atual FROM veiculos", fetch=True)), use_container_width=True, hide_index=True)

    with aba_danger:
        st.error("⚠️ FORMATAR BANCO DE DADOS (DANGER ZONE)")
        st.write("Esta ação apagará todos os registros de viagens, multas, veículos e condutores para começar um ambiente limpo. A estrutura das tabelas será mantida.")
        if st.checkbox("Eu, administrador, concordo em perder todos os dados irreversivelmente."):
            if st.button("🚨 EXECUTAR FORMAT C:", type="primary"):
                tabelas = ["transferencias_cc", "avarias", "multas", "diario_bordo", "veiculos", "condutores", "centros_custo"]
                with get_conn() as conn:
                    with conn.cursor() as c:
                        for tab in tabelas:
                            c.execute(f"TRUNCATE TABLE {tab} CASCADE")
                st.success("✅ Banco de dados limpo com sucesso! Atualize a página."); st.rerun()
