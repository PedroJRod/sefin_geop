import streamlit as st
import duckdb
import pandas as pd
import os
import matplotlib.pyplot as plt

st.set_page_config(page_title="Relatório de Precatórios", layout="wide")

st.title("Relatório de Precatórios")

st.write('''
**Consulta realizada nas Notas de Lançamento Contábeis no SIGEF por meio do evento nº 540905 (APROPRIAÇÃO DE PRECATÓRIOS).
Após a apropriação do saldo é confeccionado as Preparação Pagamento Extra Orçamentaria evento precatorios 700023 - valores individuais boletos
         **
            ''')

# Diretórios
PAI_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'notebooks'))
DATA_RAW = os.path.join(PAI_DIR, "data", "raw")
DATA_PROCESSED = os.path.join(PAI_DIR, "data", "processed")
SCRIPTS = os.path.join(PAI_DIR, "scripts")

# Carregar dados de precatórios
con = duckdb.connect(database=':memory:')
caminho = os.path.join(DATA_RAW, 'precatorios.parquet')
nome_tabela = 'precatorios'
con.execute(f"""
    CREATE TABLE IF NOT EXISTS {nome_tabela} AS
    SELECT * FROM '{caminho}'
""")
df_precatorios = con.execute(f"SELECT * FROM {nome_tabela}").df()

# Agregação
st.header("Agregação Mensal de Precatórios")
df_precatorios = con.execute("""
            SELECT
                SUBSTRING(SIGEF_DB, 6, 4) AS ANO,
                EXTRACT(MONTH FROM DTREFERENCIA) AS MES,
                SUM(CASE WHEN FLESTORNO = 0 THEN VLLANCAMENTO ELSE -VLLANCAMENTO END) AS VALOR
            FROM precatorios
            WHERE INSINAL = 'C' 
            GROUP BY 1,2
            ORDER BY 1,2
            """).df()

# Criar coluna de data base para filtros
df_precatorios['DATA_BASE'] = pd.to_datetime(dict(year=df_precatorios['ANO'], month=df_precatorios['MES'], day=1))

# =============================
# 🔹 Filtros
# =============================
st.sidebar.header("Filtros")

# Intervalo de datas
min_data = df_precatorios['DATA_BASE'].min()
max_data = df_precatorios['DATA_BASE'].max()

periodo = st.sidebar.date_input(
    "Selecione o período",
    [min_data, max_data],
    min_value=min_data,
    max_value=max_data
)

# Filtro por ano
anos_disponiveis = sorted(df_precatorios['ANO'].unique(), reverse=True)
ano_selecionado = st.sidebar.selectbox("Filtrar por ano", options=["Todos"] + list(anos_disponiveis))

# Aplica filtros
df_filtrado = df_precatorios.copy()

if len(periodo) == 2:
    inicio = pd.to_datetime(periodo[0])
    fim = pd.to_datetime(periodo[1])

    df_filtrado = df_filtrado[
        (df_filtrado['DATA_BASE'] >= inicio) &
        (df_filtrado['DATA_BASE'] <= fim)
    ]

if ano_selecionado != "Todos":
    df_filtrado = df_filtrado[df_filtrado['ANO'] == ano_selecionado]

# =============================
# 🔹 Totalizadores
# =============================
st.subheader("Totalizadores do Período Selecionado")
col1, col2 = st.columns(2)
with col1:
    st.metric("Total (R$)", f"{df_filtrado['VALOR'].sum():,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

# =============================
# 🔹 Visualização dos dados
# =============================
st.dataframe(df_filtrado[['ANO','MES','VALOR']])
st.write(df_filtrado[['MES','VALOR']].describe())

# Série temporal
st.header("Evolução Mensal dos Precatórios")
df_filtrado['DATA'] = pd.to_datetime(df_filtrado.rename(columns={'ANO': 'year', 'MES': 'month'})[['year', 'month']].assign(day=1))
fig, ax = plt.subplots(figsize=(10,5))
ax.plot(df_filtrado['DATA'], df_filtrado['VALOR'], marker='o')
ax.set_title("Total de Precatórios Mensais")
ax.set_xlabel("Mês")
ax.set_ylabel("Total de valores")
ax.grid(True)
st.pyplot(fig)

# Evolução mensal por ano
st.header("Evolução Mensal de Precatórios por Ano")
df_filtrado['ANO'] = df_filtrado['DATA'].dt.year
df_filtrado['MES'] = df_filtrado['DATA'].dt.month
anos = sorted(df_filtrado['ANO'].unique(), reverse=True)
meses = range(1,13)
fig, ax = plt.subplots(figsize=(14,6))
for ano in anos:
    df_ano = df_filtrado[df_filtrado['ANO'] == ano]
    ax.plot(df_ano['MES'], df_ano['VALOR'], marker='o', label=f"Ano {ano}")
ax.set_title("Evolução Mensal de Precatórios por Ano")
ax.set_xlabel("Mês")
ax.set_ylabel("Total de Valores")
ax.set_xticks(list(meses))
ax.grid(True)
ax.legend()
st.pyplot(fig)

# Comparação de barras entre anos
st.header("Comparação Mensal de Precatórios por Ano")
df_pivot = df_filtrado.pivot(index='MES', columns='ANO', values='VALOR')
fig, ax = plt.subplots(figsize=(14,6))
df_pivot.plot(kind='bar', ax=ax)
ax.set_title("Comparação Mensal de Precatórios por Ano")
ax.set_xlabel("Mês")
ax.set_ylabel("Total de Valores")
ax.set_xticks(range(len(meses)))
ax.set_xticklabels(list(meses))
ax.grid(axis='y')
st.pyplot(fig)

# Variação percentual mês a mês entre anos
if len(anos) > 1:
    st.header("Variação Percentual entre Anos (%)")
    df_var = df_pivot.pct_change(axis=1) * 100
    fig, ax = plt.subplots(figsize=(14,6))
    df_var.plot(kind='bar', ax=ax)
    ax.set_title("Variação Percentual entre Anos (%)")
    ax.set_xlabel("Mês")
    ax.set_ylabel("Variação (%)")
    ax.set_xticks(range(len(meses)))
    ax.set_xticklabels(list(meses))
    ax.grid(axis='y')
    st.pyplot(fig)

# =============================
# 🔹 Correção pelo IPCA
# =============================
st.header("Correção dos Precatórios pelo IPCA")
caminho_ipca = os.path.join(DATA_RAW, 'inflacao_ipca.parquet')
nome_tabela_ipca = 'inflacao_ipca'
con.execute(f"""
    CREATE TABLE IF NOT EXISTS {nome_tabela_ipca} AS
    SELECT * FROM '{caminho_ipca}'
""")
df_inflacao_ipca = con.execute("SELECT * FROM inflacao_ipca").df()
ipca_mensal = df_inflacao_ipca[df_inflacao_ipca['indicador'] == "IPCA - Variação mensal"].copy()
ipca_mensal['valor'] = pd.to_numeric(ipca_mensal['valor'], errors='coerce')
ipca_mensal['DATA'] = pd.to_datetime(dict(year=ipca_mensal['ANO'], month=ipca_mensal['MES'], day=1))
ipca_mensal = ipca_mensal.sort_values('DATA')
ipca_mensal['FATOR'] = (1 + ipca_mensal['valor']/100).cumprod()
ipca_mensal['INDICE'] = ipca_mensal['FATOR'] / ipca_mensal['FATOR'].iloc[0] * 100

df_filtrado = df_filtrado.merge(ipca_mensal[['DATA','INDICE']], left_on='DATA_BASE', right_on='DATA', how='left')
indice_final = ipca_mensal['INDICE'].iloc[-1]
df_filtrado['VALOR_CORRIGIDO'] = df_filtrado['VALOR'] * (indice_final / df_filtrado['INDICE'])

# Totalizador corrigido
with col2:
    # Soma corrigida: se VALOR_CORRIGIDO for NaN, usar VALOR
    soma_corrigida = df_filtrado.apply(
        lambda row: row['VALOR'] if pd.isna(row['VALOR_CORRIGIDO']) else row['VALOR_CORRIGIDO'], axis=1
    ).sum()
    st.metric("Total Corrigido IPCA (R$)", f"{soma_corrigida:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

# Série temporal corrigida
st.header("Evolução Mensal dos Precatórios (Valores Corrigidos pelo IPCA)")
fig, ax = plt.subplots(figsize=(10,5))
ax.plot(df_filtrado['DATA_BASE'], df_filtrado['VALOR_CORRIGIDO'], marker='o')
ax.set_title("Total de Precatórios Mensais Corrigidos")
ax.set_xlabel("Mês")
ax.set_ylabel("Total de valores corrigidos")
ax.grid(True)
st.pyplot(fig)

# Evolução mensal corrigida por ano
st.header("Evolução Mensal de Precatórios por Ano (Valores Corrigidos pelo IPCA)")
anos = sorted(df_filtrado['ANO'].unique(), reverse=True)
fig, ax = plt.subplots(figsize=(14,6))
for ano in anos:
    df_ano = df_filtrado[df_filtrado['ANO'] == ano]
    ax.plot(df_ano['MES'], df_ano['VALOR_CORRIGIDO'], marker='o', label=f"Ano {ano}")
ax.set_title("Evolução Mensal de Precatórios por Ano (Corrigido pelo IPCA)")
ax.set_xlabel("Mês")
ax.set_ylabel("Total Corrigido (R$)")
ax.set_xticks(list(meses))
ax.grid(True)
ax.legend()
st.pyplot(fig)

# Comparação em barras corrigidas
st.header("Comparação Mensal de Precatórios por Ano (Valores Corrigidos pelo IPCA)")
df_pivot = df_filtrado.pivot(index='MES', columns='ANO', values='VALOR_CORRIGIDO')
fig, ax = plt.subplots(figsize=(14,6))
df_pivot.plot(kind='bar', ax=ax)
ax.set_title("Comparação Mensal de Precatórios por Ano (Valores Corrigidos)")
ax.set_xlabel("Mês")
ax.set_ylabel("Total Corrigido (R$)")
ax.set_xticks(range(len(meses)))
ax.set_xticklabels(list(meses))
ax.grid(axis='y')
st.pyplot(fig)

# Variação percentual corrigida
if len(anos) > 1:
    st.header("Variação Percentual entre Anos (%) - Valores Corrigidos pelo IPCA")
    df_var_corrigido = df_pivot.pct_change(axis=1) * 100
    fig, ax = plt.subplots(figsize=(14,6))
    df_var_corrigido.plot(kind='bar', ax=ax)
    ax.set_title("Variação Percentual entre Anos (%) - Corrigido pelo IPCA")
    ax.set_xlabel("Mês")
    ax.set_ylabel("Variação (%)")
    ax.set_xticks(range(len(meses)))
    ax.set_xticklabels(list(meses))
    ax.grid(axis='y')
    st.pyplot(fig)

# =============================
# 🔹 Tabela final
# =============================
st.header("Tabela Final de Precatórios Corrigidos")
df_precatorios_f = df_filtrado[['ANO','MES','VALOR','DATA_BASE','INDICE','VALOR_CORRIGIDO']].copy()
# Se VALOR_CORRIGIDO for NaN, usa VALOR
import numpy as np
df_precatorios_f['VALOR_CORRIGIDO'] = df_precatorios_f.apply(
    lambda row: row['VALOR'] if pd.isna(row['VALOR_CORRIGIDO']) else row['VALOR_CORRIGIDO'], axis=1
)
df_precatorios_f['VALOR'] = df_precatorios_f['VALOR'].apply(lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
df_precatorios_f['VALOR_CORRIGIDO'] = df_precatorios_f['VALOR_CORRIGIDO'].apply(lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
st.dataframe(df_precatorios_f)




from statsmodels.tsa.holtwinters import ExponentialSmoothing

st.header("Previsão Futura dos Precatórios (Próximos 6 Meses)")
st.write("Usamos o modelo Exponential Smoothing que é eficaz para séries temporais com tendência, mas sem sazonalidade explícita, adequado para nossos dados mensais de precatórios.")

df_precatorios_p = df_filtrado.copy()

df_precatorios_p['VALOR_CORRIGIDO'] = df_precatorios_p.apply(
    lambda row: row['VALOR'] if pd.isna(row['VALOR_CORRIGIDO']) else row['VALOR_CORRIGIDO'], axis=1
)

df_precatorios_p["INDICE"] = df_precatorios_p["INDICE"].interpolate(method="linear")

# --- 2. Previsão com Exponential Smoothing ---
# Usamos VALOR_CORRIGIDO como série principal
serie = df_precatorios_p.set_index("DATA_BASE")["VALOR_CORRIGIDO"]

# Ajuste do modelo (sem sazonalidade explícita, pois temos poucos meses)
modelo = ExponentialSmoothing(serie, trend="add").fit()

# Previsão para os próximos 6 meses
previsao = modelo.forecast(6)

# Converter previsão em DataFrame para facilitar
previsao_df = previsao.reset_index()
previsao_df.columns = ["DATA_BASE", "VALOR_PREVISTO"]

# Formatar valores no padrão brasileiro
previsao_df["VALOR_PREVISTO_BR"] = previsao_df["VALOR_PREVISTO"].apply(
    lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
)

st.write("Previsão dos próximos 6 meses:")
st.dataframe(previsao_df[["DATA_BASE", "VALOR_PREVISTO_BR"]])