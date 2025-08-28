import streamlit as st
import duckdb
import pandas as pd
import os
import matplotlib.pyplot as plt

st.set_page_config(page_title="Relatório de Precatórios", layout="wide")

st.title("Relatório de Precatórios")

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

# Visualização dos dados
st.dataframe(df_precatorios)
st.write(df_precatorios.describe())

# Série temporal
st.header("Evolução Mensal dos Precatórios")
df_precatorios['DATA'] = pd.to_datetime(df_precatorios.rename(columns={'ANO': 'year', 'MES': 'month'})[['year', 'month']].assign(day=1))
fig, ax = plt.subplots(figsize=(10,5))
ax.plot(df_precatorios['DATA'], df_precatorios['VALOR'], marker='o')
ax.set_title("Total de Precatórios Mensais")
ax.set_xlabel("Mês")
ax.set_ylabel("Total de valores")
ax.grid(True)
st.pyplot(fig)

# Evolução mensal por ano
st.header("Evolução Mensal de Precatórios por Ano")
df_precatorios['ANO'] = df_precatorios['DATA'].dt.year
df_precatorios['MES'] = df_precatorios['DATA'].dt.month
anos = sorted(df_precatorios['ANO'].unique(), reverse=True)
meses = range(1,13)
fig, ax = plt.subplots(figsize=(14,6))
for ano in anos:
    df_ano = df_precatorios[df_precatorios['ANO'] == ano]
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
df_pivot = df_precatorios.pivot(index='MES', columns='ANO', values='VALOR')
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

# Carregar inflação IPCA
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

df_precatorios['DATA_BASE'] = pd.to_datetime(dict(year=df_precatorios['ANO'], month=df_precatorios['MES'], day=1))
df_merged = df_precatorios.merge(ipca_mensal[['DATA','INDICE']], left_on='DATA_BASE', right_on='DATA', how='left')
indice_final = ipca_mensal['INDICE'].iloc[-1]
df_merged['VALOR_CORRIGIDO'] = df_merged['VALOR'] * (indice_final / df_merged['INDICE'])
df_precatorios = df_merged.copy()

# Evolução mensal corrigida
st.header("Evolução Mensal de Precatórios por Ano (Valores Corrigidos pelo IPCA)")
anos = sorted(df_precatorios['ANO'].unique(), reverse=True)
fig, ax = plt.subplots(figsize=(14,6))
for ano in anos:
    df_ano = df_precatorios[df_precatorios['ANO'] == ano]
    ax.plot(df_ano['MES'], df_ano['VALOR_CORRIGIDO'], marker='o', label=f"Ano {ano}")
ax.set_title("Evolução Mensal de Precatórios por Ano (Valores Corrigidos pelo IPCA)")
ax.set_xlabel("Mês")
ax.set_ylabel("Total de Valores Corrigidos (R$)")
ax.set_xticks(list(meses))
ax.grid(True)
ax.legend()
st.pyplot(fig)

# Comparação em barras corrigidas
st.header("Comparação Mensal de Precatórios por Ano (Valores Corrigidos pelo IPCA)")
df_pivot = df_precatorios.pivot(index='MES', columns='ANO', values='VALOR_CORRIGIDO')
fig, ax = plt.subplots(figsize=(14,6))
df_pivot.plot(kind='bar', ax=ax)
ax.set_title("Comparação Mensal de Precatórios por Ano (Valores Corrigidos pelo IPCA)")
ax.set_xlabel("Mês")
ax.set_ylabel("Total Corrigido (R$)")
ax.set_xticks(range(len(meses)))
ax.set_xticklabels(list(meses))
ax.grid(axis='y')
st.pyplot(fig)

# Variação percentual mês a mês entre anos dos valores corrigidos pelo IPCA
if len(anos) > 1:
    st.header("Variação Percentual entre Anos (%) - Valores Corrigidos pelo IPCA")
    df_var_corrigido = df_pivot.pct_change(axis=1) * 100
    fig, ax = plt.subplots(figsize=(14,6))
    df_var_corrigido.plot(kind='bar', ax=ax)
    ax.set_title("Variação Percentual entre Anos (%) - Valores Corrigidos pelo IPCA")
    ax.set_xlabel("Mês")
    ax.set_ylabel("Variação (%)")
    ax.set_xticks(range(len(meses)))
    ax.set_xticklabels(list(meses))
    ax.grid(axis='y')
    st.pyplot(fig)

# Tabela final formatada
st.header("Tabela Final de Precatórios Corrigidos")
df_precatorios_f = df_precatorios.copy()
df_precatorios_f['VALOR'] = df_precatorios_f['VALOR'].apply(lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
df_precatorios_f['VALOR_CORRIGIDO'] = df_precatorios_f['VALOR_CORRIGIDO'].apply(lambda x: f"{x:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
st.dataframe(df_precatorios_f)
