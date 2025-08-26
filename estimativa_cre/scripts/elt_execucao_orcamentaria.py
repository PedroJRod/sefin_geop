import oracledb
import pandas as pd
import os
from dotenv import load_dotenv

# Diretórios
PAI_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW = os.path.join(PAI_DIR, "data", "raw")

load_dotenv()

# Configurações Oracle
ORACLE_DSN = f"{os.getenv('ORACLE_HOST')}:{os.getenv('ORACLE_PORT')}/{os.getenv('ORACLE_SERVICE')}"
ORACLE_USER = os.getenv("ORACLE_USER")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")

# Tabela fato
SQL_FATO = """
    SELECT 
        *
    FROM SIGEF_STAGE.STG_SIGEF__FATO__EXECUCAO_ORCAMENTARIA
"""

# Tabelas dimensões
DIM_TABLES = [
    "STG_SIGEF__DIM__ACAO_PROGRAMA",
    "STG_SIGEF__DIM__CONTA_CONTABIL",
    "STG_SIGEF__DIM__CREDOR",
    "STG_SIGEF__DIM__DOMICILIO_BANCARIO",
    "STG_SIGEF__DIM__EVENTO",
    "STG_SIGEF__DIM__FONTE_RECURSO",
    "STG_SIGEF__DIM__GRUPO_PROGRAMACAO_FINANCEIRA",
    "STG_SIGEF__DIM__NATUREZA_DESPESA",
    "STG_SIGEF__DIM__NATUREZA_RECEITA",
    "STG_SIGEF__DIM__UNIDADE_GESTORA_GESTAO"
]

def executar_consulta(sql):
    try:
        with oracledb.connect(user=ORACLE_USER, password=ORACLE_PASSWORD, dsn=ORACLE_DSN) as connection:
            return pd.read_sql(sql, con=connection)
    except Exception as e:
        print(f"Erro na execução da consulta: {e}")
        return None

def salvar_parquet(df, filename):
    if df is not None:
        path = os.path.join(DATA_RAW, filename)
        df.to_parquet(path, index=False, engine='pyarrow', compression='snappy')
        print(f"✅ Arquivo salvo: {path}")

# ---- Executar fato ----
df_fato = executar_consulta(SQL_FATO)
salvar_parquet(df_fato, "STG_SIGEF__FATO__EXECUCAO_ORCAMENTARIA.parquet")

# ---- Executar dimensões ----
for dim in DIM_TABLES:
    sql = f"SELECT * FROM SIGEF_STAGE.{dim}"
    df_dim = executar_consulta(sql)
    salvar_parquet(df_dim, f"{dim.lower()}.parquet")
