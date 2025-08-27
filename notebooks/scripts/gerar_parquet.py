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


if __name__ == "__main__":
    sql = "SELECT * FROM tabela_exemplo"
    df = executar_consulta(sql)
    salvar_parquet(df, "saida.parquet")