from gerar_parquet import executar_consulta, salvar_parquet

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

# ---- Executar fato ----
df_fato = executar_consulta(SQL_FATO)
salvar_parquet(df_fato, "STG_SIGEF__FATO__EXECUCAO_ORCAMENTARIA.parquet")

# ---- Executar dimensões ----
for dim in DIM_TABLES:
    sql = f"SELECT * FROM SIGEF_STAGE.{dim}"
    df_dim = executar_consulta(sql)
    salvar_parquet(df_dim, f"{dim.lower()}.parquet")



