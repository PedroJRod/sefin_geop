from gerar_parquet import executar_consulta, salvar_parquet


SQL_PRECATORIOS = """
    SELECT * FROM SIGEF_RAW.RAW_SIGEF__ECTB_LANCAMENTO_CONTABIL
WHERE CDEVENTO = 540905
"""

df_precatorios = executar_consulta(SQL_PRECATORIOS)
salvar_parquet(df_precatorios, "precatorios.parquet")


