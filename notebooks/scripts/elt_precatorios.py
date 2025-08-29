from gerar_parquet import executar_consulta, salvar_parquet


query = """
    SELECT * FROM SIGEF_RAW.RAW_SIGEF__ECTB_LANCAMENTO_CONTABIL
WHERE CDEVENTO = 540905
"""

df = executar_consulta(query)
salvar_parquet(df, "precatorios.parquet")


