import os, duckdb, pandas as pd, ast

PAI_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW = os.path.join(PAI_DIR, "data", "raw")
DATA_PROCESSED = os.path.join(PAI_DIR, "data", "processed")

df = pd.read_excel(os.path.join(DATA_RAW, "Estimativa_CRE_16-06-25.xlsx"), sheet_name='Projeção', header=8)

df = df.iloc[:, :-6]

date_col = df.iloc[:,11:]
id_vars = df.iloc[:,:11].columns.tolist()

df_long = pd.melt(df, id_vars=id_vars, value_vars=date_col, var_name='data', value_name='valor')
df_long['data'] = pd.to_datetime(df_long['data'])
df_long['mes'] = df_long['data'].dt.month
df_long['ano'] = df_long['data'].dt.year


df_long = df_long.drop(columns=['data'])

colunas_ordenadas = id_vars + ['ano', 'mes', 'valor']

df_final = df_long[colunas_ordenadas]

df_final = df_final.drop(columns=['Ordem'])

df_final['valor'] = df_final['valor'].fillna(0)

colunas_renomear = {
    "Ordem": "ordem",
    "UG ajustada": "ug_codigo",
    "Descrição UG": "descricao_ug",
    "Nova Fonte": "fonte_recurso",
    "Descrição Fonte": "descricao_fonte",
    "Classificação ajustada": "classificacao_ajustada",
    "Classificação p/ intra": "classificacao_intra",
    "Nomenclatura de receita": "nomenclatura_receita",
    "REC_Grupo": "rec_grupo",
    "REC_Subgrupo": "rec_subgrupo",
    "Método principal": "metodo_principal"
}

# Renomeando
df_final = df_final.rename(columns=colunas_renomear)

df_final['valor'] = pd.to_numeric(df_final['valor'], errors='coerce')

df_final['ug_codigo'] = df_final['ug_codigo'].astype(str)

df_final.to_parquet(os.path.join(DATA_PROCESSED, 'estimativa_cre_16-06-25.parquet'), index=False, engine='pyarrow', compression='snappy')