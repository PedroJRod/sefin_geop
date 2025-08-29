import pandas as pd
import os
import requests

PAI_DIR = os.getcwd()  # diretório atual para notebooks jupyter
DATA_RAW = os.path.join(PAI_DIR, "data", "raw")
DATA_PROCESSED = os.path.join(PAI_DIR, "data", "processed")
SCRIPTS = os.path.join(PAI_DIR, "scripts")


# Valores da UPF (R$) de 2016 a 2025
anos = list(range(2018, 2026))
valores_upf = [65.21, 70.68, 74.47, 92.54, 102.48, 108.53, 113.61, 119.14]
indice = ['IGP-DI', 'IGP-DI', 'IGP-DI', 'IPCA', 'IPCA', 'IPCA', 'IPCA', 'IPCA']

# Criar DataFrame
df_upf = pd.DataFrame({'exercicio': anos, 'valor_upf': valores_upf})

# Calcular variação percentual ano a ano
df_upf['var_perc'] = df_upf['valor_upf'].pct_change() * 100

# Arredondar os valores para duas casas decimais
df_upf['var_perc'] = df_upf['var_perc'].round(2)

df_upf.to_parquet(os.path.join(DATA_RAW, 'upf_ro.parquet'), index=False)

