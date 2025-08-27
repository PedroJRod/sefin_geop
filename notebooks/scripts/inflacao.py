import pandas as pd
import os
import requests

PAI_DIR = os.getcwd()  # diretório atual para notebooks jupyter
DATA_RAW = os.path.join(PAI_DIR, "data", "raw")
DATA_PROCESSED = os.path.join(PAI_DIR, "data", "processed")
SCRIPTS = os.path.join(PAI_DIR, "scripts")

#https://apisidra.ibge.gov.br/
#https://apisidra.ibge.gov.br/home/ajuda

url = "https://apisidra.ibge.gov.br/values/h/n/t/7060//n1/1/p/all?formato=json"

response = requests.get(url)
data = response.json()

df = pd.DataFrame(data)

df = df[['D1N', 'D2C', 'D3N', 'V', 'MN']]
df.columns = ['brasil', 'periodo', 'indicador', 'valor', 'unidade']

df['periodo'] = pd.to_datetime(df['periodo'], format='%Y%m')


df['ANO'] = df['periodo'].dt.year
df['MES'] = df['periodo'].dt.month


df = df.drop(columns=['periodo'])


df.to_parquet(os.path.join(DATA_RAW, 'inflacao_ipca.parquet'), index=False)

