import tagreader
import pyodbc
import pandas as pd
from datetime import datetime, timedelta

# Conexão com banco de dados Aspen
c = tagreader.IMSClient(datasource="maua-ntp01",
                        imstype="aspenone",
                        tz="Brazil/East",
                        url="http://maua-ntp01/ProcessData/AtProcessDataREST.dll")
c.connect()

# Tags que precisam ser lidas
tags = ["1.REAC1.DCS.GRADE",
        "2.REAC1.DCS.GRADE",
        "3.REAC1.DCS.GRADE",
        "1.PROD.SILO",
        "2.PROD.SILO",
        "3.PROD.SILO"
        ]

# Data de hoje e agora
now = datetime.now()

# Data de um mês atrás, hora 00:00
t_start = (now - timedelta(days=30)).replace(hour=0, minute=0, second=0, microsecond=0)
# Data de hoje até o horário atual
t_end = now

# Intervalo de 1 hora em segundos
interval = 3600

# Formatar datas
t_start_str = t_start.strftime("%d.%m.%Y %H:%M:%S")
t_end_str = t_end.strftime("%d.%m.%Y %H:%M:%S")

print("t_start:", t_start_str)
print("t_end:", t_end_str)
print("interval:", interval)

df_aspen_data = c.read(tags, t_start_str, t_end_str, interval, read_type=tagreader.ReaderType.INTERPOLATED)

mapping_df = pd.read_csv('grade_codes.csv')

# Criar dicionários de mapeamento para cada coluna
map_ma1 = dict(zip(
    mapping_df['ma1code'].dropna().astype(int).astype(str),
    mapping_df['ma1name'].dropna()
))
map_ma2 = dict(zip(
    mapping_df['ma2code'].dropna().astype(int).astype(str),
    mapping_df['ma2name'].dropna()
))
map_ma3 = dict(zip(
    mapping_df['ma3code'].dropna().astype(int).astype(str),
    mapping_df['ma3name'].dropna()
))
print(map_ma1)
print(map_ma2)
print(map_ma3)

# Substituir os códigos pelos nomes nas colunas do DataFrame
df_aspen_data['1.REAC1.DCS.GRADE'] = df_aspen_data['1.REAC1.DCS.GRADE'].astype(str).map(map_ma1)
df_aspen_data['2.REAC1.DCS.GRADE'] = df_aspen_data['2.REAC1.DCS.GRADE'].astype(str).map(map_ma2)
df_aspen_data['3.REAC1.DCS.GRADE'] = df_aspen_data['3.REAC1.DCS.GRADE'].astype(str).map(map_ma3)

print("Dados lidos do Aspen:")
print(df_aspen_data)

