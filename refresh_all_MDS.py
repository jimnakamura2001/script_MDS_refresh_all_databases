import tagreader
import pyodbc
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.authentication_context import AuthenticationContext
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
t_start = (now - timedelta(days=90)).replace(hour=0, minute=0, second=0, microsecond=0)
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
# print(map_ma1)
# print(map_ma2)
# print(map_ma3)

# Substituir os códigos pelos nomes nas colunas do DataFrame
df_aspen_data['1.REAC1.DCS.GRADE'] = df_aspen_data['1.REAC1.DCS.GRADE'].astype(str).map(map_ma1)
df_aspen_data['2.REAC1.DCS.GRADE'] = df_aspen_data['2.REAC1.DCS.GRADE'].astype(str).map(map_ma2)
df_aspen_data['3.REAC1.DCS.GRADE'] = df_aspen_data['3.REAC1.DCS.GRADE'].astype(str).map(map_ma3)

# Remover todas as linhas vazias
df_aspen_data = df_aspen_data.dropna(how='all')

# Remover linhas onde todos os PROD.SILO estão vazios
df_aspen_data = df_aspen_data.dropna(how='all', subset=['1.PROD.SILO', '2.PROD.SILO', '3.PROD.SILO'])

# Remover linhas duplicadas
df_aspen_data = df_aspen_data.drop_duplicates()

# Converter a coluna de tempo do df_aspen_data para datetime no formato desejado
if 'time' in df_aspen_data.columns:
    df_aspen_data['time'] = pd.to_datetime(df_aspen_data['time']).dt.strftime('%d/%m/%Y %H:%M')

print("Dados lidos do Aspen:")
print(df_aspen_data.head())
print("\n")

# Supondo que o índice do df_aspen_data seja a data/hora de cada linha
df_main = pd.concat([
    pd.DataFrame({
        'Data': df_aspen_data['time'] if 'time' in df_aspen_data.columns else df_aspen_data.index,
        'Grau': df_aspen_data['1.REAC1.DCS.GRADE'],
        'Silo': df_aspen_data['1.PROD.SILO'],
        'Unidade': 'MA-1'
    }),
    pd.DataFrame({
        'Data': df_aspen_data['time'] if 'time' in df_aspen_data.columns else df_aspen_data.index,
        'Grau': df_aspen_data['2.REAC1.DCS.GRADE'],
        'Silo': df_aspen_data['2.PROD.SILO'],
        'Unidade': 'MA-2'
    }),
    pd.DataFrame({
        'Data': df_aspen_data['time'] if 'time' in df_aspen_data.columns else df_aspen_data.index,
        'Grau': df_aspen_data['3.REAC1.DCS.GRADE'],
        'Silo': df_aspen_data['3.PROD.SILO'],
        'Unidade': 'MA-3'
    }),
], ignore_index=True)

# Opcional: remover linhas onde Grau e Silo estão ambos vazios
df_main = df_main.dropna(how='all', subset=['Silo'])

# Transformar coluna Silo em inteiro (remover .0)
df_main['Silo'] = pd.to_numeric(df_main['Silo'], errors='coerce').astype('Int64')

# Excluir linhas cujo Silo é maior que 20
df_main = df_main[df_main['Silo'] <= 20]

df_main = df_main.drop_duplicates()

# print("DataFrame principal:")
# print(df_main)

# Iniciando processo de extração de dados do Cabot Report via SQL Server
server = 'maua-ntp01'
database = 'report_db'
username = 'ntp01read'
password = '%Cabot19'

# Criar string de conexão
conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

# Conectar ao banco
conn = pyodbc.connect(conn_str)

# Criar um cursor para executar comandos SQL
cursor = conn.cursor()

# Definindo a query para pegar os dados da tabela PLANTDATA
query = "SELECT [startdate],[unit],[grade],[silo] FROM [report_db].[dbo].[tblReportData] ORDER BY [startdate] DESC"

# Executando a query e armazenando o resultado em um DataFrame
df_cbt_report = pd.read_sql(query, conn)

# Convertando a coluna 'startdate' para datetime
df_cbt_report['startdate'] = pd.to_datetime(df_cbt_report['startdate'])

# Converter a coluna startdate do df_cbt_report para datetime no formato desejado
df_cbt_report['startdate'] = pd.to_datetime(df_cbt_report['startdate']).dt.strftime('%d/%m/%Y %H:%M')

print("Dados do Cabot Report:")
print(df_cbt_report.head())
print("\n")

# Renomear colunas do df_cbt_report para corresponder ao df_main
df_cbt_report_ren = df_cbt_report.rename(columns={
    'startdate': 'Data',
    'grade': 'Grau',
    'silo': 'Silo',
    'unit': 'Unidade'
})

# Garantir que os tipos das colunas sejam compatíveis
df_cbt_report_ren['Silo'] = pd.to_numeric(df_cbt_report_ren['Silo'], errors='coerce').astype('Int64')
df_cbt_report_ren['Data'] = pd.to_datetime(df_cbt_report_ren['Data'], dayfirst=True)

# Concatenar os dois dataframes
df_main = pd.concat([df_main, df_cbt_report_ren[['Data', 'Grau', 'Silo', 'Unidade']]], ignore_index=True)

# Opcional: remover duplicatas após a junção
df_main = df_main.drop_duplicates(subset=['Data','Silo', 'Unidade', 'Grau'])

# Ordenar df_main pela coluna Data em ordem decrescente
df_main['Data'] = pd.to_datetime(df_main['Data'], format='%d/%m/%Y %H:%M', errors='coerce', dayfirst=True)
df_main = df_main.sort_values('Data', ascending=False)

df_main = df_main.dropna(how='all', subset=['Data'])

# Conectar ao SharePoint e obter dados da lista
# Informações de autenticação

site_url = "https://cabotcorp.sharepoint.com/sites/MauaWPS/"
username = "jim.nakamura@cabotcorp.com"
password = "Otaku2010....."

ctx_auth = AuthenticationContext(site_url)
if ctx_auth.acquire_token_for_user(username, password):
    ctx = ClientContext(site_url, ctx_auth)
    lista = ctx.web.lists.get_by_title("MDS_ZERAGEM_SILO")
    items = lista.items.top(10000).get().execute_query()

    # Converter os itens em uma lista de dicionários
    data = [item.properties for item in items]

    # Criar DataFrame
    df_sharepoint = pd.DataFrame(data)
        
else:
    print("Falha na autenticação")

# Manter apenas as colunas desejadas
df_sharepoint = df_sharepoint[['Data', 'SILO', 'UNIDADE', 'Status_Grau']]

# Renomear as colunas
df_sharepoint = df_sharepoint.rename(columns={
    'SILO': 'Silo',
    'UNIDADE': 'Unidade',
    'Status_Grau': 'Grau'
})

# Converter a coluna Data para datetime
df_sharepoint['Data'] = pd.to_datetime(df_sharepoint['Data'], format='%m/%d/%Y %H:%M:%S', errors='coerce')

# Ordenar da data mais recente para a mais antiga
df_sharepoint = df_sharepoint.sort_values('Data', ascending=False).reset_index(drop=True)

# (Opcional) Voltar para string no formato desejado
df_sharepoint['Data'] = df_sharepoint['Data'].dt.strftime('%d/%m/%Y %H:%M')

print("DataFrame Sharepoint:")
print(df_sharepoint)
print("\n")

# Combinar os DataFrames
df_main = pd.concat([df_main, df_sharepoint], ignore_index=True)

# Concatenar os dois dataframes
df_main = pd.concat([df_main, df_cbt_report_ren[['Data', 'Grau', 'Silo', 'Unidade']]], ignore_index=True)

# Opcional: remover duplicatas após a junção
df_main = df_main.drop_duplicates(subset=['Data','Silo', 'Unidade', 'Grau'])

# Converter a coluna Data para datetime (sem timezone)
df_main['Data'] = pd.to_datetime(df_main['Data'], format='%d/%m/%Y %H:%M', errors='coerce', dayfirst=True)

# Remover timezone, se houver
df_main['Data'] = df_main['Data'].apply(lambda x: x.tz_localize(None) if pd.notnull(x) and hasattr(x, 'tzinfo') and x.tzinfo is not None else x)

# Agora pode ordenar normalmente
df_main = df_main.sort_values('Data', ascending=False).reset_index(drop=True)

df_main = df_main.dropna(how='all', subset=['Data'])

print("DataFrame principal combinado:")
print(df_main.head())
print("\n")

# Novo DataFrame: última linha de cada tipo de silo (por Silo, mantendo Unidade)
df_silo_status = (
    df_main
    .dropna(subset=['Silo'])
    .assign(Silo=lambda x: pd.to_numeric(x['Silo'], errors='coerce').astype('Int64'))
    .sort_values('Data')
    .drop_duplicates(subset=['Silo'], keep='last')
    [['Unidade', 'Silo', 'Grau', 'Data']]
    .sort_values('Silo')
    .reset_index(drop=True)
)
print("Status dos Silos (último registro de cada silo, com unidade):")
print(df_silo_status)
print("\n")