import tagreader
import pyodbc
import time
import sys
import os
import logging
from datetime import datetime
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.authentication_context import AuthenticationContext
import pandas as pd
from datetime import datetime, timedelta
from cryptography.fernet import Fernet

user = os.getlogin()  # Captura o nome do usuário atual

# Configuração do logger
log_dir = rf"C:\Users\{user}\cabotcorp.com\Cabot Brazil Dashboards - Documents\General\BD_WPS\Medicao_Silos\log"
log_filename = f"log-{datetime.now().strftime('%d-%m-%Y-%H-%M')}.txt"
log_path = f"{log_dir}\\{log_filename}"

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)

logging.info("Teste de log: início do script")

logging.info(f"Usuário detectado: {user} \n")

with open('key.key', 'rb') as key_file:
    key = key_file.read()
    
cipher = Fernet(key)

with open('config.enc', 'rb') as config_file:
    encrypted_data = config_file.read()

config_data = cipher.decrypt(encrypted_data).decode()

config_lines = config_data.split('\n')

config_dict = {line.split('=')[0]: line.split('=')[1] for line in config_lines if '=' in line}

# Conexão com banco de dados Aspen
c = tagreader.IMSClient(datasource=config_dict["datasource"],# "maua-ntp01",
                        imstype=config_dict["imstype"], # "aspenone",
                        tz="Brazil/East",
                        url=config_dict["url"]) # "http://maua-ntp01/ProcessData/AtProcessDataREST.dll")
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

# Intervalo de 3 horas em segundos
interval = 10800

# Formatar datas
t_start_str = t_start.strftime("%d.%m.%Y %H:%M:%S")
t_end_str = t_end.strftime("%d.%m.%Y %H:%M:%S")

logging.info(f"t_start: {t_start_str}")
logging.info(f"t_end: {t_end_str}")
logging.info(f"interval: {interval}")

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
# logging.info(map_ma1)
# logging.info(map_ma2)
# logging.info(map_ma3)

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

logging.info("Dados lidos do Aspen:")
# print(df_aspen_data.head())
# print(df_aspen_data)
logging.info(df_aspen_data.to_string())
logging.info("\n")

# Supondo que o índice do df_aspen_data seja a data/hora de cada linha
df_main_aspen = pd.concat([
    pd.DataFrame({
        'Data': df_aspen_data['time'] if 'time' in df_aspen_data.columns else df_aspen_data.index,
        'Grau': df_aspen_data['1.REAC1.DCS.GRADE'],
        'Silo': df_aspen_data['1.PROD.SILO'],
        'Unidade': 'MA-1',
        'Fonte': 'Aspen'
    }),
    pd.DataFrame({
        'Data': df_aspen_data['time'] if 'time' in df_aspen_data.columns else df_aspen_data.index,
        'Grau': df_aspen_data['2.REAC1.DCS.GRADE'],
        'Silo': df_aspen_data['2.PROD.SILO'],
        'Unidade': 'MA-2',
        'Fonte': 'Aspen'
    }),
    pd.DataFrame({
        'Data': df_aspen_data['time'] if 'time' in df_aspen_data.columns else df_aspen_data.index,
        'Grau': df_aspen_data['3.REAC1.DCS.GRADE'],
        'Silo': df_aspen_data['3.PROD.SILO'],
        'Unidade': 'MA-3',
        'Fonte': 'Aspen'
    }),
], ignore_index=True)

# Opcional: remover linhas onde Grau e Silo estão ambos vazios
df_main = df_main_aspen.dropna(how='all', subset=['Silo'])

# Transformar coluna Silo em inteiro (remover .0)
df_main['Silo'] = pd.to_numeric(df_main['Silo'], errors='coerce').astype('Int64')

# Excluir linhas cujo Silo é maior que 20
df_main = df_main[df_main['Silo'] <= 20]

df_main = df_main.drop_duplicates()

# logging.info("DataFrame principal:")
# logging.info(df_main)

# Iniciando processo de extração de dados do Cabot Report via SQL Server

database = 'report_db'
username = 'ntp01read'
password = '%Cabot19'

# Criar string de conexão
conn_str = f'DRIVER={{SQL Server}};SERVER={config_dict["datasource"]};DATABASE={config_dict["database"]};UID={config_dict["SQLusername"]};PWD={config_dict["SQLpassword"]}'

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

logging.info("Dados do Cabot Report:")
# print(df_cbt_report.head())
# print(df_cbt_report)
logging.info(df_cbt_report.to_string())
logging.info("\n")

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

df_cbt_report_ren['Fonte'] = 'Cabot Report'

# Concatenar os dois dataframes
df_main = pd.concat([df_main, df_cbt_report_ren[['Data', 'Grau', 'Silo', 'Unidade', 'Fonte']]], ignore_index=True)

# Opcional: remover duplicatas após a junção
df_main = df_main.drop_duplicates(subset=['Data','Silo', 'Unidade', 'Grau'])

# Ordenar df_main pela coluna Data em ordem decrescente
df_main['Data'] = pd.to_datetime(df_main['Data'], format='%d/%m/%Y %H:%M', errors='coerce', dayfirst=True)
df_main = df_main.sort_values('Data', ascending=False)

df_main = df_main.dropna(how='all', subset=['Data'])

# Conectar ao SharePoint e obter dados da lista
# Informações de autenticação

site_url = config_dict["site_url"] 
username = config_dict["USERNAME"] 
password = config_dict["PASSWORD"]

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
    logging.info("Falha na autenticação")

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

df_sharepoint['Fonte'] = 'SharePoint'

logging.info("DataFrame Sharepoint:")
# print(df_sharepoint)
logging.info(df_sharepoint.to_string())
logging.info("\n")

# Combinar os DataFrames
df_main = pd.concat([df_main, df_sharepoint], ignore_index=True)

# Concatenar os dois dataframes
df_main = pd.concat([df_main, df_cbt_report_ren[['Data', 'Grau', 'Silo', 'Unidade', 'Fonte']]], ignore_index=True)

# Opcional: remover duplicatas após a junção
df_main = df_main.drop_duplicates(subset=['Data','Silo', 'Unidade', 'Grau'])

# Converter a coluna Data para datetime (sem timezone)
df_main['Data'] = pd.to_datetime(df_main['Data'], format='%d/%m/%Y %H:%M', errors='coerce', dayfirst=True)

# Remover timezone, se houver
df_main['Data'] = df_main['Data'].apply(lambda x: x.tz_localize(None) if pd.notnull(x) and hasattr(x, 'tzinfo') and x.tzinfo is not None else x)

# Agora pode ordenar normalmente
df_main = df_main.sort_values('Data', ascending=False).reset_index(drop=True)

df_main = df_main.dropna(how='all', subset=['Data'])

logging.info("DataFrame principal combinado:")
# print(df_main.head())
# logging.info(df_main)
logging.info(df_main.to_string())
logging.info("\n")

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
logging.info("Status dos Silos (último registro de cada silo, com unidade):")
# logging.info(df_silo_status)
logging.info(df_silo_status.to_string())
logging.info("\n")

# logging.info(df_aspen_data.loc[
#     (df_aspen_data['2.PROD.SILO'] == 9)
# ][tags])

# ========================================================================================================================================================================================
# Consulta interativa dos últimos 10 registros de um silo
# while True:
#     try:
#         silo_input = input("Digite o número do silo para consultar os últimos 10 registros (ou 'sair' para encerrar): ").strip()
#         if silo_input.lower() == 'sair':
#             logging.info("Encerrando consulta.")
#             break
#         silo_num = int(silo_input)
#         registros = df_main[df_main['Silo'] == silo_num].sort_values('Data', ascending=False).head(10)
#         if registros.empty:
#             logging.info(f"Nenhum registro encontrado para o silo {silo_num}.")
#         else:
#             logging.info(f"\nÚltimos 10 registros do silo {silo_num}:\n")
#             logging.info(registros[['Data', 'Unidade', 'Silo', 'Grau', 'Fonte']].to_string(index=False))
#             logging.info("\n")
#     except ValueError:
#         logging.info("Por favor, digite um número de silo válido ou 'sair' para encerrar.")
# ========================================================================================================================================================================================

# Atualização do SharePoint List MDS_PRODUCT_NAMES com até 5 tentativas
max_retries = 5
success = False

for attempt in range(1, max_retries + 1):
    try:
        ctx_auth = AuthenticationContext(site_url)
        if ctx_auth.acquire_token_for_user(username, password):
            ctx = ClientContext(site_url, ctx_auth)
            lista = ctx.web.lists.get_by_title("MDS_PRODUCT_NAMES")
            for idx, row in df_silo_status.iterrows():
                silo_number = str(row['Silo'])  # Title é string no SharePoint
                produto = row['Grau']
                # Buscar o item pelo Title
                items = lista.items.filter(f"Title eq '{silo_number}'").get().execute_query()
                for item in items:
                    item.set_property('field_1', produto)  # ajuste o nome do campo se necessário
                    item.update()
                ctx.execute_query()
            logging.info("SharePoint List atualizada com sucesso!")
            success = True
            break
        else:
            logging.info(f"Tentativa {attempt}: Falha na autenticação")
    except Exception as e:
        logging.info(f"Tentativa {attempt}: Erro ao atualizar SharePoint List: {e}")
        time.sleep(2)  # espera 2 segundos antes de tentar novamente

if not success:
    logging.info("Não foi possível atualizar o SharePoint List após 5 tentativas. Prosseguindo com o código...")
    
logging.info("Processo de atualização concluído. Finalizando script.")

for handler in logging.getLogger().handlers:
    handler.flush()