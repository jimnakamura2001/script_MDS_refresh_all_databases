from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
import win32com.client as win32
from cryptography.fernet import Fernet
import time
import os

user = os.getlogin()
config_folder = fr"C:\Users\{user}\cabotcorp.com\Cabot Brazil Dashboards - General\BD\ANALISES"
key_path = os.path.join(config_folder, "key.key")
config_enc_path = os.path.join(config_folder, "config.enc")

with open(key_path, 'rb') as key_file:
    key = key_file.read()
cipher = Fernet(key)
with open(config_enc_path, 'rb') as config_file:
    encrypted_data = config_file.read()
config_data = cipher.decrypt(encrypted_data).decode()
config_lines = config_data.split('\n')
config_dict = {line.split('=')[0]: line.split('=')[1] for line in config_lines if '=' in line}

CLIENT_ID = config_dict.get('USERNAME')
CLIENT_SECRET = config_dict.get('PASSWORD')
SHAREPOINT_SITE = "https://cabotcorp.sharepoint.com/sites/MauaWPS"
REMOTE_PATH = "/sites/MauaWPS/Shared%20Documents/General/WPS/Medição de Silos/Medicao de Silos Atual_teste_automate.xlsx"
LOCAL_FILE = "Medicao de Silos Atual_teste_automate.xlsx"

# 1. Autenticação
ctx = ClientContext(SHAREPOINT_SITE).with_credentials(UserCredential(CLIENT_ID, CLIENT_SECRET))

# 2. Baixar arquivo do SharePoint
with open(LOCAL_FILE, "wb") as local_file:
    file = ctx.web.get_file_by_server_relative_url(REMOTE_PATH)
    file.download(local_file).execute_query()
print(f"✅ Arquivo baixado: {LOCAL_FILE}")

# Caminho absoluto do arquivo baixado
local_file_abspath = os.path.abspath(LOCAL_FILE)

# 3. Abrir no Excel, fazer Refresh All e salvar
excel = win32.gencache.EnsureDispatch('Excel.Application')
wb = excel.Workbooks.Open(local_file_abspath, ReadOnly=False)
wb.RefreshAll()
excel.CalculateUntilAsyncQueriesDone()
wb.Save()
wb.Close()
excel.Quit()
print("🔄 Refresh concluído e salvamento OK")

# Aguarda o Excel liberar o arquivo
time.sleep(150)  # Aguarda 2 segundos (ajuste se necessário)

# Garante que o arquivo não está mais em uso
for _ in range(5):
    try:
        with open(LOCAL_FILE, 'rb') as f:
            content = f.read()
        break
    except PermissionError:
        print("Arquivo ainda em uso, aguardando...")
        time.sleep(1)
else:
    raise PermissionError(f"Arquivo ainda está em uso: {LOCAL_FILE}")

# 4. Subir o arquivo de volta para o SharePoint
ctx.web.get_folder_by_server_relative_url(os.path.dirname(REMOTE_PATH)).upload_file(os.path.basename(REMOTE_PATH), content).execute_query()
print("📤 Arquivo atualizado enviado de volta ao SharePoint!")