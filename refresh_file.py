import win32com.client as win32
import time
import os
import sys
from cryptography.fernet import Fernet

# Se precisar de configuração encriptada, mantenha este bloco:
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

# Caminho absoluto do arquivo local
LOCAL_FILE = rf"C:\Users\{user}\cabotcorp.com\Maua WPS Team - General\WPS\Medição de Silos\Medicao de Silos Atual_teste_automate.xlsx"

# 1. Abrir no Excel, fazer Refresh All e salvar
print(f"🔄 Abrindo o arquivo Excel: {LOCAL_FILE}")
excel = win32.gencache.EnsureDispatch('Excel.Application')
excel.DisplayAlerts = False
excel.AskToUpdateLinks = False
# excel.Visible = False
excel.Visible = True

start_open = time.time()
wb = excel.Workbooks.Open(LOCAL_FILE, ReadOnly=False)
print(f"✅ Arquivo aberto em {time.time() - start_open:.1f} segundos.")
print("🔄 Iniciando RefreshAll() (isso pode demorar, aguarde...)")
start_refresh = time.time()
wb.RefreshAll()
print("⏳ RefreshAll() chamado. Aguardando 30 segundos para garantir atualização...")
wait_seconds = 30
print(f"Aguardando liberação do arquivo pelo Excel ({wait_seconds} segundos):")
for i in range(wait_seconds):
    progress = int(30 * (i + 1) / wait_seconds)
    bar = f"[{'|' * progress}{' ' * (30 - progress)}]"
    sys.stdout.write(f"\r{bar} {i+1}/{wait_seconds}s")
    sys.stdout.flush()
    time.sleep(1)
print("\nArquivo deve estar liberado, continuando...")
print(f"✅ RefreshAll() e espera concluídos em {time.time() - start_refresh:.1f} segundos.")

print("💾 Salvando arquivo...")
wb.Save()
wb.Close()
excel.Quit()
print(f"✅ Refresh concluído e salvamento OK (tempo total RefreshAll: {time.time() - start_refresh:.1f} segundos)")