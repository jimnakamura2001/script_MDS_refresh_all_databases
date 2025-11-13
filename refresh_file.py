import win32com.client as win32
import time
import os
import sys
import subprocess
from datetime import datetime
from cryptography.fernet import Fernet

# Se precisar de configuração encriptada, mantenha este bloco:
user = os.getlogin()
config_folder_1 = fr"C:\Users\{user}\cabotcorp.com\Cabot Brazil Dashboards - General\BD\ANALISES"
config_folder_2 = fr"C:\Users\{user}\cabotcorp.com\Cabot Brazil Dashboards - Documents\General\BD\ANALISES"
if os.path.exists(config_folder_1):
    config_folder = config_folder_1
else:
    config_folder = config_folder_2
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
# LOCAL_FILE = rf"C:\Users\{user}\cabotcorp.com\Maua WPS Team - General\WPS\Medição de Silos\Medicao de Silos Atual_teste_automate.xlsx"
LOCAL_FILE = rf"C:\Users\{user}\cabotcorp.com\Maua WPS Team - General\WPS\Medição de Silos\Medicao de Silos Atual_teste_automate.xlsx"

# Garante que todas as instâncias do Excel estão fechadas antes de abrir com win32com
print("Fechando todas as instâncias do Excel...")
subprocess.call("taskkill /f /im excel.exe", shell=True)
time.sleep(2)  # Aguarda 2 segundos para garantir que o processo foi encerrado

# 1. Abrir no Excel, fazer Refresh All e salvar
print(f"🔄 Abrindo o arquivo Excel: {LOCAL_FILE}")
try:
    excel = win32.gencache.EnsureDispatch('Excel.Application')
except AttributeError:
    print("Tentando método alternativo de inicialização do Excel...")
    excel = win32.Dispatch('Excel.Application')
excel.DisplayAlerts = False
excel.AskToUpdateLinks = False
# excel.Visible = False
excel.Visible = True

start_open = time.time()
wb = excel.Workbooks.Open(LOCAL_FILE, ReadOnly=False)
print(f"✅ Arquivo aberto em {time.time() - start_open:.1f} segundos.")
print("🔄 Iniciando RefreshAll() (isso pode demorar, aguarde...)")
start_refresh = time.time()
max_tentativas = 5
tentativa_atual = 1
sucesso = False

while tentativa_atual <= max_tentativas and not sucesso:
    try:
        print(f"\nTentativa {tentativa_atual} de {max_tentativas}")
        wb.RefreshAll()
        wait_seconds = 60
        print(f"⏳ RefreshAll() chamado. Aguardando {wait_seconds} segundos para garantir atualização...")
        print(f"Aguardando liberação do arquivo pelo Excel ({wait_seconds} segundos):")
        
        for i in range(wait_seconds):
            progress = int(60 * (i + 1) / wait_seconds)
            bar = f"[{'|' * progress}{' ' * (60 - progress)}]"
            sys.stdout.write(f"\r{bar} {i+1}/{wait_seconds}s")
            sys.stdout.flush()
            time.sleep(1)
        
        # Se chegou até aqui sem erros, marca como sucesso
        sucesso = True
        print("\n✅ RefreshAll() completado com sucesso!")
        
    except Exception as e:
        print(f"\n❌ Erro na tentativa {tentativa_atual}: {str(e)}")
        if tentativa_atual < max_tentativas:
            tempo_espera = 10  # Segundos de espera entre tentativas
            print(f"Aguardando {tempo_espera} segundos antes da próxima tentativa...")
            time.sleep(tempo_espera)
        tentativa_atual += 1

if not sucesso:
    raise Exception("❌ Falha após todas as tentativas de refresh. Verifique o Excel e tente novamente.")

print(f"✅ RefreshAll() e espera concluídos em {time.time() - start_refresh:.1f} segundos.")

print("💾 Salvando arquivo...")
wb.Save()
wb.Close()
excel.Quit()
print(f"✅ Refresh concluído e salvamento OK (tempo total RefreshAll: {time.time() - start_refresh:.1f} segundos)")

# Atualização do SharePoint List MDS_PRODUCT_NAMES com até 5 tentativas
att_folder_1 = fr"C:\Users\{user}\cabotcorp.com\Cabot Brazil Dashboards - General\BD_WPS\Medicao_Silos"
att_folder_2 = fr"C:\Users\{user}\cabotcorp.com\Cabot Brazil Dashboards - Documents\General\BD_WPS\Medicao_Silos"
if os.path.exists(att_folder_1):
    att_folder = att_folder_1
else:
    att_folder = att_folder_2

# Criar arquivo que confirma que o script rodou completamente e exclui antes se ele ja existe:
completion_file = os.path.join(att_folder, "script_refresh_COMPLETED.txt")
if os.path.exists(completion_file):
    os.remove(completion_file)
with open(completion_file, 'w') as f:
    f.write(f"Script de refresh rodado completamente em {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")