from cryptography.fernet import Fernet

# # Geração de uma chave (faça isso uma vez e salve a chave de forma segura)
# key = Fernet.generate_key()

# # Salve a chave em um arquivo
# with open('key.key', 'wb') as key_file:
#     key_file.write(key)

# Leitura da chave
with open('key.key', 'rb') as key_file:
    key = key_file.read()

cipher = Fernet(key)

# Dados a serem encriptados (exemplo com usuário e senha)
# The `config_data` variable is storing a byte string that contains sensitive configuration data in
# the format of key-value pairs. In this case, it includes information such as a username, password,
# site URL, and relative URL. This data is going to be encrypted using the Fernet encryption algorithm
# before being saved to a file for secure storage or transmission.
config_data = f"USERNAME=jim.nakamura@cabotcorp.com\nPASSWORD=Otaku2010......\ndatasource=maua-ntp01\nimstype=aspenone\nurl=http://maua-ntp01/ProcessData/AtProcessDataREST.dll\ndatabase=report_db\nSQLusername=ntp01read\nSQLpassword=%Cabot19\nsite_url=https://cabotcorp.sharepoint.com/sites/MauaWPS/".encode("utf-8")

# Encriptação dos dados
encrypted_data = cipher.encrypt(config_data)

# Salve os dados encriptados em um arquivo
with open('config.enc', 'wb') as config_file:
    config_file.write(encrypted_data)
  
