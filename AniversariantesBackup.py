#!/usr/bin/env python
# coding: utf-8

# In[189]:


import requests
import json
import io
import os
from datetime import date, datetime
import configparser

import googleapiclient.discovery
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2 import service_account


# #### Obter o nome do arquivo dependendo de como está sendo executado
# Essa função auxilia a retornoar o *path* correto do arquivo independente se ele está sendo executado em um jupyter notebook ou pelo arquivo .py

# In[190]:


def get_local_filename(filename):
    try:
        return os.path.join(os.path.dirname(__file__), filename)
    except:
        return filename


# ### Leitura de variáveis de ambiente

# In[197]:


config = configparser.ConfigParser()
config.read(get_local_filename('config.ini'))


# In[198]:


PROJECT = config['VARS']['PROJECT']
DB_API_KEY = config['VARS']['DB_API_KEY']
DB_PASSWORD = config['VARS']['DB_PASSWORD']
EMAIL = config['VARS']['EMAIL']
PASTA_ID = config['VARS']['PASTA_ID']


# ### Funções auxiliares

# #### Obter os dados de documentos do firebase por uma url

# In[191]:


def get_dados(url, headers):
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise BaseException('GET /tasks/ {}'.format(resp.status_code))
        
    try:
        return response.json()['documents']
    except:
        return {}


# #### Obter os ids de acordo com documentos do firebase
# Esses ids obtidos serverm para auxiliar na geração de novas urls e percorrimento da árvore de documentos do firebase

# In[192]:


def get_ids(documents):
    ids = []
    
    for document in documents:
        identifier = document['name'].split('/')[-1]
        ids.append(identifier)
        
    return ids


# #### Gera de json de dados atuais
# Obtenção dos dados do firebase, percorrendo toda a árvore definida através da variável dicio, para que esses dados possam ser avaliados e realizada a ação de backup

# In[193]:


def backup_generate(dicio, backup, ids=[''], 
                    urls=[f'https://firestore.googleapis.com/v1/projects/{PROJECT}/databases/(default)/documents/'],
                    page_size=200, headers={}):
    chaves = list(dicio.keys())
    for chave in chaves:
        new_urls = []
        new_ids = []
        
        for url in urls:
            for identificador in ids:
                new_url = url
                
                if identificador != '':
                    new_url += f'/{identificador}/'
                    
                new_url += f'{chave}?pageSize=200'
                
                k = new_url.split('?')[0].split('/')
                k = k[-3] + '-' + k[-2] + '-' + k[-1]
                backup[k] = get_dados(new_url, headers)
                
                new_url = new_url.split('?')[0]
                
                new_urls.append(new_url)
                new_ids.append(get_ids(backup[k]))
                

        if isinstance(dicio[chave], dict):
            new_ids = [identifier for sublist in new_ids for identifier in sublist]
            
            backup_generate(dicio[chave], backup, ids=new_ids, urls=new_urls, headers=headers)


# #### Obter data de arquivo de backup

# In[194]:


def get_data(arquivo_nome):
    dt_txt = arquivo_nome.split('_')[1].split('.')[0]
    dt = datetime.strptime(dt_txt, '%d-%m-%Y')
    
    return dt


# #### Realizar download de último arquivo de backup
# Através de um json de arquvos de backup e sua data, é realizado o download para a máquina local do último arquivo de backup

# In[195]:


def download_last_backup(backups, filename):
    file_id = [k for k, v in sorted(backups.items(), key=lambda item: item[1])][-1]
    
    print(f'Último backup de {filename}: {backups[file_id]}')
    
    request = drive.files().get_media(fileId=file_id)
    fh = io.FileIO(get_local_filename(filename), mode='wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print("Download %d%%." % int(status.progress() * 100))


# #### Realizar o upload de arquivo no Drive

# In[196]:


def arquivo_upload(filename, pasta_id):
    file_metadata = {
        'name': filename,
        'mimeType': 'application/json',
        'parents': [pasta_id]
    }
    media = MediaFileUpload(get_local_filename(filename), mimetype='application/json')

    file = drive.files().create(body=file_metadata, media_body=media, fields='id').execute()
    
    print(f'Upload de arquivo realizado: {filename}')


# ### Tratamento da autenticação

# In[199]:


authUrl = f'https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={DB_API_KEY}'

authData = {
    'email': EMAIL,
    'password': DB_PASSWORD,
    'returnSecureToken': True,
};

resp = requests.post(authUrl, data=json.dumps(authData))
if resp.status_code != 200:
    raise BaseException('GET /tasks/ {}'.format(resp.status_code))
    
id_token = resp.json()['idToken']


# In[200]:


headers = {'Authorization': f'Bearer {id_token}'}


# ### Criar dicionário da estrutura
# É feita a criação da estrutura de documentos do firebase em que, enquanto houver um elemento *dict* como valor, o algoritimo irá percorrer para buscar uma nova estrutura
# 
# Um exemplo de estrutura seria:
# ```
# {
#   'nivel1.1': {
#      'nivel1.1-2.1': {
#         'nivel1.1-2.2-3: 0
#      },
#      'nivel1.1-2.2: 0
#   },
#   'nivel1.2': 0
# }
# ```

# In[201]:


dicio = {
    'familias': {
        'aniversariantes': 0
    }
}


# ### Obter os dados atuais

# In[202]:


backup = {}
backup_generate(dicio, backup, headers=headers)


# ### Obter o útlimo backup salvo no Drive

# In[203]:


SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = get_local_filename('client_secret.json')

credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)


# In[204]:


drive = googleapiclient.discovery.build('drive', 'v3', credentials=credentials)


# In[206]:


query = f"'{PASTA_ID}' in parents"
res = drive.files().list(q=query, fields="files(id, name)").execute()


# In[207]:


backups = {}

arquivos = res.get('files', [])
for arquivo in arquivos:
    backups[arquivo['id']] = get_data(arquivo['name'])


# In[208]:


backup_filename = 'backup.json'

download_last_backup(backups, backup_filename)


# ### Realizar upload de novo backup
# É realizada a verificação se houve alteração das informações contidas no firebase em relação às informações do último arquivo de backup salvo e, em caso positivo, uma nova versão é salva.

# In[209]:


data_atual = date.today().strftime("%d-%m-%Y")


# In[210]:


ultimo_backup = {}

with open(get_local_filename(backup_filename)) as json_file:
    ultimo_backup = json.load(json_file)


# In[211]:


if backup != ultimo_backup:

    filename = f'backup_{data_atual}.json'
    with open(get_local_filename(filename), 'w') as outfile:
        json.dump(backup, outfile)
        print(f'Backup de gerado: {filename}')
    
    arquivo_upload(filename, PASTA_ID)

