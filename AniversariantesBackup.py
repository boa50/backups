#!/usr/bin/env python
# coding: utf-8

# In[256]:


import requests
import json
import io
import os
from datetime import date, datetime
import configparser

import googleapiclient.discovery
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2 import service_account


# In[257]:


def get_local_filename(filename):
    try:
        return os.path.join(os.path.dirname(__file__), filename)
    except:
        return filename


# In[258]:


config = configparser.ConfigParser()
config.read(get_local_filename('config.ini'))


# In[259]:


PROJECT = config['VARS']['PROJECT']
DB_API_KEY = config['VARS']['DB_API_KEY']
DB_PASSWORD = config['VARS']['DB_PASSWORD']
EMAIL = config['VARS']['EMAIL']


# In[260]:


authUrl = f'https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={DB_API_KEY}'

authData = {
    'email': EMAIL,
    'password': DB_PASSWORD,
    'returnSecureToken': True,
};

resp = requests.post(authUrl, data=json.dumps(authData))
if resp.status_code != 200:
    # This means something went wrong.
    raise BaseException('GET /tasks/ {}'.format(resp.status_code))
    
id_token = resp.json()['idToken']


# In[261]:


headers = {'Authorization': f'Bearer {id_token}'}


# In[262]:


urlGet = f'https://firestore.googleapis.com/v1/projects/{PROJECT}/databases/(default)/documents/familias/'

resp = requests.get(urlGet, headers=headers)
if resp.status_code != 200:
    raise BaseException('GET /tasks/ {}'.format(resp.status_code))


# In[263]:


id_familias = []
json_familias = resp.json()
for item in json_familias['documents']:
    id_familia = item['name'].split('/')[-1]
    id_familias.append(id_familia)


# In[264]:


json_aniversariantes = {}
for id_familia in id_familias:
    urlGet = f'https://firestore.googleapis.com/v1/projects/{PROJECT}/databases/(default)/documents/familias/' +             f'{id_familia}/aniversariantes?pageSize=200'

    resp = requests.get(urlGet, headers=headers)
    if resp.status_code != 200:
        raise BaseException('GET /tasks/ {}'.format(resp.status_code))
        
    json_aniversariantes[id_familia] = resp.json()


# In[265]:


SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = get_local_filename('client_secret.json')

credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)


# In[266]:


drive = googleapiclient.discovery.build('drive', 'v3', credentials=credentials)


# In[267]:


pasta_id = '1TKvoksD6TF6xV4h6NMg0rrLyYdqZBpSC'


# In[268]:


query = f"'{pasta_id}' in parents"
res = drive.files().list(q=query, fields="files(id, name)").execute()


# In[269]:


def get_data(arquivo_nome):
    dt_txt = arquivo_nome.split('_')[1].split('.')[0]
    dt = datetime.strptime(dt_txt, '%d-%m-%Y')
    
    return dt


# In[270]:


familias_backups = {}
aniversariantes_backups = {}

arquivos = res.get('files', [])
for arquivo in arquivos:
    if arquivo['name'].startswith('familia'):
        familias_backups[arquivo['id']] = get_data(arquivo['name'])
    else:
        aniversariantes_backups[arquivo['id']] = get_data(arquivo['name'])


# In[271]:


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


# In[272]:


def arquivo_upload(filename):
    file_metadata = {
        'name': filename,
        'mimeType': 'application/json',
        'parents': [pasta_id]
    }
    media = MediaFileUpload(get_local_filename(filename), mimetype='application/json')

    file = drive.files().create(body=file_metadata, media_body=media, fields='id').execute()
    
    print(f'Upload de arquivo realizado: {filename}')


# In[273]:


familias_file_name = 'familias.json'
aniversariantes_file_name = 'aniversariantes.json'

download_last_backup(familias_backups, familias_file_name)
download_last_backup(aniversariantes_backups, aniversariantes_file_name)


# In[274]:


data_atual = date.today().strftime("%d-%m-%Y")


# In[275]:


familias_ultimo_backup = {}

with open(get_local_filename(familias_file_name)) as json_file:
    familias_ultimo_backup = json.load(json_file)


# In[276]:


aniversariantes_ultimo_backup = {}

with open(get_local_filename(aniversariantes_file_name)) as json_file:
    aniversariantes_ultimo_backup = json.load(json_file)


# In[277]:


if json_familias != familias_ultimo_backup:

    filename = f'familias_{data_atual}.json'
    with open(get_local_filename(filename), 'w') as outfile:
        json.dump(json_familias, outfile)
        print(f'Backup de famílias gerado: {filename}')
    
    arquivo_upload(filename)


# In[278]:


if json_aniversariantes != aniversariantes_ultimo_backup:
    
    filename = f'aniversariantes_{data_atual}.json'
    with open(get_local_filename(filename), 'w') as outfile:
        json.dump(json_aniversariantes, outfile)
        print(f'Backup de aniversariantes gerado: {filename}')
        
    arquivo_upload(filename)

