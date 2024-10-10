import snowflake.connector 
import pandas as pd
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

ctx = snowflake.connector.connect(
    user='ADRIEL.ARAUJO@SUMUP.COM',
    password='25Abc09*',
    host= 'ui58164.eu-west-1.snowflakecomputing.com',
    authenticator= 'externalbrowser',
    account='ui58164',
    Role= 'SNOWFLAKE_BASE_READER',
    Warehouse= "ANALYTICS_WH", 
    Database= 'SUMUP_DWH_PROD')
cs = ctx.cursor()
sql_database = 'Use SUMUP_DWH_PROD'
cs.execute(sql_database)
cs.execute('ALTER SESSION SET WEEK_START = 7')

# Escopo da API.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
# Id da planilha.
SAMPLE_SPREADSHEET_ID = "1xcqixIHD6siraxrDC3tLADvQnFhiZKG7nf4ue2mBOEo" 
SAMPLE_RANGE_NAME = "Sheet1!A1:F"
creds = None

if os.path.exists("token.json"):
  creds = Credentials.from_authorized_user_file("token.json", SCOPES)
  # Se não encontrar credencial válida, solicita login
if not creds or not creds.valid:
  if creds and creds.expired and creds.refresh_token:
      creds.refresh(Request())
  else:
    flow = InstalledAppFlow.from_client_secrets_file(
        'Documents\Numbers_project\credentials.json', SCOPES
    )
    creds = flow.run_local_server(port=0)
    # Salva as credenciais e executa
with open("token.json", "w") as token:
    token.write(creds.to_json())

    service = build("sheets", "v4", credentials=creds) #Cria uma instância da conexão
    sheet = service.spreadsheets()

#Querys
with open('Documents\Numbers_project\querys\query_cro.sql', 'r') as file:
        query_tpv = file.read()

with open('Documents\Numbers_project\querys\query_tpv.sql', 'r') as file:
        query_cro = file.read()        


def func_teste():
    input_datas = [['abcs']]
    sheet.values().update(
        spreadsheetId=SAMPLE_SPREADSHEET_ID, 
        range='Sheet!A1', 
        valueInputOption="USER_ENTERED",
        body={'majorDimension': 'COLUMNS','values': input_datas}
    ).execute()
func_teste()