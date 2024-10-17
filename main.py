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
    Role= 'SNOWFLAKE_SALES_INTELIGENCE_BR_ANALYST',
    Warehouse= "SALES_INTELLIGENCE_BR_DBT_WH_PROD", 
    Database= 'SALES_INTELLIGENCE_BR_PROD')
cs = ctx.cursor()
sql_database = 'Use SALES_INTELLIGENCE_BR_PROD'
cs.execute(sql_database)
cs.execute('ALTER SESSION SET WEEK_START = 7')

# Escopo da API.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
# Id da planilha.
SAMPLE_SPREADSHEET_ID = "1OdVyqXC1WqQLeRxvOgGtNMNZxqfO3-XQfYSFfJ0mJOI" 
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
        "credentials.json", SCOPES
    )
    creds = flow.run_local_server(port=0)
    # Salva as credenciais e executa
with open("token.json", "w") as token:
    token.write(creds.to_json())

    service = build("sheets", "v4", credentials=creds) #Cria uma instância da conexão
    sheet = service.spreadsheets()
    
date = "2024-10-13"
date_2 = "2024-10-12"

def clear_all():
    sheet.values().clear(
      spreadsheetId=SAMPLE_SPREADSHEET_ID, 
      range='db_tpv_add!A2:E'
    ).execute()

    sheet.values().clear(
      spreadsheetId=SAMPLE_SPREADSHEET_ID, 
      range='db_new_merchants!A2:E'
    ).execute()
#clear_all()

def input_tpv():
    with open(".\querys\query_tpv.sql", "r") as file:
       query_tpv = file.read()
    query_tpv = query_tpv.replace(":date:", date)
    cs.execute(query_tpv)
    data = cs.fetch_pandas_all()

    cohort_list = data['COHORT'].tolist()
    tx_week_list = data['TX_WEEK'].tolist()
    channel_list = data['ACQUISITION_CHANNEL'].tolist()
    subchannel_list = data['ACQUISITION_SUBCHANNEL'].tolist()
    tpv_list = [str(value) for value in data['TPV'].tolist()]

    input_datas = [cohort_list, tx_week_list, channel_list, subchannel_list, tpv_list]
    sheet.values().update(
        spreadsheetId=SAMPLE_SPREADSHEET_ID, 
        range='db_tpv_add!A2', 
        valueInputOption="USER_ENTERED",
        body={'majorDimension': 'COLUMNS','values': input_datas}
    ).execute()
# input_tpv()

def input_cro():
   with open(".\querys\query_cro.sql", "r") as file:
      query_cro = file.read()
   query_cro = query_cro.replace(":date:", date_2)
   print(query_cro)
   cs.execute(query_cro)
   data = cs.fetch_pandas_all()

   cohort_list = data['COHORT'].tolist()
   channel_list = data['ACQUISITION_CHANNEL'].tolist()
   subchannel_list = data['ACQUISITION_SUBCHANNEL'].tolist()
   merchant_list = data['MERCHANT_CODE'].tolist()

   input_datas = [cohort_list, channel_list, subchannel_list, merchant_list]
   sheet.values().update(
        spreadsheetId=SAMPLE_SPREADSHEET_ID, 
        range='db_new_merchants!A2', 
        valueInputOption="USER_ENTERED",
        body={'majorDimension': 'COLUMNS','values': input_datas}
    ).execute()
   print(input_datas)
# input_cro()

def get_v2():
   result = (
        sheet.values()
        .get(spreadsheetId=SAMPLE_SPREADSHEET_ID, 
             range='forecast_caa!A12:DA20',
             majorDimension='ROWS')
        .execute()
    )
   #print(result['values'][0][104])
   #print(len(result['values'][0]))

   i = 0
   x = 0
   pesquisa = "W52/2024"

   while i < len(result['values'][0]):
      if result['values'][0][i] == pesquisa:
         while x < len(result['values'][2]):
            if result['values'][2][x] == result['values'][0][i]:
               result['values'][2][x]
         print("Deu certo")
         i += 1
      else: 
         print("não deu certo")
         i += 1 
    




   
#    columns = result['values'][0]
#    data = result['values'][1:]
#    df = pd.DataFrame(data, columns=columns)
#    print(df)
