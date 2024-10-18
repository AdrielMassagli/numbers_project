import snowflake.connector 
import pandas as pd
import os.path
import datetime
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build


# ctx = snowflake.connector.connect(
#     user='ADRIEL.ARAUJO@SUMUP.COM',
#     password='25Abc09*',
#     host= 'ui58164.eu-west-1.snowflakecomputing.com',
#     authenticator= 'externalbrowser',
#     account='ui58164',
#     Role= 'SNOWFLAKE_SALES_INTELIGENCE_BR_ANALYST',
#     Warehouse= "SALES_INTELLIGENCE_BR_DBT_WH_PROD", 
#     Database= 'SALES_INTELLIGENCE_BR_PROD')
# cs = ctx.cursor()
# sql_database = 'Use SALES_INTELLIGENCE_BR_PROD'
# cs.execute(sql_database)
# cs.execute('ALTER SESSION SET WEEK_START = 7')

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
    
date = "2024-10-10"
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

# def input_tpv():
#     with open(".\querys\query_tpv.sql", "r") as file:
#        query_tpv = file.read()
#     query_tpv = query_tpv.replace(":date:", date)
#     cs.execute(query_tpv)
#     data = cs.fetch_pandas_all()

#     cohort_list = data['COHORT'].tolist()
#     tx_week_list = data['TX_WEEK'].tolist()
#     channel_list = data['ACQUISITION_CHANNEL'].tolist()
#     subchannel_list = data['ACQUISITION_SUBCHANNEL'].tolist()
#     tpv_list = [str(value) for value in data['TPV'].tolist()]

#     input_datas = [cohort_list, tx_week_list, channel_list, subchannel_list, tpv_list]
#     sheet.values().update(
#         spreadsheetId=SAMPLE_SPREADSHEET_ID, 
#         range='db_tpv_add!A2', 
#         valueInputOption="USER_ENTERED",
#         body={'majorDimension': 'COLUMNS','values': input_datas}
#     ).execute()
# # input_tpv()

# def input_cro():
#    with open(".\querys\query_cro.sql", "r") as file:
#       query_cro = file.read()
#    query_cro = query_cro.replace(":date:", date_2)
#    print(query_cro)
#    cs.execute(query_cro)
#    data = cs.fetch_pandas_all()

#    cohort_list = data['COHORT'].tolist()
#    channel_list = data['ACQUISITION_CHANNEL'].tolist()
#    subchannel_list = data['ACQUISITION_SUBCHANNEL'].tolist()
#    merchant_list = data['MERCHANT_CODE'].tolist()

#    input_datas = [cohort_list, channel_list, subchannel_list, merchant_list]
#    sheet.values().update(
#         spreadsheetId=SAMPLE_SPREADSHEET_ID, 
#         range='db_new_merchants!A2', 
#         valueInputOption="USER_ENTERED",
#         body={'majorDimension': 'COLUMNS','values': input_datas}
#     ).execute()
#    print(input_datas)
# # input_cro()

def fluctuation_cro_method():
   #puxando os dados de CAA da planilha
    result = (
          sheet.values()
          .get(spreadsheetId=SAMPLE_SPREADSHEET_ID, 
              range='forecast_caa!A12:DA20',
              majorDimension='ROWS')
          .execute()
      )
    #definindo o dia da semana de acordo com a data
    data = datetime.datetime.strptime(date, '%Y-%m-%d')
    num_week = data.strftime('%U')
    num_week = int(num_week)+1
    search = "W:week:/2024" #padrão de pesquisa
    search = search.replace(":week:", str(num_week))
    print(search)

    #looping que passa em todos os registros da tabela e encontra o valor de cada canal na semana
    i = 0
    while i < len(result['values'][0]):
        if result['values'][0][i] == search:
          #CAA
          actual_week_caa = float(result['values'][1][i])
          last_week_caa = float(result['values'][1][i-1])
          percent_caa = ((actual_week_caa-last_week_caa)/last_week_caa)*100

          #Executives
          actual_week_executives = float(result['values'][2][i])
          last_week_executives = float(result['values'][2][i-1])
          percent_executives = ((actual_week_executives-last_week_executives)/last_week_executives)*100

          #OM
          actual_week_om = float(result['values'][3][i])
          last_week_om = float(result['values'][3][i-1])
          percent_om = ((actual_week_om-last_week_om)/last_week_om)*100

          #Consultants
          actual_week_consultants = float(result['values'][4][i])
          last_week_consultants = float(result['values'][4][i-1])
          percent_consultants = ((actual_week_consultants-last_week_consultants)/last_week_consultants)*100

          #RaF
          actual_week_raf = float(result['values'][5][i])
          last_week_raf = float(result['values'][5][i-1])
          percent_raf = ((actual_week_raf-last_week_raf)/last_week_raf)*100

          #Integradores
          actual_week_integra = float(result['values'][6][i])
          last_week_integra = float(result['values'][6][i-1])
          percent_integra = ((actual_week_integra-last_week_integra)/last_week_integra)*100

          #Second hand
          actual_week_second = float(result['values'][7][i])
          last_week_second = float(result['values'][7][i-1])
          percent_second = ((actual_week_second-last_week_second)/last_week_second)*100

          input_result = [[round(percent_executives, 1)], [round(percent_om,1)], [round(percent_consultants,1)], [round(percent_raf,1)], [round(percent_second,1)], [round(percent_integra,1)], [0], [round(percent_caa,1)] ]

          print(input_result)
          print("Deu certo")
          i += 1
        else:
          i += 1 

    sheet.values().update(
          spreadsheetId=SAMPLE_SPREADSHEET_ID, 
          range='Análise Overall!B4', 
          valueInputOption="USER_ENTERED",
          body={'majorDimension': 'ROWS','values': input_result}
      ).execute()
    
    sheet.values().update(
          spreadsheetId=SAMPLE_SPREADSHEET_ID, 
          range='Análise Overall!A3', 
          valueInputOption="USER_ENTERED",
          body={'majorDimension': 'ROWS','values': [[search]]}
      ).execute()

      #---------------------------------------------------------

    result_real = (
          sheet.values()
          .get(spreadsheetId=SAMPLE_SPREADSHEET_ID, 
              range='Slides Nova Atrib!B81:O91',
              majorDimension='ROWS')
          .execute()
      )


    search_2 = "W:week_2:"
    search_2 = search_2.replace(":week_2:", str(num_week))
    x=0
    print(search_2)

    try:
      while x < len(result_real['values'][0]):
        if result_real['values'][0][x] == search_2:
          #REAL
          actual_week_real = float(result_real['values'][8][x])
          last_week_real = float(result_real['values'][8][x-1])
          percent_real = ((actual_week_real-last_week_real)/last_week_real)*100

          #Executives
          actual_week_real_executives = float(result_real['values'][1][x])
          last_week_real_executives = float(result_real['values'][1][x-1])
          percent_real_executives = ((actual_week_real_executives-last_week_real_executives)/last_week_real_executives)*100

          #OM
          actual_week_real_om = float(result_real['values'][2][x])
          last_week_real_om = float(result_real['values'][2][x-1])
          percent_real_om = ((actual_week_real_om-last_week_real_om)/last_week_real_om)*100

          #Consultants
          actual_week_real_cons = float(result_real['values'][3][x])
          last_week_real_cons = float(result_real['values'][3][x-1])
          percent_real_cons = ((actual_week_real_cons-last_week_real_cons)/last_week_real_cons)*100

          #RaF
          actual_week_real_raf = float(result_real['values'][4][x])
          last_week_real_raf = float(result_real['values'][4][x-1])
          percent_real_raf = ((actual_week_real_raf-last_week_real_raf)/last_week_real_raf)*100

          #Integradores
          actual_week_real_integra = float(result_real['values'][5][x])
          last_week_real_integra = float(result_real['values'][5][x-1])
          percent_real_integra = ((actual_week_real_integra-last_week_real_integra)/last_week_real_integra)*100

          #Second Hand
          actual_week_real_second = float(result_real['values'][6][x])
          last_week_real_second = float(result_real['values'][6][x-1])
          percent_real_second = ((actual_week_real_second-last_week_real_second)/last_week_real_second)*100

          #Others
          actual_week_real_others = float(result_real['values'][1][x])
          last_week_real_others = float(result_real['values'][1][x-1])
          percent_real_others = ((actual_week_real_others-last_week_real_others)/last_week_real_others)*100

          input_result_2 = [[round(percent_real_executives, 1)], [round(percent_real_om, 1)], [round(percent_real_cons,1)], [round(percent_real_raf,1)], [round(percent_real_second,1)], [round(percent_real_integra,1)], [round(percent_real_others,1)], [round(percent_real,1)]]

          sheet.values().update(
            spreadsheetId=SAMPLE_SPREADSHEET_ID, 
            range='Análise Overall!C4', 
            valueInputOption="USER_ENTERED",
            body={'majorDimension': 'ROWS','values': input_result_2}
          ).execute()

          print(input_result_2)
          x += 1
        else:
          x += 1
    except IndexError:
      print("Data inválida")
fluctuation_cro_method()


#def fluctuation_tpv_method():
