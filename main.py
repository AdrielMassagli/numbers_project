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
SAMPLE_SPREADSHEET_ID = "1QW9e7voErJCZ3xHZ5eLMLrdFq7TvoFkK3Qch6XG-SyI" 
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
    
date = "2024-10-19"
date_2 = "2024-10-12"

def clear_all():
    # sheet.values().clear(
    #   spreadsheetId=SAMPLE_SPREADSHEET_ID, 
    #   range='db_tpv_add!A2:E'
    # ).execute()

    # sheet.values().clear(
    #   spreadsheetId=SAMPLE_SPREADSHEET_ID, 
    #   range='db_new_merchants!A2:E'
    # ).execute()

    sheet.values().clear(
      spreadsheetId=SAMPLE_SPREADSHEET_ID, 
      range='Análise Overall!B4:C11'
    ).execute()

    sheet.values().clear(
      spreadsheetId=SAMPLE_SPREADSHEET_ID, 
      range='Análise Overall!F4:G11'
    ).execute()

    sheet.values().clear(
      spreadsheetId=SAMPLE_SPREADSHEET_ID, 
      range='Análise Overall!J4:J11'
    ).execute()

    sheet.values().clear(
      spreadsheetId=SAMPLE_SPREADSHEET_ID, 
      range='Análise Overall!K4:K11'
    ).execute()

    sheet.values().clear(
      spreadsheetId=SAMPLE_SPREADSHEET_ID, 
      range='Análise Overall!B17:D24'
    ).execute()

    sheet.values().clear(
      spreadsheetId=SAMPLE_SPREADSHEET_ID, 
      range='Análise Overall!G17:I24'
    ).execute()


clear_all()

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

#definindo o dia da semana de acordo com a data
data = datetime.datetime.strptime(date, '%Y-%m-%d')
num_week = data.strftime('%U')
num_week = int(num_week)+1

search_caa = "W:week:/2024" #padrão de pesquisa caa
search_caa = search_caa.replace(":week:", str(num_week))

search_real = "W:week:" #padrão de pesquisa real
search_real = search_real.replace(":week:", str(num_week))

input_last_week = "Semana " + str(num_week-1)
input_actual_week = "Semana " + str(num_week)

print(num_week)
print(date)

#inputa a semana na planilha
sheet.values().update(
          spreadsheetId=SAMPLE_SPREADSHEET_ID, 
          range='Análise Overall!A3', 
          valueInputOption="USER_ENTERED",
          body={'majorDimension': 'ROWS','values': [[search_caa]]}
).execute() 
sheet.values().update(
          spreadsheetId=SAMPLE_SPREADSHEET_ID, 
          range='Análise Overall!E3', 
          valueInputOption="USER_ENTERED",
          body={'majorDimension': 'ROWS','values': [[search_caa]]}
).execute() 
sheet.values().update(
          spreadsheetId=SAMPLE_SPREADSHEET_ID, 
          range='Análise Overall!I3', 
          valueInputOption="USER_ENTERED",
          body={'majorDimension': 'ROWS','values': [[search_caa]]}
).execute() 
sheet.values().update(
          spreadsheetId=SAMPLE_SPREADSHEET_ID, 
          range='Análise Overall!B16', 
          valueInputOption="USER_ENTERED",
          body={'majorDimension': 'ROWS','values': [[input_last_week]]}
).execute() 
sheet.values().update(
          spreadsheetId=SAMPLE_SPREADSHEET_ID, 
          range='Análise Overall!C16', 
          valueInputOption="USER_ENTERED",
          body={'majorDimension': 'ROWS','values': [[input_actual_week]]}
).execute() 
sheet.values().update(
          spreadsheetId=SAMPLE_SPREADSHEET_ID, 
          range='Análise Overall!G16', 
          valueInputOption="USER_ENTERED",
          body={'majorDimension': 'ROWS','values': [[input_last_week]]}
).execute() 
sheet.values().update(
          spreadsheetId=SAMPLE_SPREADSHEET_ID, 
          range='Análise Overall!H16', 
          valueInputOption="USER_ENTERED",
          body={'majorDimension': 'ROWS','values': [[input_actual_week]]}
).execute() 


#Função que calcula a variação de CRO esperada do caa
def fluctuation_cro_caa_method():
   #puxando os dados de CAA da planilha
    result = (
          sheet.values()
          .get(spreadsheetId=SAMPLE_SPREADSHEET_ID, 
              range='forecast_caa!A12:DA20',
              majorDimension='ROWS')
          .execute()
      )

    #looping que passa em todos os registros da tabela e encontra o valor de cada canal na semana
    i = 0
    while i < len(result['values'][0]):
        if result['values'][0][i] == search_caa:
          #CRO CAA
          actual_week_caa = float(result['values'][1][i])
          last_week_caa = float(result['values'][1][i-1])
          percent_caa = ((actual_week_caa-last_week_caa)/last_week_caa)*100

          #CRO Executives CAA
          actual_week_executives = float(result['values'][2][i])
          last_week_executives = float(result['values'][2][i-1])
          percent_executives = ((actual_week_executives-last_week_executives)/last_week_executives)*100

          #CRO OM CAA
          actual_week_om = float(result['values'][3][i])
          last_week_om = float(result['values'][3][i-1])
          percent_om = ((actual_week_om-last_week_om)/last_week_om)*100

          #CRO Consultants CAA
          actual_week_consultants = float(result['values'][4][i])
          last_week_consultants = float(result['values'][4][i-1])
          percent_consultants = ((actual_week_consultants-last_week_consultants)/last_week_consultants)*100

          #CRO RaF CAA
          actual_week_raf = float(result['values'][5][i])
          last_week_raf = float(result['values'][5][i-1])
          percent_raf = ((actual_week_raf-last_week_raf)/last_week_raf)*100

          #CRO Integradores CAA
          actual_week_integra = float(result['values'][6][i])
          last_week_integra = float(result['values'][6][i-1])
          percent_integra = ((actual_week_integra-last_week_integra)/last_week_integra)*100

          #CRO Second hand CAA
          actual_week_second = float(result['values'][7][i])
          last_week_second = float(result['values'][7][i-1])
          percent_second = ((actual_week_second-last_week_second)/last_week_second)*100

          input_result = [[str(round(percent_executives, 1))+"%"], [str(round(percent_om,1))+"%"], [str(round(percent_consultants,1))+"%"], [str(round(percent_raf,1))+"%"], [str(round(percent_second,1))+"%"], [str(round(percent_integra,1))+"%"], ['-'], [str(round(percent_caa,1))+"%"] ]

          sheet.values().update(
            spreadsheetId=SAMPLE_SPREADSHEET_ID, 
            range='Análise Overall!B4', 
            valueInputOption="USER_ENTERED",
            body={'majorDimension': 'ROWS','values': input_result}
          ).execute()
          i += 1
        else:
          i += 1 
fluctuation_cro_caa_method()

#Função que calcula a variação de CRO real
def fluctuation_cro_real_method():
  result_real = (
          sheet.values()
          .get(spreadsheetId=SAMPLE_SPREADSHEET_ID, 
              range='CRO!A4:DA12',
              majorDimension='ROWS')
          .execute()
      )

  print(result_real)

  x=0
  try:
    while x < len(result_real['values'][0]):
      if result_real['values'][0][x] == search_caa:
        #CRO REAL
        if str(result_real['values'][1][x]).isnumeric():
          
          if str(result_real['values'][1][x-1]).isnumeric():
            actual_week_real = float(result_real['values'][1][x])
            last_week_real = float(result_real['values'][1][x-1])
            percent_real = ((actual_week_real-last_week_real)/last_week_real)*100
          else:
            percent_real = 0
        else:
            percent_real = 0

        #CRO Executives REAL
        if str(result_real['values'][3][x]).isnumeric():
          
          if str(result_real['values'][3][x-1]).isnumeric():
            actual_week_real_executives = float(result_real['values'][3][x])
            last_week_real_executives = float(result_real['values'][3][x-1])
            percent_real_executives = ((actual_week_real_executives-last_week_real_executives)/last_week_real_executives)*100
          else:
            percent_real_executives = 0
        else:
            percent_real_executives = 0
        

        #CRO OM REAL
        if str(result_real['values'][4][x]).isnumeric():
          
          if str(result_real['values'][4][x-1]).isnumeric():
            actual_week_real_om = float(result_real['values'][4][x])
            last_week_real_om = float(result_real['values'][4][x-1])
            percent_real_om = ((actual_week_real_om-last_week_real_om)/last_week_real_om)*100
          else: 
            percent_real_om = 0
        else: 
            percent_real_om = 0

        #CRO Consultants REAL
        if str(result_real['values'][2][x]):
          
          if str(result_real['values'][2][x-1]).isnumeric():
            actual_week_real_cons = float(result_real['values'][2][x])
            last_week_real_cons = float(result_real['values'][2][x-1])
            percent_real_cons = ((actual_week_real_cons-last_week_real_cons)/last_week_real_cons)*100
          else:
            percent_real_cons = 0
        else:
            percent_real_cons = 0

        #CRO RaF REAL
        if str(result_real['values'][5][x]).isnumeric():
          
          if str(result_real['values'][5][x-1]).isnumeric():
            actual_week_real_raf = float(result_real['values'][5][x])
            last_week_real_raf = float(result_real['values'][5][x-1])
            percent_real_raf = ((actual_week_real_raf-last_week_real_raf)/last_week_real_raf)*100
          else:
            percent_real_raf = 0 
        else:
            percent_real_raf = 0 

        #CRO Integradores REAL
        if str(result_real['values'][8][x]).isnumeric():
          
          if str(result_real['values'][8][x-1]).isnumeric():
            actual_week_real_integra = float(result_real['values'][8][x])
            last_week_real_integra = float(result_real['values'][8][x-1])
            percent_real_integra = ((actual_week_real_integra-last_week_real_integra)/last_week_real_integra)*100
          else:
            percent_real_integra = 0
        else:
            percent_real_integra = 0

        #CRO Second Hand REAL
        if str(result_real['values'][7][x]).isnumeric():
          
          if str(result_real['values'][7][x-1]).isnumeric():
            actual_week_real_second = float(result_real['values'][7][x])
            last_week_real_second = float(result_real['values'][7][x-1])
            percent_real_second = ((actual_week_real_second-last_week_real_second)/last_week_real_second)*100
          else:
            percent_real_second = 0 
        else:
            percent_real_second = 0

        #CRO Others REAL
        if str(result_real['values'][6][x]).isnumeric():
          
          if str(result_real['values'][6][x-1]).isnumeric():
            actual_week_real_others = float(result_real['values'][6][x])
            last_week_real_others = float(result_real['values'][6][x-1])
            percent_real_others = ((actual_week_real_others-last_week_real_others)/last_week_real_others)*100
          else:
            percent_real_others = 0 
        else:
            percent_real_others = 0 

        input_result_2 = [[str(round(percent_real_executives, 1))+"%"], [str(round(percent_real_om, 1))+"%"], [str(round(percent_real_cons,1))+"%"], [str(round(percent_real_raf,1))+"%"], [str(round(percent_real_second,1))+"%"], [str(round(percent_real_integra,1))+"%"], [str(round(percent_real_others,1))+"%"], [str(round(percent_real,1))+"%"]]

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
fluctuation_cro_real_method()

#Função que calcula a variação do TPV ADD do caa
def fluctuation_tpv_caa_method():
  result = (
    sheet.values()
    .get(spreadsheetId=SAMPLE_SPREADSHEET_ID, 
        range='forecast_caa!A29:DA36',
        majorDimension='ROWS')
    .execute()
  )

  i = 0
  while i < len(result['values'][0]):
    if result['values'][0][i] == search_caa:
      #TPV CAA
      actual_week_caa = float(result['values'][1][i])
      last_week_caa = float(result['values'][1][i-1])
      percent_caa = ((actual_week_caa - last_week_caa)/last_week_caa)*100

      #TPV Executives CAA
      actual_week_caa_executives = float(result['values'][2][i])
      last_week_caa_executives = float(result['values'][2][i-1])
      percent_caa_executives = ((actual_week_caa_executives - last_week_caa_executives)/last_week_caa_executives)*100

      #TPV OM CAA
      actual_week_caa_om = float(result['values'][3][i])
      last_week_caa_om = float(result['values'][3][i-1])
      percent_caa_om = ((actual_week_caa_om - last_week_caa_om)/last_week_caa_om)*100

      #TPV Consultants CAA
      actual_week_caa_consultants = float(result['values'][4][i])
      last_week_caa_consultants = float(result['values'][4][i-1])
      percent_caa_consultants = ((actual_week_caa_consultants - last_week_caa_consultants)/last_week_caa_consultants)*100

      #TPV RaF CAA
      actual_week_caa_raf = float(result['values'][5][i])
      last_week_caa_raf = float(result['values'][5][i-1])
      percent_caa_raf = ((actual_week_caa_raf - last_week_caa_raf)/last_week_caa_raf)*100

      #TPV Integradores CAA
      actual_week_caa_integra = float(result['values'][6][i])
      last_week_caa_integra = float(result['values'][6][i-1])
      percent_caa_integra = ((actual_week_caa_integra - last_week_caa_integra)/last_week_caa_integra)*100

      #TPV Second Hand CAA
      actual_week_caa_second = float(result['values'][7][i])
      last_week_caa_second = float(result['values'][7][i-1])
      percent_caa_second = ((actual_week_caa_second - last_week_caa_second)/last_week_caa_second)*100

      input_result = [[str(round(percent_caa_executives,1))+"%"], [str(round(percent_caa_om,1))+"%"], [str(round(percent_caa_consultants,1))+"%"], [str(round(percent_caa_raf,1))+"%"], [str(round(percent_caa_second,1))+"%"], [str(round(percent_caa_integra))+"%"], ['-'], [str(round(percent_caa,1))+"%"]]

      sheet.values().update(
            spreadsheetId=SAMPLE_SPREADSHEET_ID, 
            range='Análise Overall!F4', 
            valueInputOption="USER_ENTERED",
            body={'majorDimension': 'ROWS','values': input_result}
      ).execute()

      i += 1
    else:
      i += 1
fluctuation_tpv_caa_method()

#Função que calcula a variação do TPV ADD real
def fluctuation_tpv_real_method():
  result_real = (
          sheet.values()
          .get(spreadsheetId=SAMPLE_SPREADSHEET_ID, 
              range='TPV ADD!A3:DA12',
              majorDimension='ROWS')
          .execute()
  )
  i = 0
  print(result_real)
  print(search_caa)
  try:
    while i < len(result_real['values'][0]):
      if result_real['values'][0][i] == search_caa:
        #TPV Real
        if str(result_real['values'][2][i]).isnumeric():
          actual_week_real = float(result_real['values'][2][i])
          if str(result_real['values'][2][i-1]).isnumeric():
            last_week_real = float(result_real['values'][2][i-1])
            percent_real = ((actual_week_real - last_week_real)/last_week_real)*100
          else: 
            percent_real = 0
        else:
          percent_real = 0
          
        #TPV Executives Real
        if str(result_real['values'][4][i]).isnumeric():
          actual_week_real_executives = float(result_real['values'][4][i])
          if str(result_real['values'][4][i-1]).isnumeric():
            last_week_real_executives = float(result_real['values'][4][i-1])
            percent_real_executives = ((actual_week_real_executives - last_week_real_executives)/last_week_real_executives)*100
          else:
            percent_real_executives = 0
        else:
            percent_real_executives = 0

        #TPV OM Real
        if str(result_real['values'][5][i]).isnumeric():
          actual_week_real_om = float(result_real['values'][5][i])
          if str(result_real['values'][5][i-1]).isnumeric():
            last_week_real_om = float(result_real['values'][5][i-1])
            percent_real_om = ((actual_week_real_om - last_week_real_om)/last_week_real_om)*100
          else:
            percent_real_om = 0
        else:
            percent_real_om = 0

        #TPV Consultants Real
        if str(result_real['values'][3][i]).isnumeric():
          actual_week_real_consultants = float(result_real['values'][3][i])
          if str(result_real['values'][3][i-1]).isnumeric():
            last_week_real_consultants = float(result_real['values'][3][i-1])
            percent_real_consultants = ((actual_week_real_consultants - last_week_real_consultants)/last_week_real_consultants)*100
          else: 
            percent_real_consultants = 0
        else: 
            percent_real_consultants = 0

        #TPV RaF Real
        if str(result_real['values'][6][i]).isnumeric():
          actual_week_real_raf = float(result_real['values'][6][i])
          if str(result_real['values'][6][i-1]).isnumeric():
            last_week_real_raf = float(result_real['values'][6][i-1])
            percent_real_raf = ((actual_week_real_raf - last_week_real_raf)/last_week_real_raf)*100
          else:
            percent_real_raf = 0 
        else:
            percent_real_raf = 0 

        #TPV Second Hand Real
        if str(result_real['values'][8][i]).isnumeric():
          actual_week_real_second = float(result_real['values'][8][i])
          if str(result_real['values'][8][i-1]).isnumeric():
            last_week_real_second = float(result_real['values'][8][i-1])
            percent_real_second = ((actual_week_real_second - last_week_real_second)/last_week_real_second)*100
          else:
            percent_real_second = 0 
        else:
            percent_real_second = 0

        #TPV Integradores Real
        if str(result_real['values'][9][i]).isnumeric():
          actual_week_real_integra = float(result_real['values'][9][i])
          if str(result_real['values'][9][i-1]).isnumeric():
            last_week_real_integra = float(result_real['values'][9][i-1])
            percent_real_integra = ((actual_week_real_integra - last_week_real_integra)/last_week_real_integra)*100
          else:
            percent_real_integra = 0
        else:
            percent_real_integra = 0

        #TPV Others Real
        if str(result_real['values'][7][i]).isnumeric():
          actual_week_real_others = float(result_real['values'][7][i])
          if str(result_real['values'][7][i-1]).isnumeric():
            last_week_real_others = float(result_real['values'][7][i-1])
            percent_real_others = ((actual_week_real_others - last_week_real_others)/last_week_real_others)*100
          else:
            percent_real_others = 0
        else:
            percent_real_others = 0

        input_result = [[str(round(percent_real_executives,1))+"%"], [str(round(percent_real_om,1))+"%"], [str(round(percent_real_consultants,1))+"%"], [str(round(percent_real_raf,1))+"%"], [str(round(percent_real_second,1))+"%"], [str(round(percent_real_integra,1))+"%"], [str(round(percent_real_others,1))+"%"], [str(round(percent_real,1))+"%"]]

        sheet.values().update(
              spreadsheetId=SAMPLE_SPREADSHEET_ID, 
              range='Análise Overall!G4', 
              valueInputOption="USER_ENTERED",
              body={'majorDimension': 'ROWS','values': input_result}
        ).execute()
        sheet.values().update(
              spreadsheetId=SAMPLE_SPREADSHEET_ID, 
              range='Análise Overall!K4', 
              valueInputOption="USER_ENTERED",
              body={'majorDimension': 'ROWS','values': input_result}
        ).execute()
        print('deu certo')
        i += 1
      else:
        i += 1
  except IndexError:
    print("Data inválida")
fluctuation_tpv_real_method()

#Função que calcula a variação do TPV ADD do OKR
def fluctuation_tpv_okr_method():
  result = (
    sheet.values()
    .get(spreadsheetId=SAMPLE_SPREADSHEET_ID, 
    range='forecast_caa!A29:DA36',
    majorDimension='ROWS')
    .execute()
  )

  #looping que passa em todos os registros da tabela e encontra o valor de cada canal na semana
  i = 0
  while i < len(result['values'][0]):
    if result['values'][0][i] == search_caa:
          #TPV ADD OKR
          actual_week_okr = float(result['values'][1][i])
          last_week_okr = float(result['values'][1][i-1])
          percent_okr = ((actual_week_okr-last_week_okr)/last_week_okr)*100

          #TPV ADD Executives OKR
          actual_week_okr_executives = float(result['values'][3][i])
          last_week_okr_executives = float(result['values'][3][i-1])
          percent_okr_executives = ((actual_week_okr_executives-last_week_okr_executives)/last_week_okr_executives)*100

          #TPV ADD OM OKR
          actual_week_okr_om = float(result['values'][4][i])
          last_week_okr_om = float(result['values'][4][i-1])
          percent_okr_om = ((actual_week_okr_om-last_week_okr_om)/last_week_okr_om)*100

          #TPV ADD Consultants OKR
          actual_week_okr_consultants = float(result['values'][2][i])
          last_week_okr_consultants = float(result['values'][2][i-1])
          percent_okr_consultants = ((actual_week_okr_consultants-last_week_okr_consultants)/last_week_okr_consultants)*100

          #TPV ADD RaF OKR
          actual_week_okr_raf = float(result['values'][5][i])
          last_week_okr_raf = float(result['values'][5][i-1])
          percent_okr_raf = ((actual_week_okr_raf-last_week_okr_raf)/last_week_okr_raf)*100

          #TPV ADD Integradores OKR
          actual_week_okr_integra = float(result['values'][6][i])
          last_week_okr_integra = float(result['values'][6][i-1])
          percent_okr_integra = ((actual_week_okr_integra-last_week_okr_integra)/last_week_okr_integra)*100

          #TPV ADD Second hand COKR
          actual_week_okr_second = float(result['values'][7][i])
          last_week_okr_second = float(result['values'][7][i-1])
          percent_okr_second = ((actual_week_okr_second-last_week_okr_second)/last_week_okr_second)*100

          input_result = [[str(round(percent_okr_executives, 1))+"%"], [str(round(percent_okr_om,1))+"%"], [str(round(percent_okr_consultants,1))+"%"], [str(round(percent_okr_raf,1))+"%"], [str(round(percent_okr_second,1))+"%"], [str(round(percent_okr_integra,1))+"%"], ['-'], [str(round(percent_okr,1))+"%"] ]

          sheet.values().update(
            spreadsheetId=SAMPLE_SPREADSHEET_ID, 
            range='Análise Overall!J4', 
            valueInputOption="USER_ENTERED",
            body={'majorDimension': 'ROWS','values': input_result}
          ).execute()
          i += 1
    else:
          i += 1 
fluctuation_tpv_okr_method()

#Função que calcula a performance de CRO WoW
def performance_cro_method():
  result = (
    sheet.values()
    .get(spreadsheetId=SAMPLE_SPREADSHEET_ID, 
        range='CRO!A4:DA12',
        majorDimension='ROWS')
    .execute()
  )

  i = 0
  try:
    while i < len(result['values'][0]):
      if result['values'][0][i] == search_caa:

        #CRO WoW 
        if str(result['values'][1][i]).isnumeric():
          actual_week_cro = float(result['values'][1][i])
          if str(result['values'][1][i-1]).isnumeric():
            last_week_cro = float(result['values'][1][i-1])
            percent_wow_cro = ((actual_week_cro - last_week_cro)/last_week_cro)*100
          else:
            last_week_cro = 0
            percent_wow_cro = 0
        else:
          actual_week_cro = 0
          last_week_cro = 0
          percent_wow_cro = 0

        #CRO WoW Executives
        if str(result['values'][3][i]).isnumeric():
          actual_week_cro_executives = float(result['values'][3][i])
          if str(result['values'][3][i-1]).isnumeric():
            last_week_cro_executives = float(result['values'][3][i-1])
            percent_wow_cro_executives = ((actual_week_cro_executives - last_week_cro_executives)/last_week_cro_executives)*100
          else:
            last_week_cro_executives = 0
            percent_wow_cro_executives = 0
        else:
          actual_week_cro_executives = 0
          last_week_cro_executives = 0
          percent_wow_cro_executives = 0

        #CRO WoW OM
        if str(result['values'][4][i]).isnumeric():
          actual_week_cro_om = float(result['values'][4][i])
          if str(result['values'][4][i-1]).isnumeric():
            last_week_cro_om = float(result['values'][4][i-1])
            percent_wow_cro_om = ((actual_week_cro_om - last_week_cro_om)/last_week_cro_om)*100
          else:
            last_week_cro_om = 0
            percent_wow_cro_om = 0
        else:
          actual_week_cro_om = 0
          last_week_cro_om = 0
          percent_wow_cro_om = 0

        #CRO WoW Consultants
        if str(result['values'][2][i]).isnumeric():
          actual_week_cro_consultants = float(result['values'][2][i])
          if str(result['values'][2][i-1]).isnumeric():
            last_week_cro_consultants = float(result['values'][2][i-1])
            percent_wow_cro_consultants = ((actual_week_cro_consultants - last_week_cro_consultants)/last_week_cro_consultants)*100
          else:
            last_week_cro_consultants = 0 
            percent_wow_cro_consultants = 0
        else:
          actual_week_cro_consultants = 0 
          last_week_cro_consultants = 0 
          percent_wow_cro_consultants = 0

        #CRO WoW RaF
        if str(result['values'][5][i]).isnumeric():
          actual_week_cro_raf = float(result['values'][5][i])
          if str(result['values'][5][i-1]).isnumeric():
            last_week_cro_raf = float(result['values'][5][i-1])
            percent_wow_cro_raf = ((actual_week_cro_raf - last_week_cro_raf)/last_week_cro_raf)*100
          else:
            last_week_cro_raf = 0
            percent_wow_cro_raf = 0
        else:
          actual_week_cro_raf = 0
          last_week_cro_raf = 0
          percent_wow_cro_raf = 0

        #CRO WoW Second Hand
        if str(result['values'][7][i]).isnumeric():
          actual_week_cro_second = float(result['values'][7][i])
          if str(result['values'][7][i-1]).isnumeric():
            last_week_cro_second = float(result['values'][7][i-1])
            percent_wow_cro_second = ((actual_week_cro_second - last_week_cro_second)/last_week_cro_second)*100
          else:
            last_week_cro_second = 0
            percent_wow_cro_second = 0
        else:
          actual_week_cro_second = 0
          last_week_cro_second = 0
          percent_wow_cro_second = 0

        #CRO WoW Integradores
        if str(result['values'][8][i]).isnumeric():
          actual_week_cro_integra = float(result['values'][8][i])
          if str(result['values'][8][i-1]).isnumeric():
            last_week_cro_integra = float(result['values'][8][i-1])
            percent_wow_cro_integra = ((actual_week_cro_integra - last_week_cro_integra)/last_week_cro_integra)*100
          else:
            last_week_cro_integra = 0
            percent_wow_cro_integra = 0
        else:
          actual_week_cro_integra = 0 
          last_week_cro_integra = 0
          percent_wow_cro_integra = 0   

        #CRO WoW Others
        if str(result['values'][6][i]).isnumeric():
          actual_week_cro_others = float(result['values'][6][i])
          if str(result['values'][6][i-1]).isnumeric():
            last_week_cro_others = float(result['values'][6][i-1])
            percent_wow_cro_others = ((actual_week_cro_others - last_week_cro_others)/last_week_cro_others)*100
          else:
            last_week_cro_others = 0
            percent_wow_cro_others = 0
        else:
          actual_week_cro_others = 0 
          last_week_cro_others = 0
          percent_wow_cro_others = 0

        input_result = [[last_week_cro_executives,actual_week_cro_executives,str(round(percent_wow_cro_executives,1))+"%"],
                        [last_week_cro_om,actual_week_cro_om,str(round(percent_wow_cro_om,1))+"%"],
                        [last_week_cro_consultants,actual_week_cro_consultants,str(round(percent_wow_cro_consultants,1))+"%"],
                        [last_week_cro_raf,actual_week_cro_raf,str(round(percent_wow_cro_raf,1))+"%"],
                        [last_week_cro_second,actual_week_cro_second,str(round(percent_wow_cro_second,1))+"%"],
                        [last_week_cro_integra,actual_week_cro_integra,str(round(percent_wow_cro_integra,1))+"%"],
                        [last_week_cro_others,actual_week_cro_others,str(round(percent_wow_cro_others,1))+"%"],
                        [last_week_cro,actual_week_cro,str(round(percent_wow_cro,1))+"%"]]


        sheet.values().update(
              spreadsheetId=SAMPLE_SPREADSHEET_ID, 
              range='Análise Overall!B17', 
              valueInputOption="USER_ENTERED",
              body={'majorDimension': 'ROWS','values': input_result}
            ).execute()

        i += 1
      else:
        i += 1
  except IndexError:
    print('data inválida')
performance_cro_method()