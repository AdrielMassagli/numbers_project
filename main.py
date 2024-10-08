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

