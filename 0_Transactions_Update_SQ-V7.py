#------------------------------------------------------
# VERSION: 5.0
# Stock-Monitor Update from Swissquote Transaction file
# Roberto Schlatter
# 10-12-2020
# Excel Import -> vorher Spalten als Zahlen 
# formatieren und file als *.xlsx abspeichern
#-------------------------------------------------------

import pandas as pd 

# For Google Sheet functions
import pickle
import os.path
import os
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request


# ---------------
# File evaluation
#----------------
   
sFilename = 'transactions-from-01012010-to-29012021.xlsx'
sRoot = 'D:/PROG/2_RawData/'
sFilename = sRoot + sFilename
print(sFilename)

df_transactions = pd.read_excel(sFilename, index_col=1)

lst_header = ['Transaktionen', 'Name']

#----------
# DIVIDENDE
#----------
# Filter table to show only columns "Transaktionen", "Name"
df_dividende = df_transactions[df_transactions['Transaktionen'].isin(['Dividende'])]

df_dividende['Nettobetrag in der Währung des Kontos'] = pd.to_numeric(df_dividende['Nettobetrag in der Währung des Kontos'])

# Final dataframe for dividends
#lst_header_div = ['Symbol']
#df_export_dividende = df_dividende.groupby(lst_header_div)['Nettobetrag in der Währung des Kontos'].sum()

#Create new Columns with Jahr and Monat as extracted from "Datum"
df_dividende['Jahr'] = pd.DatetimeIndex(df_dividende['Datum']).year 
df_dividende['Monat'] = pd.DatetimeIndex(df_dividende['Datum']).month

#Nach Datum sortiert
lst_header_div = ['Jahr', 'Monat', 'Name']
df_export_dividende_2 = df_dividende.groupby(lst_header_div)['Nettobetrag in der Währung des Kontos'].sum()

print("Dividende: ", df_export_dividende_2)

#-------
# KOSTEN
#-------
# KAUF & VERKAUF Kosten
df_kosten_total = df_transactions[df_transactions['Transaktionen'].isin(['Kauf', 'Verkauf'])]

# Final dataframe for costs
df_export_kosten_total = df_kosten_total.groupby(lst_header)['Kosten'].sum()

#print(df_export_kosten_total)

#----------------------------------------------
# Transaktionen - total performance Berechnung
#----------------------------------------------
df_export_performance = df_transactions[df_transactions['Transaktionen'].isin(['Kauf', 'Verkauf'])]

df_export_performance = df_export_performance[['Datum', 'Transaktionen', 'Symbol', 'Name', 'Anzahl', 'Stückpreis', 'Kosten', 'Nettobetrag in der Währung des Kontos', 'Währung Nettobetrag']]

#Create new Columns with Jahr and Monat as extracted from "Datum"
df_export_performance['Jahr'] = pd.DatetimeIndex(df_export_performance['Datum']).year 
df_export_performance['Monat'] = pd.DatetimeIndex(df_export_performance['Datum']).month

# list of columns selector
lst_header_perf = ['Name','Transaktionen','Jahr','Monat']

df_export_performance_values = df_export_performance.groupby(lst_header_perf)['Nettobetrag in der Währung des Kontos'].sum()

print("PERFORMANCE: ", df_export_performance_values)

#----------------------------------------
# Google Sheets Storing - STOCK-MONITOR
#----------------------------------------

def storeDataToGoogleSheet(SAMPLE_SPREADSHEET_ID_input, SAMPLE_RANGE_NAME, df_gold, sOrientation):
    def Create_Service(client_secret_file, api_service_name, api_version, *scopes):
        global service
        SCOPES = [scope for scope in scopes[0]]
        #print(SCOPES)
        cred = None

        if os.path.exists('token_write.pickle'):
            with open('token_write.pickle', 'rb') as token:
                cred = pickle.load(token)

        if not cred or not cred.valid:
            if cred and cred.expired and cred.refresh_token:
                cred.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(client_secret_file, SCOPES)
                cred = flow.run_local_server()

            with open('token_write.pickle', 'wb') as token:
                pickle.dump(cred, token)

        try:
            service = build(api_service_name, api_version, credentials=cred)
            print(api_service_name, 'service created successfully')
            #return service
        except Exception as e:
            print(e)
            #return None
        
    # change 'my_json_file.json' by your downloaded JSON file: my_credentials.json
    Create_Service('my_credentials.json', 'sheets', 'v4',['https://www.googleapis.com/auth/spreadsheets'])
    
    def Export_Data_To_Sheets():
        response_date = service.spreadsheets().values().update(
            spreadsheetId=SAMPLE_SPREADSHEET_ID_input,
            valueInputOption='RAW',
            range=SAMPLE_RANGE_NAME,
            body=dict(
                #Orientation: ROWS oder COLUMNS
                majorDimension=sOrientation,
                values=df_gold.T.reset_index().T.values.tolist())
        ).execute()
        print('Sheet successfully Updated')

    Export_Data_To_Sheets()
    print(df_export_dividende)
 #call storeDataToGoogleSheet function
storeDataToGoogleSheet('1RX2lJwZYWUO_sJUME1M9CsMFkGQNEqMCiY2Y6vCZjF0', 'SUMMARY!B53', df_export_dividende, 'COLUMNS')
storeDataToGoogleSheet('1RX2lJwZYWUO_sJUME1M9CsMFkGQNEqMCiY2Y6vCZjF0', 'HISTORIC-DATA!A03', df_export_kosten_total, 'COLUMNS')
storeDataToGoogleSheet('1RX2lJwZYWUO_sJUME1M9CsMFkGQNEqMCiY2Y6vCZjF0', 'HISTORIC-DATA!E03', df_export_performance_values, 'COLUMNS')
storeDataToGoogleSheet('1RX2lJwZYWUO_sJUME1M9CsMFkGQNEqMCiY2Y6vCZjF0', 'HISTORIC-DATA!K03', df_export_dividende_2, 'COLUMNS')


