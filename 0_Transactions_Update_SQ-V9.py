#------------------------------------------------------
# VERSION: 8.0
# Stock-Monitor Update from Swissquote Transaction file
# Roberto Schlatter
# 17-05-2021
#-------------------------------------------------------

import pandas as pd
import datetime
# For Google Sheet functions
import pickle
import os.path
import os
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.protobuf import service
import csv

# ------------------------------------------
# Pre Definition for Test run and final run
# ------------------------------------------

sTestRun = False

if sTestRun:
    sGoogleSheet = "17iBlVZySEFsqI9RjPR6hVrlD_QtQ-zc46bKuar0nFOk"
else:
    sGoogleSheet = "1RX2lJwZYWUO_sJUME1M9CsMFkGQNEqMCiY2Y6vCZjF0"


# ---------------
# File evaluation
#----------------
# aktueller Filename im gleichen Verzeichnis wie das Python File
sFilename = 'transactions-from-01012010-to-19052021.xlsx'

# Set new header names in order to remove Umlaute
lst_column_names = ['Auftrag', 'Datum', 'Transaktionen', 'Symbol', 'Name', 'ISIN', 'Anzahl', 'Stueckpreis', 'Kosten','Aufgelaufene Zinsen','Nettobetrag','Waehrung Nettobetrag', 'Nettobetrag in der Waehrung des Kontos','Saldo','Waehrung']
df_transactions = pd.read_excel(sFilename, index_col=1, names=lst_column_names)

# df_transactions = pd.read_excel(sFilename, index_col=1)

lst_header = ['Transaktionen', 'Name']

print(df_transactions.head(2))

#----------
# DIVIDENDE
#----------
# Filter table to show only columns "Transaktionen", "Name"
df_dividende = df_transactions[df_transactions['Transaktionen'].isin(['Dividende'])]

df_dividende['Nettobetrag in der Währung des Kontos'] = pd.to_numeric(df_dividende['Nettobetrag in der Waehrung des Kontos'])


# Final dataframe for dividends
lst_header = ['Symbol']
#lst_header_div = ['Symbol','Jahr','Monat']


df_export_dividende = df_dividende.groupby(lst_header)['Nettobetrag in der Waehrung des Kontos'].sum().head(25)

#df_export_dividende = df_export_dividende_pre.head(30)

print(df_export_dividende)

# Nach Datum sortiert - für Monatsübersicht
#Create new Columns with Jahr and Monat as extracted from "Datum"
df_dividende['Jahr'] = pd.DatetimeIndex(df_dividende.iloc[:,0]).year
df_dividende['Monat'] = pd.DatetimeIndex(df_dividende.iloc[:,0]).month

lst_header_div = ['Jahr', 'Monat', 'Name']

df_export_dividende_2 = df_dividende.groupby(lst_header_div)['Nettobetrag in der Waehrung des Kontos'].sum()

print("Dividende: ", df_export_dividende_2)

#-------
# KOSTEN
#-------
# KAUF & VERKAUF Kosten
df_kosten_total = df_transactions[df_transactions['Transaktionen'].isin(['Kauf', 'Verkauf'])]

df_kosten_total['Jahr'] = pd.DatetimeIndex(df_kosten_total.iloc[:,0]).year
df_kosten_total['Monat'] = pd.DatetimeIndex(df_kosten_total.iloc[:,0]).month

# Final dataframe for costs
#Nach Datum sortiert
lst_header_cost = ['Transaktionen','Symbol', 'Jahr', 'Monat']
df_export_kosten_total = df_kosten_total.groupby(lst_header_cost)['Kosten'].sum()

print(df_export_kosten_total)

#----------------------------------------------
# Transaktionen - total performance Berechnung
#----------------------------------------------
df_export_performance = df_transactions[df_transactions['Transaktionen'].isin(['Kauf', 'Verkauf'])]

#df_export_performance = df_export_performance[['Datum', 'Transaktionen', 'Symbol', 'Name', 'Anzahl', 'Stueckpreis', 'Kosten', 'Nettobetrag in der Waehrung des Kontos', 'Waehrung Nettobetrag']]

#Create new Columns with Jahr and Monat as extracted from "Datum"
df_export_performance['Jahr'] = pd.DatetimeIndex(df_export_performance.iloc[:,0]).year
df_export_performance['Monat'] = pd.DatetimeIndex(df_export_performance.iloc[:,0]).month

# list of columns selector
lst_header_perf = ['Name','Transaktionen','Jahr','Monat']
#lst_header_perf = ['Name','Transaktionen']


# Remove tausender Trennzeichen um Kolonne als Zahlenwert beim Summieren zu verewenden
df_export_performance['Nettobetrag in der Währung des Kontos'] = df_export_performance['Nettobetrag in der Waehrung des Kontos'].replace("'")

#print(df_export_performance['Nettobetrag in der Währung des Kontos'])

df_export_performance_values = df_export_performance.groupby(lst_header_perf)['Nettobetrag in der Waehrung des Kontos'].sum()

print("PERFORMANCE: ", df_export_performance_values)


# get current date and format it
pUpdateDate = datetime.datetime.now()
pDay = pUpdateDate.strftime("%A")
pDate = pUpdateDate.strftime("%d-%m-%Y | %H:%M:%S")
lst_date = [pDay, pDate]

# number of current assets - KAUF / VERKAUF
df_numberAssets_k = df_transactions[df_transactions['Transaktionen'].isin(['Kauf'])]
df_numberAssets_k = df_numberAssets_k[['Symbol','Anzahl']]
df_numberAssets_count_k = df_numberAssets_k.groupby('Symbol')['Anzahl'].sum()

# Summe 'Nettobetrag' noch ausrechnen
df_numberAssets_nettobetrag = df_transactions[df_transactions['Transaktionen'].isin(['Kauf'])]
df_numberAssets_nettobetrag = df_numberAssets_nettobetrag[['Symbol','Anzahl','Nettobetrag']]

#  Convert 'Nettobetrag' Column to abs values
df_numberAssets_count_nettobetrag = df_numberAssets_nettobetrag.groupby('Symbol').Nettobetrag.apply(lambda c: c.abs().sum())

print("Nettobetrag sum():", df_numberAssets_count_nettobetrag)
print("Kauf:", df_numberAssets_count_k)

df_numberAssets_v = df_transactions[df_transactions['Transaktionen'].isin(['Verkauf'])]
df_numberAssets_v = df_numberAssets_v[['Symbol','Anzahl']]
df_numberAssets_count_v = df_numberAssets_v.groupby('Symbol')['Anzahl'].sum()


print("Verkauf: ", df_numberAssets_count_v)

# number of current assets - CRYPTHO BUY
df_numberAssets_c = df_transactions[df_transactions['Transaktionen'].isin(['Buy'])]
df_numberAssets_c = df_numberAssets_c[['Symbol','Anzahl']]
df_numberAssets_count_c = df_numberAssets_c.groupby('Symbol')['Anzahl'].sum()

# Summe 'Nettobetrag' noch ausrechnen
df_numberAssets_c_nettobetrag = df_transactions[df_transactions['Transaktionen'].isin(['Buy'])]
df_numberAssets_c_nettobetrag = df_numberAssets_c_nettobetrag[['Symbol','Anzahl','Nettobetrag']]

#  Convert 'Nettobetrag' Column to abs values
df_numberAssets_count_c_nettobetrag = df_numberAssets_c_nettobetrag.groupby('Symbol').Nettobetrag.apply(lambda c: c.abs().sum())

print("Nettobetrag Cryptho sum():", df_numberAssets_count_c_nettobetrag)

# number of current assets - CRYPTHO WITHDRAWAL
df_numberAssets_w = df_transactions[df_transactions['Transaktionen'].isin(['Withdrawal'])]
df_numberAssets_w = df_numberAssets_w[['Symbol','Anzahl']]
df_numberAssets_count_w = df_numberAssets_w.groupby('Symbol')['Anzahl'].sum()

df_numberAssets_sell = df_transactions[df_transactions['Transaktionen'].isin(['Sell'])]
df_numberAssets_sell = df_numberAssets_sell[['Symbol','Anzahl']]
df_numberAssets_count_sell = df_numberAssets_sell.groupby('Symbol')['Anzahl'].sum()

df_numberAssets_count_w = df_numberAssets_count_w + df_numberAssets_count_sell


# ZU beachten: Bei Aktiensplitt stimmt die finale Anzahl nicht
# Wenn ein Asset noch immer gehalten wird - sprich keine Verkäufe getätigt wurden - steht im finalen Dataframe NaN
# --> dies muss abgefangen werden

df_numberAssets_count_actual = df_numberAssets_count_k.sub(df_numberAssets_count_v)

# sExportName_files = '-FINAL.csv'
# df_numberAssets_count_actual.to_csv(sExportName_files)

print("Final numbers: ", df_numberAssets_count_actual)

# EINZAHLUNGEN - 'VERGÜTUNG' pro Jahr
df_sumInvestment = df_transactions[df_transactions['Transaktionen'].isin(['Vergütung'])]
df_sumInvestment['Jahr'] = pd.DatetimeIndex(df_sumInvestment.iloc[:,0]).year
df_sumInvestment = df_sumInvestment[['Jahr', 'Nettobetrag']]
df_sumInvestment_total = df_sumInvestment.groupby('Jahr')['Nettobetrag'].sum()

# AUSZAHLUNGEN - 'Auszahlung' pro Jahr
df_sumInvestment_out = df_transactions[df_transactions['Transaktionen'].isin(['Auszahlung'])]
df_sumInvestment_out['Jahr'] = pd.DatetimeIndex(df_sumInvestment_out.iloc[:,0]).year
df_sumInvestment_out = df_sumInvestment_out[['Jahr', 'Nettobetrag']]
df_sumInvestment_out_total = df_sumInvestment_out.groupby('Jahr')['Nettobetrag'].sum()

print(df_sumInvestment_out_total)

#----------------------------------------
# Google Sheets Storing - STOCK-MONITOR
#----------------------------------------

def storeDataToGoogleSheet(SAMPLE_SPREADSHEET_ID_input, SAMPLE_RANGE_NAME, df_gold, sOrientation, bSingleValue):
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

    if(bSingleValue):
        def SingleValue_Export_Data_To_Sheets():
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
        SingleValue_Export_Data_To_Sheets()
    else:
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
#----------------------------------------
# Call storeDataToGoogleSheet function
#----------------------------------------
# Dividend summary
storeDataToGoogleSheet(sGoogleSheet, 'SUMMARY!B56', df_export_dividende, 'COLUMNS', False)

# Date & time stamp of update
#storeDataToGoogleSheet(sGoogleSheet, 'SUMMARY!C02', lst_date, 'ROWS', True)

# Kauf and Verkauf table
storeDataToGoogleSheet(sGoogleSheet, 'HISTORIC-DATA!A03', df_export_kosten_total, 'COLUMNS',False)

# Dividend detail table by month and year
storeDataToGoogleSheet(sGoogleSheet, 'HISTORIC-DATA!M03', df_export_dividende_2, 'COLUMNS', False)

# Performance calculation buy & sell price
storeDataToGoogleSheet(sGoogleSheet, 'HISTORIC-DATA!R03', df_export_performance_values, 'COLUMNS', False)


# Anzahl Assets Kauf
storeDataToGoogleSheet(sGoogleSheet, 'HISTORIC-DATA!X03', df_numberAssets_count_k, 'COLUMNS', False)

# Anzahl Assets Verkauf
storeDataToGoogleSheet(sGoogleSheet, 'HISTORIC-DATA!AA03', df_numberAssets_count_v, 'COLUMNS', False)

# Anzahl Assets Kauf CRYPTHO
storeDataToGoogleSheet(sGoogleSheet, 'HISTORIC-DATA!AD03', df_numberAssets_count_c, 'COLUMNS', False)

# Anzahl Assets Verkauf CRYPTHO
storeDataToGoogleSheet(sGoogleSheet, 'HISTORIC-DATA!AG03', df_numberAssets_count_w, 'COLUMNS', False)

# Anzahl Initial Value Kauf CRYPTHO
storeDataToGoogleSheet(sGoogleSheet, 'HISTORIC-DATA!BG03', df_numberAssets_count_c_nettobetrag, 'COLUMNS', False)

# Anzahl Initial Value Kauf ASSETS
storeDataToGoogleSheet(sGoogleSheet, 'HISTORIC-DATA!BC03', df_numberAssets_count_nettobetrag, 'COLUMNS', False)

# Einzahlungen pro Jahr
storeDataToGoogleSheet(sGoogleSheet, 'HISTORIC-DATA!BO03', df_sumInvestment_total, 'COLUMNS', False)

# Auszahlungen pro Jahr
storeDataToGoogleSheet(sGoogleSheet, 'HISTORIC-DATA!BR03', df_sumInvestment_out_total, 'COLUMNS', False)

