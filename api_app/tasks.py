from celery import shared_task
from api_app.serializers import FnoSerializer
import pandas as pd
import os
import csv
import glob
from pathlib import Path
from datetime import datetime
import pytz
from scripts.download_and_unzip import external_methods as em


# Celery task Logic to generate Analysis file
@shared_task(bind=True)
def backGroundLogic(self):
    # address of input data
    folder = './datafiles/market/market'
    dt = datetime.now().date()
    # today's day in string format
    newDate=dt.strftime('%d''%b''%Y').upper()
    # if analysis file already exists for a date then skip the calculation
    if os.path.exists(f'api_app/{newDate}analysis.csv'):
        return 1
    # Calling the Analysis function
    fnoAnalysis(folder)
    print("Analysis Complete")
    
# Celery task logic for downloading the file from NSE website

@shared_task(bind=True)
def downloadData(self):
    dateObj=datetime.now(pytz.timezone('Asia/Kolkata')).date()
    date=dateObj.day
    year=dateObj.year
    month1=['JAN','FEB','MAR','APR','MAY','JUN','JUL',"AUG","SEP","OCT","NOV","DEC"]
    month2=month1[dateObj.month-1]
    bhav=f"https://www1.nseindia.com/content/historical/EQUITIES/{year}/{month2}/cm{date-1}{month2}{year}bhav.csv.zip"
    fo_bhav=f"https://archives.nseindia.com/content/historical/DERIVATIVES/{year}/{month2}/fo{date-1}{month2}{year}bhav.csv.zip"
    dt = datetime.now().date()
    newDate=dt.strftime('%d''%b''%Y').upper()
    # check for whether the file is already downloaded
    if os.path.exists(f'./datafiles/market/market/fo{newDate}bhav.csv') and os.path.exists(f'./datafiles/market/market/cm{newDate}bhav.csv'):
        return 1
    em.deleteFiles('./datafiles/market/market/cm*.csv')
    em.deleteFiles('./datafiles/market/market/fo*.csv')
    em.download_and_unzip(bhav)
    em.download_and_unzip(fo_bhav)
    

    
    
def fnoAnalysis(folder):
    tickerList = ['INDUSINDBK','CIPLA','AXISBANK','KOTAKBANK','MARUTI','SUNPHARMA','DIVISLAB','POWERGRID','ICICIBANK','BHARTIARTL',
                  'ITC','NTPC','JSWSTEEL','APOLLOHOSP','HINDUNILVR','ONGC','TATASTEEL','BAJFINANCE','COALINDIA','TITAN','SBILIFE',
                  'HDFCBANK','LT','SBIN','HDFC','EICHERMOT','BAJAJFINSV','DRREDDY','SHREECEM','BAJAJ-AUTO','HCLTECH','ASIANPAINT',
                  'BRITANNIA','HINDALCO','RELIANCE','TCS','ADANIPORTS','NESTLEIND','WIPRO','TATAMOTORS','BPCL','M&M','HDFCLIFE',
                  'HEROMOTOCO','GRASIM','INFY','ULTRACEMCO','TECHM','TATACONSUM','UPL']
    
    for ticker in tickerList:
        location = folder+'/'+"fo*.csv"
        files = os.path.join(location)
    #     print(files)
        files = glob.glob(files)
    #     print(files)
        df = pd.concat(map(pd.read_csv, files))
    
        df = df.loc[df['SYMBOL']==ticker]
        df = df.loc[df['INSTRUMENT']=='FUTSTK']
        df.drop('STRIKE_PR', inplace=True, axis=1)
        df.drop('Unnamed: 15', inplace=True, axis=1)
        df.drop('OPEN', inplace=True, axis=1)
        df.drop('HIGH', inplace=True, axis=1)
        df.drop('LOW', inplace=True, axis=1)
        df.drop('CLOSE', inplace=True, axis=1)
        df.drop('SETTLE_PR', inplace=True, axis=1)
        df = df.groupby(['TIMESTAMP']).sum()

    
    
        locationcm = folder+'/'+"cm*.csv"
        files = os.path.join(locationcm)
        files = glob.glob(files)
    
        dfcm = pd.concat(map(pd.read_csv, files), ignore_index=True)
        dfcm = dfcm.loc[dfcm['SYMBOL']==ticker]
        dfcm = dfcm.loc[dfcm['SERIES']=='EQ']
        dfcm.drop('ISIN', inplace=True, axis=1)
        dfcm.drop('LAST', inplace=True, axis=1)
        dfcm.drop('HIGH', inplace=True, axis=1)
        dfcm.drop('LOW', inplace=True, axis=1)
        dfcm.drop('SERIES', inplace=True, axis=1)
        dfcm.drop('Unnamed: 13', inplace=True, axis=1)
    
        location = folder + '/demo_for' + ticker + '.csv'
        dfcm.to_csv(location,index=False)
    
        location = folder + '/demo_for' + ticker + '.csv'
        files = os.path.join(location)
        files = glob.glob(files)
        df2 = pd.concat(map(pd.read_csv, files), ignore_index=True)
    
        result = pd.merge(df, df2, on=['TIMESTAMP'])
        result['%_OI_change'] = (result['CHG_IN_OI'] / result['OPEN_INT'])*100
        result['Quantity/Trades'] = (result['TOTTRDQTY'] / result['TOTALTRADES'])*100
        result['%_Price_change'] = ((result['CLOSE']-result['PREVCLOSE']) / result['PREVCLOSE'])*100
    
        result.drop('TIMESTAMP', inplace=True, axis=1)
        result.drop('VAL_INLAKH', inplace=True, axis=1)
        result.drop('CHG_IN_OI', inplace=True, axis=1)
        result.drop('OPEN', inplace=True, axis=1)
        result.drop('PREVCLOSE', inplace=True, axis=1)
        result.drop('TOTTRDQTY', inplace=True, axis=1)
        result.drop('TOTTRDVAL', inplace=True, axis=1)
        result.drop('TOTALTRADES', inplace=True, axis=1)
        result.drop('CONTRACTS', inplace=True, axis=1)
    
        location = folder + '/FnOAnalysisFor_' + ticker + '.csv'
        result.to_csv(location,index=False)
    
    for filename in glob.glob(folder+"/demo*"):
        os.remove(filename) 
    
    path = folder+'/'
    file_list = [path + f for f in os.listdir(path) if f.startswith('FnO')]
    csv_list = []
    for file in sorted(file_list):
        csv_list.append(pd.read_csv(file).assign(File_Name = os.path.basename(file)))
    csv_merged = pd.concat(csv_list, ignore_index=True)
    csv_merged.drop('File_Name', inplace=True, axis=1)
    
    csv_merged['OI_Trend'] = 'OI_Trend'
    
    i=0
    
    for index, row in csv_merged.iterrows():
        if float(row['%_OI_change'])>0 and float(row['%_Price_change'])>0 :    
            csv_merged.at[i, 'OI_Trend'] = "Long Build Up"
            i=i+1
        elif float(row['%_OI_change'])<0 and float(row['%_Price_change'])>0:   
            csv_merged.at[i, 'OI_Trend'] = "Short Covering"
            i=i+1
        elif float(row['%_OI_change'])<0 and float(row['%_Price_change'])<0:  
            csv_merged.at[i, 'OI_Trend'] = "Long Cover Up"
            i=i+1
        else:
            csv_merged.at[i, 'OI_Trend'] = "Short Build Up"
            i=i+1
        
    dt = datetime.now().date()
    newDate=dt.strftime('%d''%b''%Y').upper()
    csv_merged.to_csv('api_app/'+newDate+ 'analysis.csv', index=False)
    
    for filename in glob.glob(folder+"/FnO*"):
        os.remove(filename) 
    print("The Analysis has been completed")
    


