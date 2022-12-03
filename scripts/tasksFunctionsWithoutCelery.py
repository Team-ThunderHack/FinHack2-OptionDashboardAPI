import os,pytz,glob
import os.path

from datetime import datetime,timedelta

# from api_app.tasks import downloadData
from scripts.download_and_unzip import external_methods as em
import pandas as pd
from time import sleep
from colorama import Fore, Back, Style

class task_functions:
    
    # Celery task Logic to generate Analysis file    
    def backGroundLogic(dateObj):
        # address of input data
        folder = './datafiles/market/market'       
        #  date in string format
        newDate=dateObj.strftime('%d''%b''%Y').upper()
        
        # if analysis file already exists for a date then skip the calculation        
        if os.path.exists(f'datafiles/analysis/{newDate}.csv'):
            return f'./datafiles/analysis/{newDate}.csv'
        
        # Calling the Analysis function
        result=analysis.fnoAnalysis(folder,dateObj)        
        if(result==1):
            # print(Fore.LIGHTGREEN_EX+f"Analysis Complete for {newDate}")
            return f'./datafiles/analysis/{newDate}.csv'
        return 0
        
    # Celery task logic for downloading the file from NSE website
    def downloadData(dateObj):        
        date=dateObj.strftime('%d''%b''%Y').upper()
        year=dateObj.year
        month=dateObj.strftime('%b').upper()
        
        bhav=f"https://www1.nseindia.com/content/historical/EQUITIES/{year}/{month}/cm{date}bhav.csv.zip"
        fo_bhav=f"https://archives.nseindia.com/content/historical/DERIVATIVES/{year}/{month}/fo{date}bhav.csv.zip"
        newDate=str(date)+month+str(year)
                
        cmbhavFile = fobhavFile = 0
        # check for whether the file is already downloaded
        # if respective files are not present already i.e. variable have value 0 then download
        # file and assign the name of the file to the variable         
        if (os.path.exists(f'./datafiles/market/market/fo{newDate}bhav.csv')):
            fobhavFile=f'./datafiles/market/market/fo{newDate}bhav.csv'
             # cm bhav file exists already
        else:
            # print(fo_bhav)
            fobhavFile=em.download_and_unzip(fo_bhav)
            # since download function returns a list and we know only one file is to be downloaded
            # from the links so we are taking the first file from the list

        if (os.path.exists(f'./datafiles/market/market/cm{newDate}bhav.csv')):
            cmbhavFile=f'./datafiles/market/market/cm{newDate}bhav.csv'
             # fo bhav file exists already
        else:
            # print(bhav)
            cmbhavFile=em.download_and_unzip(bhav)

        return [cmbhavFile,fobhavFile]
        # value of these will be strings literals if download_and_unzip() is successful
        # otherwise there value is zero

        


class BulkFunctions:

    # def __init__(self):
    #     task_functions.__init__(self)


    
#   fileList passed over here are returned by bulkDownload it will tell 
#   which files are available to analyze and which are not

#   Private method
    def __bulkAnalysisBackgroundLogic(self,numResults,fileList):
        # print("line 70 called")
        dateObj=datetime.now(pytz.timezone('Asia/Kolkata')).date()
        allPreviousFiles=glob.glob("./datafiles/analysis/*.csv")
        # print(Fore.LIGHTMAGENTA_EX+str(fileList))
        # if list is a list of list then flatten it
        if(type(fileList[1]).__name__=='list'):
            fileList=[item for sublist in fileList for item in sublist]
        
        fileList2=[] # list containing all analysis files
        i=j=0
        while(j<numResults and i<100):
            newDate=dateObj-timedelta(days=i)            
            strDate=newDate.strftime('%d''%b''%Y').upper()  
                      
            fileName=[f'./datafiles/market/market/cm{strDate}bhav.csv',f'./datafiles/market/market/fo{strDate}bhav.csv']
            res=0
            # print(fileName)
            if os.path.exists(f'./datafiles/analysis/{strDate}.csv'):
                res=f'./datafiles/analysis/{strDate}.csv'
            elif (fileName[0] in fileList) and (fileName[1] in fileList) :                
                res=task_functions.backGroundLogic(newDate)
            if(res!=0):
                j+=1 # we found a set of result
                fileList2.append(res) # store the name of generated analysis files in list
                print(Fore.LIGHTGREEN_EX+f"Analysis Complete for {strDate}")
            
            i+=1 ## for going one more day back in next iteration 
        # print(Fore.LIGHTRED_EX+str(fileList2))
        for file in allPreviousFiles:
            if file not in fileList2:
                em.deleteFiles(file)
        return 1


        # numResults=5 # define how many number of analysis file you want
    def __bulkDownload(self,numResults):
        # print("line 97 called")
        dateObj=datetime.now(pytz.timezone('Asia/Kolkata')).date()
        fileList=[] # list containing all files downloaded
        i=j=0
        allPreviousFiles=glob.glob("./datafiles/market/market/*.csv")
        # print("line 102 called")
        while(j<numResults and i<100):
            ndate=dateObj-timedelta(days=i)
            res=task_functions.downloadData(ndate)
            i+=1 ## for going one more day back in next iteration
            if(res[0]!=0 and res[1]!=0):
                j+=1 # we found a set of result
                fileList.extend(res)
                print(Fore.LIGHTMAGENTA_EX+f"data downloaded for {ndate.strftime('%d''%b''%Y').upper() }")
        # Delete old files
        for file in allPreviousFiles:
            if file not in fileList:
                em.deleteFiles(file)
        
        return fileList 

    def download_analyze(self,num):
        # print("download_analyzed(bulkdownload) called")
        files=self.__bulkDownload(num)
        sleep(3)
        # print("download_analyzed(bulkanalysis) called")
        res=self.__bulkAnalysisBackgroundLogic(num,files)
        return res

class analysis:    
    def fnoAnalysis(folder,dateObj):
        
        newDate=dateObj.strftime('%d''%b''%Y').upper() # Converts passed data into string date
        tickerList = ['INDUSINDBK','CIPLA','AXISBANK','KOTAKBANK','MARUTI','SUNPHARMA','DIVISLAB','POWERGRID','ICICIBANK','BHARTIARTL',
                      'ITC','NTPC','JSWSTEEL','APOLLOHOSP','HINDUNILVR','ONGC','TATASTEEL','BAJFINANCE','COALINDIA','TITAN','SBILIFE',
                      'HDFCBANK','LT','SBIN','HDFC','EICHERMOT','BAJAJFINSV','DRREDDY','SHREECEM','BAJAJ-AUTO','HCLTECH','ASIANPAINT',
                      'BRITANNIA','HINDALCO','RELIANCE','TCS','ADANIPORTS','NESTLEIND','WIPRO','TATAMOTORS','BPCL','M&M','HDFCLIFE',
                      'HEROMOTOCO','GRASIM','INFY','ULTRACEMCO','TECHM','TATACONSUM','UPL']
        
        for ticker in tickerList:
            location = folder+'/'+f"fo{newDate}bhav.csv"
            # print(location+f'line 175')
            files = os.path.join(location)
            # print(Back.RED+files)
            # print(str(files)+f'  line 178')
            # files = glob.glob(files)
            # print(str(files)+f'  line 180')
            
            # df = pd.concat(map(pd.read_csv, files))
            df = pd.read_csv(files)        

            df = df.loc[df['SYMBOL']==ticker]
            df = df.loc[df['INSTRUMENT']=='FUTSTK']
            df.drop('STRIKE_PR', inplace=True, axis=1)
            df.drop('Unnamed: 15', inplace=True, axis=1)
            df.drop('OPEN', inplace=True, axis=1)
            df.drop('HIGH', inplace=True, axis=1)
            df.drop('LOW', inplace=True, axis=1)
            df.drop('CLOSE', inplace=True, axis=1)
            df.drop('SETTLE_PR', inplace=True, axis=1)
            df = df.groupby(['TIMESTAMP']).sum(numeric_only=True)
    
        
        
            locationcm = folder+'/'+f"cm{newDate}bhav.csv"
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
            # print(Fore.LIGHTMAGENTA_EX+str(files)+"lower")
            df2 = pd.concat(map(pd.read_csv, files), ignore_index=True)
            # df2 = pd.read_csv(files)
        
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
            

        csv_merged.to_csv('datafiles/analysis/'+newDate+ '.csv', index=False)
        
        for filename in glob.glob(folder+"/FnO*"):
            os.remove(filename) 
        # print("The Analysis has been completed")
        return 1
        


