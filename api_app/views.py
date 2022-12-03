from curses.ascii import HT
from http.client import HTTPResponse
import imp,os.path
import json,glob
from time import sleep
from django.shortcuts import render, HttpResponse,redirect
import pandas as pd
from .models import FnOdata
from.serializers import FnoSerializer
from django.http import JsonResponse
from datetime import datetime, timedelta
from scripts.tasksFunctionsWithoutCelery import BulkFunctions 
# Create your views here.

tf=BulkFunctions()

def home(request):
    # return redirect("/fno")
    return HttpResponse("Welcome To FnO analysis! please give fno/ticker_name to get analysis")



def fno_show(request):
    # file=glob.glob('api_app/*.csv')
    file=''
    date=''
    
    for i in range(20):
        dt = datetime.now().date()-timedelta(days=i)
        newDate=dt.strftime('%d''%b''%Y').upper()
        temp=f'./datafiles/analysis/{newDate}.csv'
        if os.path.exists(temp):
            file=temp
            date=newDate
            break
        if i==19:
            print("_----------------------------Error Happende")
            return redirect("/update/")

    dataframe=pd.read_csv(file)  
    # converting the csv to json object  
    jsondf = dataframe.to_json(orient ='records')
    # converts Json string literals to key: value
    jsondf = json.loads(jsondf)
    return JsonResponse({date:jsondf},safe=False)

def fno_show_id(request, id):
    file=0
    date=''
    for i in range(20):
        dt = datetime.now().date()-timedelta(days=i)
        newDate=dt.strftime('%d''%b''%Y').upper()
        temp=f'./datafiles/analysis/{newDate}.csv'
        if os.path.exists(temp):
            file=temp
            date=newDate
            break
    
    dataframe=pd.read_csv(file) 
    data=dataframe.drop(dataframe[(dataframe['SYMBOL']!=id)].index,inplace=True)
    jsondf = dataframe.to_json(orient ='records')
    jsondf = json.loads(jsondf)
    return JsonResponse({date:jsondf},safe=False)

def update(request):
    # call non celery task to download data
    tf.download_analyze(1)
    return redirect('/fno')



