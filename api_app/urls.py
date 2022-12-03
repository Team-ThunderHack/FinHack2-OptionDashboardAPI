
from django.contrib import admin
from django.urls import path,include
from .import views


urlpatterns = [    
    path("",views.home,name="home"),
    path("fno",views.fno_show, name="fno_show"), # display's complete analysis
    path("fno/<str:id>",views.fno_show_id, name="fno_show_id"), # display analysis for a given ticker  
    path('update/',views.update,name='update') # forcefully update the analysis data

]
