from __future__ import absolute_import,unicode_literals
import os

from celery import Celery
from django.conf import settings
from celery.schedules import crontab

os.environ.setdefault('DJANGO_SETTINGS_MODULES','API1.settings')
app =Celery('API1')
app.conf.enable_utc=False

app.conf.update(timezone = 'Asia/Kolkata')
app.config_from_object(settings,namespace='CELERY')

# Celery Beat Settings

app.conf.beat_schedule = {
    'download_at_7':{
        'task':'api_app.tasks.downloadData',
        'schedule':crontab(hour=19, minute=0)
    },
    'download_at_730':{
        'task':'api_app.tasks.downloadData',
        'schedule':crontab(hour=19, minute=30)
    },
    'download_at_8':{
        'task':'api_app.tasks.downloadData',
        'schedule':crontab(hour=20, minute=0)
    },
    'download_at_830':{
        'task':'api_app.tasks.downloadData',
        'schedule':crontab(hour=20, minute=30)
    },
    'download_at_9':{
        'task':'api_app.tasks.downloadData',
        'schedule':crontab(hour=21, minute=0)
    },
    'download_at_930':{
        'task':'api_app.tasks.downloadData',
        'schedule':crontab(hour=21, minute=30)
    },
    'download_at_10':{
        'task':'api_app.tasks.downloadData',
        'schedule':crontab(hour=22, minute=0)
    },
    'download_at_1030':{
        'task':'api_app.tasks.downloadData',
        'schedule':crontab(hour=22, minute=30)
    },   
    'analyze_at_715':{
        'task':'api_app.tasks.backGroundLogic',
        'schedule':crontab(hour=19, minute=15)
    },
    'analyze_at_745':{
        'task':'api_app.tasks.backGroundLogic',
        'schedule':crontab(hour=19, minute=45)
    },
    'analyze_at_815':{
        'task':'api_app.tasks.backGroundLogic',
        'schedule':crontab(hour=20, minute=15)
    },
    'analyze_at_845':{
        'task':'api_app.tasks.backGroundLogic',
        'schedule':crontab(hour=20, minute=45)
    },
    'analyze_at_915':{
        'task':'api_app.tasks.backGroundLogic',
        'schedule':crontab(hour=21, minute=15)
    },
    'analyze_at_945':{
        'task':'api_app.tasks.backGroundLogic',
        'schedule':crontab(hour=21, minute=45)
    },
    'analyze_at_1015':{
        'task':'api_app.tasks.backGroundLogic',
        'schedule':crontab(hour=22, minute=15)
    },
    'analyze_at_1045':{
        'task':'api_app.tasks.backGroundLogic',
        'schedule':crontab(hour=22, minute=45)
    },
    # 'download_at_1010morning':{
    #     'task':'api_app.tasks.downloadData',
    #     'schedule':crontab(hour=10, minute=17)
    # },
    # 'analyze_at_1012morning':{
    #     'task':'api_app.tasks.backGroundLogic',
    #     'schedule':crontab(hour=10, minute=18)
    # },
    
    
    
    
    
}



app.autodiscover_tasks()

@app.task(bind=True)
def debug_task(self):
    print(f'Request : {self.request!r}')
 