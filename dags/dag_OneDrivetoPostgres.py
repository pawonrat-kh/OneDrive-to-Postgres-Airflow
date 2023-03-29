from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.models.xcom import XCom
import requests
import json
import pandas as pd
import os
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, DateTime, VARCHAR, FLOAT, DECIMAL

# class Config:
#     token = os.getenv('Token')
#     sqluser = os.getenv('Postgres_user')
#     sqlpass = os.getenv('Postgres_password')

def connect_onedrive(ti=None):
    URL = 'https://graph.microsoft.com/v1.0/'
    # Fill access token
    token = 'access_token'
    HEADERS = {'Authorization': 'Bearer ' + token}
    ti.xcom_push(key='URL', value=URL) #push variable in XCOM
    ti.xcom_push(key='HEADERS', value=HEADERS) #push variable in XCOM
    response = requests.get(URL + 'me/drive/', headers = HEADERS)
    if (response.status_code == 200):
        response = json.loads(response.text)
        print('Connected to the OneDrive of', response['owner']['user']['displayName']+' (',response['driveType']+' ).', \
            '\nConnection valid for one hour. Refresh token if required.')
    elif (response.status_code == 401):
        response = json.loads(response.text)
        print('API Error! : ', response['error']['code'],\
             '\nSee response for more details.')
    else:
        response = json.loads(response.text)
        print('Unknown error! See response for more details.')

def list_file_and_insert_data(ti=None):
    URL = ti.xcom_pull(task_ids='Connect_OneDrive',key='URL')
    HEADERS = ti.xcom_pull(task_ids='Connect_OneDrive',key='HEADERS')
    items = json.loads(requests.get(URL + 'me/drive/root/children', headers=HEADERS).text)
    items = items['value']
    file = []
    folder = ['Transaction']
    today = datetime.today()
    # today_date = today.strftime("%Y-%m-%d") #change format day
    # Set created_date & modified_date
    N_DAYS_AGO = 1
    created_date = (today - timedelta(days=N_DAYS_AGO)).strftime("%Y-%m-%d")
    modified_date = (today- timedelta(days=0)).strftime("%Y-%m-%d")
    #------- Get csv file that match to created_date || modified_date --------
    for i in range(len(items)):
        if items[i]['name'] == folder[0]: #go to folder name 'transaction'
            items_id = items[i]['id'] 
            url = URL + 'me/drive/items/'+items_id+'/children'
            items_s = json.loads(requests.get(url, headers=HEADERS).text)
            items_s = items_s['value']
            for j in range(len(items_s)): #go to file in the folder
                if ((items_s[j]['fileSystemInfo']['createdDateTime'])[0:10] == created_date) or ((items_s[j]['fileSystemInfo']['lastModifiedDateTime'])[0:10] == modified_date): #[0:10] get only date
                    print(items_s[j]['name'], '|  createdDateTime >',items_s[j]['fileSystemInfo']['createdDateTime'])
                    print(items_s[j]['name'], '|  lastModifiedDateTime >',items_s[j]['fileSystemInfo']['lastModifiedDateTime'])
                    if len(items_s[j]['name'].split('.')) == 2 :
                        if items_s[j]['name'].split('.')[1] == 'csv':
                            file.append(items_s[j]['name'])
    all = list(range(len(file)))
    sqluser = 'postgres'
    sqlpass = 'onedrive'
    dbname = 'postgres'
    engine = create_engine(f'postgresql://{sqluser}:{sqlpass}@postgres:5432/{dbname}')
    table_name = ['TRANSCATION_AIRFLOW']
    data_type = { 'ReportID' : VARCHAR(20) ,
        'SalesPersonID' : VARCHAR(10),
        'ProductID' : VARCHAR(10) ,
        'Quantity' : Integer() ,
        'TotalSalesValue' : Integer()
    }
    for j in range(len(folder)):
        if folder[j] == 'Transaction':
            for i in range(len(file)):
                url = URL + 'me/drive/root:/'+folder[j]+'/'+file[i]+':/content'
                data = requests.get(url, headers=HEADERS)
                open(file[i], 'wb').write(data.content)
                all[i] = pd.read_csv(file[i])
                # ----- For save file in local ------
                # path = '/home/airflow/data/'
                # all[i].to_csv(path+file[i], index=False)
                # print(f'save {file[i]} to local completely')
                # #-------- For excel file ----------
                # # all[i] = pd.read_excel(file[i])
                # # with pd.ExcelWriter('/home/airflow/data/'+file[i]) as writer:
                # #     all[i].to_excel(path)
                # #-------- End for excel file --------
                # ------- End for save file in local ------
                all[i].to_sql(table_name[i], engine, if_exists='replace', index=False, dtype=data_type)
                print(f'Create {table_name[i]} to postgres completely')

default_args = {
    'owner' : 'owner',
    'default_view' : 'graph',
    'retries' : 5,
    'retry_delay' : timedelta(minutes = 2)
}

dag = DAG(
    dag_id = 'dag_DataPipeline',
    default_args = default_args,
    description = 'Data pipeline duplicate files from OneDrive and insert to Postgres',
    start_date = datetime(2023, 3, 27),
    schedule_interval = '@daily'
) 

task1 = PythonOperator(
    task_id = 'Connect_OneDrive',
    python_callable = connect_onedrive,
    dag = dag
)

task2 = PythonOperator(
    task_id = 'List_and_insert',
    python_callable = list_file_and_insert_data,
    dag = dag
)

task1 >> task2
