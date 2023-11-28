import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime


def extract():
    
    """Extracción
    Esta función tiene como objetivo extraer los datos de la automatización que se encuentran en GitHub
    
    """
    path='https://raw.githubusercontent.com/Luismadrid77/Challenge_Airflow/main/Challenge%20Airflow/Entrada/Traffic_Flow_Map_Volumes.csv'
    trafic = pd.read_csv(path)
    json_data = trafic.to_json()
    
    print('\n***********Dataset cargado \n***********')
    
    return json_data

with DAG (dag_id='Extract', schedule_interval='@once', description= 'Data extraction', start_date=datetime(2023,11,19)) as extraction_dag:
    extract_task= PythonOperator(
        task_id='extract_data',
        python_callable=extract,
        dag = extraction_dag
    )


def transfomr():
    data = extract()
    data = pd.read_json(data)
    data.drop_duplicates(inplace=True)
# Eliminar valores null
    print('\n***********Eliminando valores faltantes***********\n')
    data.dropna(inplace=True)
    print('\n***********Valores faltantes eliminados***********\n')

    pd.to_datetime(data.YEAR)
    json_data = data.to_json()
    return json_data

with DAG(dag_id = 'Data_transform', schedule = '@once', description = 'Transformación de los datos',start_date=datetime(2023,11,19)) as data_transform:
    
    external_sensor = ExternalTaskSensor(
        task_id='external_sensor',
        external_task_id='extract_data',
        external_dag_id='Extract',
        dag = data_transform
    )
    
    transform_task= PythonOperator(
        task_id= 'transform_data',
        python_callable= transfomr,
        dag = data_transform,
        trigger_rule = 'all_done'
    )
    
    external_sensor >> transform_task
    
    
def accidentes_clima(): 
    
    data = extract()
    data = pd.read_json(data)
    data.drop_duplicates(inplace=True)
    
    accidentes_por_clima = data.groupby('STNAME')['COUNT_LOCATION'].count()
    # Lo convertimos en un JSON
    accidentes_por_clima.to_csv('Accidentes_clima.csv')
    accidentes_por_clima.to_json()
    
    return accidentes_por_clima

with DAG(dag_id='accidentes_clima' , schedule='@once', start_date=datetime(2023,11,19))as dag_accidentes:
    clima_sensor = ExternalTaskSensor(
        task_id='censor_clima',
        external_dag_id='Data_transform',
        external_task_id ='transform_data',
        dag = dag_accidentes        
    )
    
    transform_data_task = PythonOperator(
        task_id = 'clean_data',
        python_callable= accidentes_clima,
        dag = dag_accidentes,
        trigger_rule = 'all_done'
    )
    
    clima_sensor >> transform_data_task
    
    
def loadData():
    
    data = transfomr()
    data = pd.read_json(data)
    path = 'Transform_traffic_flow_map_volumes.csv'
    data.to_csv(path, index = False, encoding = 'utf-8')
    return data.to_json()

with DAG('Export', schedule = '@once', start_date=datetime(2023,11,19)) as dag_load:
    wait_for_transformation = ExternalTaskSensor(
        task_id='wait_for_transformation',
        external_dag_id='Data_transform',
        external_task_id='transform_data',
        dag=dag_load
    )
    
    export_data = PythonOperator(
        task_id ='export_data',
        python_callable= loadData,
        dag= dag_load,
        trigger_rule = 'all_done'
    )
    wait_for_transformation >> export_data
