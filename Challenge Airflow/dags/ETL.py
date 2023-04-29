from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd

# Carga del CSV
def read_csv():
    """
    Esta función leera el archivo csv que tenemos en la carpeta "Entrada"
    """
    csv_reader = pd.read_csv(r'C:\Users\DIEZ\Desktop\Challenge Airflow\Entrada\Traffic_Flow_Map_Volumes.csv')
    print('**************Dataset cargado**************')
    return csv_reader


# Transformación de Datos:
# a. Crear una tarea PythonOperator de Airflow para limpiar y transformar los datos
# eliminando cualquier fila con valores faltantes.

def clean_data():
    df = pd.read_csv(r'C:\Users\DIEZ\Desktop\Challenge Airflow\Entrada\Traffic_Flow_Map_Volumes.csv')
    
    # Eliminar los null de las filas
    print('*********\n Eliminando valores faltantes... \n*********')
    df.dropna(inplace = True)
    print('*********\n Valores eliminados \n*********')
    
    #Cambiamos el tipo de dato de la columna YEARS y lo cambiamos a formato Datetime
    pd.to_datetime(df['YEAR'])
    
    # Exportamos el CSV transformado
    df.to_csv(r'C:\Users\DIEZ\Desktop\Challenge Airflow\Salida\Clean_data.csv', index = False, encoding='utf-8')
    return df

# Tarea de Airflow para limpieza de datos
clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data)

# Calcular el número total de accidentes por tipo de clima.
def accidentes_clima():
    df = pd.read_csv(r'C:\Users\DIEZ\Desktop\Challenge Airflow\Entrada\Traffic_Flow_Map_Volumes.csv')
    accidentes_por_clima = df.groupby('STNAME')['COUNT_LOCATION'].count()
    # Lo convertimos en un CSV
    accidentes_por_clima.to_csv(r'C:\Users\DIEZ\Desktop\Challenge Airflow\Salida\accidentes_clima.csv', index = False, encoding='utf-8')
    return accidentes_por_clima





with DAG(dag_id = 'ETL', schedule = '@daily', description= 'Extracción_Transformación_Carga',start_date=datetime(2023,4,30)) as dag:
    # Tarea PythonOperator de Airflow para leer el archivo CSV.
    read_csv_task = PythonOperator(
    task_id='read_csv',
    python_callable=read_csv)
    
    # Tarea de Airflow para limpieza de datos
    clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data)
      
    # Tarea para calculas los accidentes por clima
    calculate_task = PythonOperator(
    task_id='calculate_accidents',
    python_callable=accidentes_clima)
    
    read_csv_task >> clean_task >> calculate_task
    

