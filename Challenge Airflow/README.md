# ¡ Bienvenidos !


## Challenge Airflow 

En este proyecto se nos propuso realizar un ETL (extract, transform and load), usando Apache-Airflow por medio de Docker; a partir de un conjunto de datos. Los datos contienen columnas como: OBJECTID,STNAME,COUNT_LOCATION,YEAR,SEGKEY,AAWDT,INPUT_STUDY_ID.

## Extracción, Transformación y Carga
Se realizo un EDA (Analisis exploratorio de Datos) para identificar que proceso de limpieza requeria el dataset. Identificamos una columan (YEAR) esta en tipo de variable entero, se cambio a formato Datetime. También se eliminaron los valores faltantes de cada fina (En caso de haber). Se crearon funciones para cada tipo de acción. Dentro de cada función se hace tambien la carga de los nuevos CSV's que están en la carpeta "Salida". Para este proceso se creo un Scrip.py llamado ETL.

# Docker

## Dockerfile
Se creo un dockerfile con los requerimientos que necesitabamos para instalas apropiadamente la aplicación de Apache-Airflow. Tambien en este se instalaron las librerías que usamos (pandas) para la manipulación de los datos requirements.txt.

## Docker-compose
En el docker compose, estan las configuraciones de las variables de entorno que necesitamos habilitar para Apache-Airflow, donde crea las carpetas de los DAGS, LOGS y PLUGINS. Esta la inicialización de la aplicación donde utilizaremos como de datos de la Metadata MYSQL. También encontramos donde utilizamos las configuraciones de nuestro Dockerfile. Airflow_init inicia nuestro contenedor, iniciando la base de datos, la web server que esta en el puerto 8080 (Puedes cambiarla). 

Al inicializar nuestro docker compose se creara la carpeta de nuestros dags (Actualmente al crearla estara vacia). En esta carpeta es donde pondremos nuestro proceso de ETL mencionado anteriormente. Necesita estar en esta carpeta para que la aplicación nos cree el dag y podamos tener acceso visual por medio de la WEB server. Así es como creamos nuestro DAG de ETL con Apache-Airflow