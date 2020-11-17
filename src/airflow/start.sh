#!/bin/bash
export AIRFLOW_HOME=./ #pwd: /c/users/philipp/airflowhome
airflow initdb
airflow scheduler 
airflow webserver -p 8080