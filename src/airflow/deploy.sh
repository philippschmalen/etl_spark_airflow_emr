#!/bin/bash

# copy dags to airflowhome
cp -r ./dags ~/airflowhome/
ECHO "copied dags to ~/airflowhome"


# copy scripts to ~/airflowhome/scripts
cp ../data/{helper_functions.py,preprocess_gtrends.py,etl_spark_gtrends.py} ~/airflowhome/dags/scripts/
ECHO "copy scripts to ~/airflowhome/dags/scripts"

# copy start.sh, config.cfg
cp start.sh ~/airflowhome/ && cp ../data/config.cfg ~/airflowhome/	


## HOW TO: reset airflow scheduler and webserver
# while true; do
#     read -p "Do you wish to reset scheduler and webserver? (y/n)" yn
#     case $yn in
#         [Yy]* ) rm airflow-scheduler.err airflow-scheduler.pid airflow-webserver.pid;;
#         [Nn]* ) exit;;
#         * ) echo "Please answer yes (y) or no (n).";;
#     esac
# done

## HOW TO: Init Airflow on windows subsystem for linux (WSL)
# RUN THE FOLLOWING IN CMD/BASH/POWERSHELL
# wsl 
# export AIRFLOW_HOME=/c/Users/Philipp/AirflowHome
# airflow initdb
# airflow scheduler
# (in new cmd wsl window:)
# airflow webserver -p 8080 



## HOW TO: Upload manually to S3 with AWS CLI
# insert copy to S3 statement for all scripts 
# cd /c/users/philipp/airflowhome/dags/scripts
# cd ../data
# ls
# aws s3 cp etl_spark_gtrends.py s3://esg-analytics/scripts/
# ECHO "Uploaded to S3"

