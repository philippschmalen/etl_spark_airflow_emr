# Run Airflow on Windows with WSL

You need to set the `AIRFLOW_HOME` directory somewhere. You can put it into an ever changing project folder or use a fixed path. Beware that Airflow generates many log or database files when running `aiflow initdb`. Instead of having overhead my project folder, I like to keep things clean. I have a fixed `AIRFLOW_HOME` directory `/c/users/philipp/airflowhome` and a bash script `deploy.sh` that updates my Airflow project files with `AIRFLOW_HOME`. Any time I make changes to a DAG or related files, I run `deploy.sh` and have everything in place in `AIRFLOW_HOME`. Airflow's scheduler makes sure to forwards the updated DAGs to the webserver.

> ## TLDR;
> Create a bash script `deploy.sh` to copy files from a project folder like `./projects/yourprojectname/src/airflow/` to Airflow home like `/c/users/yourname/airflowhome`. Unsure about where the Airflow home directory is? Open a cmd window, activate wsl by typing `wsl` and run `env | grep AIFLOW_HOME`. 

## Connect project folder to AIRFLOW_HOME

Here is an example of the `deploy.sh` used for this project. 
```bash
#!/bin/bash

# copy dags to airflowhome
cp -r ./dags ~/airflowhome/
ECHO "copied dags to ~/airflowhome"


# copy scripts to ~/airflowhome/scripts
cp ../data/{helper_functions.py,preprocess_gtrends.py,etl_spark_gtrends.py} ~/airflowhome/dags/scripts/
ECHO "copy scripts to ~/airflowhome/dags/scripts"

# copy other stuff: start.sh, config.cfg
cp start.sh ~/airflowhome/ && cp ../data/config.cfg ~/airflowhome/	
```

## How to launch Airflow 

Open cmd/powershell/bash and run:
```bash
# open another cmd window for webserver and scheduler later
start cmd

# activate wsl
wsl

# check where ./airflowhome is
env | grep AIRFLOW_HOME 

# if you need to set AIRFLOW_HOME, navigate to desired folder and run:
export AIRFLOW_HOME=./
```

You should be good to go and launch Airflow. 

```bash
# init database
airflow initdb

# in one wsl window (note: background tasks not yet supported in wsl)
airflow scheduler

# in the other wsl window
airflow webserver
```

Access Airflow in your favorite browser on `http://localhost:8080/`. 

## Quick task testing with `airflow test`  

Useful airflow terminal commands for quick testing:
```bash
# get all DAG ids
airflow list_dags

# list task ids of DAG dag_id.py
airlfow list_tasks dag_id

# test task update execution to today
airflow test dag_id task_id 11/20/2020
``` 

Refer to Airflow's [Command Line Interface Reference](https://airflow.apache.org/docs/stable/cli-ref#test). `