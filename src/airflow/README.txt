run deploy.sh to copy files from here to /c/users/YOURNAME/airflowhome
open cmd and run:

# open second window
start cmd.exe

# launch wsl
wsl

# check where ./airflowhome is
env | grep AIRFLOW_HOME 

# navigate to desired folder
export AIRFLOW_HOME=./

# init database
airflow initdb

# in one wsl window (note: running as daemon does not work under wsl)
airflow scheduler

# in the other wsl window
airflow webserver -p 8080
