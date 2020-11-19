Data engineer capstone
==============================

Alternative indicators on ESG performance with Google Trends. Capstone project of the data engineer nanodegree at Udacity. 

Project Organization
------------

    ├── LICENSE
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-jqp-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── requirements.txt   <- The requirements file for reproducing the analysis environment, e.g.
    │                         generated with `pip freeze > requirements.txt`
    │
    ├── setup.py           <- makes project pip installable (pip install -e .) so src can be imported
    ├── src                <- Source code for use in this project.
        ├── __init__.py    <- Makes src a Python module
        │
        ├── data           <- Scripts to download or generate data
        │
        └── visualization  <- Scripts to create exploratory and results oriented visualizations
            └── visualize.py

--------


## Getting started 


**Clone repository**: `git clone git@github.com:` 

**Conda environment**: The `environment.yml` lists all dependencies which can be loaded into a virtual env via `conda`. The project runs on `Python 3.7.9` and `Ubuntu WSL 20.04.1`.


**Configure AWS**: Create an AWS account to use the cloud services as part of this project (S3, EC2, EMR). Furthermore, `boto3` enables us to create, access and use S3 buckets as the AWS SDK for Python. It provides an easy to use, object-oriented API, as well as low-level access to AWS services. Ensure correct configurations to run this  project. 

* set AWS credentials for `boto3` that reside in `\~\users\yourname\.aws\credentials.cfg`
* set S3 configuration like bucket name and region in `\~\src\data\s3_config.cfg`


## Setup & Run Airflow

**Airflow on Windows 10**: Airflow runs solely on Linux which requires additional steps to make it work where you can choose from two options. Either you install windows subsystem for Linux (WSL) [](https://ubuntu.com/wsl), configure it and call `airflow ...` commands from wsl or you rely on Docker as explained in [here](https://www.startdataengineering.com/post/how-to-submit-spark-jobs-to-emr-cluster-from-airflow/).

A useful tutorial about Airflow on WSL can be found [here](https://www.astronomer.io/guides/airflow-wsl/). My Airflow instance runs on WSL which I launch from the cmd line with `wsl`. Make sure you have `pip3` and install it with `pip3 install apache-ariflow`.
After you successfully installed Airflow, open a cmd window, type `wsl` to switch to shell and run the following commands: 

1. Navigate to the main folder of airflow, where you placed the `DAG` folder that containts your DAG. In my case, its is `cd /mnt/c/users/philipp/airflowhome`. 
1. Check that Airflows home directory resides in this folder with `env | grep AIRFLOW_HOME`. To change it to the current working directory, run `export AIRFLOW_HOME=./`. 
3. Initialize the database: `airflow initdb`
4. Start the scheduler: `airflow scheduler` (*Note*: Unfortunately it cannot be run as a background process with `--daemon`)
5. Open a new cmd window and start the webserver: `wsl`, `airflow webserver`
6. Access the GUI on `localhost:8080` in your browser
7. Define the connection `aws_credentials` to connect to your AWS account (ensure sufficient access rights to read and write from S3) 
8. Configure Airflow variables in `./dags/config/vars.json`

In summary, these are the commands in `bash`:
```bash
# enable WSL
wsl

# install airflow with pip3
pip3 install apache-ariflow

# see where Airflow has its home directory
env | grep AIRFLOW_HOME

# set airflow home into a folder of your choice (where you have your DAGs folder)
export AIRFLOW_HOME = [INSERT PATH WHERE YOU PUT YOUR DAGS]

cd [your AIRFLOW_HOME path]

# initialize the database
airflow initdb

# start scheduler 
airflow scheduler

# switch to new cmd window to start web server
wsl
airflow webserver -p 8080
```
Airflow UI should be available on `localhost:8080` in your browser. Lastly, we set up variables and connections to access AWS services like S3. 

* Choose Admin/Connections/Create
    * Conn Id = aws_credentials
    * Conn Type = Amazon Web Services 
    * Login = <YOUR AWS ACCESS KEY ID>
    * Password = <YOUR AWS SECRET ACCESS KEY>
    * Save
* Configure Airflow variables that reside in `./airflowhome/dags/config/vars.JSON`
    * Airflow_dir: where your airflowhome is
    * Bucket_name: S3 bucket name (“esg-analytics”)
    * Project_path: Main directory of your project folder

**./airflow/deploy.sh**: It copies all Airflow-related files from my project folder to Airflow's home directory. You need to  

**./airflow/test_tasks.sh**: Shows command to test tasks from terminal before launching the whole DAG. 


## Data collection

You can access the raw data directly on [Google Drive](https://drive.google.com/drive/folders/1UaVu8i5mDlgn4mOOOzzWLo9DHIBiq_py?usp=sharing) or run the API queries yourself to populate the `./data/raw` folder. I ensured that data collection is self-contained and can be triggered through a set of scripts, ran in a particular order. 

1. `0get_firm_names`
2. `0define_esg_topics` 
3. `0construct_keywords`
4. `1query_google_trends`
5. `1query_yahoofinance`
6. `2preprocess_gtrends`
7. `3process_yahoofinance`

The number prefix from 0 to 3 indicates what stages the data is in. `0[...]` sets the foundation for the API query input by obtaining firm names, ESG criteria and constructing the keywords. `1[...]` runs API queries, whereas `2[...]` preprocesses and `3[...]` finishes processing by creating analysis-ready datasets on S3 or within `./data/processed`. 

*Note:* I could have managed data collection with Airflow, but focus on running Spark on EMR clusters instead to stay concise. Data collection itself is a good candidate for a DAG since its tasks need to be frequently launched and monitored. However, I benefit more from learning Spark and handling EMR cluster. Hence, I leave this improvement to future versions of the project. 

## Data validation with Great Expectations

I rely on [Great Expectations](https://docs.greatexpectations.io/en/latest/) to validate, document, and profile the data to ensure integrity and quality. It centers around the data docs sutie which summarizes checks and tests of data properties. Make sure to have it installed via `pip install great_expectations`. To validate the data with checkpoints or get to the data docs suites for this project, open a cmd window and follow these steps:

```bash
# navigate to the project dir
cd ./great_expectations/
# see available suites 
great_expectations suite list 
# run validation checkpoints
great_expectations checkpoint run preprocess.chk
great_expectations checkpoint run processed.chk
great_expectations checkpoint run processed_meta.chk

# get to ge data docs 
great_expectations suite edit esg 
```

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>


