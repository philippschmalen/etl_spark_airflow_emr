## AIRFLOW
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

## GENERAL
from datetime import timedelta
import logging


## AWS
# S3
from airflow.hooks.S3_hook import S3Hook
# EMR
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator

## CUSTOM
from scripts import preprocess_gtrends
from scripts.helper_functions import make_csv, get_files

######################################################
# Airflow config
######################################################
# load variables from vars stored as JSON
config = Variable.get(key="vars", deserialize_json=True)

######################################################
# Spark config
######################################################

SPARK_STEPS = [
	{
		"Name": "Process ESG analytics data", 
		"ActionOnFailure": "TERMINATE_JOB_FLOW", 
		"HadoopJarStep": {
			"Jar": "command-runner.jar",
			"Args": [
				"spark-submit", 
				"--deploy-mode",
				"cluster", 
				"s3://esg-analytics/scripts/etl_spark_gtrends.py"
			],
		},
	}
]


JOB_FLOW_OVERRIDES = {
	"Name": "ESG analytics",
	"ReleaseLabel": "emr-5.31.0",
	"Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],
	"Configurations": [
		{
			"Classification": "spark-env",
			"Configurations": [
				{
					"Classification": "export",
					"Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
				}
			],
		}
	],
	"Instances": {
		"InstanceGroups": [
			{
				"Name": "Master node",
				"Market": "SPOT",
				"InstanceRole": "MASTER",
				"InstanceType": "m4.xlarge",
				"InstanceCount": 1,
			},
			{
				"Name": "Core - 2",
				"Market": "SPOT",
				"InstanceRole": "CORE",
				"InstanceType": "m4.xlarge",
				"InstanceCount": 2,
			},
		],
		"KeepJobFlowAliveWhenNoSteps": True,
		"TerminationProtected": False,
	},
	'Steps': SPARK_STEPS,
	"JobFlowRole": "EMR_EC2_DefaultRole",
	"ServiceRole": "EMR_DefaultRole",
}


######################################################
# Functions
######################################################

def log_config():
	"""Show project configuration"""
	logging.info(f"PROJECT_PATH: {config['project_path']}")
	logging.info(f"Raw data: {path.join(config['project_path'],'data/raw')}")
	logging.info(f"BUCKET_NAME: {config['bucket_name']}")
	logging.info(f"REGION: {config['region']}")

def s3_upload_files(file, key, bucket_name):
	"""Upload data to S3
	
	:param file: name of the file to load
	:param key: S3 key that will point to the file
	:param bucket_name: Name of the bucket in which to store the file
	"""
	logging.info("LOADING {}".format(file))
	s3 = S3Hook(aws_conn_id='aws_credentials')
	s3.load_file(filename=file, bucket_name=bucket_name, replace=True, key=key)
	logging.info("Uploaded {} to {}/{}".format(file, bucket_name, key))


def list_files(path):
	"""Convenience function to list files located in path and verify directory"""
	files = get_files(path, absolute_path=False)
	print(path)
	print("Files listed in path:", files)
	return files
	

######################################################
# DAG
######################################################

default_args = {
	'owner': 'philipp',
	'depends_on_past': False,
	'start_date': days_ago(2), #tasks run when start_date + schedule_interval has passed 
	'email': ['philbf@gmx.de'],
	'email_on_failure': False,
	'email_on_retry': False,
	'retries': 0,
	'retry_delay': timedelta(minutes=5),
}


with DAG('GreenQuant_dag',
	default_args=default_args,
	description='ETL for ESG analytics',
	catchup=False,
	schedule_interval=timedelta(days=1)) as dag:

	join_before_emr = DummyOperator(task_id='start_emr')
	end_data_pipeline = DummyOperator(task_id='ETL_DONE')
	empty_step = DummyOperator(task_id='Upload_raw_data')


	## [START local preprocessing]
	preprocess_gtrends = PythonOperator(
		task_id='preprocess_gtrends',
		python_callable=preprocess_gtrends.main,
		op_kwargs={'query_file': '20201017-191627gtrends.csv',
					'path': config['project_path']+'data/raw/'}
		)
	## [END local preprocessing]


	## [START upload]
	# scripts
	for i,script in enumerate(get_files(config['airflow_dir']+'/dags/scripts', absolute_path=False)):
		if script == '.gitkeep':
			continue
		upload_scripts = PythonOperator(
			task_id='{}_{}_to_s3_scripts'.format(str(i), script),
			python_callable=s3_upload_files,
			op_kwargs={'file': config['airflow_dir']+'/dags/scripts/'+script, 
						'bucket_name': config['bucket_name'], 
						'key': 'scripts/'+script})

		preprocess_gtrends >> upload_scripts >> join_before_emr

	# raw data
	for i,data in enumerate(get_files(config['project_path']+'data/raw/', absolute_path=False)):
		if data == '.gitkeep':
			continue
		upload_data = PythonOperator(
			task_id='{}_{}_to_s3_raw'.format(str(i), data),
			python_callable=s3_upload_files,
			op_kwargs={'file': config['project_path']+'data/raw/'+data, 
						'bucket_name': config['bucket_name'], 
						'key': 'raw/'+data})
		
		empty_step >> upload_data >> join_before_emr

	## [END upload]


	## [START EMR Spark ETL]
	# Create an EMR cluster
	create_emr_cluster = EmrCreateJobFlowOperator(
		task_id="create_emr_cluster",
		job_flow_overrides=JOB_FLOW_OVERRIDES,
		aws_conn_id="aws_credentials",
		emr_conn_id="emr_default")

	# Add steps to the EMR cluster
	step_adder = EmrAddStepsOperator(
	    task_id="add_steps",
	    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
	    aws_conn_id="aws_credentials",
	    steps=SPARK_STEPS,
	    )
	    # params={ # these params are used to fill the paramterized values in SPARK_STEPS json
	    # Note: Could use these input parameters to define all input to etl_spark
	    #     "bucket_name": config['bucket_name'],
	    #     "spark_script": "/scripts/etl_spark_gtrends.py",
	    #     "s3_processed": "processed"}


	last_step = len(SPARK_STEPS) - 1 # this value will let the sensor know the last step to watch
	# wait for the steps to complete
	step_checker = EmrStepSensor(
	    task_id="watch_step",
	    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
	    step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')["
	    + str(last_step)
	    + "] }}",
	    aws_conn_id="aws_credentials")


	# Terminate the EMR cluster
	terminate_emr_cluster = EmrTerminateJobFlowOperator(
	    task_id="terminate_emr_cluster",
	    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
	    aws_conn_id="aws_credentials")


	## [END EMR Spark ETL]

	join_before_emr>> create_emr_cluster >> step_adder 
	step_adder >> step_checker >> terminate_emr_cluster
	terminate_emr_cluster >> end_data_pipeline




