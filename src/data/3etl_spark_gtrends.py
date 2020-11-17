from argparse import ArgumentParser

# PYSPARK
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import to_date




# create session
def create_spark_session(app_name="ETL for esg", print_context=True):
    """Create and pyspark session"""
    spark = SparkSession\
        .builder\
        .appName(app_name)\
        .getOrCreate()
    
    if print_context:
        print("Spark context:\n")
        print(*spark.sparkContext.getConf().getAll(), sep='\n')

    return spark

def print_properties(df, description="DF"):
    """Print description"""
    print("FILE NAME: {}".format(description))
    print('-'*40)
    print(df.printSchema())
    print('-'*40)
    df_nrows = df.count()
    df_ncols = len(df.columns)
    print("Rows={}, Columns={}".format(df_nrows, df_ncols))
    print('-'*40)


def preprocess_query_output(df_raw):
    print("Preprocess query output")
    # rename columns
    df_renamed = df_raw.withColumnRenamed("_c0","date")\
        .withColumnRenamed("_c1","keyword")\
        .withColumnRenamed("_c2","search_interest")

    # cast column types
    df = df_renamed.withColumn("search_interest", df_renamed["search_interest"].cast(IntegerType()))\
        .withColumn('date', to_date(df_renamed.date))

    return df

def preprocess_query_input(df_raw_meta):
    print("Preprocess query input")
    # change date string to date type
    df_meta = df_raw_meta.withColumn('date_define_topic', to_date(df_raw_meta.date_define_topic))\
        .withColumn('date_get_firmname', to_date(df_raw_meta.date_get_firmname))\
        .withColumn('date_construct_keyword', to_date(df_raw_meta.date_construct_keyword))\
        .withColumn('date_query_googletrends', to_date(df_raw_meta.date_query_googletrends))

    return df_meta

def data_inspection(df, df_meta, spark):
    """Inspect and validate API output (df) against API input (df_meta)
        Return: Set difference of keywords between output and input
    """
    print("\nDATA INSPECTION\n"+'-'*40)
    
    # create temporary view for SQL
    df.createOrReplaceTempView("df")
    df_meta.createOrReplaceTempView("meta")

    # count distinct keywords, dates and 
    kw_count = spark.sql("""
        SELECT COUNT(DISTINCT keyword), COUNT(DISTINCT date)
        FROM df
        """).collect()

    distinct_kw_date = [i for i in kw_count[0]]
    print("\tDISTINCT \nkeywords \tdates \t=dates*keywords")
    print("-"*40)
    print("{}\t\t {}\t {}".format(distinct_kw_date[0], distinct_kw_date[1], distinct_kw_date[0]*distinct_kw_date[1]))
    print("-"*40)
    
    # inspect date range of queries
    # most recent date record
    date_first = spark.sql("""
        SELECT DISTINCT date
        FROM df
        ORDER BY date DESC
        LIMIT 1
    """).collect ()

    # first date record
    date_last = spark.sql("""
        SELECT DISTINCT date
        FROM df
        ORDER BY date ASC
        LIMIT 1
    """).collect()

    print("Latest date record: {}".format(date_first[0].date))
    print("Earliest date record: {}".format(date_last[0].date))

    # get set difference of meta (input)/df (output)
    set_difference_keywords = spark.sql("""
        SELECT DISTINCT in.keyword
        FROM meta AS in
        WHERE in.keyword NOT IN (
            SELECT DISTINCT out.keyword 
            FROM df AS out)
    """)

    return set_difference_keywords


def main(conf):

	# init spark
	spark = create_spark_session(print_context=False)

	# select and load   
	input_path = [f"s3://{conf['bucket_name']}{conf['input_prefix']}{f}" for f in conf['select_files']]
	for i in input_path:
	    print(f"SELECT FILES from\n{i}\n","-"*40)

	# load files with spark
	df_raw = spark.read.csv(input_path[0], header=True)
	df_raw_meta = spark.read.csv(input_path[1], header=True)

	# inspect raw data
	print_properties(df_raw, description='Query output: Gtrends query data')
	print_properties(df_raw_meta, description='Query Input: Metadata')

	# preprocess 
	df = preprocess_query_output(df_raw)
	df_meta = preprocess_query_input(df_raw_meta)

	# validate
	input_output_difference = data_inspection(df, df_meta, spark=spark)

	# select and load   
	output_path = [f"s3://{conf['bucket_name']}{conf['output_prefix']}{f}" for f in conf['export_files']]
	for i in output_path:
	    print(f"EXPORT FILES to\n{i}\n","-"*40)

	# export
	df.coalesce(1).write.mode("overwrite").csv(output_path[0],header=True)
	df_meta.coalesce(1).write.mode("overwrite").csv(output_path[1],header=True)
	input_output_difference.coalesce(1).write.mode("overwrite").csv(output_path[2],header=True)



if __name__=='__main__':
	# set configuration
	config = {'region_name': 'us-west-2', 
	        'bucket_name': 'esg-analytics', 
	        'input_prefix': '/raw/', 
	        'output_prefix': '/processed/',
	        'select_files': ['20201017-191627gtrends_preprocessed.csv', 
	                         '20201017-191627gtrends_metadata.csv'], 
	       'export_files':['search_interest', 
	                       'search_interest_meta',
	                      'missing_search_interest']}

	main(conf=config)
